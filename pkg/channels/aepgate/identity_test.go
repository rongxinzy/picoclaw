package aepgate

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/aep"
)

// mappingServer fakes the control plane with a switchable mapping set and a
// reload counter.
type mappingServer struct {
	server    *httptest.Server
	loads     atomic.Int64
	mappings  atomic.Value // string JSON body
	forbidden atomic.Bool
}

func newMappingServer(t *testing.T, mappings string) *mappingServer {
	t.Helper()
	f := &mappingServer{}
	f.mappings.Store(mappings)
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/aep/v1/auth/password/login", "/aep/v1/auth/refresh":
			_, _ = io.WriteString(w, `{"accessToken":"at","refreshToken":"rt","modelAccessToken":"mt","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"s"}`)
		case "/aep/v1/admin/identity-sources/src-1/mappings":
			if f.forbidden.Load() {
				w.Header().Set("Content-Type", "application/problem+json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, `{"type":"about:blank","title":"Forbidden","status":403,"code":"ACCESS_DENIED"}`)
				return
			}
			f.loads.Add(1)
			_, _ = io.WriteString(w, f.mappings.Load().(string))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *mappingServer) manager() *aep.Manager {
	return aep.NewManager(aep.Config{
		BaseURL: f.server.URL, DeploymentID: "demo",
		Username: "u", Password: "p", SessionID: "s",
	})
}

func TestIdentityResolverConstructorValidation(t *testing.T) {
	if _, err := NewIdentityResolver(nil, "src"); err == nil {
		t.Fatal("nil manager must fail")
	}
	if _, err := NewIdentityResolver(&aep.Manager{}, ""); err == nil {
		t.Fatal("empty source must fail")
	}
}

func TestIdentityResolverResolvesActiveUserMappingsOnly(t *testing.T) {
	f := newMappingServer(t, `{"mappings":[
		{"sourceId":"src-1","externalSubjectType":"user","externalId":"ou_alice","localSubjectId":"user-alice","status":"active"},
		{"sourceId":"src-1","externalSubjectType":"user","externalId":"ou_bob","localSubjectId":"user-bob","status":"disabled"},
		{"sourceId":"src-1","externalSubjectType":"team","externalId":"t_1","localSubjectId":"dept","status":"active"}],"nextCursor":null}`)
	m := f.manager()
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	r, err := NewIdentityResolver(m, "src-1")
	if err != nil {
		t.Fatal(err)
	}

	if id, err := r.Resolve(context.Background(), "ou_alice"); err != nil || id != "user-alice" {
		t.Fatalf("alice = %q, %v", id, err)
	}
	// Disabled and non-user mappings resolve as unmapped, without error.
	if id, err := r.Resolve(context.Background(), "ou_bob"); err != nil || id != "" {
		t.Fatalf("bob = %q, %v", id, err)
	}
	if id, err := r.Resolve(context.Background(), "t_1"); err != nil || id != "" {
		t.Fatalf("team subject = %q, %v", id, err)
	}
	if id, err := r.Resolve(context.Background(), ""); err != nil || id != "" {
		t.Fatalf("empty = %q, %v", id, err)
	}
	// All lookups after the first share one load (cache is fresh).
	if got := f.loads.Load(); got != 1 {
		t.Fatalf("loads = %d, want 1", got)
	}
}

func TestIdentityResolverNegativeTTLAndRefresh(t *testing.T) {
	f := newMappingServer(t, `{"mappings":[],"nextCursor":null}`)
	m := f.manager()
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	r, err := NewIdentityResolver(m, "src-1")
	if err != nil {
		t.Fatal(err)
	}

	// Cold miss loads once and is remembered for the negative window.
	if id, err := r.Resolve(context.Background(), "ou_new"); err != nil || id != "" {
		t.Fatalf("cold miss = %q, %v", id, err)
	}
	if id, err := r.Resolve(context.Background(), "ou_new"); err != nil || id != "" {
		t.Fatalf("warm miss = %q, %v", id, err)
	}
	if got := f.loads.Load(); got != 1 {
		t.Fatalf("loads = %d, want 1", got)
	}

	// After the negative TTL passes, a miss reloads and finds the new entry.
	time.Sleep(identityNegativeTTL + 20*time.Millisecond)
	f.mappings.Store(`{"mappings":[{"sourceId":"src-1","externalSubjectType":"user","externalId":"ou_new","localSubjectId":"user-new","status":"active"}],"nextCursor":null}`)
	if id, err := r.Resolve(context.Background(), "ou_new"); err != nil || id != "user-new" {
		t.Fatalf("refreshed = %q, %v", id, err)
	}
	if got := f.loads.Load(); got != 2 {
		t.Fatalf("loads = %d, want 2", got)
	}
}

func TestIdentityResolverFailsClosedWithPermissionHint(t *testing.T) {
	f := newMappingServer(t, `{}`)
	f.forbidden.Store(true)
	m := f.manager()
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	r, err := NewIdentityResolver(m, "src-1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Resolve(context.Background(), "ou_alice")
	if err == nil {
		t.Fatal("denied listing must fail closed")
	}
	if got := err.Error(); !strings.Contains(got, "identity.read") {
		t.Fatalf("error must name the permission: %q", got)
	}
}
