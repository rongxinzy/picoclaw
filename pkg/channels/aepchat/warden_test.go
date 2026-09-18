package aepchat

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/config"
)

// scopeServer serves login/metadata plus per-user retrieval contexts keyed
// by the userId query parameter.
func scopeServer(t *testing.T, contexts map[string]string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/aep/v1/auth/password/login":
			_, _ = io.WriteString(w, `{"accessToken":"at","refreshToken":"rt","modelAccessToken":"mt","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"t"}`)
		case "/aep/v1/auth/refresh":
			_, _ = io.WriteString(w, `{"accessToken":"at2","refreshToken":"rt2","modelAccessToken":"mt2","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"t"}`)
		case "/aep/v1/metadata":
			_, _ = io.WriteString(w, `{"service":"aep","deploymentId":"demo"}`)
		case "/aep/v1/user/models":
			_, _ = io.WriteString(w, `{"models":[]}`)
		case "/aep/v1/user/heartbeat":
			_, _ = io.WriteString(w, `{"serverTime":"2026-09-18T00:00:00Z","controlEvents":{"pending":false},"nextHeartbeatAfterSeconds":60}`)
		case "/aep/v1/user/me":
			_, _ = io.WriteString(w, `{"user":{"id":"resident-1","displayName":"Resident","kind":"agent"},"deploymentId":"demo","roles":[]}`)
		case "/aep/v1/admin/data-scope/context":
			body, ok := contexts[r.URL.Query().Get("userId")]
			if !ok {
				t.Errorf("unexpected scope lookup for %s", r.URL.Query().Get("userId"))
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if body == "" { // explicitly expected miss: 404 without failing
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = io.WriteString(w, body)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func wardenManager(t *testing.T, server *httptest.Server) *aep.Manager {
	t.Helper()
	m := aep.NewManager(aep.Config{
		BaseURL: server.URL, DeploymentID: "demo",
		Username: "resident", Password: "resident-password-123", SessionID: "t",
	})
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("manager start: %v", err)
	}
	t.Cleanup(m.Stop)
	return m
}

func TestWardenRoutesSubordinatesToForksAndPeersToResident(t *testing.T) {
	server := scopeServer(t, map[string]string{
		// The resident sees its whole department subtree.
		"resident-1": `{"principalId":"resident-1","deploymentId":"demo","orgScope":["dept-a","dept-a-eng"],"ownTeamIds":["dept-a"],"roleScope":[]}`,
		// A level-2 employee strictly inside the subtree.
		"sub-1": `{"principalId":"sub-1","deploymentId":"demo","orgScope":["dept-a-eng"],"ownTeamIds":["dept-a-eng"],"roleScope":[]}`,
		// A peer department head: owns the home team itself.
		"peer-1": `{"principalId":"peer-1","deploymentId":"demo","orgScope":["dept-a","dept-a-eng"],"ownTeamIds":["dept-a"],"roleScope":[]}`,
		// An outsider from a sibling department.
		"sibling-1": `{"principalId":"sibling-1","deploymentId":"demo","orgScope":["dept-b"],"ownTeamIds":["dept-b"],"roleScope":[]}`,
	})
	manager := wardenManager(t, server)

	spawned := map[string]bool{}
	warden, err := newWardenWithProvider(manager, "dept-a", func(ctx context.Context, requester *aep.Principal, requesterCtx *aep.RetrievalContext) (string, error) {
		spawned[requester.UserID] = true
		return "http://127.0.0.1:18901", nil
	})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		userID   string
		wantFork bool
	}{
		{"sub-1", true},      // strictly inside the subtree → ephemeral fork
		{"peer-1", false},    // owns the home team → resident
		{"sibling-1", false}, // outside the subtree → resident (peer rule)
	}
	for _, tc := range cases {
		target, err := warden.Route(context.Background(), &aep.Principal{UserID: tc.userID})
		if err != nil {
			t.Fatalf("%s route: %v", tc.userID, err)
		}
		if tc.wantFork && target == "" {
			t.Fatalf("%s should be routed to a fork", tc.userID)
		}
		if !tc.wantFork && target != "" {
			t.Fatalf("%s should reach the resident, got %q", tc.userID, target)
		}
	}
	if !spawned["sub-1"] || len(spawned) != 1 {
		t.Fatalf("exactly the subordinate should spawn a fork: %v", spawned)
	}
}

func TestWardenFailsClosedOnUnresolvableRequesterScope(t *testing.T) {
	// The requester has no context entry: the lookup fails and no fallback
	// to the resident happens, because an unknown scope must not widen.
	server := scopeServer(t, map[string]string{
		"resident-1": `{"principalId":"resident-1","deploymentId":"demo","orgScope":["dept-a"],"ownTeamIds":["dept-a"],"roleScope":[]}`,
		"ghost":      "", // expected miss: the scope lookup 404s
	})
	manager := wardenManager(t, server)
	warden, err := newWardenWithProvider(manager, "dept-a", func(context.Context, *aep.Principal, *aep.RetrievalContext) (string, error) {
		t.Fatal("fork provider must not be called when scope resolution fails")
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := warden.Route(context.Background(), &aep.Principal{UserID: "ghost"}); err == nil {
		t.Fatal("expected scope resolution failure to propagate")
	}
}

func TestSupervisorValidatesConfiguration(t *testing.T) {
	if _, err := NewSupervisor(&config.Config{AEP: config.AEPConfig{Enabled: true}}, nil); err == nil {
		t.Fatal("missing warden settings should fail")
	}
	if _, err := NewSupervisor(&config.Config{AEP: config.AEPConfig{Enabled: true}}, &config.WardenSettings{RuntimeRoleID: "runner"}); err == nil {
		t.Fatal("missing supervisor credentials should fail")
	}
}
