package aep

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const loginBody = `{"accessToken":"at-1","refreshToken":"rt-1","modelAccessToken":"mt-1","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"picoclaw-test"}`

// newAEPTestServer serves a login/refresh/metadata/models/heartbeat flow and
// records which endpoints were hit.
type aepTestServer struct {
	mu            sync.Mutex
	logins        int
	refreshes     int
	heartbeats    int
	refreshCode   int // 0 = success; otherwise HTTP status for refresh
	heartbeatCode int
	server        *httptest.Server
}

func newAEPTestServer(t *testing.T) *aepTestServer {
	t.Helper()
	s := &aepTestServer{}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/aep/v1/auth/password/login":
			s.logins++
			_, _ = io.WriteString(w, loginBody)
		case "/aep/v1/auth/refresh":
			s.refreshes++
			if s.refreshCode != 0 {
				w.Header().Set("Content-Type", "application/problem+json")
				w.WriteHeader(s.refreshCode)
				_, _ = io.WriteString(w, fmt.Sprintf(`{"title":"Denied","status":%d,"code":"TOKEN_INVALID"}`, s.refreshCode))
				return
			}
			_, _ = io.WriteString(w, `{"accessToken":"at-2","refreshToken":"rt-2","modelAccessToken":"mt-2","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"picoclaw-test"}`)
		case "/aep/v1/metadata":
			_, _ = io.WriteString(w, `{"service":"aep-control-service","deploymentId":"demo","modelGateway":{"baseUrl":"https://gw.example.test/v1","protocol":"openai-compatible"}}`)
		case "/aep/v1/user/models":
			_, _ = io.WriteString(w, `{"models":[{"id":"qwen","displayName":"Qwen","enabled":true}]}`)
		case "/aep/v1/user/me":
			_, _ = io.WriteString(w, `{"user":{"id":"picoclaw-test","displayName":"Helper","kind":"agent"},"deploymentId":"demo","roles":[]}`)
		case "/aep/v1/admin/data-scope/context":
			if r.URL.Query().Get("userId") == "picoclaw-test" {
				_, _ = io.WriteString(w, `{"principalId":"picoclaw-test","deploymentId":"demo","orgScope":["dept-a"],"ownTeamIds":["dept-a"],"roleScope":[]}`)
			} else {
				_, _ = io.WriteString(w, `{"principalId":"user-a","deploymentId":"demo","orgScope":["dept-a","dept-b"],"ownTeamIds":["dept-a"],"roleScope":[]}`)
			}
		case "/aep/v1/user/heartbeat":
			s.heartbeats++
			if s.heartbeatCode != 0 {
				w.WriteHeader(s.heartbeatCode)
				_, _ = io.WriteString(w, fmt.Sprintf(`{"title":"Denied","status":%d,"code":"TOKEN_INVALID"}`, s.heartbeatCode))
				return
			}
			_, _ = io.WriteString(w, `{"serverTime":"2026-09-18T08:00:00Z","controlEvents":{"pending":false,"watermark":"0"},"nextHeartbeatAfterSeconds":60}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(s.server.Close)
	return s
}

func (s *aepTestServer) counts() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logins, s.refreshes, s.heartbeats
}

func (s *aepTestServer) setRefreshStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshCode = code
}

func (s *aepTestServer) setHeartbeatStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeatCode = code
}

func newTestManager(t *testing.T, baseURL string) *Manager {
	t.Helper()
	m := NewManager(Config{
		BaseURL:      baseURL,
		DeploymentID: "demo",
		Username:     "helper-1",
		Password:     "agent-password-123",
		SessionID:    "picoclaw-test",
	})
	t.Cleanup(m.Stop)
	return m
}

func TestStartEstablishesSessionGatewayAndModels(t *testing.T) {
	fake := newAEPTestServer(t)
	m := newTestManager(t, fake.server.URL)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if tok, err := m.AccessToken(ctx); err != nil || tok != "at-1" {
		t.Fatalf("access token = %q, err = %v", tok, err)
	}
	if tok, err := m.ModelAccessToken(ctx); err != nil || tok != "mt-1" {
		t.Fatalf("model token = %q, err = %v", tok, err)
	}
	gateway, err := m.GatewayBaseURL()
	if err != nil || gateway != "https://gw.example.test/v1" {
		t.Fatalf("gateway = %q, err = %v", gateway, err)
	}
	models := m.Models()
	if len(models) != 1 || models[0].ID != "qwen" {
		t.Fatalf("models = %+v", models)
	}
	if m.SessionID() != "picoclaw-test" {
		t.Fatalf("session id = %q", m.SessionID())
	}
}

func TestAccessTokenRefreshesWhenNearExpiry(t *testing.T) {
	fake := newAEPTestServer(t)
	m := newTestManager(t, fake.server.URL)
	current := time.Now()
	m.now = func() time.Time { return current }

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	// Advance past 4/5 of the 3600s TTL so the cached token is stale.
	current = current.Add(3500 * time.Second)
	tok, err := m.AccessToken(ctx)
	if err != nil {
		t.Fatalf("access token: %v", err)
	}
	if tok != "at-2" {
		t.Fatalf("expected refreshed token at-2, got %q", tok)
	}
	_, refreshes, _ := fake.counts()
	if refreshes != 1 {
		t.Fatalf("refresh calls = %d, want 1", refreshes)
	}
}

func TestRefreshRejectionFallsBackToPasswordLogin(t *testing.T) {
	fake := newAEPTestServer(t)
	m := newTestManager(t, fake.server.URL)
	current := time.Now()
	m.now = func() time.Time { return current }

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	fake.setRefreshStatus(http.StatusUnauthorized)

	current = current.Add(3500 * time.Second)
	tok, err := m.AccessToken(ctx)
	if err != nil {
		t.Fatalf("access token: %v", err)
	}
	if tok != "at-1" {
		t.Fatalf("expected re-login token at-1, got %q", tok)
	}
	logins, refreshes, _ := fake.counts()
	if refreshes != 1 || logins != 2 {
		t.Fatalf("logins = %d, refreshes = %d; want 2 logins and 1 rejected refresh", logins, refreshes)
	}
}

func TestPasswordChangeRequiredFailsStart(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"accessToken":"at-1","refreshToken":"rt-1","expiresIn":7200,"passwordChangeRequired":true}`)
	}))
	defer server.Close()

	m := newTestManager(t, server.URL)
	err := m.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "PASSWORD_CHANGE_REQUIRED") {
		t.Fatalf("expected PASSWORD_CHANGE_REQUIRED, got %v", err)
	}
}

func TestBeatReportsOnlineAndHonorsServerInterval(t *testing.T) {
	fake := newAEPTestServer(t)
	m := newTestManager(t, fake.server.URL)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	next := m.beat(context.Background())
	if next != 60*time.Second {
		t.Fatalf("next interval = %v, want 60s from server", next)
	}
	if !m.Online() {
		t.Fatal("manager should be online after accepted heartbeat")
	}
	if _, _, beats := fake.counts(); beats != 1 {
		t.Fatalf("heartbeats = %d, want 1", beats)
	}
}

func TestModelTokenMissingWhenAccountHasNoModelScopes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"accessToken":"at-1","refreshToken":"rt-1","expiresIn":3600,"modelAccessExpiresIn":0,"deploymentId":"demo","sessionId":"picoclaw-test"}`)
	}))
	defer server.Close()

	m := newTestManager(t, server.URL)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := m.ModelAccessToken(context.Background()); err == nil {
		t.Fatal("expected error for missing model scopes")
	}
	if tok, err := m.AccessToken(context.Background()); err != nil || tok != "at-1" {
		t.Fatalf("access token should still work: %q %v", tok, err)
	}
}

func TestLoginBodyNeverLeaksIntoErrorStrings(t *testing.T) {
	// Transport failures must not echo request bodies or query strings.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("transport closed before handler")
	}))
	server.Close() // immediately closed: forces a transport error

	m := NewManager(Config{
		BaseURL:      server.URL,
		DeploymentID: "demo",
		Username:     "helper-1",
		Password:     "agent-password-123",
		SessionID:    "picoclaw-test",
	})
	err := m.Start(context.Background())
	if err == nil {
		t.Fatal("expected failure against closed server")
	}
	var serialized []byte
	if b, ok := err.(*Problem); ok {
		serialized, _ = json.Marshal(b)
	} else {
		serialized = []byte(err.Error())
	}
	if strings.Contains(string(serialized), "agent-password-123") {
		t.Fatalf("password leaked into error: %s", serialized)
	}
}

func TestManagerScopeSelfAndRegistry(t *testing.T) {
	fake := newAEPTestServer(t)
	m := newTestManager(t, fake.server.URL)

	SetDefaultManager(m)
	t.Cleanup(func() { SetDefaultManager(nil) })
	if DefaultManager() != m {
		t.Fatal("default manager registry round-trip failed")
	}

	if got, err := m.DataScopeContext(context.Background(), "user-a"); err != nil || got.PrincipalID != "user-a" {
		t.Fatalf("Manager.DataScopeContext = %+v, %v", got, err)
	}
	if got, err := m.SelfUserID(context.Background()); err != nil || got != "picoclaw-test" {
		t.Fatalf("SelfUserID = %q, %v", got, err)
	}
	self, err := m.SelfContext(context.Background())
	if err != nil || self.PrincipalID != "picoclaw-test" {
		t.Fatalf("SelfContext = %+v, %v", self, err)
	}
	// SelfContext caches: the second call must not refetch.
	if _, err := m.SelfContext(context.Background()); err != nil {
		t.Fatalf("cached SelfContext: %v", err)
	}
}

func TestRefreshModelsReturnsErrors(t *testing.T) {
	fake := newAEPTestServer(t)
	m := newTestManager(t, fake.server.URL)
	if _, err := m.RefreshModels(context.Background()); err != nil {
		t.Fatalf("refresh against a healthy server must succeed: %v", err)
	}

	dead := Config{
		BaseURL: "http://127.0.0.1:1", DeploymentID: "demo",
		Username: "helper", Password: "agent-password-123", SessionID: "t",
	}
	offline := NewManager(dead)
	t.Cleanup(offline.Stop)
	if err := offline.Start(context.Background()); err == nil {
		t.Fatal("expected login failure against a dead endpoint")
	}
}

func TestBeatRecoversFromAuthFailure(t *testing.T) {
	fake := newAEPTestServer(t)
	m := newTestManager(t, fake.server.URL)
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// An auth-rejected heartbeat triggers the recovery refresh, which
	// succeeds against the fake and keeps the manager usable.
	current := time.Now()
	m.now = func() time.Time { return current }
	current = current.Add(3600 * time.Second)
	fake.setHeartbeatStatus(http.StatusUnauthorized)
	next := m.beat(context.Background())
	if next <= 0 {
		t.Fatalf("beat returned a non-positive interval: %v", next)
	}
	if tok, err := m.AccessToken(context.Background()); err != nil || tok == "" {
		t.Fatalf("post-recovery access token: %q %v", tok, err)
	}
}
