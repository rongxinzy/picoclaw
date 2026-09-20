package aepgate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/config"
)

// supervisorFixture fakes the control plane and hosts a "child gateway"
// health endpoint on a fixed local port so spawn() succeeds without a real
// picoclaw binary.
type supervisorFixture struct {
	server     *httptest.Server
	healthLn   net.Listener
	healthPort int

	mu      sync.Mutex
	created int
	deleted int
	revoked int
}

func newSupervisorFixture(t *testing.T) *supervisorFixture {
	t.Helper()
	f := &supervisorFixture{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/aep/v1/auth/password/login":
			_, _ = io.WriteString(w, `{"accessToken":"sup-at","refreshToken":"sup-rt","modelAccessToken":"sup-mt","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"sup"}`)
		case "/aep/v1/auth/refresh":
			_, _ = io.WriteString(w, `{"accessToken":"sup-at2","refreshToken":"sup-rt2","modelAccessToken":"sup-mt2","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"sup"}`)
		case "/aep/v1/metadata":
			_, _ = io.WriteString(w, `{"service":"aep","deploymentId":"demo"}`)
		case "/aep/v1/user/models":
			_, _ = io.WriteString(w, `{"models":[{"id":"m1","enabled":true}]}`)
		case "/aep/v1/user/heartbeat":
			_, _ = io.WriteString(w, `{"serverTime":"2026-09-19T00:00:00Z","controlEvents":{"pending":false},"nextHeartbeatAfterSeconds":60}`)
		case "/aep/v1/admin/data-scope/context":
			_, _ = io.WriteString(w, `{"principalId":"sub-1","deploymentId":"demo","orgScope":["dept-child"],"ownTeamIds":["dept-child"],"roleScope":[]}`)
		case "/aep/v1/admin/agents":
			f.mu.Lock()
			f.created++
			f.mu.Unlock()
			_, _ = w.Write([]byte(fmt.Sprintf(`{"id":"fork-%d","username":"eph-x","displayName":"Fork","homeTeamId":"dept-child","ephemeral":true,"expiresAt":"%s"}`,
				f.created, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))))
		default:
			if strings.HasPrefix(r.URL.Path, "/aep/v1/admin/sessions/") && strings.HasSuffix(r.URL.Path, "/revoke") {
				f.mu.Lock()
				f.revoked++
				f.mu.Unlock()
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if len(r.URL.Path) > len("/aep/v1/admin/agents/") && r.Method == http.MethodDelete {
				f.mu.Lock()
				f.deleted++
				f.mu.Unlock()
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(f.server.Close)

	// A fixed-port health endpoint standing in for the fork's gateway.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.healthLn = ln
	f.healthPort = ln.Addr().(*net.TCPAddr).Port
	mux := http.NewServeMux()
	mux.HandleFunc("/aepchat/v1/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	go func() { _ = http.Serve(ln, mux) }()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

func (f *supervisorFixture) counters() (int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.created, f.deleted, f.revoked
}

func newTestSupervisor(t *testing.T, f *supervisorFixture) *Supervisor {
	t.Helper()
	cfg := &config.Config{}
	cfg.AEP = config.AEPConfig{
		Enabled: true, BaseURL: f.server.URL, DeploymentID: "demo",
		SupervisorUsername: "sup", HomeTeamID: "dept-home",
	}
	cfg.AEP.SupervisorPassword = *config.NewSecureString("sup-password-123")
	settings := &config.WardenSettings{
		RuntimeRoleID: "runner", TTLMinutes: 30, PortRangeStart: f.healthPort,
	}
	supervisor, err := NewSupervisor(cfg, settings)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	t.Cleanup(supervisor.Stop)
	// The spawned child only needs to stay alive long enough for the health
	// probe; /bin/true exits immediately and the exit is ignored.
	supervisor.binary = "/bin/true"
	return supervisor
}

func TestSupervisorSpawnsReusesAndCleansUpForks(t *testing.T) {
	f := newSupervisorFixture(t)
	supervisor := newTestSupervisor(t, f)
	requester := &aep.Principal{UserID: "sub-1", DisplayName: "Wen"}
	scope := &aep.RetrievalContext{PrincipalID: "sub-1", OwnTeamIDs: []string{"dept-child"}}

	fork, err := supervisor.Ensure(context.Background(), requester, scope)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if fork.URL() != fmt.Sprintf("http://127.0.0.1:%d", f.healthPort) {
		t.Fatalf("fork URL = %s", fork.URL())
	}
	// A second conversation with the same requester reuses the fork.
	again, err := supervisor.Ensure(context.Background(), requester, scope)
	if err != nil || again != fork {
		t.Fatalf("reuse: %v %p!=%p", err, again, fork)
	}
	created, deleted, revoked := f.counters()
	if created != 1 || deleted != 0 || revoked != 0 {
		t.Fatalf("after reuse: created=%d deleted=%d revoked=%d", created, deleted, revoked)
	}

	// Cleanup revokes the session, deletes the account, and removes the home.
	home := fork.homeDir
	supervisor.cleanupAccount(context.Background(), fork)
	created, deleted, revoked = f.counters()
	if revoked != 1 || deleted != 1 {
		t.Fatalf("after cleanup: deleted=%d revoked=%d", deleted, revoked)
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatalf("fork home not removed: %v", err)
	}
}

func TestSupervisorSpawnFailureCleansAccount(t *testing.T) {
	f := newSupervisorFixture(t)
	// Stop the fixture's health server and rebind the port with a listener
	// that never answers the health probe, so the child stays unhealthy.
	_ = f.healthLn.Close()
	reserved, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", f.healthPort))
	if err != nil {
		t.Fatal(err)
	}
	defer reserved.Close()

	supervisor := newTestSupervisor(t, f)
	supervisor.binary = "/bin/true"
	supervisor.healthTimeout = 2 * time.Second
	requester := &aep.Principal{UserID: "sub-1", DisplayName: "Wen"}
	scope := &aep.RetrievalContext{PrincipalID: "sub-1", OwnTeamIDs: []string{"dept-child"}}

	// The spawn times out against the reserved port and rolls the account
	// back.
	if _, err := supervisor.Ensure(context.Background(), requester, scope); err == nil {
		t.Fatal("expected spawn failure against a dead health endpoint")
	}
	created, deleted, _ := f.counters()
	if created != 1 || deleted != 1 {
		t.Fatalf("failed spawn must roll back the account: created=%d deleted=%d", created, deleted)
	}
}

func TestForkHelpersAndSanitize(t *testing.T) {
	expiry := time.Now().Add(time.Hour)
	fork := &fork{expiresAt: expiry}
	if !fork.healthy() {
		t.Fatal("fresh fork must be healthy")
	}
	fork.expiresAt = time.Now().Add(-time.Minute)
	if fork.healthy() {
		t.Fatal("expired fork must not be healthy")
	}
	if got := sanitize("User-UUID_123-with!!specials"); len(got) == 0 || got == "User-UUID_123-with!!specials" {
		t.Fatalf("sanitize = %q", got)
	}
	if got := sanitize(""); got == "" || got[len(got)-1] == '-' {
		t.Fatalf("sanitize('') = %q", got)
	}
	if got := sanitize("UPPERCASE"); got != "uppercase" {
		t.Fatalf("sanitize must lowercase: %q", got)
	}
	long := ""
	for i := 0; i < 40; i++ {
		long += "a"
	}
	if got := sanitize(long); len(got) > 24 {
		t.Fatalf("sanitize must cap length: %d", len(got))
	}
	if len(randomSuffix()) != 10 {
		t.Fatal("randomSuffix length")
	}
	// residentModels with no default manager returns nil.
	if got := (&Supervisor{}).residentModels(); got != nil {
		t.Fatalf("residentModels without a manager = %v", got)
	}
}

func TestLaunchChildWritesConfigAndEnv(t *testing.T) {
	f := newSupervisorFixture(t)
	supervisor := newTestSupervisor(t, f)
	supervisor.binary = "/bin/true"
	home, err := os.MkdirTemp("", "fork-cfg-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(home)
	forkObj := &fork{
		username: "eph-x", sessionID: "fork-session-1", homeDir: home,
		port: f.healthPort, expiresAt: time.Now().Add(time.Hour),
	}
	childCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := supervisor.launchChild(childCtx, forkObj); err != nil {
		t.Fatalf("launchChild: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(home, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg["version"] != float64(3) || cfg["knowledge"] == nil {
		t.Fatalf("child config = %v", cfg)
	}
}

func TestWardenConstructorValidation(t *testing.T) {
	if _, err := NewWarden(nil, &Supervisor{}, "home"); err == nil {
		t.Fatal("nil manager must fail")
	}
	m := aep.NewManager(aep.Config{BaseURL: "http://unused.local", DeploymentID: "demo", Username: "u", Password: "p", SessionID: "s"})
	if _, err := NewWarden(m, nil, "home"); err == nil {
		t.Fatal("nil supervisor must fail")
	}
	if _, err := NewWarden(m, &Supervisor{}, ""); err == nil {
		t.Fatal("empty home team must fail")
	}
}

func TestSupervisorLoggerBridge(t *testing.T) {
	supervisorLogger{}.Infof("i %d", 1)
	supervisorLogger{}.Warnf("w")
	supervisorLogger{}.Errorf("e")
}

func TestReaperSweepsExpiredForks(t *testing.T) {
	f := newSupervisorFixture(t)
	supervisor := newTestSupervisor(t, f)
	_, deadCancel := context.WithCancel(context.Background())
	t.Cleanup(deadCancel)
	dead := &fork{
		requesterID: "sub-1", agentID: "fork-1", username: "eph-x",
		sessionID: "fork-dead", homeDir: t.TempDir(), cancel: deadCancel,
		expiresAt: time.Now().Add(-time.Minute),
	}
	alive := &fork{requesterID: "sub-2", agentID: "fork-2", username: "eph-y", expiresAt: time.Now().Add(time.Hour)}
	supervisor.forks["sub-1"] = dead
	supervisor.forks["sub-2"] = alive
	supervisor.reapInterval = 30 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go supervisor.reaper(ctx)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		supervisor.mu.Lock()
		_, swept := supervisor.forks["sub-1"]
		_, kept := supervisor.forks["sub-2"]
		supervisor.mu.Unlock()
		if !swept && kept {
			_, _, revoked := f.counters()
			if revoked >= 1 {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("reaper did not sweep the expired fork in time")
}

func TestResidentModelsUsesDefaultManager(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/aep/v1/auth/password/login", "/aep/v1/auth/refresh":
			_, _ = io.WriteString(w, `{"accessToken":"at","refreshToken":"rt","modelAccessToken":"mt","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"s"}`)
		case "/aep/v1/metadata":
			_, _ = io.WriteString(w, `{"service":"aep","deploymentId":"demo"}`)
		case "/aep/v1/user/models":
			_, _ = io.WriteString(w, `{"models":[{"id":"m1","enabled":true},{"id":"m2","enabled":false}]}`)
		case "/aep/v1/user/heartbeat":
			_, _ = io.WriteString(w, `{"serverTime":"2026-09-19T00:00:00Z","controlEvents":{"pending":false},"nextHeartbeatAfterSeconds":60}`)
		}
	}))
	defer fake.Close()
	m := aep.NewManager(aep.Config{BaseURL: fake.URL, DeploymentID: "demo", Username: "u", Password: "p", SessionID: "s"})
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	aep.SetDefaultManager(m)
	t.Cleanup(func() { aep.SetDefaultManager(nil) })

	got := (&Supervisor{}).residentModels()
	if len(got) != 1 || got[0] != "m1" {
		t.Fatalf("residentModels = %v, want enabled m1 only", got)
	}
}

func TestNewWardenSuccess(t *testing.T) {
	m := aep.NewManager(aep.Config{BaseURL: "http://unused.local", DeploymentID: "demo", Username: "u", Password: "p", SessionID: "s"})
	if _, err := NewWarden(m, &Supervisor{}, "home"); err != nil {
		t.Fatalf("NewWarden success: %v", err)
	}
	if _, err := newWardenWithProvider(m, "home", func(context.Context, *aep.Principal, *aep.RetrievalContext) (string, error) { return "", nil }); err != nil {
		t.Fatalf("provider constructor: %v", err)
	}
	if _, err := newWardenWithProvider(m, "home", nil); err == nil {
		t.Fatal("nil provider must fail")
	}
}
