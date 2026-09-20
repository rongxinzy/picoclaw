package aepchat

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

// controlPlaneFixture fakes the supervisor-facing control plane (login,
// ephemeral agent provisioning) and hosts a "child gateway" health endpoint
// on a fixed local port so warden wiring succeeds without a real picoclaw
// binary.
type controlPlaneFixture struct {
	server     *httptest.Server
	healthLn   net.Listener
	healthPort int
}

func newControlPlaneFixture(t *testing.T) *controlPlaneFixture {
	t.Helper()
	f := &controlPlaneFixture{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/aep/v1/auth/password/login", "/aep/v1/auth/refresh":
			_, _ = io.WriteString(w, `{"accessToken":"at","refreshToken":"rt","modelAccessToken":"mt","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"s"}`)
		case "/aep/v1/admin/agents":
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"id":"eph-1","username":"eph-1","displayName":"Fork","homeTeamId":"dept-home","displayTitle":"","ephemeral":true,"expiresAt":"2030-01-01T00:00:00Z"}`)
		case "/aep/v1/auth/sessions/revoke":
			w.WriteHeader(http.StatusNoContent)
		case "/aep/v1/admin/agents/eph-1":
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(f.server.Close)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.healthLn = ln
	f.healthPort = ln.Addr().(*net.TCPAddr).Port
	mux := http.NewServeMux()
	mux.HandleFunc("/aepchat/v1/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	go func() { _ = http.Serve(ln, mux) }()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

func TestChannelLifecycleProxyAndEdges(t *testing.T) {
	f := newIDFixture(t)
	cfg := config.DefaultConfig()
	cfg.AEP = config.AEPConfig{
		Enabled: true, BaseURL: f.baseURL, DeploymentID: "demo",
		Username: "helper", HomeTeamID: "dept-home",
	}
	cfg.AEP.Password = *config.NewSecureString("agent-password-123")
	b := bus.NewMessageBus()
	t.Cleanup(func() { b.Close() })
	ch, err := New("aepchat", &config.Channel{Enabled: true, Type: "aepchat"}, &config.AEPChatSettings{HistoryLimit: 3}, cfg, b)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if ch.WebhookPath() != "/aepchat/" {
		t.Fatalf("WebhookPath = %s", ch.WebhookPath())
	}
	if ch.IsRunning() {
		t.Fatal("channel must not run before Start")
	}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !ch.IsRunning() {
		t.Fatal("channel must run after Start")
	}
	// History trimming keeps the buffer bounded.
	for i := 0; i < 6; i++ {
		if _, err := ch.Send(context.Background(), bus.OutboundMessage{ChatID: "trim", Content: strings.Repeat("m", i)}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	ch.Stop(context.Background())
	if ch.IsRunning() {
		t.Fatal("channel must stop")
	}
	if _, err := ch.Send(context.Background(), bus.OutboundMessage{ChatID: "trim", Content: "x"}); err == nil {
		t.Fatal("Send must fail when not running")
	}

	// Proxy: the channel forwards a request verbatim, headers included.
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer proxied" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer target.Close()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/aepchat/v1/chats/c/messages"
		r.Header.Set("Authorization", "Bearer proxied")
		ch.proxyTo(w, r, target.URL)
	}))
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/aepchat/v1/chats/c/messages")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("proxy = %v %v", err, resp)
	}

	// Edge routes: unknown route and bad method.
	edge := httptest.NewServer(ch)
	defer edge.Close()
	if resp, _ := http.Get(edge.URL + "/aepchat/v1/unknown"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown route = %d", resp.StatusCode)
	}
	putReq, _ := http.NewRequest(http.MethodPut, edge.URL+"/aepchat/v1/chats/c/messages", nil)
	if resp, _ := http.DefaultClient.Do(putReq); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("bad method = %d", resp.StatusCode)
	}
}

func TestHandlePostEdgeCases(t *testing.T) {
	f := newIDFixture(t)
	ch, _ := newTestChannel(t, f)
	ts := httptest.NewServer(ch)
	defer ts.Close()

	// Oversized text is rejected before any state changes.
	big := strings.Repeat("x", 20000)
	status, body := postRaw(t, ts.URL+"/aepchat/v1/chats/big/messages", f.token("user-1"), `{"text":"`+big+`"}`)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized = %d %s", status, body)
	}

	// A chat id with a slash is rejected.
	status, body = postRaw(t, ts.URL+"/aepchat/v1/chats/a%2Fb/messages", f.token("user-1"), `{"text":"hi"}`)
	if status != http.StatusBadRequest || !strings.Contains(body, "INVALID_CHAT") {
		t.Fatalf("slashed chat = %d %s", status, body)
	}

	// Malformed JSON body.
	status, _ = postRaw(t, ts.URL+"/aepchat/v1/chats/cx/messages", f.token("user-1"), `{not-json`)
	if status != http.StatusBadRequest {
		t.Fatalf("bad json = %d", status)
	}

	// Blank text.
	status, _ = postRaw(t, ts.URL+"/aepchat/v1/chats/cx/messages", f.token("user-1"), `{"text":"   "}`)
	if status != http.StatusBadRequest {
		t.Fatalf("blank = %d", status)
	}
}

func TestNewWithWardenSuccessAndProxyFailure(t *testing.T) {
	fixture := newControlPlaneFixture(t)
	resident := aep.NewManager(aep.Config{
		BaseURL: fixture.server.URL, DeploymentID: "demo",
		Username: "helper", Password: "agent-password-123", SessionID: "resident",
	})
	if err := resident.Start(context.Background()); err != nil {
		t.Fatalf("resident start: %v", err)
	}
	t.Cleanup(resident.Stop)
	aep.SetDefaultManager(resident)
	t.Cleanup(func() { aep.SetDefaultManager(nil) })
	cfg := config.DefaultConfig()
	cfg.AEP = config.AEPConfig{
		Enabled: true, BaseURL: fixture.server.URL, DeploymentID: "demo",
		Username: "helper", HomeTeamID: "dept-home", SupervisorUsername: "sup",
	}
	cfg.AEP.Password = *config.NewSecureString("agent-password-123")
	cfg.AEP.SupervisorPassword = *config.NewSecureString("sup-password-123")
	b := bus.NewMessageBus()
	t.Cleanup(func() { b.Close() })
	ch, err := New("aepchat", &config.Channel{Enabled: true, Type: "aepchat"}, &config.AEPChatSettings{
		Warden: &config.WardenSettings{RuntimeRoleID: "runner", PortRangeStart: fixture.healthPort},
	}, cfg, b)
	if err != nil {
		t.Fatalf("New with warden: %v", err)
	}
	t.Cleanup(func() { _ = ch.Stop(context.Background()) })
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start with warden: %v", err)
	}

	// Proxy failures surface as 502.
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/aepchat/v1/chats/x/messages"
		ch.proxyTo(w, r, "http://127.0.0.1:1")
	}))
	defer dead.Close()
	if resp, err := http.Get(dead.URL); err != nil || resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("proxy failure = %v %v", err, resp)
	}
}

func postRaw(t *testing.T, url, token, body string) (int, string) {
	t.Helper()
	resp := post(t, url, token, body)
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, string(raw)
}
