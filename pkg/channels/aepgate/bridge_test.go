package aepgate

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/config"
)

// relayEndpoint fakes a fork's relay endpoint and records the submitted turn.
func relayEndpoint(t *testing.T, reply string) (*httptest.Server, *RelayTurn) {
	t.Helper()
	var seen RelayTurn
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/aepchat/v1/relay/turns" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer fork-relay-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &seen)
		encoded, _ := json.Marshal(map[string]any{"reply": reply, "seq": 1})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write(encoded)
	}))
	t.Cleanup(server.Close)
	return server, &seen
}

func portOfURL(raw string) int {
	idx := strings.LastIndex(raw, ":")
	port, _ := strconv.Atoi(raw[idx+1:])
	return port
}

// bridgeFixture assembles a Bridge against fake control plane + warden.
type bridgeFixture struct {
	mappings *mappingServer
	scopes   *httptest.Server
	relay    *httptest.Server
	seenTurn *RelayTurn
	bridge   *Bridge
	replies  []string
}

type forkMode int

const (
	noFork forkMode = iota // warden answers with nil (resident)
	liveFork              // fake fork points at this fixture's relay endpoint
	deadFork              // fake fork points at an unreachable port
)

// newBridgeFixture wires a subordinate-serving warden; the fake fork honors
// the requested mode.
func newBridgeFixture(t *testing.T, mappings string, scopes map[string]string, mode forkMode) *bridgeFixture {
	t.Helper()
	f := &bridgeFixture{}
	f.mappings = newMappingServer(t, mappings)
	f.relay, f.seenTurn = relayEndpoint(t, "fork answer")

	residentManager := aep.NewManager(aep.Config{
		BaseURL: f.mappings.server.URL, DeploymentID: "demo",
		Username: "resident", Password: "resident-password-123", SessionID: "t",
	})
	if err := residentManager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(residentManager.Stop)

	f.scopes = scopeServer(t, scopes)
	wardenManager := aep.NewManager(aep.Config{
		BaseURL: f.scopes.URL, DeploymentID: "demo",
		Username: "resident", Password: "resident-password-123", SessionID: "t",
	})
	if err := wardenManager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wardenManager.Stop)
	relayPort := portOfURL(f.relay.URL)
	warden, err := newWardenWithProvider(wardenManager, "dept-a",
		func(context.Context, *aep.Principal, *aep.RetrievalContext) (*fork, error) {
			switch mode {
			case noFork:
				return nil, nil
			case deadFork:
				return &fork{port: 1, relaySecret: "fork-relay-secret"}, nil
			default:
				return &fork{port: relayPort, relaySecret: "fork-relay-secret"}, nil
			}
		})
	if err != nil {
		t.Fatal(err)
	}

	f.bridge = &Bridge{
		resolver:      mustResolver(t, residentManager, "src-1"),
		gate:          &Gate{warden: warden},
		relay:         NewRelayClient(),
		unmappedReply: defaultUnmappedReply,
		limiter:       newRejectionLimiter(),
	}
	return f
}

func mustResolver(t *testing.T, manager *aep.Manager, sourceID string) *IdentityResolver {
	t.Helper()
	resolver, err := NewIdentityResolver(manager, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func (f *bridgeFixture) intercept(t *testing.T, req InterceptRequest) Verdict {
	t.Helper()
	f.replies = nil
	return f.bridge.Intercept(context.Background(), req, func(text string) error {
		f.replies = append(f.replies, text)
		return nil
	})
}

var subordinateScopes = map[string]string{
	"resident-1": `{"principalId":"resident-1","deploymentId":"demo","orgScope":["dept-a","dept-a-eng"],"ownTeamIds":["dept-a"],"roleScope":[]}`,
	"user-sub":   `{"principalId":"user-sub","deploymentId":"demo","orgScope":["dept-a-eng"],"ownTeamIds":["dept-a-eng"],"roleScope":[]}`,
}

const subMapping = `{"mappings":[{"sourceId":"src-1","externalSubjectType":"user","externalId":"ou_sub","localSubjectId":"user-sub","status":"active"}],"nextCursor":null}`

func TestBridgeRejectsUnmappedSenderAndRateLimits(t *testing.T) {
	f := newBridgeFixture(t, `{"mappings":[],"nextCursor":null}`, nil, noFork)

	req := InterceptRequest{Platform: "feishu", PlatformSenderID: "ou_stranger", ChatID: "oc_1", ChatType: "direct", Text: "hi"}
	verdict := f.intercept(t, req)
	if verdict.Action != ActionStop {
		t.Fatalf("unmapped verdict = %+v", verdict)
	}
	if len(f.replies) != 1 || !strings.Contains(f.replies[0], "绑定") {
		t.Fatalf("replies = %v", f.replies)
	}
	// Second message inside the window: silent.
	verdict = f.intercept(t, req)
	if verdict.Action != ActionStop || len(f.replies) != 0 {
		t.Fatalf("rate-limited = %+v %v", verdict, f.replies)
	}
}

func TestBridgeRoutesPeerToResidentWithMappedIdentity(t *testing.T) {
	// The requester owns the home team → peer → resident, identity rewritten.
	scopes := map[string]string{
		"resident-1": subordinateScopes["resident-1"],
		"user-peer":  `{"principalId":"user-peer","deploymentId":"demo","orgScope":["dept-a"],"ownTeamIds":["dept-a"],"roleScope":[]}`,
	}
	mappings := `{"mappings":[{"sourceId":"src-1","externalSubjectType":"user","externalId":"ou_peer","localSubjectId":"user-peer","status":"active"}],"nextCursor":null}`
	f := newBridgeFixture(t, mappings, scopes, noFork)

	verdict := f.intercept(t, InterceptRequest{
		Platform: "feishu", PlatformSenderID: "ou_peer", ChatID: "oc_1", ChatType: "direct",
		Text: "dept report", DisplayName: "Peer Wang",
	})
	if verdict.Action != ActionResident || verdict.AEPUserID != "user-peer" || verdict.DisplayName != "Peer Wang" {
		t.Fatalf("verdict = %+v", verdict)
	}
	if len(f.replies) != 0 {
		t.Fatalf("resident turn must not reply through the bridge: %v", f.replies)
	}
	// Missing display name falls back to the user id.
	verdict = f.intercept(t, InterceptRequest{Platform: "feishu", PlatformSenderID: "ou_peer", ChatID: "oc_1", Text: "again"})
	if verdict.DisplayName != "user-peer" {
		t.Fatalf("fallback name = %q", verdict.DisplayName)
	}
}

func TestBridgeRelaysSubordinateToFork(t *testing.T) {
	f := newBridgeFixture(t, subMapping, subordinateScopes, liveFork)

	verdict := f.intercept(t, InterceptRequest{
		Platform: "wecom", PlatformSenderID: "ou_sub", ChatID: "wr_9", ChatType: "direct",
		Text: "帮我出部门报告", DisplayName: "Sub Li",
	})
	if verdict.Action != ActionStop {
		t.Fatalf("fork verdict = %+v", verdict)
	}
	if len(f.replies) != 1 || f.replies[0] != "fork answer" {
		t.Fatalf("replies = %v", f.replies)
	}
	// The relay turn carries the prefixed chat id and the resolved requester.
	if f.seenTurn.ChatID != "wecom:wr_9" || f.seenTurn.RequesterUserID != "user-sub" ||
		f.seenTurn.SourceChannel != "wecom" || f.seenTurn.Text != "帮我出部门报告" {
		t.Fatalf("relay turn = %+v", f.seenTurn)
	}
}

func TestBridgeMediaForkApologyAndOutage(t *testing.T) {
	f := newBridgeFixture(t, subMapping, subordinateScopes, liveFork)

	// Media from a subordinate: text-only apology, no relay call.
	verdict := f.intercept(t, InterceptRequest{
		Platform: "feishu", PlatformSenderID: "ou_sub", ChatID: "oc_2", ChatType: "direct",
		Text: "[image]", HasMedia: true,
	})
	if verdict.Action != ActionStop || len(f.replies) != 1 || !strings.Contains(f.replies[0], "文本") {
		t.Fatalf("media = %+v %v", verdict, f.replies)
	}
	if f.seenTurn.ChatID != "" {
		t.Fatalf("media must not reach the relay: %+v", f.seenTurn)
	}

	// Control-plane outage: unavailable apology, fail closed. The warm
	// mapping cache must be bypassed so the resolver re-queries the (now
	// forbidden) control plane.
	f.mappings.forbidden.Store(true)
	f.bridge.resolver.mu.Lock()
	f.bridge.resolver.entries = nil
	f.bridge.resolver.loadedAt = time.Time{}
	f.bridge.resolver.mu.Unlock()
	verdict = f.intercept(t, InterceptRequest{
		Platform: "feishu", PlatformSenderID: "ou_sub", ChatID: "oc_3", ChatType: "direct", Text: "hi",
	})
	if verdict.Action != ActionStop || len(f.replies) != 1 || !strings.Contains(f.replies[0], "不可用") {
		t.Fatalf("outage = %+v %v", verdict, f.replies)
	}
}

func TestBridgeWardenFailureFailsClosed(t *testing.T) {
	// Scope lookup misses for the mapped requester → routing error → apology.
	scopes := map[string]string{
		"resident-1": subordinateScopes["resident-1"],
		"user-sub":   "", // explicitly expected miss
	}
	f := newBridgeFixture(t, subMapping, scopes, liveFork)

	verdict := f.intercept(t, InterceptRequest{
		Platform: "feishu", PlatformSenderID: "ou_sub", ChatID: "oc_4", Text: "hi",
	})
	if verdict.Action != ActionStop || len(f.replies) != 1 || !strings.Contains(f.replies[0], "无法应答") {
		t.Fatalf("warden failure = %+v %v", verdict, f.replies)
	}
}

func TestBridgeRelayFailureApologizes(t *testing.T) {
	// The fork points at an unreachable port: relay fails → apology.
	f := newBridgeFixture(t, subMapping, subordinateScopes, deadFork)
	verdict := f.intercept(t, InterceptRequest{
		Platform: "feishu", PlatformSenderID: "ou_sub", ChatID: "oc_5", Text: "hi",
	})
	if verdict.Action != ActionStop || len(f.replies) != 1 || !strings.Contains(f.replies[0], "无法应答") {
		t.Fatalf("relay failure = %+v %v", verdict, f.replies)
	}
}

func TestNewBridgeValidatesSettings(t *testing.T) {
	m := &aep.Manager{}
	if _, err := NewBridge(m, &config.Config{}, nil); err == nil {
		t.Fatal("nil settings must fail")
	}
	if _, err := NewBridge(m, &config.Config{}, &config.AEPChannelSettings{}); err == nil {
		t.Fatal("missing identity source must fail")
	}
}

func TestGateAcquireReleaseLifecycle(t *testing.T) {
	resetGate := func() {
		gateMu.Lock()
		if gate != nil {
			gate.refs = 0
			gate.supervisor.Stop()
			gate = nil
		}
		gateMu.Unlock()
	}
	t.Cleanup(resetGate)
	resetGate()

	fixture := newSupervisorFixture(t)
	cfg := config.DefaultConfig()
	cfg.AEP = config.AEPConfig{
		Enabled: true, BaseURL: fixture.server.URL, DeploymentID: "demo",
		Username: "helper", HomeTeamID: "dept-home", SupervisorUsername: "sup",
	}
	cfg.AEP.Password = *config.NewSecureString("agent-password-123")
	cfg.AEP.SupervisorPassword = *config.NewSecureString("sup-password-123")
	ws := &config.WardenSettings{RuntimeRoleID: "runner", PortRangeStart: fixture.healthPort}

	// The gateway boot path installs the resident session before channels.
	resident := aep.NewManager(aep.Config{
		BaseURL: fixture.server.URL, DeploymentID: "demo",
		Username: "helper", Password: "agent-password-123", SessionID: "resident",
	})
	if err := resident.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(resident.Stop)
	aep.SetDefaultManager(resident)
	t.Cleanup(func() { aep.SetDefaultManager(nil) })

	g1, err := AcquireGate(cfg, ws)
	if err != nil {
		t.Fatal(err)
	}
	g2, err := AcquireGate(cfg, ws)
	if err != nil {
		t.Fatal(err)
	}
	if g1 != g2 {
		t.Fatal("gate must be a process-wide singleton")
	}
	g1.Release()
	// Nil-gate semantics without warden settings.
	nilGate, err := AcquireGate(cfg, nil)
	if err != nil || nilGate != nil {
		t.Fatalf("nil settings gate = %v, %v", nilGate, err)
	}
	if fork, err := nilGate.Route(context.Background(), &aep.Principal{UserID: "u"}); err != nil || fork != nil {
		t.Fatalf("nil gate route = %v, %v", fork, err)
	}
	g2.Release() // last reference tears the supervisor down
	if gate != nil {
		t.Fatal("gate must be torn down after the last release")
	}
	if _, err := AcquireGate(cfg, ws); err != nil {
		t.Fatalf("re-acquire after teardown: %v", err)
	}
}
