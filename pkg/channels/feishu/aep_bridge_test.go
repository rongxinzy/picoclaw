//go:build amd64 || arm64 || riscv64 || mips64 || ppc64

package feishu

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

// aepControlPlane fakes the surfaces the resident session and the identity
// resolver touch.
func aepControlPlane(t *testing.T, mappings string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/aep/v1/auth/password/login", "/aep/v1/auth/refresh":
			_, _ = io.WriteString(w, `{"accessToken":"at","refreshToken":"rt","modelAccessToken":"mt","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"s"}`)
		case "/aep/v1/metadata":
			_, _ = io.WriteString(w, `{"service":"aep","deploymentId":"demo"}`)
		case "/aep/v1/user/models":
			_, _ = io.WriteString(w, `{"models":[]}`)
		case "/aep/v1/user/heartbeat":
			_, _ = io.WriteString(w, `{"serverTime":"2026-09-20T00:00:00Z","controlEvents":{"pending":false},"nextHeartbeatAfterSeconds":60}`)
		case "/aep/v1/admin/identity-sources/src-f/mappings":
			_, _ = io.WriteString(w, mappings)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func newAEPFeishuChannel(t *testing.T, baseURL string) (*FeishuChannel, *bus.MessageBus, *[]string) {
	t.Helper()
	manager := aep.NewManager(aep.Config{
		BaseURL: baseURL, DeploymentID: "demo",
		Username: "helper", Password: "agent-password-123", SessionID: "t",
	})
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Stop)
	aep.SetDefaultManager(manager)
	t.Cleanup(func() { aep.SetDefaultManager(nil) })

	appCfg := config.DefaultConfig()
	appCfg.AEP = config.AEPConfig{
		Enabled: true, BaseURL: baseURL, DeploymentID: "demo",
		Username: "helper", Password: *config.NewSecureString("agent-password-123"),
	}
	b := bus.NewMessageBus()
	t.Cleanup(func() { b.Close() })
	ch, err := NewFeishuChannel(
		&config.Channel{Enabled: true, Type: "feishu"},
		&config.FeishuSettings{
			AppID: "cli_app", AppSecret: *config.NewSecureString("app-secret"),
			AEP: &config.AEPChannelSettings{IdentitySourceID: "src-f"},
		},
		appCfg, b,
	)
	if err != nil {
		t.Fatalf("NewFeishuChannel: %v", err)
	}
	sent := &[]string{}
	var mu sync.Mutex
	ch.sendTextFn = func(_ context.Context, _, text string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		*sent = append(*sent, text)
		return "om_mock", nil
	}
	return ch, b, sent
}

func ptr[T any](v T) *T { return &v }

func feishuTextEvent(chatID, senderOpenID, text string) *larkim.P2MessageReceiveV1 {
	return &larkim.P2MessageReceiveV1{
		Event: &larkim.P2MessageReceiveV1Data{
			Sender: &larkim.EventSender{
				SenderId: &larkim.UserId{OpenId: ptr(senderOpenID)},
			},
			Message: &larkim.EventMessage{
				MessageId:   ptr("om_" + time.Now().Format("150405.000000000")),
				ChatId:      ptr(chatID),
				ChatType:    ptr("p2p"),
				MessageType: ptr("text"),
				Content:     ptr(`{"text":"` + text + `"}`),
			},
		},
	}
}

func TestFeishuAEPBridgeRewritesResidentIdentity(t *testing.T) {
	server := aepControlPlane(t, `{"mappings":[{"sourceId":"src-f","externalSubjectType":"user","externalId":"ou_alice","localSubjectId":"user-alice","status":"active"}],"nextCursor":null}`)
	ch, b, sent := newAEPFeishuChannel(t, server.URL)

	if err := ch.handleMessageReceive(context.Background(), feishuTextEvent("oc_1", "ou_alice", "dept report")); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-b.InboundChan():
		if msg.Context.SenderID != "user-alice" {
			t.Fatalf("sender = %q, want the resolved AEP user", msg.Context.SenderID)
		}
		if msg.Context.Raw["aep_user_id"] != "user-alice" || msg.Context.Raw["platform_sender_id"] != "ou_alice" {
			t.Fatalf("raw = %v", msg.Context.Raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resident turn was not published")
	}
	if len(*sent) != 0 {
		t.Fatalf("resident turn must not send through the bridge: %v", *sent)
	}
}

func TestFeishuAEPBridgeRejectsUnmappedSender(t *testing.T) {
	server := aepControlPlane(t, `{"mappings":[],"nextCursor":null}`)
	ch, b, sent := newAEPFeishuChannel(t, server.URL)

	if err := ch.handleMessageReceive(context.Background(), feishuTextEvent("oc_2", "ou_stranger", "hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-b.InboundChan():
		t.Fatalf("unmapped sender must not reach the agent: %+v", msg)
	case <-time.After(200 * time.Millisecond):
	}
	if len(*sent) != 1 || !strings.Contains((*sent)[0], "绑定") {
		t.Fatalf("rejection = %v", *sent)
	}
}

func TestFeishuPlatformDomainOverride(t *testing.T) {
	appCfg := config.DefaultConfig()
	settings := &config.FeishuSettings{AppID: "cli_app", AppSecret: *config.NewSecureString("s"), Domain: "http://127.0.0.1:19099"}
	ch, err := NewFeishuChannel(&config.Channel{Enabled: true, Type: "feishu"}, settings, appCfg, bus.NewMessageBus())
	if err != nil {
		t.Fatal(err)
	}
	if got := ch.platformDomain(); got != "http://127.0.0.1:19099" {
		t.Fatalf("domain = %q", got)
	}
	settings.Domain = ""
	settings.IsLark = true
	if got := ch.platformDomain(); got != "https://open.larksuite.com" && !strings.Contains(got, "larksuite") {
		t.Fatalf("lark domain = %q", got)
	}
}
