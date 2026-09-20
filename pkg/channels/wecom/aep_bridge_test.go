package wecom

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

func wecomAEPControlPlane(t *testing.T, mappings string) *httptest.Server {
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
		case "/aep/v1/admin/identity-sources/src-w/mappings":
			_, _ = io.WriteString(w, mappings)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func newAEPWeComChannel(t *testing.T, baseURL string, messageBus *bus.MessageBus) (*WeComChannel, *[]wecomCommand) {
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
	cfg := &config.WeComSettings{BotID: "bot-1"}
	cfg.SetSecret("secret-1")
	cfg.AEP = &config.AEPChannelSettings{IdentitySourceID: "src-w"}
	ch, err := NewChannel(&config.Channel{Type: config.ChannelWeCom, Enabled: true}, cfg, appCfg, messageBus)
	if err != nil {
		t.Fatalf("NewChannel: %v", err)
	}
	ch.ctx = context.Background()
	ch.SetRunning(true)
	commands := &[]wecomCommand{}
	ch.commandSend = func(cmd wecomCommand, _ time.Duration) (wecomEnvelope, error) {
		*commands = append(*commands, cmd)
		return wecomTestAck(nil), nil
	}
	return ch, commands
}

func wecomTextMsg(msgID, chatID, userID, text string) wecomIncomingMessage {
	msg := wecomIncomingMessage{
		MsgID:    msgID,
		ChatID:   chatID,
		ChatType: "single",
		MsgType:  "text",
		Text: &struct {
			Content string `json:"content"`
		}{Content: text},
	}
	msg.From.UserID = userID
	return msg
}

func TestWeComAEPBridgeRewritesResidentIdentity(t *testing.T) {
	server := wecomAEPControlPlane(t, `{"mappings":[{"sourceId":"src-w","externalSubjectType":"user","externalId":"wk_alice","localSubjectId":"user-alice","status":"active"}],"nextCursor":null}`)
	messageBus := bus.NewMessageBus()
	t.Cleanup(func() { messageBus.Close() })
	ch, commands := newAEPWeComChannel(t, server.URL, messageBus)

	if err := ch.dispatchIncoming("req-1", wecomTextMsg("msg-1", "chat-1", "wk_alice", "部门报告")); err != nil {
		t.Fatal(err)
	}
	select {
	case inbound := <-messageBus.InboundChan():
		if inbound.Context.SenderID != "user-alice" {
			t.Fatalf("sender = %q, want resolved AEP user", inbound.Context.SenderID)
		}
		if inbound.Context.Raw["aep_user_id"] != "user-alice" || inbound.Context.Raw["platform_sender_id"] != "wk_alice" {
			t.Fatalf("raw = %v", inbound.Context.Raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resident turn was not published")
	}
	// Only the opening stream chunk ran; the bridge sent nothing.
	if len(*commands) != 1 || (*commands)[0].Cmd != wecomCmdRespondMsg {
		t.Fatalf("commands = %+v", *commands)
	}
}

func TestWeComAEPBridgeRejectsUnmappedThroughStreamReply(t *testing.T) {
	server := wecomAEPControlPlane(t, `{"mappings":[],"nextCursor":null}`)
	messageBus := bus.NewMessageBus()
	t.Cleanup(func() { messageBus.Close() })
	ch, commands := newAEPWeComChannel(t, server.URL, messageBus)

	if err := ch.dispatchIncoming("req-2", wecomTextMsg("msg-2", "chat-2", "wk_stranger", "hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case inbound := <-messageBus.InboundChan():
		t.Fatalf("unmapped sender must not reach the agent: %+v", inbound)
	case <-time.After(200 * time.Millisecond):
	}
	// Opening chunk + the rejection consuming the queued turn.
	if len(*commands) != 2 {
		t.Fatalf("commands = %d, want 2: %+v", len(*commands), *commands)
	}
	rejection := (*commands)[1]
	if rejection.Cmd != wecomCmdRespondMsg {
		t.Fatalf("rejection command = %+v", rejection)
	}
	encoded, err := json.Marshal(rejection.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), "绑定") {
		t.Fatalf("rejection body = %s", encoded)
	}
}
