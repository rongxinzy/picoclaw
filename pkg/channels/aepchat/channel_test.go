package aepchat

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

// idFixture serves JWKS and /user/me for one signing key with switchable
// identity (human or agent).
type idFixture struct {
	server  *httptest.Server
	priv    ed25519.PrivateKey
	kid     string
	kind    string
	userID  string
	baseURL string
}

func newIDFixture(t *testing.T) *idFixture {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f := &idFixture{priv: priv, kid: "k1", kind: "human", userID: "user-1"}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/jwks.json":
			pub := f.priv.Public().(ed25519.PublicKey)
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
				"kty": "OKP", "crv": "Ed25519", "use": "sig", "alg": "EdDSA", "kid": f.kid,
				"x": base64.RawURLEncoding.EncodeToString(pub),
			}}})
		case "/aep/v1/user/me":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"user":         map[string]any{"id": f.userID, "displayName": "Zhang San", "kind": f.kind},
				"deploymentId": "demo",
				"roles":        []string{"employee"},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	f.baseURL = f.server.URL
	t.Cleanup(f.server.Close)
	return f
}

func (f *idFixture) token(sub string) string {
	claims := jwt.MapClaims{
		"deployment_id": "demo", "session_id": "s1", "token_use": "aep",
		"iss": f.baseURL, "sub": sub, "aud": []string{"aep-control"},
		"exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Add(-time.Minute).Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	tok.Header["kid"] = f.kid
	signed, err := tok.SignedString(f.priv)
	if err != nil {
		panic(err)
	}
	return signed
}

func newTestChannel(t *testing.T, f *idFixture) (*Channel, *bus.MessageBus) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.AEP = config.AEPConfig{
		Enabled: true, BaseURL: f.baseURL, DeploymentID: "demo",
		Username: "helper", Password: *config.NewSecureString("agent-password-123"),
	}
	b := bus.NewMessageBus()
	t.Cleanup(func() { b.Close() })
	ch, err := New("aepchat", &config.Channel{Enabled: true, Type: "aepchat"}, &config.AEPChatSettings{}, cfg, b)
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("start channel: %v", err)
	}
	return ch, b
}

func post(t *testing.T, url, token, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestPostRequiresValidHumanToken(t *testing.T) {
	f := newIDFixture(t)
	ch, _ := newTestChannel(t, f)
	ts := httptest.NewServer(ch)
	defer ts.Close()
	url := ts.URL + "/aepchat/v1/chats/c1/messages"

	if resp := post(t, url, "", `{"text":"hi"}`); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token = %d, want 401", resp.StatusCode)
	}
	if resp := post(t, url, "garbage", `{"text":"hi"}`); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("garbage token = %d, want 401", resp.StatusCode)
	}

	f.kind, f.userID = "agent", "agent-1"
	if resp := post(t, url, f.token("agent-1"), `{"text":"hi"}`); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("agent token = %d, want 403", resp.StatusCode)
	}

	f.kind, f.userID = "human", "user-1"
	resp := post(t, url, f.token("user-1"), `{"text":"hi"}`)
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("human token = %d %s, want 202", resp.StatusCode, body)
	}
}

func TestPostPublishesInboundWithEnterpriseIdentity(t *testing.T) {
	f := newIDFixture(t)
	ch, b := newTestChannel(t, f)
	ts := httptest.NewServer(ch)
	defer ts.Close()

	resp := post(t, ts.URL+"/aepchat/v1/chats/chat-9/messages", f.token("user-1"), `{"text":"hello agent","chatType":"group"}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("post = %d", resp.StatusCode)
	}

	select {
	case msg := <-b.InboundChan():
		if msg.Context.ChatID != "chat-9" || msg.Context.ChatType != "group" {
			t.Fatalf("context = %+v", msg.Context)
		}
		if msg.Sender.Platform != "aepchat" || msg.Sender.PlatformID != "user-1" || msg.Sender.DisplayName != "Zhang San" {
			t.Fatalf("sender = %+v", msg.Sender)
		}
		if msg.Context.SenderID != "user-1" || msg.Context.Raw["aep_user_id"] != "user-1" {
			t.Fatalf("identity raw = %+v", msg.Context.Raw)
		}
		if msg.Content != "hello agent" {
			t.Fatalf("content = %q", msg.Content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inbound message was not published")
	}
}

func TestPollReturnsAgentRepliesAndGuardsParticipants(t *testing.T) {
	f := newIDFixture(t)
	ch, _ := newTestChannel(t, f)
	ts := httptest.NewServer(ch)
	defer ts.Close()
	chatURL := ts.URL + "/aepchat/v1/chats/chat-1/messages"

	if resp := post(t, chatURL, f.token("user-1"), `{"text":"hi"}`); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("post = %d", resp.StatusCode)
	}
	// Simulate the agent reply through the channel Send path.
	if _, err := ch.Send(context.Background(), bus.OutboundMessage{ChatID: "chat-1", Content: "Hello AEP"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	get := func(token, after string) (int, string) {
		req, _ := http.NewRequest(http.MethodGet, chatURL+after, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	status, body := get(f.token("user-1"), "")
	if status != http.StatusOK || !strings.Contains(body, `"from":"agent"`) || !strings.Contains(body, "Hello AEP") || !strings.Contains(body, `"from":"user"`) {
		t.Fatalf("poll = %d %s", status, body)
	}
	status, body = get(f.token("user-1"), "?after=2")
	if status != http.StatusOK || strings.Contains(body, "Hello AEP") {
		t.Fatalf("after cursor should skip old records: %d %s", status, body)
	}

	// A different human never wrote to this chat and cannot read it.
	f.userID, f.kind = "user-2", "human"
	status, _ = get(f.token("user-2"), "")
	if status != http.StatusForbidden {
		t.Fatalf("non-participant poll = %d, want 403", status)
	}
}

func TestHealthRoute(t *testing.T) {
	f := newIDFixture(t)
	ch, _ := newTestChannel(t, f)
	ts := httptest.NewServer(ch)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/aepchat/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health = %d", resp.StatusCode)
	}
}
