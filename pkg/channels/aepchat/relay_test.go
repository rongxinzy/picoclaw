package aepchat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

func newRelayChannel(t *testing.T) (*Channel, *bus.MessageBus, *httptest.Server) {
	t.Helper()
	f := newIDFixture(t)
	cfg := config.DefaultConfig()
	cfg.AEP = config.AEPConfig{
		Enabled: true, BaseURL: f.baseURL, DeploymentID: "demo",
		Username: "helper", Password: *config.NewSecureString("agent-password-123"),
	}
	b := bus.NewMessageBus()
	t.Cleanup(func() { b.Close() })
	ch, err := New("aepchat", &config.Channel{Enabled: true, Type: "aepchat"}, &config.AEPChatSettings{
		RelaySecret: *config.NewSecureString("relay-secret-1"),
	}, cfg, b)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ch.Stop(context.Background()) })
	ch.relayTimeout = 300 * time.Millisecond
	ts := httptest.NewServer(ch)
	t.Cleanup(ts.Close)
	return ch, b, ts
}

func relayPost(t *testing.T, url, secret, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url+"/aepchat/v1/relay/turns", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func finalOutbound(chatID, content string) bus.OutboundMessage {
	return bus.OutboundMessage{
		ChatID:  chatID,
		Content: content,
		Context: bus.InboundContext{Raw: map[string]string{"outbound_kind": "final"}},
	}
}

func TestRelayTurnRejectsDisabledAndUnauthorized(t *testing.T) {
	f := newIDFixture(t)
	cfg := config.DefaultConfig()
	cfg.AEP = config.AEPConfig{
		Enabled: true, BaseURL: f.baseURL, DeploymentID: "demo",
		Username: "helper", Password: *config.NewSecureString("agent-password-123"),
	}
	b := bus.NewMessageBus()
	t.Cleanup(func() { b.Close() })
	ch, err := New("aepchat", &config.Channel{Enabled: true, Type: "aepchat"}, &config.AEPChatSettings{}, cfg, b)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	disabled := httptest.NewServer(ch)
	defer disabled.Close()
	status, body := relayPost(t, disabled.URL, "any", `{"chatID":"c","requesterUserID":"u","text":"hi"}`)
	if status != http.StatusServiceUnavailable || body["code"] != "RELAY_DISABLED" {
		t.Fatalf("disabled = %d %v", status, body)
	}

	_, bus2, authorized := newRelayChannel(t)
	_ = bus2
	status, body = relayPost(t, authorized.URL, "wrong-secret", `{"chatID":"c","requesterUserID":"u","text":"hi"}`)
	if status != http.StatusUnauthorized || body["code"] != "RELAY_UNAUTHORIZED" {
		t.Fatalf("unauthorized = %d %v", status, body)
	}
	status, body = relayPost(t, authorized.URL, "relay-secret-1", `{"chatID":"","requesterUserID":"u","text":"hi"}`)
	if status != http.StatusBadRequest || body["code"] != "INVALID_REQUEST" {
		t.Fatalf("invalid = %d %v", status, body)
	}
}

func TestRelayTurnReturnsFinalReply(t *testing.T) {
	ch, b, ts := newRelayChannel(t)

	go func() {
		msg := <-b.InboundChan()
		if got := msg.Context.SenderID; got != "user-sub" {
			t.Errorf("relay sender = %q", got)
		}
		if msg.Context.Raw["relay"] != "1" || msg.Context.Raw["aep_user_id"] != "user-sub" || msg.Context.Raw["relay_source"] != "feishu" {
			t.Errorf("relay raw = %v", msg.Context.Raw)
		}
		if _, err := ch.Send(context.Background(), finalOutbound(msg.Context.ChatID, "scoped answer")); err != nil {
			t.Errorf("Send: %v", err)
		}
	}()

	status, body := relayPost(t, ts.URL, "relay-secret-1",
		`{"chatID":"feishu:oc_1","chatType":"direct","requesterUserID":"user-sub","requesterDisplayName":"Subordinate","text":"dept report","sourceChannel":"feishu"}`)
	if status != http.StatusAccepted || body["reply"] != "scoped answer" {
		t.Fatalf("relay = %d %v", status, body)
	}
}

func TestRelayTurnFIFOPerChat(t *testing.T) {
	ch, b, ts := newRelayChannel(t)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2; i++ {
			msg := <-b.InboundChan()
			// Finals complete in the turns' arrival order (session
			// serialization); each waiter must see its own turn's reply.
			if _, err := ch.Send(context.Background(), finalOutbound(msg.Context.ChatID, msg.Content)); err != nil {
				t.Errorf("Send: %v", err)
			}
		}
	}()

	var first, second map[string]any
	var mu sync.Mutex
	var calls sync.WaitGroup
	calls.Add(2)
	go func() {
		defer calls.Done()
		status, body := relayPost(t, ts.URL, "relay-secret-1",
			`{"chatID":"feishu:oc_1","requesterUserID":"user-sub","text":"turn-one"}`)
		if status != http.StatusAccepted {
			t.Errorf("first = %d", status)
		}
		mu.Lock()
		first = body
		mu.Unlock()
	}()
	go func() {
		defer calls.Done()
		status, body := relayPost(t, ts.URL, "relay-secret-1",
			`{"chatID":"feishu:oc_1","requesterUserID":"user-sub","text":"turn-two"}`)
		if status != http.StatusAccepted {
			t.Errorf("second = %d", status)
		}
		mu.Lock()
		second = body
		mu.Unlock()
	}()
	calls.Wait()
	wg.Wait()

	// The replies may interleave in arrival, but each requester text maps to
	// its own reply: reply content equals the turn's text.
	if first["reply"] != "turn-one" && first["reply"] != "turn-two" {
		t.Fatalf("first reply = %v", first)
	}
	if first["reply"] == second["reply"] {
		t.Fatalf("waiters got the same reply: %v / %v", first, second)
	}
}

func TestRelayTurnTimeoutSkipsStaleFinal(t *testing.T) {
	ch, b, ts := newRelayChannel(t)

	// Turn one times out: no final is produced within the (shortened) relay
	// timeout, and the request context is not cancelled early.
	status, body := relayPost(t, ts.URL, "relay-secret-1",
		`{"chatID":"feishu:oc_2","requesterUserID":"user-sub","text":"doomed"}`)
	if status != http.StatusGatewayTimeout || body["code"] != "RELAY_TURN_TIMEOUT" {
		t.Fatalf("timeout = %d %v", status, body)
	}
	// Drain the published inbound.
	select {
	case <-b.InboundChan():
	default:
		t.Fatal("expected the doomed turn to be published")
	}
	// The abandoned turn's final arrives late and must be discarded, not
	// delivered to the next waiter.
	if _, err := ch.Send(context.Background(), finalOutbound("feishu:oc_2", "late")); err != nil {
		t.Fatal(err)
	}
	if got := ch.relaySkips["feishu:oc_2"]; got != 0 {
		t.Fatalf("skip consumed before a waiter exists: %d", got)
	}

	// A fresh relay turn still receives exactly its own reply.
	done := make(chan struct{})
	go func() {
		defer close(done)
		msg := <-b.InboundChan()
		if _, err := ch.Send(context.Background(), finalOutbound(msg.Context.ChatID, "fresh answer")); err != nil {
			t.Errorf("Send: %v", err)
		}
	}()
	status, body = relayPost(t, ts.URL, "relay-secret-1",
		`{"chatID":"feishu:oc_2","requesterUserID":"user-sub","text":"fresh"}`)
	if status != http.StatusAccepted || body["reply"] != "fresh answer" {
		t.Fatalf("fresh = %d %v", status, body)
	}
	<-done
}

func TestRelayTurnBusyCap(t *testing.T) {
	ch, _, ts := newRelayChannel(t)
	// Saturate the waiter queue directly (white-box): 16 pending turns.
	for i := 0; i < 16; i++ {
		if c := ch.registerRelayWaiter("feishu:oc_3"); c == nil {
			t.Fatalf("waiter %d rejected early", i)
		}
	}
	status, body := relayPost(t, ts.URL, "relay-secret-1",
		`{"chatID":"feishu:oc_3","requesterUserID":"user-sub","text":"overflow"}`)
	if status != http.StatusServiceUnavailable || body["code"] != "RELAY_BUSY" {
		t.Fatalf("busy = %d %v", status, body)
	}
}
