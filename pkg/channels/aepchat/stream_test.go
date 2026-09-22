package aepchat

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

// sseEvent is one parsed SSE frame from the test reader.
type sseEvent struct {
	kind string
	data string
}

type sseReader struct {
	events chan sseEvent
	closer func()
}

// openStream connects to the chat stream with the given token and cursor.
func openStream(t *testing.T, ts *httptest.Server, token, chatID string, after string) *sseReader {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/aepchat/v1/chats/"+chatID+"/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if after != "" {
		q := req.URL.Query()
		q.Set("after", after)
		req.URL.RawQuery = q.Encode()
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("stream status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		resp.Body.Close()
		t.Fatalf("content type = %q", ct)
	}
	events := make(chan sseEvent, 32)
	r := &sseReader{events: events, closer: func() { resp.Body.Close() }}
	t.Cleanup(r.closer)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		var kind string
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				kind = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				select {
				case events <- sseEvent{kind: kind, data: strings.TrimPrefix(line, "data: ")}:
				default:
				}
				kind = ""
			case line == "":
				// frame boundary
			case strings.HasPrefix(line, ":"):
				// keep-alive comment
			}
		}
	}()
	return r
}

func waitEvent(t *testing.T, r *sseReader, kind string) string {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-r.events:
			if ev.kind == kind {
				return ev.data
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s event", kind)
		}
	}
}

func newStreamChannel(t *testing.T, f *idFixture) (*Channel, *bus.MessageBus, *httptest.Server) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.AEP = config.AEPConfig{
		Enabled: true, BaseURL: f.baseURL, DeploymentID: "demo",
		Username: "helper", Password: *config.NewSecureString("agent-password-123"),
	}
	b := bus.NewMessageBus()
	t.Cleanup(func() { b.Close() })
	ch, err := New("aepchat", &config.Channel{Enabled: true, Type: "aepchat"}, &config.AEPChatSettings{
		RelaySecret: *config.NewSecureString("relay-secret-1"),
		Streaming:   config.StreamingConfig{Enabled: true},
	}, cfg, b)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ch.Stop(context.Background()) })
	ts := httptest.NewServer(ch)
	t.Cleanup(ts.Close)
	return ch, b, ts
}

func postMessage(t *testing.T, ts *httptest.Server, token, chatID, text string) int {
	t.Helper()
	body := `{"text":"` + text + `"}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/aepchat/v1/chats/"+chatID+"/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestBeginStreamDisabledByConfig(t *testing.T) {
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
	if _, err := ch.BeginStream(context.Background(), "chat-1"); err == nil {
		t.Fatal("BeginStream should fail when streaming is disabled")
	}
}

func TestStreamReplayThenLiveRecords(t *testing.T) {
	f := newIDFixture(t)
	ch, _, ts := newStreamChannel(t, f)
	token := f.token(f.userID)

	if code := postMessage(t, ts, token, "chat-1", "hello"); code != http.StatusAccepted {
		t.Fatalf("post = %d", code)
	}
	if _, err := ch.Send(context.Background(), bus.OutboundMessage{ChatID: "chat-1", Content: "hi there"}); err != nil {
		t.Fatal(err)
	}

	// Replay from zero returns both records in order.
	r := openStream(t, ts, token, "chat-1", "0")
	first := waitEvent(t, r, evKindRecord)
	if !strings.Contains(first, `"from":"user"`) || !strings.Contains(first, "hello") {
		t.Fatalf("first record = %s", first)
	}
	second := waitEvent(t, r, evKindRecord)
	if !strings.Contains(second, `"from":"agent"`) || !strings.Contains(second, "hi there") {
		t.Fatalf("second record = %s", second)
	}

	// Live phase: a later append reaches the open stream.
	go func() {
		time.Sleep(200 * time.Millisecond)
		if _, err := ch.Send(context.Background(), bus.OutboundMessage{ChatID: "chat-1", Content: "live reply"}); err != nil {
			t.Errorf("Send: %v", err)
		}
	}()
	live := waitEvent(t, r, evKindRecord)
	if !strings.Contains(live, "live reply") {
		t.Fatalf("live record = %s", live)
	}

	// Cursor filtering: after=2 replays only the live record (seq 3).
	r2 := openStream(t, ts, token, "chat-1", "2")
	only := waitEvent(t, r2, evKindRecord)
	if !strings.Contains(only, "live reply") {
		t.Fatalf("filtered replay = %s", only)
	}
	select {
	case ev := <-r2.events:
		if ev.kind == evKindRecord {
			t.Fatalf("unexpected extra record: %s", ev.data)
		}
	case <-time.After(300 * time.Millisecond):
	}
}

func TestStreamDraftsFromStreamer(t *testing.T) {
	f := newIDFixture(t)
	ch, _, ts := newStreamChannel(t, f)
	token := f.token(f.userID)
	postMessage(t, ts, token, "chat-2", "report please")

	streamer, err := ch.BeginStream(context.Background(), "chat-2")
	if err != nil {
		t.Fatal(err)
	}
	r := openStream(t, ts, token, "chat-2", "0")
	// consume the replayed user record
	waitEvent(t, r, evKindRecord)

	if err := streamer.Update(context.Background(), "生成中"); err != nil {
		t.Fatal(err)
	}
	if got := waitEvent(t, r, evKindDraft); !strings.Contains(got, "生成中") {
		t.Fatalf("draft = %s", got)
	}
	if err := streamer.Update(context.Background(), "生成中，已汇总"); err != nil {
		t.Fatal(err)
	}
	if got := waitEvent(t, r, evKindDraft); !strings.Contains(got, "已汇总") {
		t.Fatalf("draft update = %s", got)
	}

	// Finalize commits the authoritative record and clears the draft.
	if err := streamer.Finalize(context.Background(), "生成中，已汇总，完成。"); err != nil {
		t.Fatal(err)
	}
	final := waitEvent(t, r, evKindRecord)
	if !strings.Contains(final, "完成。") {
		t.Fatalf("final record = %s", final)
	}
	clear := waitEvent(t, r, evKindDraft)
	if !strings.Contains(clear, `"text":""`) {
		t.Fatalf("draft clear = %s", clear)
	}

	// The record is queryable through the polling GET too.
	ch.mu.Lock()
	records := len(ch.history["chat-2"])
	ch.mu.Unlock()
	if records != 2 {
		t.Fatalf("history records = %d", records)
	}
}

func TestStreamFinalizeWakesRelayWaiter(t *testing.T) {
	f := newIDFixture(t)
	ch, _, _ := newStreamChannel(t, f)

	waiter := ch.registerRelayWaiter("feishu:oc_sse")
	if waiter == nil {
		t.Fatal("relay waiter rejected")
	}
	streamer, err := ch.BeginStream(context.Background(), "feishu:oc_sse")
	if err != nil {
		t.Fatal(err)
	}
	var got string
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case got = <-waiter:
		case <-time.After(2 * time.Second):
		}
	}()
	if err := streamer.Finalize(context.Background(), "relay final"); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if got != "relay final" {
		t.Fatalf("relay waiter got %q", got)
	}
}

func TestStreamAuthAndParticipation(t *testing.T) {
	f := newIDFixture(t)
	_, _, ts := newStreamChannel(t, f)

	// Missing token.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/aepchat/v1/chats/chat-3/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token = %d", resp.StatusCode)
	}

	// Non-participant: a different human who never wrote to the chat. The
	// chat first needs a user record from user-1; the fixture then serves
	// the stranger identity (/user/me must match the token sub).
	if code := postMessage(t, ts, f.token(f.userID), "chat-3", "seed"); code != http.StatusAccepted {
		t.Fatalf("seed post = %d", code)
	}
	f.userID, f.kind = "user-stranger", "human"
	t.Cleanup(func() { f.userID, f.kind = "user-1", "human" })
	token := f.token("user-stranger")
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/aepchat/v1/chats/chat-3/stream", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("stranger = %d", resp.StatusCode)
	}

	// Wrong method (back to the participant identity).
	f.userID, f.kind = "user-1", "human"
	token = f.token(f.userID)
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/aepchat/v1/chats/chat-3/stream", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST stream = %d", resp.StatusCode)
	}
}
