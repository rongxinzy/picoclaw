package a2a

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

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

func newTestA2A(t *testing.T) (*Channel, *bus.MessageBus) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.AEP = config.AEPConfig{
		Enabled: true, BaseURL: "http://127.0.0.1:1", DeploymentID: "demo",
		Username: "resident-1",
	}
	cfg.AEP.Password = *config.NewSecureString("agent-password-123")
	b := bus.NewMessageBus()
	t.Cleanup(func() { b.Close() })
	ch, err := New("a2a", &config.Channel{Enabled: true, Type: "a2a"}, &config.A2ASettings{PublicURL: "http://peer:18800"}, cfg, b)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ch.Stop(context.Background()) })
	return ch, b
}

func TestCardServing(t *testing.T) {
	ch, _ := newTestA2A(t)
	if ch.WebhookPath() != "/a2a/" || ch.HealthPath() != "/.well-known/agent-card.json" {
		t.Fatalf("paths = %s / %s", ch.WebhookPath(), ch.HealthPath())
	}
	// Mount like the channel manager does: the card at the well-known path,
	// JSON-RPC under the /a2a/ subtree.
	mux := http.NewServeMux()
	mux.Handle(ch.HealthPath(), http.HandlerFunc(ch.HealthHandler))
	mux.Handle(ch.WebhookPath(), ch)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/.well-known/agent-card.json")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("card = %v %v", err, resp)
	}
	var card AgentCard
	_ = json.NewDecoder(resp.Body).Decode(&card)
	if card.Name != "resident-1" || card.URL != "http://peer:18800" || len(card.Authentication.Schemes) != 1 {
		t.Fatalf("card = %+v", card)
	}
	// The card mirror on the subtree behaves the same.
	mirror, err := http.Get(ts.URL + "/a2a/card.json")
	if err != nil || mirror.StatusCode != http.StatusOK {
		t.Fatalf("mirror = %v %v", err, mirror)
	}
}

func TestServeRPCRejectsBadRequestsAndUnknownMethods(t *testing.T) {
	ch, _ := newTestA2A(t)
	ts := httptest.NewServer(ch)
	defer ts.Close()

	resp, body := rpcPost(t, ts.URL+"/a2a/", "garbage", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "-32600") {
		t.Fatalf("garbage = %d %s", resp.StatusCode, body)
	}
	// Authentication gates every RPC method, known or not.
	resp, body = rpcPost(t, ts.URL+"/a2a/", `{"jsonrpc":"2.0","id":1,"method":"bogus"}`, "tok")
	if !strings.Contains(body, "-32001") {
		t.Fatalf("auth gate = %s", body)
	}
	// Missing bearer token.
	resp, body = rpcPost(t, ts.URL+"/a2a/", `{"jsonrpc":"2.0","id":1,"method":"tasks/get","params":{"taskId":"x"}}`, "")
	if !strings.Contains(body, "-32001") {
		t.Fatalf("no token = %s", body)
	}
	// Unknown sub-route answers with a JSON-RPC error.
	if resp, _ := http.Get(ts.URL + "/a2a/other"); resp.StatusCode != http.StatusOK {
		t.Fatalf("other route status = %d", resp.StatusCode)
	}
}

func TestHandleSendEnforcesActAsAndChain(t *testing.T) {
	ch, _ := newTestA2A(t)

	rec := &recordWriter{}
	// No act-as → rejected.
	ch.handleSend(rec, json.RawMessage("1"), &aep.PeerPrincipal{UserID: "agent-1"}, Message{
		Role: "user", Parts: []Part{{Kind: "text", Text: "hi"}},
	})
	if !strings.Contains(rec.body, "-32003") {
		t.Fatalf("missing act-as = %s", rec.body)
	}
	// No text part → rejected.
	ch.handleSend(rec, json.RawMessage("1"), &aep.PeerPrincipal{UserID: "agent-1"}, Message{
		Role: "user", Metadata: map[string]string{MetadataActAs: "u1"},
	})
	if !strings.Contains(rec.body, "-32602") {
		t.Fatalf("empty text = %s", rec.body)
	}
	// Chain too deep → rejected.
	ch.handleSend(rec, json.RawMessage("1"), &aep.PeerPrincipal{UserID: "agent-1"}, Message{
		Role:  "user",
		Parts: []Part{{Kind: "text", Text: "hi"}},
		Metadata: map[string]string{
			MetadataActAs: "u1", MetadataChain: "a>b>c",
		},
	})
	if !strings.Contains(rec.body, "-32004") {
		t.Fatalf("chain depth = %s", rec.body)
	}
}

func TestHandleSendCreatesTaskAndCompletesViaSend(t *testing.T) {
	ch, b := newTestA2A(t)

	var seenInbound bool
	done := make(chan struct{})
	go func() {
		for msg := range b.InboundChan() {
			seenInbound = true
			if msg.Context.Channel != "a2a" || msg.Context.SenderID != "user-9" {
				t.Errorf("inbound context = %+v", msg.Context)
			}
			if msg.Context.Raw["a2a_caller"] != "agent-1" {
				t.Errorf("chain metadata = %+v", msg.Context.Raw)
			}
			close(done)
			return
		}
	}()

	rec := &recordWriter{}
	ch.handleSend(rec, json.RawMessage("1"), &aep.PeerPrincipal{UserID: "agent-1"}, Message{
		Role:  "user",
		Parts: []Part{{Kind: "text", Text: "please report"}},
		Metadata: map[string]string{
			MetadataActAs: "user-9", MetadataCaller: "agent-1", MetadataChain: "agent-0",
		},
	})
	var task Task
	if err := json.Unmarshal([]byte(rec.body[strings.Index(rec.body, `"result"`):]), &task); err != nil {
		// decode from full envelope instead
		var envelope struct {
			Result Task `json:"result"`
		}
		if err2 := json.Unmarshal([]byte(rec.body), &envelope); err2 != nil {
			t.Fatalf("send response = %s (%v)", rec.body, err2)
		}
		task = envelope.Result
	}
	if task.ID == "" || task.State != TaskStateWorking || task.ActAs != "user-9" {
		t.Fatalf("task = %+v", task)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("inbound message was not published")
	}
	if !seenInbound {
		t.Fatal("inbound flag")
	}

	// The outbound reply completes the task.
	if _, err := ch.Send(context.Background(), bus.OutboundMessage{ChatID: task.ID, Content: "A2A_REPLY"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	finished, err := ch.WaitForTask(task.ID, time.Second)
	if err != nil || finished.State != TaskStateCompleted {
		t.Fatalf("wait = %+v %v", finished, err)
	}
	if len(finished.Artifacts) != 1 || finished.Artifacts[0].Text != "A2A_REPLY" {
		t.Fatalf("artifacts = %+v", finished.Artifacts)
	}

	// tasks/get returns the snapshot; unknown ids fail.
	rec2 := &recordWriter{}
	ch.handleGet(rec2, json.RawMessage("2"), task.ID)
	if !strings.Contains(rec2.body, "completed") {
		t.Fatalf("get = %s", rec2.body)
	}
	ch.handleGet(rec2, json.RawMessage("3"), "missing")
	if !strings.Contains(rec2.body, "-32005") {
		t.Fatalf("missing task = %s", rec2.body)
	}
}

func TestSendEdgeCasesAndStop(t *testing.T) {
	ch, _ := newTestA2A(t)
	if _, err := ch.Send(context.Background(), bus.OutboundMessage{ChatID: "nope", Content: "x"}); err != nil {
		t.Fatalf("unknown chat send = %v", err)
	}
	if _, err := ch.Send(context.Background(), bus.OutboundMessage{ChatID: "", Content: "x"}); err != nil {
		t.Fatalf("empty chat = %v", err)
	}
	if _, err := ch.WaitForTask("ghost", time.Millisecond); err == nil {
		t.Fatal("unknown task wait must fail")
	}
	// A task created then Stop fails it and wakes the waiter.
	rec := &recordWriter{}
	ch.handleSend(rec, json.RawMessage("1"), &aep.PeerPrincipal{UserID: "a"}, Message{
		Role: "user", Parts: []Part{{Kind: "text", Text: "x"}},
		Metadata: map[string]string{MetadataActAs: "u"},
	})
	var envelope struct {
		Result Task `json:"result"`
	}
	_ = json.Unmarshal([]byte(rec.body), &envelope)
	go func() {
		task, err := ch.WaitForTask(envelope.Result.ID, 2*time.Second)
		if err != nil || task.State != TaskStateFailed {
			t.Errorf("stop-failed wait = %+v %v", task, err)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	if err := ch.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
}

func TestParseChainAndMessageText(t *testing.T) {
	if got := parseChain(" a > b > > c "); len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Fatalf("parseChain = %v", got)
	}
	if got := parseChain(""); got != nil {
		t.Fatalf("empty chain = %v", got)
	}
	if messageText(Message{}) != "" {
		t.Fatal("empty message text")
	}
	if messageText(Message{Parts: []Part{{Kind: "text", Text: "  hi  "}}}) != "hi" {
		t.Fatal("text extraction")
	}
}

func TestNewRejectsIncompleteConfig(t *testing.T) {
	if _, err := New("a2a", &config.Channel{}, nil, config.DefaultConfig(), bus.NewMessageBus()); err == nil {
		t.Fatal("incomplete aep config must fail")
	}
}

func TestClientInvokeAndGet(t *testing.T) {
	var mu sync.Mutex
	var lastAuth, lastBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		lastAuth, lastBody = r.Header.Get("Authorization"), string(body)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":"1","result":{"id":"task-7","state":"completed","aep_act_as":"u1","aep_caller":"a1","artifacts":[{"kind":"text","text":"done"}]}}`)
	}))
	defer server.Close()

	client := NewClient()
	task, err := client.Invoke(context.Background(), InvokeOptions{
		BaseURL: server.URL, Token: "tok", Caller: "a1", ActAs: "u1", Chain: []string{"a0"},
	}, "hello")
	if err != nil || task.ID != "task-7" || task.ActAs != "u1" {
		t.Fatalf("invoke = %+v %v", task, err)
	}
	if lastAuth != "Bearer tok" || !strings.Contains(lastBody, `"aep_act_as_user":"u1"`) || !strings.Contains(lastBody, "a0") || !strings.Contains(lastBody, "a1") {
		t.Fatalf("request auth/body = %s / %s", lastAuth, lastBody)
	}
	snapshot, err := client.GetTask(context.Background(), server.URL, "tok", "task-7")
	if err != nil || snapshot.State != TaskStateCompleted {
		t.Fatalf("get = %+v %v", snapshot, err)
	}

	// Server-side rejection surfaces as an error with the code.
	rejecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":"1","error":{"code":-32004,"message":"chain"}}`)
	}))
	defer rejecting.Close()
	if _, err := client.Invoke(context.Background(), InvokeOptions{BaseURL: rejecting.URL, Token: "t", ActAs: "u"}, "x"); err == nil || !strings.Contains(err.Error(), "-32004") {
		t.Fatalf("rejection = %v", err)
	}
}

func TestClientTransportAndDecodeFailures(t *testing.T) {
	client := NewClient()
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := dead.URL
	dead.Close()
	if _, err := client.Invoke(context.Background(), InvokeOptions{BaseURL: url, Token: "t", ActAs: "u"}, "x"); err == nil {
		t.Fatal("expected transport error")
	}
	badJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `nope`)
	}))
	defer badJSON.Close()
	if _, err := client.GetTask(context.Background(), badJSON.URL, "t", "x"); err == nil {
		t.Fatal("expected decode error")
	}
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":"1"}`)
	}))
	defer empty.Close()
	if _, err := client.GetTask(context.Background(), empty.URL, "t", "x"); err == nil {
		t.Fatal("expected missing result error")
	}
	non200 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer non200.Close()
	if _, err := client.GetTask(context.Background(), non200.URL, "t", "x"); err == nil || !strings.Contains(err.Error(), "502") {
		t.Fatalf("non-200 = %v", err)
	}
}

type recordWriter struct {
	body string
}

func (r *recordWriter) Header() http.Header { return http.Header{} }
func (r *recordWriter) Write(b []byte) (int, error) {
	r.body += string(b)
	return len(b), nil
}
func (r *recordWriter) WriteHeader(int) {}

func rpcPost(t *testing.T, url, body, token string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, string(raw)
}

func TestIsRunningAndInvokeAndWait(t *testing.T) {
	ch, _ := newTestA2A(t)
	if !ch.IsRunning() {
		t.Fatal("must report running after Start")
	}

	// InvokeAndWait polls until the task is terminal.
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.RequestURI(), "message/send") || callCount == 1 {
			_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":"1","result":{"id":"task-8","state":"working"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":"1","result":{"id":"task-8","state":"completed","artifacts":[{"kind":"text","text":"ok"}]}}`)
	}))
	defer server.Close()
	task, err := NewClient().InvokeAndWait(context.Background(), InvokeOptions{
		BaseURL: server.URL, Token: "t", ActAs: "u1", Caller: "a1",
	}, "go", 5*time.Second)
	if err != nil || task.State != TaskStateCompleted || task.Artifacts[0].Text != "ok" {
		t.Fatalf("invokeAndWait = %+v %v", task, err)
	}
	if callCount < 2 {
		t.Fatalf("expected polling, calls = %d", callCount)
	}
}

func TestFireTelemetryWithoutManager(t *testing.T) {
	// No default manager registered: fire is a no-op that must not block.
	// aep.DefaultManager is nil in this test binary.
	newTelemetry("http://127.0.0.1:1", "demo").fire("agent.invoke", "received", map[string]string{"task_id": "x"})
}
