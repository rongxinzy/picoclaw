package openai_compat

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestProviderChat_TokenSourceOverridesStaticKey(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": "ok"}, "finish_reason": "stop"}},
		})
	}))
	defer server.Close()

	p := NewProvider("static-key", server.URL, "",
		WithTokenSource(func(_ context.Context) (string, error) { return "dynamic-token", nil }))
	_, err := p.Chat(
		t.Context(),
		[]Message{{Role: "user", Content: "hi"}},
		nil,
		"test-model",
		nil,
	)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if gotAuth != "Bearer dynamic-token" {
		t.Fatalf("authorization = %q, want rotating session token", gotAuth)
	}
}

func TestProviderChat_TokenSourceErrorFailsRequest(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	p := NewProvider("static-key", server.URL, "",
		WithTokenSource(func(_ context.Context) (string, error) { return "", errors.New("session expired") }))
	_, err := p.Chat(
		t.Context(),
		[]Message{{Role: "user", Content: "hi"}},
		nil,
		"test-model",
		nil,
	)
	if err == nil {
		t.Fatal("expected error when the token source fails")
	}
	if hits.Load() != 0 {
		t.Fatalf("request was sent despite token error (%d hits)", hits.Load())
	}
}

func TestProviderChatStream_TokenSourceApplied(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	p := NewProvider("", server.URL, "",
		WithTokenSource(func(_ context.Context) (string, error) { return "stream-token", nil }))
	_, err := p.ChatStream(
		t.Context(),
		[]Message{{Role: "user", Content: "hi"}},
		nil,
		"test-model",
		nil,
		func(accumulated string) {},
	)
	if err != nil {
		t.Fatalf("chat stream: %v", err)
	}
	if gotAuth != "Bearer stream-token" {
		t.Fatalf("authorization = %q, want session token on stream requests", gotAuth)
	}
}
