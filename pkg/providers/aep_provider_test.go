package providers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
)

func TestCreateAEPProviderRequiresRunningSession(t *testing.T) {
	SetAEPTokenSource(nil)
	_, _, err := CreateProviderFromConfig(&config.ModelConfig{
		ModelName: "qwen",
		Provider:  "aep",
		Model:     "qwen3-32b",
		APIBase:   "http://127.0.0.1:1/v1",
	})
	if err == nil || !strings.Contains(err.Error(), "AEP session") {
		t.Fatalf("expected session-required error, got %v", err)
	}
}

func TestCreateAEPProviderUsesSessionTokenSource(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": "ok"}, "finish_reason": "stop"}},
		})
	}))
	defer server.Close()

	SetAEPTokenSource(func(ctx context.Context) (string, error) { return "mt-live", nil })
	t.Cleanup(func() { SetAEPTokenSource(nil) })

	provider, modelID, err := CreateProviderFromConfig(&config.ModelConfig{
		ModelName: "qwen",
		Provider:  "aep",
		Model:     "qwen3-32b",
		APIBase:   server.URL,
	})
	if err != nil {
		t.Fatalf("create provider: %v", err)
	}
	if modelID != "qwen3-32b" {
		t.Fatalf("modelID = %q, want the bare upstream id", modelID)
	}
	httpProvider, ok := provider.(*HTTPProvider)
	if !ok {
		t.Fatalf("provider type = %T, want *HTTPProvider", provider)
	}
	resp, err := httpProvider.Chat(
		t.Context(),
		[]Message{{Role: "user", Content: "hi"}},
		nil,
		modelID,
		nil,
	)
	if err != nil {
		t.Fatalf("chat through aep provider: %v", err)
	}
	if gotAuth != "Bearer mt-live" {
		t.Fatalf("authorization = %q, want the session model token", gotAuth)
	}
	if resp == nil || resp.Content != "ok" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestCreateAEPProviderRequiresAPIBase(t *testing.T) {
	SetAEPTokenSource(func(ctx context.Context) (string, error) { return "mt-live", nil })
	t.Cleanup(func() { SetAEPTokenSource(nil) })

	_, _, err := CreateProviderFromConfig(&config.ModelConfig{
		ModelName: "qwen",
		Provider:  "aep",
		Model:     "qwen3-32b",
	})
	if err == nil || !strings.Contains(err.Error(), "api_base") {
		t.Fatalf("expected api_base-required error, got %v", err)
	}
}
