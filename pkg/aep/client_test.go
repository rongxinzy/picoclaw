package aep

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPasswordLoginPostsCredentialsAndContractHeaders(t *testing.T) {
	var gotPath, gotVersion, gotAuth, gotContentType string
	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotVersion = r.Header.Get("X-AEP-Protocol-Version")
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accessToken":"at-1","refreshToken":"rt-1","modelAccessToken":"mt-1","expiresIn":7200,"modelAccessExpiresIn":7200,"deploymentId":"demo","sessionId":"picoclaw-test"}`))
	}))
	defer server.Close()

	tokens, p := NewClient(server.URL).PasswordLogin(context.Background(), "demo", "helper-1", "agent-password-123", "picoclaw-test")
	if p != nil {
		t.Fatalf("login problem: %v", p)
	}
	if gotPath != "/aep/v1/auth/password/login" {
		t.Fatalf("path = %s", gotPath)
	}
	if gotVersion != "1.0" {
		t.Fatalf("protocol version header = %q", gotVersion)
	}
	if gotAuth != "" {
		t.Fatalf("login must not send Authorization, got %q", gotAuth)
	}
	if !strings.HasPrefix(gotContentType, "application/json") {
		t.Fatalf("content type = %q", gotContentType)
	}
	for key, want := range map[string]string{"deploymentId": "demo", "sessionId": "picoclaw-test", "username": "helper-1", "password": "agent-password-123"} {
		if gotBody[key] != want {
			t.Fatalf("body[%s] = %q, want %q", key, gotBody[key], want)
		}
	}
	if tokens.AccessToken != "at-1" || tokens.RefreshToken != "rt-1" || tokens.ModelAccessToken != "mt-1" {
		t.Fatalf("tokens = %+v", tokens)
	}
}

func TestAuthenticatedGetSendsBearerToken(t *testing.T) {
	var gotAuth, gotVersion string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotVersion = r.Header.Get("X-AEP-Protocol-Version")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"models":[{"id":"enterprise-qwen-32b","displayName":"Qwen","protocol":"openai-compatible","endpoint":"https://gw.example.test/v1","upstreamModel":"qwen3-32b","enabled":true,"isDefault":true,"contextWindow":131072}]}`))
	}))
	defer server.Close()

	models, p := NewClient(server.URL).Models(context.Background(), "at-1")
	if p != nil {
		t.Fatalf("models problem: %v", p)
	}
	if gotAuth != "Bearer at-1" || gotVersion != "1.0" {
		t.Fatalf("headers = %q / %q", gotAuth, gotVersion)
	}
	if len(models) != 1 || models[0].ID != "enterprise-qwen-32b" || models[0].UpstreamModel != "qwen3-32b" {
		t.Fatalf("models = %+v", models)
	}
}

func TestProblemParsingCapturesCodeAndRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.Header().Set("Retry-After", "17")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"type":"https://aep.example/problems/rate-limited","title":"Too many requests","status":429,"detail":"Shared login backoff is active.","code":"RATE_LIMITED","requestId":"req-1"}`))
	}))
	defer server.Close()

	_, p := NewClient(server.URL).Refresh(context.Background(), "rt-x", "picoclaw-test")
	if p == nil {
		t.Fatal("expected problem")
	}
	if p.Code != "RATE_LIMITED" || p.Status != http.StatusTooManyRequests || p.RetryAfter != "17" {
		t.Fatalf("problem = %+v", p)
	}
	if !strings.Contains(p.Error(), "RATE_LIMITED") {
		t.Fatalf("error string = %q", p.Error())
	}
}

func TestRedactURLStripsQueryStrings(t *testing.T) {
	msg := `Get "http://localhost:8080/aep/v1/auth/password/login?token=secret&x=1": connection refused`
	got := redactURL(msg)
	if strings.Contains(got, "secret") {
		t.Fatalf("query not redacted: %q", got)
	}
	if !strings.Contains(got, "?[redacted]") {
		t.Fatalf("unexpected redaction: %q", got)
	}
}
