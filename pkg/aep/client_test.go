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

func TestLifecycleAndScopeClientEndpoints(t *testing.T) {
	var seenAuth, seenPath string
	var seenBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		seenPath = r.Method + " " + r.URL.Path
		if r.Body != nil && r.ContentLength > 0 {
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &seenBody)
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case seenPath == "GET /aep/v1/admin/data-scope/context":
			_, _ = w.Write([]byte(`{"principalId":"u1","deploymentId":"demo","orgScope":["dept-a"],"ownTeamIds":["dept-a"],"roleScope":[]}`))
		case seenPath == "GET /aep/v1/user/me":
			_, _ = w.Write([]byte(`{"user":{"id":"u1","displayName":"Zhang","kind":"human"},"deploymentId":"demo","roles":["employee"]}`))
		case seenPath == "POST /aep/v1/admin/agents":
			_, _ = w.Write([]byte(`{"id":"agent-9","username":"eph-x","displayName":"Fork","homeTeamId":"dept-a","displayTitle":"","ephemeral":true,"expiresAt":"2030-01-01T00:00:00Z"}`))
		case seenPath == "DELETE /aep/v1/admin/agents/agent-9":
			w.WriteHeader(http.StatusNoContent)
		case seenPath == "POST /aep/v1/admin/sessions/s-1/revoke":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected %s", seenPath)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client := NewClient(server.URL)

	scope, p := client.DataScopeContext(context.Background(), "at", "u1")
	if p != nil || scope.PrincipalID != "u1" || len(scope.OrgScope) != 1 {
		t.Fatalf("DataScopeContext = %+v, %v", scope, p)
	}
	if seenAuth != "Bearer at" {
		t.Fatalf("authorization = %q", seenAuth)
	}

	principal, p := client.CurrentUser(context.Background(), "at")
	if p != nil || principal.UserID != "u1" || principal.Kind != "human" {
		t.Fatalf("CurrentUser = %+v, %v", principal, p)
	}

	record, p := client.CreateAgent(context.Background(), "at", EphemeralAgentInput{
		Username: "eph-x", DisplayName: "Fork", Password: "long-password-123",
		RoleIDs: []string{"runner"}, HomeTeamID: "dept-a",
		Ephemeral: true, ExpiresAt: "2030-01-01T00:00:00Z", ScopeFromUserID: "u1",
	})
	if p != nil || record.ID != "agent-9" || !record.Ephemeral {
		t.Fatalf("CreateAgent = %+v, %v", record, p)
	}
	if seenBody["scopeFromUserId"] != "u1" || seenBody["ephemeral"] != true {
		t.Fatalf("CreateAgent body = %v", seenBody)
	}

	if p := client.DeleteAgent(context.Background(), "at", "agent-9"); p != nil {
		t.Fatalf("DeleteAgent: %v", p)
	}
	if p := client.RevokeSession(context.Background(), "at", "s-1"); p != nil {
		t.Fatalf("RevokeSession: %v", p)
	}
}

func TestProblemErrorWithoutCode(t *testing.T) {
	p := &Problem{Title: "boom", Status: 500}
	if got := p.Error(); got != "aep: boom:  (500)" && !strings.Contains(got, "boom") {
		t.Fatalf("Error() = %q", got)
	}
}

func TestMetadataEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/aep/v1/metadata" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"service":"aep","deploymentId":"demo","modelGateway":{"baseUrl":"https://gw/v1","protocol":"openai-compatible"}}`))
	}))
	defer server.Close()
	meta, p := NewClient(server.URL).Metadata(context.Background())
	if p != nil || meta.DeploymentID != "demo" || meta.ModelGateway.BaseURL != "https://gw/v1" {
		t.Fatalf("Metadata = %+v, %v", meta, p)
	}
}
