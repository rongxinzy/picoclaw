package tools

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/session"
	toolshared "github.com/sipeed/picoclaw/pkg/tools/shared"
)

// newScopeServer serves login/metadata plus a configurable data-scope
// context for one requester.
func newScopeServer(t *testing.T, contextBody string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var contextCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/aep/v1/auth/password/login":
			_, _ = io.WriteString(w, `{"accessToken":"at-1","refreshToken":"rt-1","modelAccessToken":"mt-1","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"t"}`)
		case "/aep/v1/auth/refresh":
			_, _ = io.WriteString(w, `{"accessToken":"at-2","refreshToken":"rt-2","modelAccessToken":"mt-2","expiresIn":3600,"modelAccessExpiresIn":3600,"deploymentId":"demo","sessionId":"t"}`)
		case "/aep/v1/metadata":
			_, _ = io.WriteString(w, `{"service":"aep-control-service","deploymentId":"demo"}`)
		case "/aep/v1/user/models":
			_, _ = io.WriteString(w, `{"models":[]}`)
		case "/aep/v1/user/heartbeat":
			_, _ = io.WriteString(w, `{"serverTime":"2026-09-18T08:00:00Z","controlEvents":{"pending":false},"nextHeartbeatAfterSeconds":60}`)
		case "/aep/v1/admin/data-scope/context":
			contextCalls.Add(1)
			_, _ = io.WriteString(w, contextBody)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server, &contextCalls
}

func newManagerAgainst(t *testing.T, server *httptest.Server) *aep.Manager {
	t.Helper()
	m := aep.NewManager(aep.Config{
		BaseURL: server.URL, DeploymentID: "demo",
		Username: "helper-1", Password: "agent-password-123", SessionID: "t",
	})
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("manager start: %v", err)
	}
	t.Cleanup(m.Stop)
	return m
}

func requesterCtx(userID string) context.Context {
	scope := &session.SessionScope{Values: map[string]string{"sender": "aepchat:" + userID}}
	return toolshared.WithToolSessionContext(context.Background(), "main", "sk-test", scope)
}

func TestDeptDataFiltersToRequesterScope(t *testing.T) {
	server, calls := newScopeServer(t, `{"principalId":"user-1","deploymentId":"demo","orgScope":["dept-a"],"ownTeamIds":["dept-a"],"roleScope":["employee"]}`)
	tool := NewDeptDataTool(newManagerAgainst(t, server))

	res := tool.Execute(requesterCtx("user-1"), map[string]any{"dataset": "monthly_sales"})
	if res.IsError || !strings.Contains(res.ForLLM, "team=dept-a") {
		t.Fatalf("expected a dept-a row, got %q", res.ContentForLLM())
	}
	if strings.Contains(res.ForLLM, "team=dept-b") {
		t.Fatalf("row leaked a team outside the requester scope: %q", res.ForLLM)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("scope resolutions = %d, want 1", got)
	}
}

func TestDeptDataDeniesTeamsOutsideRequesterScope(t *testing.T) {
	server, _ := newScopeServer(t, `{"principalId":"user-1","deploymentId":"demo","orgScope":["dept-a"],"ownTeamIds":["dept-a"],"roleScope":["employee"]}`)
	tool := NewDeptDataTool(newManagerAgainst(t, server))

	res := tool.Execute(requesterCtx("user-1"), map[string]any{"dataset": "monthly_sales", "team": "dept-b"})
	if !strings.Contains(res.ForLLM, "DENIED team=dept-b") {
		t.Fatalf("expected denial, got %q", res.ContentForLLM())
	}
}

func TestDeptDataExplicitDenyWinsInsideScope(t *testing.T) {
	server, _ := newScopeServer(t, `{"principalId":"user-1","deploymentId":"demo","orgScope":["dept-a","dept-b"],"ownTeamIds":["dept-a"],"roleScope":["employee"],"deniedResources":[{"kind":"team","id":"dept-b"}]}`)
	tool := NewDeptDataTool(newManagerAgainst(t, server))

	res := tool.Execute(requesterCtx("user-1"), map[string]any{"dataset": "monthly_sales"})
	if !strings.Contains(res.ForLLM, "DENIED team=dept-b (explicit deny)") || !strings.Contains(res.ForLLM, "team=dept-a revenue") {
		t.Fatalf("explicit deny should mask only dept-b: %q", res.ContentForLLM())
	}
}

func TestDeptDataRequiresRequesterIdentity(t *testing.T) {
	server, _ := newScopeServer(t, `{}`)
	tool := NewDeptDataTool(newManagerAgainst(t, server))

	res := tool.Execute(context.Background(), map[string]any{"dataset": "monthly_sales"})
	if !strings.Contains(res.ContentForLLM(), "no requesting employee") {
		t.Fatalf("expected identity requirement, got %q", res.ContentForLLM())
	}
}

func TestDeptDataRowsAreDeterministic(t *testing.T) {
	server, _ := newScopeServer(t, `{"principalId":"user-1","deploymentId":"demo","orgScope":["dept-a"],"ownTeamIds":["dept-a"],"roleScope":[]}`)
	tool := NewDeptDataTool(newManagerAgainst(t, server))

	first := tool.Execute(requesterCtx("user-1"), map[string]any{"dataset": "monthly_sales"}).ForLLM
	second := tool.Execute(requesterCtx("user-1"), map[string]any{"dataset": "monthly_sales"}).ForLLM
	if first == "" || first != second {
		t.Fatalf("rows must be deterministic: %q vs %q", first, second)
	}
	if !strings.HasPrefix(first, "dataset=monthly_sales team=dept-a revenue_usd_cents=") {
		t.Fatalf("unexpected row shape: %q", first)
	}
}
