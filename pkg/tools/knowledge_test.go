package tools

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
)

// knServer fakes the WeKnora search endpoint and records the last request.
func knServer(t *testing.T) (*httptest.Server, *atomic.Value, *atomic.Int32) {
	t.Helper()
	var lastRequest atomic.Value
	var keyCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keyCalls.Add(1)
		if r.URL.Path != "/api/v1/knowledge-search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("X-API-Key") != "kn-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		lastRequest.Store(string(body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"success":true,"data":[`+
			`{"knowledge_id":"k1","knowledge_title":"HR Handbook","content":"salary bands","score":0.92},`+
			`{"knowledge_id":"k2","knowledge_title":"RD Guide","content":"release process","score":0.81}]}`)
	}))
	t.Cleanup(server.Close)
	return server, &lastRequest, &keyCalls
}

func knConfig(baseURL string, teamKB map[string][]string) *config.KnowledgeConfig {
	return &config.KnowledgeConfig{
		Enabled: true, BaseURL: baseURL,
		APIKey: *config.NewSecureString("kn-secret"), TeamKBMap: teamKB,
	}
}

func TestKnowledgeSearchScopesToRequesterTeams(t *testing.T) {
	kn, lastRequest, _ := knServer(t)
	server, _ := newScopeServer(t, `{"principalId":"u1","deploymentId":"demo","orgScope":["dept-rd"],"ownTeamIds":["dept-rd"],"roleScope":[]}`)
	tool := NewKnowledgeSearchTool(newManagerAgainst(t, server), knConfig(kn.URL, map[string][]string{
		"dept-rd": {"kb-rd"}, "dept-hr": {"kb-hr"},
	}))

	res := tool.Execute(requesterCtx("u1"), map[string]any{"query": "release process"})
	captured := ""
	if v, ok := lastRequest.Load().(string); ok {
		captured = v
	}
	if !strings.Contains(res.ContentForLLM(), "kb-rd") || strings.Contains(captured, "kb-hr") {
		t.Fatalf("search leaked outside the requester's department KBs: request=%s result=%s",
			captured, res.ContentForLLM())
	}
	if !strings.Contains(res.ContentForLLM(), "release process") {
		t.Fatalf("passage content missing: %s", res.ContentForLLM())
	}
}

func TestKnowledgeSearchHonorsExplicitGrantAndDeny(t *testing.T) {
	kn, lastRequest, _ := knServer(t)
	// Requester from dept-rd with an explicit grant for kb-finance and an
	// explicit deny for kb-rd (deny wins even inside the inherited set).
	server, _ := newScopeServer(t, `{"principalId":"u1","deploymentId":"demo","orgScope":["dept-rd"],"ownTeamIds":["dept-rd"],"roleScope":[],"allowedResources":[{"kind":"knowledge_base","id":"kb-finance"}],"deniedResources":[{"kind":"knowledge_base","id":"kb-rd"}]}`)
	tool := NewKnowledgeSearchTool(newManagerAgainst(t, server), knConfig(kn.URL, map[string][]string{
		"dept-rd": {"kb-rd"},
	}))

	res := tool.Execute(requesterCtx("u1"), map[string]any{"query": "budget"})
	request := ""
	if v, ok := lastRequest.Load().(string); ok {
		request = v
	}
	if !strings.Contains(request, "kb-finance") || strings.Contains(request, "kb-rd") {
		t.Fatalf("expected kb-finance only, request=%s", request)
	}
	if !strings.Contains(res.ContentForLLM(), "searched knowledge bases: kb-finance") {
		t.Fatalf("result scope echo wrong: %s", res.ContentForLLM())
	}
}

func TestKnowledgeSearchFailsClosedWithoutScopeOrIdentity(t *testing.T) {
	kn, _, calls := knServer(t)
	server, _ := newScopeServer(t, `{"principalId":"u1","deploymentId":"demo","orgScope":["dept-x"],"ownTeamIds":["dept-x"],"roleScope":[]}`)
	tool := NewKnowledgeSearchTool(newManagerAgainst(t, server), knConfig(kn.URL, nil))

	// No KBs mapped for the requester's teams → nothing searched.
	res := tool.Execute(requesterCtx("u1"), map[string]any{"query": "anything"})
	if !strings.Contains(res.ContentForLLM(), "no knowledge bases") {
		t.Fatalf("expected closed-scope message, got %s", res.ContentForLLM())
	}
	// No requester identity at all → closed.
	res = tool.Execute(context.Background(), map[string]any{"query": "anything"})
	if !strings.Contains(res.ContentForLLM(), "no requesting employee") {
		t.Fatalf("expected identity requirement, got %s", res.ContentForLLM())
	}
	if calls.Load() != 0 {
		t.Fatalf("WeKnora must not be called on closed paths (%d calls)", calls.Load())
	}
}
