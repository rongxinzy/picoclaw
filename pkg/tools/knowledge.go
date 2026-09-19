package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/config"
	toolshared "github.com/sipeed/picoclaw/pkg/tools/shared"
	"github.com/sipeed/picoclaw/pkg/weknora"
)

// KnowledgeSearchTool is the retrieval PEP for the WeKnora knowledge
// platform: every search is scoped to the REQUESTING employee's data-scope
// context resolved live from AEP — department inheritance (visible teams
// mapped to knowledge bases) plus explicit knowledge_base grants, minus
// explicit denies. AEP is the single authorization truth; WeKnora's own
// workspace permissions stay deployment isolation and the scoped API key is
// defense in depth, never the permission model. Unresolvable scope fails
// closed.
type KnowledgeSearchTool struct {
	manager     *aep.Manager
	client      *weknora.Client
	teamKBMap   map[string][]string
	maxPassages int
}

// NewKnowledgeSearchTool binds the PEP tool to the resident/fork session and
// the WeKnora deployment.
func NewKnowledgeSearchTool(manager *aep.Manager, cfg *config.KnowledgeConfig) *KnowledgeSearchTool {
	max := 5
	if cfg != nil && cfg.MaxPassages > 0 {
		max = cfg.MaxPassages
	}
	return &KnowledgeSearchTool{
		manager:     manager,
		client:      weknora.NewClient(cfg.BaseURL, cfg.APIKey.String()),
		teamKBMap:   cfg.TeamKBMap,
		maxPassages: max,
	}
}

func (t *KnowledgeSearchTool) Name() string { return "knowledge_search" }

func (t *KnowledgeSearchTool) Description() string {
	return "Search the enterprise knowledge base (WeKnora) for the requesting employee. Results are automatically restricted to the requester's data scope; knowledge bases outside the scope are never searched."
}

func (t *KnowledgeSearchTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type":        "string",
				"description": "What to search for in the knowledge base",
			},
			"top_k": map[string]any{
				"type":        "integer",
				"description": "Maximum passages to return",
				"minimum":     1,
				"maximum":     t.maxPassages,
			},
		},
		"required": []string{"query"},
	}
}

func (t *KnowledgeSearchTool) Execute(ctx context.Context, args map[string]any) *toolshared.ToolResult {
	query, _ := args["query"].(string)
	query = strings.TrimSpace(query)
	if query == "" {
		return toolshared.NewToolResult("query is required")
	}
	topK := t.maxPassages
	if raw, ok := args["top_k"].(float64); ok && int(raw) > 0 && int(raw) <= t.maxPassages {
		topK = int(raw)
	}
	requester := requesterUserID(ctx)
	if requester == "" {
		return toolshared.NewToolResult("no requesting employee is attached to this conversation; knowledge cannot be scoped")
	}
	if t.manager == nil {
		return toolshared.NewToolResult("the AEP session is not running; authorization cannot be evaluated")
	}
	scope, err := t.manager.DataScopeContext(ctx, requester)
	if err != nil {
		// Fail closed: without an authorization answer, nothing is searched.
		return toolshared.NewToolResult(fmt.Sprintf("requester data scope could not be resolved: %v", err))
	}

	allowed := t.allowedKnowledgeBases(scope)
	if len(allowed) == 0 {
		return toolshared.NewToolResult("the requester's data scope includes no knowledge bases; nothing to search")
	}
	kbIDs := make([]string, 0, len(allowed))
	for kb := range allowed {
		kbIDs = append(kbIDs, kb)
	}
	sort.Strings(kbIDs)

	passages, err := t.client.Search(ctx, query, kbIDs, topK)
	if err != nil {
		return toolshared.NewToolResult(fmt.Sprintf("knowledge search failed: %v", err))
	}
	if len(passages) == 0 {
		return toolshared.NewToolResult(fmt.Sprintf("no passages matched in the requester's knowledge bases (%s)", strings.Join(kbIDs, ", ")))
	}
	var out []string
	for i, p := range passages {
		out = append(out, fmt.Sprintf("[%d] %s (score %.3f)\n%s", i+1, p.Title, p.Score, p.Content))
	}
	return toolshared.NewToolResult(fmt.Sprintf("searched knowledge bases: %s\n%s", strings.Join(kbIDs, ", "), strings.Join(out, "\n\n")))
}

// allowedKnowledgeBases derives the searchable KB set: department
// inheritance from visible teams, plus explicit knowledge_base grants,
// minus explicit denies (deny always wins).
func (t *KnowledgeSearchTool) allowedKnowledgeBases(scope *aep.RetrievalContext) map[string]bool {
	allowed := make(map[string]bool)
	for _, team := range scope.OrgScope {
		for _, kb := range t.teamKBMap[team] {
			if kb != "" {
				allowed[kb] = true
			}
		}
	}
	for _, ref := range scope.AllowedResources {
		if ref.Kind == "knowledge_base" && ref.ID != "" {
			allowed[ref.ID] = true
		}
	}
	for _, ref := range scope.DeniedResources {
		if ref.Kind == "knowledge_base" {
			delete(allowed, ref.ID)
		}
	}
	return allowed
}
