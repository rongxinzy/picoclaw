package tools

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/sipeed/picoclaw/pkg/aep"
	toolshared "github.com/sipeed/picoclaw/pkg/tools/shared"
)

// DeptDataTool answers department dataset queries for the employee who is
// talking to the digital employee. Authorization is delegated: rows are
// filtered to the REQUESTER's data-scope context, resolved live from the
// control service — never to the digital employee's own scope. An explicit
// deny wins, and an unresolvable scope fails closed.
type DeptDataTool struct {
	manager *aep.Manager
}

// NewDeptDataTool binds the tool to a live AEP session manager.
func NewDeptDataTool(manager *aep.Manager) *DeptDataTool {
	return &DeptDataTool{manager: manager}
}

func (t *DeptDataTool) Name() string { return "dept_data" }

func (t *DeptDataTool) Description() string {
	return "Query department datasets on behalf of the requesting employee. Results are filtered to the requester's data scope; teams outside the scope are denied."
}

func (t *DeptDataTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"dataset": map[string]any{
				"type":        "string",
				"enum":        []string{"monthly_sales"},
				"description": "department dataset to query",
			},
			"team": map[string]any{
				"type":        "string",
				"description": "optional team id to restrict the query to",
			},
		},
		"required": []string{"dataset"},
	}
}

func (t *DeptDataTool) Execute(ctx context.Context, args map[string]any) *toolshared.ToolResult {
	dataset, _ := args["dataset"].(string)
	if dataset != "monthly_sales" {
		return toolshared.NewToolResult("unknown dataset; only monthly_sales exists")
	}
	requester := requesterUserID(ctx)
	if requester == "" {
		return toolshared.NewToolResult("no requesting employee is attached to this conversation; the dataset cannot be scoped")
	}
	if t.manager == nil {
		return toolshared.NewToolResult("the AEP session is not running; authorization cannot be evaluated")
	}
	scope, err := t.manager.DataScopeContext(ctx, requester)
	if err != nil {
		// Fail closed: without an authorization answer, no rows leave.
		return toolshared.NewToolResult(fmt.Sprintf("data scope could not be resolved: %v", err))
	}
	denied := deniedTeams(scope)

	team, _ := args["team"].(string)
	if team != "" {
		if denied[team] {
			return toolshared.NewToolResult(fmt.Sprintf("DENIED team=%s: an explicit deny rule excludes this team", team))
		}
		if !containsTeam(scope.OrgScope, team) {
			return toolshared.NewToolResult(fmt.Sprintf("DENIED team=%s: outside the requester's data scope (visible: %s)", team, strings.Join(sortedCopy(scope.OrgScope), ", ")))
		}
		return toolshared.NewToolResult(row(dataset, team))
	}

	visible := sortedCopy(scope.OrgScope)
	if len(visible) == 0 {
		return toolshared.NewToolResult("the requester's data scope is empty; nothing to report")
	}
	var rows []string
	for _, tid := range visible {
		if denied[tid] {
			rows = append(rows, fmt.Sprintf("DENIED team=%s (explicit deny)", tid))
			continue
		}
		rows = append(rows, row(dataset, tid))
	}
	return toolshared.NewToolResult(strings.Join(rows, "; "))
}

// row renders one deterministic fake dataset row per team. The values are
// stable per (dataset, team) so E2E assertions are exact.
func row(dataset, team string) string {
	return fmt.Sprintf("dataset=%s team=%s revenue_usd_cents=%d orders=%d",
		dataset, team, stableNumber(dataset+":revenue:"+team, 100_000, 900_000), stableNumber(dataset+":orders:"+team, 10, 9_999))
}

func stableNumber(salt string, min, max int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(salt))
	span := max - min + 1
	return min + int(h.Sum32())%span
}

// requesterUserID resolves the requester identity for the current turn:
// the deterministic tool-context sender first, then the session-scope
// "sender" dimension ("aepchat:<user id>") as a fallback.
func requesterUserID(ctx context.Context) string {
	if id := toolshared.ToolSenderID(ctx); id != "" {
		return strings.TrimPrefix(id, "aepchat:")
	}
	scope := toolshared.ToolSessionScope(ctx)
	if scope == nil {
		return ""
	}
	return strings.TrimPrefix(scope.Values["sender"], "aepchat:")
}

func deniedTeams(scope *aep.RetrievalContext) map[string]bool {
	out := make(map[string]bool)
	for _, ref := range scope.DeniedResources {
		if ref.Kind == "team" {
			out[ref.ID] = true
		}
	}
	return out
}

func containsTeam(teams []string, team string) bool {
	for _, t := range teams {
		if t == team {
			return true
		}
	}
	return false
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
