package tools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/a2a"
	"github.com/sipeed/picoclaw/pkg/aep"
	toolshared "github.com/sipeed/picoclaw/pkg/tools/shared"
)

// InvokeAgentTool lets one digital employee call another as a tool. The
// delegation iron rule holds by construction: the task always names the
// ORIGINAL human requester as the principal it acts on, so the callee's
// tools resolve that requester's scope — never the calling agent's own,
// wider authority. With no human requester attached (background work) the
// calling agent acts as itself.
type InvokeAgentTool struct {
	manager  *aep.Manager
	client   *a2a.Client
	peers    map[string]string
	selfName string
}

// NewInvokeAgentTool binds the peer-caller tool to the session and peers.
func NewInvokeAgentTool(manager *aep.Manager, peers map[string]string) *InvokeAgentTool {
	return &InvokeAgentTool{
		manager: manager, client: a2a.NewClient(), peers: peers,
		selfName: managerSessionUser(manager),
	}
}

func managerSessionUser(manager *aep.Manager) string {
	if manager == nil {
		return ""
	}
	if id, err := manager.SelfUserID(context.Background()); err == nil {
		return id
	}
	return ""
}

func (t *InvokeAgentTool) Name() string { return "invoke_agent" }

func (t *InvokeAgentTool) Description() string {
	return "Ask another digital employee to handle a task. The other agent sees exactly the same requesting employee and their data scope; authority never widens across the call."
}

func (t *InvokeAgentTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"peer": map[string]any{
				"type":        "string",
				"description": "Configured peer name to invoke",
				"enum":        peerNames(t.peers),
			},
			"message": map[string]any{
				"type":        "string",
				"description": "The task for the peer, as plain text",
			},
		},
		"required": []string{"peer", "message"},
	}
}

func peerNames(peers map[string]string) []string {
	out := make([]string, 0, len(peers))
	for name := range peers {
		out = append(out, name)
	}
	return out
}

func (t *InvokeAgentTool) Execute(ctx context.Context, args map[string]any) *toolshared.ToolResult {
	peer, _ := args["peer"].(string)
	message, _ := args["message"].(string)
	message = strings.TrimSpace(message)
	baseURL, ok := t.peers[peer]
	if !ok || message == "" {
		return toolshared.NewToolResult("unknown peer or empty message; peers are configured by the deployment")
	}
	if t.manager == nil {
		return toolshared.NewToolResult("the AEP session is not running; the peer call cannot be authorized")
	}
	// The act-as principal: the human requester when present, otherwise the
	// calling agent itself for background work.
	actAs := requesterUserID(ctx)
	if actAs == "" {
		actAs = t.selfName
	}
	token, err := t.manager.AccessToken(ctx)
	if err != nil {
		return toolshared.NewToolResult(fmt.Sprintf("caller session unavailable: %v", err))
	}
	task, err := t.client.InvokeAndWait(ctx, a2a.InvokeOptions{
		BaseURL: baseURL, Token: token, Caller: t.selfName, ActAs: actAs,
	}, message, 120*time.Second)
	if err != nil {
		return toolshared.NewToolResult(fmt.Sprintf("peer invocation failed: %v", err))
	}
	if task.State != a2a.TaskStateCompleted {
		return toolshared.NewToolResult(fmt.Sprintf("peer task %s ended in state %s", task.ID, task.State))
	}
	var parts []string
	for _, artifact := range task.Artifacts {
		parts = append(parts, artifact.Text)
	}
	return toolshared.NewToolResult(strings.Join(parts, "\n"))
}
