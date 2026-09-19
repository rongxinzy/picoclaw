package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
)

const (
	channelName        = "a2a"
	requiredPermission = "agents.invoke"
	defaultMaxChain    = 3
	taskWaitTimeout    = 5 * time.Minute
)

// Channel is both the A2A endpoint and the reply sink: inbound peer tasks
// enter the agent loop through the message bus, and the agent's outbound
// replies complete the task they belong to.
type Channel struct {
	*channels.BaseChannel

	auth         *aep.Authenticator
	deploymentID string
	cardURL      string
	agentName    string
	maxChain     int
	telemetry    *telemetry

	mu      sync.Mutex
	tasks   map[string]*Task
	signals map[string]chan struct{}
	running bool
	stopped sync.Once
	ctx     context.Context
}

// New builds the A2A endpoint for one digital employee runtime.
func New(channelName string, bc *config.Channel, settings *config.A2ASettings, cfg *config.Config, b *bus.MessageBus) (*Channel, error) {
	if cfg == nil || !cfg.AEP.Enabled || !cfg.AEP.IsComplete() {
		return nil, errors.New("a2a requires a complete aep config")
	}
	maxChain := defaultMaxChain
	if settings != nil && settings.MaxChainDepth > 0 {
		maxChain = settings.MaxChainDepth
	}
	publicURL := ""
	if settings != nil {
		publicURL = settings.PublicURL
	}
	ch := &Channel{
		BaseChannel:  channels.NewBaseChannel(channelName, bc, b, []string{}),
		auth:         aep.NewAuthenticator(cfg.AEP.BaseURL),
		deploymentID: cfg.AEP.DeploymentID,
		cardURL:      publicURL,
		agentName:    cfg.AEP.Username,
		maxChain:     maxChain,
		telemetry:    newTelemetry(cfg.AEP.BaseURL, cfg.AEP.DeploymentID),
		tasks:        make(map[string]*Task),
		signals:      make(map[string]chan struct{}),
	}
	ch.SetOwner(ch)
	return ch, nil
}

// Start marks the endpoint ready.
func (c *Channel) Start(ctx context.Context) error {
	c.ctx = ctx
	c.running = true
	return nil
}

// Stop shuts the endpoint down and fails every pending task. Idempotent.
func (c *Channel) Stop(_ context.Context) error {
	c.stopped.Do(func() {
		c.running = false
		c.mu.Lock()
		defer c.mu.Unlock()
		for id, task := range c.tasks {
			if task.State == TaskStateSubmitted || task.State == TaskStateWorking {
				task.State = TaskStateFailed
				task.UpdatedAt = time.Now().UTC()
			}
			if signal, ok := c.signals[id]; ok {
				close(signal)
				delete(c.signals, id)
			}
		}
	})
	return nil
}

func (c *Channel) IsRunning() bool { return c.running }

// Send completes the task the outbound reply belongs to.
func (c *Channel) Send(_ context.Context, msg bus.OutboundMessage) ([]string, error) {
	if !c.running {
		return nil, channels.ErrNotRunning
	}
	if msg.ChatID == "" || msg.Content == "" {
		return nil, nil
	}
	c.mu.Lock()
	task, ok := c.tasks[msg.ChatID]
	signal := c.signals[msg.ChatID]
	c.mu.Unlock()
	if !ok {
		return nil, nil // not an A2A chat; nothing to do
	}
	c.mu.Lock()
	task.Artifacts = append(task.Artifacts, Artifact{Kind: "text", Text: msg.Content})
	task.State = TaskStateCompleted
	task.UpdatedAt = time.Now().UTC()
	c.mu.Unlock()
	if signal != nil {
		select {
		case <-signal:
		default:
			close(signal)
			c.mu.Lock()
			delete(c.signals, msg.ChatID)
			c.mu.Unlock()
		}
	}
	return []string{msg.ChatID}, nil
}

// WebhookPath mounts the JSON-RPC subtree on the shared gateway mux.
func (c *Channel) WebhookPath() string { return "/a2a/" }

// HealthPath serves the agent card at the A2A well-known location.
func (c *Channel) HealthPath() string { return "/.well-known/agent-card.json" }

// HealthHandler serves the agent card.
func (c *Channel) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(c.Card())
}

// Card renders this runtime's agent card.
func (c *Channel) Card() AgentCard {
	return AgentCard{
		Name:               c.agentName,
		Description:        "AEP digital employee",
		URL:                c.cardURL,
		Version:            "1.0.0",
		ProtocolVersion:    "1.0",
		Capabilities:       CardCapabilities{},
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain"},
		Provider:           "AEP",
		Authentication:     CardAuth{Schemes: []string{"bearer"}},
	}
}

// ServeHTTP self-routes the A2A subtree: POST "" is JSON-RPC, GET card.json
// is a card mirror for peers that cannot reach the well-known path.
func (c *Channel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sub := strings.TrimPrefix(r.URL.Path, "/a2a/")
	switch {
	case sub == "card.json" && r.Method == http.MethodGet:
		c.HealthHandler(w, r)
		return
	case sub == "" && r.Method == http.MethodPost:
		c.serveRPC(w, r)
		return
	default:
		writeJSONRPCError(w, nil, -32601, "method not found")
	}
}

func (c *Channel) serveRPC(w http.ResponseWriter, r *http.Request) {
	var request struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  struct {
			Message Message `json:"message"`
			TaskID  string  `json:"taskId"`
		} `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.JSONRPC != "2.0" {
		writeJSONRPCError(w, nil, -32600, "invalid JSON-RPC request")
		return
	}
	peer, code, problem := c.authenticatePeer(r)
	if problem != "" {
		writeJSONRPCError(w, request.ID, code, problem)
		return
	}
	switch request.Method {
	case "message/send":
		c.handleSend(w, request.ID, peer, request.Params.Message)
	case "tasks/get":
		c.handleGet(w, request.ID, request.Params.TaskID)
	default:
		writeJSONRPCError(w, request.ID, -32601, "unknown method "+request.Method)
	}
}

func (c *Channel) authenticatePeer(r *http.Request) (*aep.PeerPrincipal, int, string) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return nil, -32001, "Authorization: Bearer <aep access token> is required"
	}
	peer, err := c.auth.AuthenticatePeer(r.Context(), c.deploymentID, strings.TrimSpace(strings.TrimPrefix(header, "Bearer ")), requiredPermission)
	if err != nil {
		if errors.Is(err, aep.ErrMissingPermission) {
			return nil, -32002, err.Error()
		}
		return nil, -32001, "token verification failed"
	}
	return peer, 0, ""
}

func (c *Channel) handleSend(w http.ResponseWriter, requestID json.RawMessage, peer *aep.PeerPrincipal, message Message) {
	if !c.running {
		writeJSONRPCError(w, requestID, -32003, "a2a endpoint is not running")
		return
	}
	text := messageText(message)
	if text == "" {
		writeJSONRPCError(w, requestID, -32602, "message must carry a text part")
		return
	}
	actAs := message.Metadata[MetadataActAs]
	if actAs == "" {
		// The delegation chain is mandatory: a task never runs under the
		// calling agent's own authority implicitly.
		writeJSONRPCError(w, requestID, -32003, "metadata.aep_act_as_user is required")
		return
	}
	chain := parseChain(message.Metadata[MetadataChain])
	if len(chain)+1 > c.maxChain {
		writeJSONRPCError(w, requestID, -32004, fmt.Sprintf("delegation chain exceeds the maximum depth of %d", c.maxChain))
		return
	}
	caller := message.Metadata[MetadataCaller]
	if caller == "" {
		caller = peer.UserID
	}

	task := &Task{
		ID: "task-" + uuid.NewString(), State: TaskStateSubmitted,
		ActAs: actAs, Caller: caller, Chain: chain,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	c.mu.Lock()
	c.tasks[task.ID] = task
	c.signals[task.ID] = make(chan struct{})
	c.mu.Unlock()

	c.telemetry.fire("agent.invoke", "received", map[string]string{
		"task_id": task.ID, "peer": caller, "act_as": actAs,
		"chain": message.Metadata[MetadataChain],
	})

	// The act-as user becomes the turn's sender, so every tool resolves the
	// ORIGINAL requester's scope — the delegation iron rule by construction.
	sender := bus.SenderInfo{
		Platform: "a2a", PlatformID: actAs,
		CanonicalID: "a2a:" + actAs, Username: actAs,
		DisplayName: "Requester " + actAs,
	}
	inboundCtx := bus.InboundContext{
		Channel: c.Name(), ChatID: task.ID, ChatType: "direct",
		SenderID: actAs, MessageID: "a2a-" + task.ID,
		Raw: map[string]string{
			"a2a_caller": caller,
			"a2a_act_as": actAs,
			"a2a_chain":  message.Metadata[MetadataChain],
		},
	}
	c.HandleInboundContext(c.ctx, task.ID, text, nil, inboundCtx, sender)

	c.mu.Lock()
	task.State = TaskStateWorking
	task.UpdatedAt = time.Now().UTC()
	c.mu.Unlock()

	writeJSONRPCResult(w, requestID, task)
}

func (c *Channel) handleGet(w http.ResponseWriter, requestID json.RawMessage, taskID string) {
	c.mu.Lock()
	task, ok := c.tasks[taskID]
	c.mu.Unlock()
	if !ok {
		writeJSONRPCError(w, requestID, -32005, "task not found")
		return
	}
	writeJSONRPCResult(w, requestID, task)
}

// WaitForTask blocks until the task reaches a terminal state.
func (c *Channel) WaitForTask(taskID string, timeout time.Duration) (*Task, error) {
	if timeout <= 0 {
		timeout = taskWaitTimeout
	}
	deadline := time.After(timeout)
	for {
		c.mu.Lock()
		task, ok := c.tasks[taskID]
		signal := c.signals[taskID]
		c.mu.Unlock()
		if !ok {
			return nil, fmt.Errorf("task %s not found", taskID)
		}
		if task.State == TaskStateCompleted || task.State == TaskStateFailed {
			return task, nil
		}
		if signal == nil {
			return nil, fmt.Errorf("task %s has no completion signal", taskID)
		}
		select {
		case <-signal:
		case <-deadline:
			return nil, fmt.Errorf("task %s did not finish within %s", taskID, timeout)
		}
	}
}

func messageText(message Message) string {
	for _, part := range message.Parts {
		if part.Kind == "text" && strings.TrimSpace(part.Text) != "" {
			return strings.TrimSpace(part.Text)
		}
	}
	return ""
}

func parseChain(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ">")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func writeJSONRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	body := map[string]any{"jsonrpc": "2.0", "result": result}
	if id != nil {
		body["id"] = id
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSONRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	body := map[string]any{
		"jsonrpc": "2.0",
		"error":   map[string]any{"code": code, "message": message},
	}
	if id != nil {
		body["id"] = id
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}

// telemetry forwards lifecycle events to the control plane; failures only
// log — telemetry never blocks the invocation path.
type telemetry struct {
	client       *aep.Client
	deploymentID string
}

func newTelemetry(baseURL, deploymentID string) *telemetry {
	return &telemetry{client: aep.NewClient(baseURL), deploymentID: deploymentID}
}

func (t *telemetry) fire(eventType, direction string, metadata map[string]string) {
	if t == nil {
		return
	}
	manager := aep.DefaultManager()
	if manager == nil {
		return
	}
	token, err := manager.AccessToken(context.Background())
	if err != nil {
		logger.Warnf("a2a: telemetry token unavailable: %v", err)
		return
	}
	event := aep.TelemetryEvent{
		EventID: uuid.NewString(), Type: eventType,
		OccurredAt: time.Now().UTC().Format(time.RFC3339Nano),
		Resource:   map[string]string{"type": "agent", "id": t.deploymentID},
		Result:     direction,
		Metadata:   metadata,
	}
	go func() {
		if p := t.client.EventsBatch(context.Background(), token, []aep.TelemetryEvent{event}); p != nil {
			logger.Warnf("a2a: telemetry upload failed: %v", p)
		}
	}()
}
