// Package a2a implements the agent-to-agent surface for digital employees:
// an A2A v1.0-style JSON-RPC endpoint (message/send, tasks/get) plus the
// agent card, anchored on AEP — every caller is authenticated with its AEP
// access token and must hold agents.invoke; every task must name the
// original requester it acts on (act-as), so the delegation chain never
// widens anyone's scope; and chain depth is capped to structurally prevent
// agent ping-pong loops.
package a2a

import "time"

// AgentCard is the discovery document (A2A agent card, served at
// /.well-known/agent-card.json).
type AgentCard struct {
	Name               string           `json:"name"`
	Description        string           `json:"description,omitempty"`
	URL                string           `json:"url"`
	Version            string           `json:"version"`
	ProtocolVersion    string           `json:"protocolVersion"`
	Capabilities       CardCapabilities `json:"capabilities"`
	DefaultInputModes  []string         `json:"defaultInputModes"`
	DefaultOutputModes []string         `json:"defaultOutputModes"`
	Provider           string           `json:"provider,omitempty"`
	Authentication     CardAuth         `json:"authentication"`
	Skills             []CardSkill      `json:"skills,omitempty"`
}

// CardCapabilities declares what the agent endpoint supports.
type CardCapabilities struct {
	Streaming           bool `json:"streaming"`
	PushNotifications   bool `json:"pushNotifications"`
	StateTransitionHook bool `json:"stateTransitionHook"`
}

// CardAuth declares the accepted authentication scheme.
type CardAuth struct {
	Schemes []string `json:"schemes"`
}

// CardSkill is one advertised capability.
type CardSkill struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// Task states per the A2A task lifecycle.
const (
	TaskStateSubmitted = "submitted"
	TaskStateWorking   = "working"
	TaskStateCompleted = "completed"
	TaskStateFailed    = "failed"
)

// Task is one agent-to-agent invocation.
type Task struct {
	ID        string     `json:"id"`
	State     string     `json:"state"`
	ActAs     string     `json:"aep_act_as"`
	Caller    string     `json:"aep_caller"`
	Chain     []string   `json:"aep_chain"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
	CreatedAt time.Time  `json:"createdAt"`
	UpdatedAt time.Time  `json:"updatedAt"`
}

// Artifact carries one output part.
type Artifact struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

// Message is the A2A message envelope (request side).
type Message struct {
	Role      string            `json:"role"`
	Parts     []Part            `json:"parts"`
	MessageID string            `json:"messageId,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Part is one content part; only text is carried in this implementation.
type Part struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

// Metadata keys carrying the delegation chain.
const (
	MetadataActAs  = "aep_act_as_user"
	MetadataCaller = "aep_caller"
	MetadataChain  = "aep_chain"
)
