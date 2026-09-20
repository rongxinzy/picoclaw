package config

// AEPConfig binds this picoclaw process to one Agent Enterprise Protocol (AEP)
// digital-employee account. When enabled, the gateway starts an AEP session
// (password login, token refresh, heartbeat presence), injects the assigned
// model catalog into model_list under the "aep" provider, and authorizes
// model calls with the session's rotating model access token.
//
// The password is the only secret. It is stored encrypted in .security.yml
// (never in config.json) and is filtered from logs by the shared
// SensitiveDataReplacer.
type AEPConfig struct {
	Enabled        bool         `json:"enabled"                  yaml:"enabled"            env:"PICOCLAW_AEP_ENABLED"`
	BaseURL        string       `json:"base_url,omitempty"       yaml:"-"                  env:"PICOCLAW_AEP_BASE_URL"`
	DeploymentID   string       `json:"deployment_id,omitempty"  yaml:"-"                  env:"PICOCLAW_AEP_DEPLOYMENT_ID"`
	Username       string       `json:"username,omitempty"       yaml:"-"                  env:"PICOCLAW_AEP_USERNAME"`
	Password       SecureString `json:"password,omitzero"        yaml:"password,omitempty" env:"PICOCLAW_AEP_PASSWORD"`
	SessionID      string       `json:"session_id,omitempty"     yaml:"-"                  env:"PICOCLAW_AEP_SESSION_ID"`
	GatewayBaseURL string       `json:"gateway_base_url,omitempty" yaml:"-"                 env:"PICOCLAW_AEP_GATEWAY_BASE_URL"`
	// HomeTeamID anchors the warden's hierarchy comparison: requesters whose
	// teams lie strictly inside this team's subtree converse with scoped
	// ephemeral forks instead of the resident instance.
	HomeTeamID string `json:"home_team_id,omitempty" yaml:"-" env:"PICOCLAW_AEP_HOME_TEAM_ID"`
	// Supervisor credentials carry the ephemeral lifecycle authority
	// (users.write). Deliberately separate from the digital employee account,
	// which never holds provisioning rights.
	SupervisorUsername string       `json:"supervisor_username,omitempty" yaml:"-"                          env:"PICOCLAW_AEP_SUPERVISOR_USERNAME"`
	SupervisorPassword SecureString `json:"supervisor_password,omitzero"  yaml:"supervisor_password,omitempty" env:"PICOCLAW_AEP_SUPERVISOR_PASSWORD"`
}

// IsComplete reports whether the mandatory connection fields are present.
func (c AEPConfig) IsComplete() bool {
	return c.BaseURL != "" && c.DeploymentID != "" && c.Username != "" && c.Password.String() != ""
}

// AEPChatSettings configures the aepchat web-chat channel. The channel has
// no secrets of its own: authentication is the requester's AEP access token,
// verified against the control-service JWKS.
type AEPChatSettings struct {
	HistoryLimit int `json:"history_limit,omitempty" yaml:"history_limit,omitempty" env:"PICOCLAW_CHANNELS_AEPCHAT_HISTORY_LIMIT"`
	// Warden enables the resident/ephemeral conversation split: requesters
	// inside the home-team subtree (strictly below) are served by scoped
	// ephemeral forks spawned on demand.
	Warden *WardenSettings `json:"warden,omitempty" yaml:"-" env:"-"`
	// RelaySecret authenticates turn-relay calls from the resident gateway
	// (IM channels routing subordinate messages through this fork). Injected
	// by the fork supervisor at spawn via PICOCLAW_AEPCHAT_RELAY_TOKEN;
	// never set on a resident instance.
	RelaySecret SecureString `json:"relay_secret,omitzero" yaml:"-" env:"PICOCLAW_AEPCHAT_RELAY_TOKEN"`
}

// AEPChannelSettings binds an IM channel (feishu, wecom, ...) to the AEP
// digital-employee model: platform senders resolve to AEP users through an
// identity source, and the warden routes subordinates to ephemeral forks.
// A nil AEP section keeps the channel's personal-assistant behavior.
type AEPChannelSettings struct {
	// IdentitySourceID is the AEP identity source whose user mappings bind
	// this platform's native sender IDs to platform users. Required.
	IdentitySourceID string `json:"identity_source_id,omitempty" yaml:"-"`
	// Warden enables the resident/ephemeral split for this channel. Optional:
	// without it every mapped requester talks to the resident instance.
	Warden *WardenSettings `json:"warden,omitempty" yaml:"-"`
	// UnmappedReply overrides the rejection text for unmapped senders.
	UnmappedReply string `json:"unmapped_reply,omitempty" yaml:"-"`
}

// WardenSettings configures ephemeral fork spawning.
type WardenSettings struct {
	// RuntimeRoleID is granted to every ephemeral fork (models/skills/data
	// scope read). Required.
	RuntimeRoleID string `json:"runtime_role_id" yaml:"-"`
	// PromptSkillID carries the department persona onto the fork. Optional.
	PromptSkillID string `json:"prompt_skill_id,omitempty" yaml:"-"`
	// TTLMinutes bounds an idle fork's lifetime (default 30). The AEP-side
	// hard expiry is always applied on top.
	TTLMinutes int `json:"ttl_minutes,omitempty" yaml:"-"`
	// PortRangeStart allocates child gateway ports from this value upwards
	// (default 18900).
	PortRangeStart int `json:"port_range_start,omitempty" yaml:"-"`
}

// A2AConfig names the peers this digital employee may invoke.
type A2AConfig struct {
	Peers map[string]string `json:"peers,omitempty" yaml:"-"` // peer name → base URL
}

// A2ASettings configures the agent-to-agent endpoint channel.
type A2ASettings struct {
	// PublicURL is advertised in the agent card (peers call it).
	PublicURL string `json:"public_url,omitempty" yaml:"-"`
	// MaxChainDepth bounds the delegation chain including this hop
	// (default 3), structurally preventing agent ping-pong loops.
	MaxChainDepth int `json:"max_chain_depth,omitempty" yaml:"-"`
}

// KnowledgeConfig binds the digital employee to a WeKnora knowledge
// deployment. Authorization stays in AEP (the PEP tool filters per
// requester); the WeKnora API key is a deployment-scoped secret whose KB
// restriction acts as defense in depth, never as the permission model.
type KnowledgeConfig struct {
	Enabled   bool                `json:"enabled"                    yaml:"enabled"      env:"PICOCLAW_KNOWLEDGE_ENABLED"`
	BaseURL   string              `json:"base_url,omitempty"         yaml:"-"            env:"PICOCLAW_KNOWLEDGE_BASE_URL"`
	APIKey    SecureString        `json:"api_key,omitzero"           yaml:"api_key,omitempty" env:"PICOCLAW_KNOWLEDGE_API_KEY"`
	TeamKBMap map[string][]string `json:"team_kb_map,omitempty"     yaml:"-"`
	// MaxPassages bounds one tool result (default 5).
	MaxPassages int `json:"max_passages,omitempty" yaml:"-"`
}
