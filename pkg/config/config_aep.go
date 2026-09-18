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
}

// IsComplete reports whether the mandatory connection fields are present.
func (c AEPConfig) IsComplete() bool {
	return c.BaseURL != "" && c.DeploymentID != "" && c.Username != "" && c.Password.String() != ""
}
