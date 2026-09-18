// AEP digital-employee integration: session boot and model-list injection.
// One picoclaw process is bound to one AEP account (kind=agent). The session
// manager owns login, token refresh, and heartbeat presence; the assigned
// model catalog is injected into model_list under the "aep" provider and
// every model call carries the session's rotating model access token.

package gateway

import (
	"context"
	"fmt"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
)

// aepLoggerBridge adapts the picoclaw logger to the aep package's minimal
// logging interface.
type aepLoggerBridge struct{}

func (aepLoggerBridge) Infof(format string, args ...any)  { logger.Infof(format, args...) }
func (aepLoggerBridge) Warnf(format string, args ...any)  { logger.Warnf(format, args...) }
func (aepLoggerBridge) Errorf(format string, args ...any) { logger.Errorf(format, args...) }

// startAEPSession logs the digital employee in and wires its rotating model
// token into the "aep" provider family. Failing to start is fatal: a digital
// employee without a control-plane session has no defined identity or
// authorization, so the gateway must not come up half-initialized.
func startAEPSession(cfg *config.Config) (*aep.Manager, error) {
	acfg := cfg.AEP
	if !acfg.IsComplete() {
		return nil, fmt.Errorf("aep.enabled is set but base_url, deployment_id, username, or password is missing")
	}
	manager := aep.NewManager(aep.Config{
		BaseURL:        acfg.BaseURL,
		DeploymentID:   acfg.DeploymentID,
		Username:       acfg.Username,
		Password:       acfg.Password.String(),
		SessionID:      acfg.SessionID,
		GatewayBaseURL: acfg.GatewayBaseURL,
		Logger:         aepLoggerBridge{},
	})
	if err := manager.Start(context.Background()); err != nil {
		return nil, fmt.Errorf("aep session for %s failed to start: %w", acfg.Username, err)
	}
	providers.SetAEPTokenSource(manager.ModelAccessToken)
	if err := injectAEPModels(cfg, manager); err != nil {
		providers.SetAEPTokenSource(nil)
		manager.Stop()
		return nil, fmt.Errorf("aep model injection failed: %w", err)
	}
	return manager, nil
}

// injectAEPModels mirrors the AEP-assigned model catalog into model_list as
// "aep" provider entries. AEP is the source of truth: an existing entry with
// the same model_name is replaced, never merged.
func injectAEPModels(cfg *config.Config, manager *aep.Manager) error {
	gateway, err := manager.GatewayBaseURL()
	if err != nil {
		return err
	}
	models := manager.Models()

	existing := make(map[string]int, len(cfg.ModelList))
	for i := range cfg.ModelList {
		existing[cfg.ModelList[i].ModelName] = i
	}
	injected := 0
	for _, m := range models {
		if !m.Enabled {
			continue
		}
		upstream := m.UpstreamModel
		if upstream == "" {
			upstream = m.ID
		}
		entry := &config.ModelConfig{
			ModelName: m.ID,
			Provider:  "aep",
			Model:     upstream,
			APIBase:   gateway,
			Enabled:   true,
		}
		if idx, ok := existing[m.ID]; ok {
			cfg.ModelList[idx] = entry
		} else {
			cfg.ModelList = append(cfg.ModelList, entry)
			existing[m.ID] = len(cfg.ModelList) - 1
		}
		injected++
	}
	if injected == 0 {
		logger.Warnf("aep: no enabled models are assigned to this account; model calls will fail until an assignment exists")
	} else {
		logger.Infof("aep: %d model(s) available via %s", injected, gateway)
	}

	// First-boot ergonomics: when no default model is configured, adopt the
	// deployment-flagged default (or the first enabled model) so the agent is
	// usable without hand-editing model config.
	if cfg.Agents.Defaults.GetModelName() == "" {
		chosen := ""
		for _, m := range models {
			if m.Enabled && m.IsDefault {
				chosen = m.ID
				break
			}
		}
		if chosen == "" {
			for _, m := range models {
				if m.Enabled {
					chosen = m.ID
					break
				}
			}
		}
		if chosen != "" {
			cfg.Agents.Defaults.ModelName = chosen
			logger.Infof("aep: default model set to %s", chosen)
		}
	}
	return nil
}

// refreshAEPForReload re-injects the catalog into a reload candidate config.
// The session itself survives reloads; connection changes need a restart.
func refreshAEPForReload(oldCfg, newCfg *config.Config, manager *aep.Manager) {
	if !newCfg.AEP.Enabled {
		logger.Warn("aep: disabling aep requires a gateway restart; keeping the current session")
	}
	if aepConfigChanged(oldCfg.AEP, newCfg.AEP) {
		logger.Warn("aep: connection settings changed; restart the gateway to apply them")
	}
	if _, err := manager.RefreshModels(context.Background()); err != nil {
		logger.Warnf("aep: model catalog refresh failed: %v", err)
	}
	if err := injectAEPModels(newCfg, manager); err != nil {
		logger.Warnf("aep: model re-injection failed: %v", err)
	}
}

func aepConfigChanged(a, b config.AEPConfig) bool {
	return a.BaseURL != b.BaseURL ||
		a.DeploymentID != b.DeploymentID ||
		a.Username != b.Username ||
		a.SessionID != b.SessionID ||
		a.GatewayBaseURL != b.GatewayBaseURL ||
		a.Password.String() != b.Password.String()
}
