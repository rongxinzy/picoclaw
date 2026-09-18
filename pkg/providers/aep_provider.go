package providers

import (
	"context"
	"fmt"
	"sync"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
)

// aepTokenSource supplies the current AEP model access token to every "aep"
// provider instance. The gateway (or one-shot agent CLI) registers it when
// the AEP session manager starts; without a session, creating an "aep"
// provider fails fast instead of sending unauthenticated traffic.
var (
	aepTokenSourceMu sync.RWMutex
	aepTokenSource   func(ctx context.Context) (string, error)
)

// SetAEPTokenSource installs (or replaces, when nil) the per-request token
// resolver for the "aep" protocol. Callers register it once per session
// manager lifecycle.
func SetAEPTokenSource(ts func(ctx context.Context) (string, error)) {
	aepTokenSourceMu.Lock()
	defer aepTokenSourceMu.Unlock()
	aepTokenSource = ts
}

func getAEPTokenSource() func(ctx context.Context) (string, error) {
	aepTokenSourceMu.RLock()
	defer aepTokenSourceMu.RUnlock()
	return aepTokenSource
}

// aepLoggerBridge adapts the picoclaw logger to the aep package's minimal
// logging interface.
type aepLoggerBridge struct{}

func (aepLoggerBridge) Infof(format string, args ...any)  { logger.Infof(format, args...) }
func (aepLoggerBridge) Warnf(format string, args ...any)  { logger.Warnf(format, args...) }
func (aepLoggerBridge) Errorf(format string, args ...any) { logger.Errorf(format, args...) }

// StartAEPSession logs the digital employee in, wires its rotating model
// token into the "aep" provider family, and injects the assigned model
// catalog into cfg. Shared by the gateway boot path and the one-shot agent
// CLI. Failing to start is fatal: a digital employee without a control-plane
// session has no defined identity or authorization, so the caller must not
// continue half-initialized.
func StartAEPSession(cfg *config.Config) (*aep.Manager, error) {
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
	SetAEPTokenSource(manager.ModelAccessToken)
	if err := InjectAEPModels(cfg, manager); err != nil {
		SetAEPTokenSource(nil)
		manager.Stop()
		return nil, fmt.Errorf("aep model injection failed: %w", err)
	}
	return manager, nil
}

// InjectAEPModels mirrors the AEP-assigned model catalog into model_list as
// "aep" provider entries. AEP is the source of truth: an existing entry with
// the same model_name is replaced, never merged.
func InjectAEPModels(cfg *config.Config, manager *aep.Manager) error {
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
		// The wire model name is the AEP model id; the gateway owns any
		// upstream-model rewrite (Model.UpstreamModel is its input, not the
		// client's concern).
		entry := &config.ModelConfig{
			ModelName: m.ID,
			Provider:  "aep",
			Model:     m.ID,
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
