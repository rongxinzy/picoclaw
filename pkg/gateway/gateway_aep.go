// AEP digital-employee integration: gateway lifecycle hooks. Session boot
// and model-list injection live in pkg/providers (shared with the one-shot
// agent CLI); this file only handles reload semantics.

package gateway

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
)

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
	if err := providers.InjectAEPModels(newCfg, manager); err != nil {
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
