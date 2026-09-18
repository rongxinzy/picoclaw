package providers

import (
	"context"
	"sync"
)

// aepTokenSource supplies the current AEP model access token to every "aep"
// provider instance. The gateway registers it when the AEP session manager
// starts (see pkg/gateway/gateway_aep.go); without a session, creating an
// "aep" provider fails fast instead of sending unauthenticated traffic.
var (
	aepTokenSourceMu sync.RWMutex
	aepTokenSource   func(ctx context.Context) (string, error)
)

// SetAEPTokenSource installs (or replaces, when nil) the per-request token
// resolver for the "aep" protocol. The gateway calls this once per session
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
