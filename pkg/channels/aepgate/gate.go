// gate.go holds the process-wide resident/ephemeral routing pair. IM
// channels (feishu, wecom) and aepchat share one supervisor session and one
// fork pool: forks are keyed by requester across channels, and a second
// supervisor would double-login and collide on the child port range.
package aepgate

import (
	"context"
	"sync"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/config"
)

// Gate is the shared warden/supervisor pair. A nil *Gate routes everyone to
// the resident instance (identity-only integration without forking).
type Gate struct {
	supervisor *Supervisor
	warden     *Warden

	mu   sync.Mutex
	refs int
}

var (
	gateMu sync.Mutex
	gate   *Gate
)

// AcquireGate returns the process-wide gate, constructing it on first use,
// and registers one reference. A nil warden settings yields a nil gate. The
// first constructor's settings win; later acquires reuse the live pair.
func AcquireGate(cfg *config.Config, ws *config.WardenSettings) (*Gate, error) {
	if ws == nil {
		return nil, nil
	}
	gateMu.Lock()
	defer gateMu.Unlock()
	if gate == nil {
		supervisor, err := NewSupervisor(cfg, ws)
		if err != nil {
			return nil, err
		}
		warden, err := NewWarden(aep.DefaultManager(), supervisor, cfg.AEP.HomeTeamID)
		if err != nil {
			supervisor.Stop()
			return nil, err
		}
		gate = &Gate{supervisor: supervisor, warden: warden}
	}
	gate.refs++
	return gate, nil
}

// Release drops one reference; the last release stops the supervisor.
func (g *Gate) Release() {
	if g == nil {
		return
	}
	gateMu.Lock()
	defer gateMu.Unlock()
	if gate != g {
		return // a newer gate replaced this one; already torn down
	}
	g.refs--
	if g.refs <= 0 {
		gate = nil
		g.supervisor.Stop()
	}
}

// Route resolves the fork serving the requester: nil means the resident
// instance answers directly. A nil gate always routes to the resident.
func (g *Gate) Route(ctx context.Context, principal *aep.Principal) (*fork, error) {
	if g == nil {
		return nil, nil
	}
	return g.warden.Route(ctx, principal)
}
