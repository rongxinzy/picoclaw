package aepchat

import (
	"context"
	"errors"
	"fmt"

	"github.com/sipeed/picoclaw/pkg/aep"
)

// Warden decides who converses with the resident digital employee and who is
// served by a scoped ephemeral fork:
//
//   - requesters owning the home team, or owning no team inside its subtree,
//     are peers-or-above → the resident instance answers directly;
//   - requesters whose teams lie strictly inside the home-team subtree are
//     subordinates → a per-requester ephemeral fork carries a frozen snapshot
//     of the requester's data scope, so knowledge can never cross the
//     requester's boundary regardless of how the model is persuaded.
//
// Both comparisons derive from the control plane with the resident account's
// own data_scope.read grant: the requester's own teams and the resident's
// subtree (its self retrieval context).
type Warden struct {
	manager    *aep.Manager
	homeTeamID string
	ensure     func(ctx context.Context, requester *aep.Principal, requesterCtx *aep.RetrievalContext) (string, error)
}

// NewWarden binds a warden to the resident session manager and its fork
// supervisor.
func NewWarden(manager *aep.Manager, supervisor *Supervisor, homeTeamID string) (*Warden, error) {
	if manager == nil {
		return nil, errors.New("aepchat warden requires a running AEP session")
	}
	if supervisor == nil {
		return nil, errors.New("aepchat warden requires an ephemeral supervisor")
	}
	if homeTeamID == "" {
		return nil, errors.New("aepchat warden requires aep.home_team_id")
	}
	return &Warden{
		manager: manager, homeTeamID: homeTeamID,
		ensure: func(ctx context.Context, requester *aep.Principal, requesterCtx *aep.RetrievalContext) (string, error) {
			fork, err := supervisor.Ensure(ctx, requester, requesterCtx)
			if err != nil {
				return "", err
			}
			return fork.URL(), nil
		},
	}, nil
}

// newWardenWithProvider is the test seam: the fork provider is injectable.
func newWardenWithProvider(manager *aep.Manager, homeTeamID string, ensure func(ctx context.Context, requester *aep.Principal, requesterCtx *aep.RetrievalContext) (string, error)) (*Warden, error) {
	if manager == nil {
		return nil, errors.New("aepchat warden requires a running AEP session")
	}
	if homeTeamID == "" {
		return nil, errors.New("aepchat warden requires aep.home_team_id")
	}
	if ensure == nil {
		return nil, errors.New("aepchat warden requires a fork provider")
	}
	return &Warden{manager: manager, homeTeamID: homeTeamID, ensure: ensure}, nil
}

// Route returns the proxy target for the requester: "" for the resident
// instance, or the base URL of the requester's ephemeral fork.
func (w *Warden) Route(ctx context.Context, principal *aep.Principal) (string, error) {
	requesterCtx, err := w.manager.DataScopeContext(ctx, principal.UserID)
	if err != nil {
		return "", fmt.Errorf("requester scope resolution failed: %w", err)
	}
	ownsHome := false
	inside := false
	for _, team := range requesterCtx.OwnTeamIDs {
		if team == w.homeTeamID {
			ownsHome = true
			continue
		}
		for _, visible := range w.selfSubtree(ctx) {
			if team == visible {
				inside = true
			}
		}
	}
	if ownsHome || !inside {
		return "", nil // peer or above: the resident answers directly
	}
	return w.ensure(ctx, principal, requesterCtx)
}

// selfSubtree is the resident's visible team set; resolution failures are
// treated as empty, which routes every requester to the resident — the
// conservative choice only for peers, and the fork path is unreachable when
// the subtree is unknown.
func (w *Warden) selfSubtree(ctx context.Context) []string {
	selfCtx, err := w.manager.SelfContext(ctx)
	if err != nil || selfCtx == nil {
		return nil
	}
	return selfCtx.OrgScope
}
