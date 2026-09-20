// identity.go resolves platform-native sender IDs (a Feishu open_id, a WeCom
// userid) to AEP platform users through the control plane's identity
// mappings. The mapping table is the single source of truth: an unmapped
// sender has no enterprise identity and is rejected fail-closed.
package aepgate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/aep"
)

const (
	// identityCacheTTL bounds how long a resolved mapping is served.
	identityCacheTTL = 60 * time.Second
	// identityNegativeTTL bounds how often an unmapped sender may trigger a
	// full mapping reload.
	identityNegativeTTL = 15 * time.Second
)

// IdentityResolver caches one identity source's user mappings and answers
// external-ID → local-user lookups. Any control-plane failure is returned as
// an error so callers can fail closed instead of treating an outage as "no
// mapping".
type IdentityResolver struct {
	manager  *aep.Manager
	sourceID string

	mu       sync.Mutex
	entries  map[string]string // external ID → local user ID (active only)
	loadedAt time.Time
}

// NewIdentityResolver binds a resolver to the resident session manager; the
// session's role must carry identity.read.
func NewIdentityResolver(manager *aep.Manager, sourceID string) (*IdentityResolver, error) {
	if manager == nil {
		return nil, errors.New("identity resolver requires a running AEP session")
	}
	if sourceID == "" {
		return nil, errors.New("identity resolver requires aep.identity_source_id")
	}
	return &IdentityResolver{manager: manager, sourceID: sourceID}, nil
}

// Resolve returns the AEP user ID for an external sender ID, or "" when the
// sender has no active mapping. Control-plane errors are returned non-nil:
// the caller must treat them as "cannot decide" and fail closed.
func (r *IdentityResolver) Resolve(ctx context.Context, externalID string) (string, error) {
	if externalID == "" {
		return "", nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.entries[externalID]; ok && time.Since(r.loadedAt) < identityCacheTTL {
		return id, nil
	}
	// Serve fresh negatives without reloading: a burst of unmapped senders
	// triggers at most one reload per negative TTL window.
	if r.entries != nil && time.Since(r.loadedAt) < identityNegativeTTL {
		return "", nil
	}
	entries, err := r.load(ctx)
	if err != nil {
		return "", err
	}
	r.entries = entries
	r.loadedAt = time.Now()
	return entries[externalID], nil
}

// load pages through the source's user mappings, keeping active bindings
// only. Disabled mappings resolve as unmapped.
func (r *IdentityResolver) load(ctx context.Context) (map[string]string, error) {
	entries := make(map[string]string)
	cursor := ""
	for {
		mappings, next, err := r.manager.IdentityMappings(ctx, r.sourceID, cursor)
		if err != nil {
			if p, ok := errAsProblem(err); ok && (p.Status == 403 || p.Code == "ACCESS_DENIED") {
				return nil, fmt.Errorf("identity mapping listing denied: grant identity.read to the resident agent role (%v)", p)
			}
			return nil, fmt.Errorf("identity mapping listing failed: %w", err)
		}
		for _, mapping := range mappings {
			if mapping.ExternalSubjectType != "user" || mapping.Status != "active" {
				continue
			}
			if mapping.LocalSubjectID != "" && mapping.ExternalID != "" {
				entries[mapping.ExternalID] = mapping.LocalSubjectID
			}
		}
		if next == "" {
			return entries, nil
		}
		cursor = next
	}
}

// errAsProblem unwraps control-plane problems carried as errors.
func errAsProblem(err error) (*aep.Problem, bool) {
	var p *aep.Problem
	if errors.As(err, &p) {
		return p, true
	}
	return nil, false
}
