// bridge.go adapts platform IM channels (feishu, wecom) to the AEP
// digital-employee model. One Intercept call per inbound message:
//
//   - resolve the platform sender to an AEP user through identity mappings
//     (fail-closed: unmapped senders get one rejection per minute, nothing
//     reaches the agent);
//   - ask the gate (warden) whether the resident or the requester's
//     ephemeral fork serves the turn;
//   - resident turns continue in-channel with the requester identity
//     rewritten onto the inbound context (tools scope by the requester);
//   - fork turns are relayed synchronously and the reply returns through
//     the channel's own send path.
package aepgate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
)

// Default user-facing texts (Chinese-first product copy).
const (
	defaultUnmappedReply = "你的账号尚未绑定企业身份，请联系管理员完成绑定后重试。"
	unavailableReply     = "企业身份服务暂时不可用，请稍后重试。"
	relayFailureReply    = "数字员工暂时无法应答，请稍后重试。"
	forkMediaOnlyText    = "临时会话当前仅支持文本消息。"
	rejectionRateWindow  = time.Minute
)

// Action tells the channel what to do after an intercept.
type Action int

const (
	// ActionResident continues the turn in the resident process; the
	// inbound context must carry the resolved AEP identity.
	ActionResident Action = iota
	// ActionStop drops the message: the bridge fully handled it (rejection,
	// apology, or a relayed fork turn).
	ActionStop
)

// Verdict is the intercept outcome.
type Verdict struct {
	Action      Action
	AEPUserID   string // valid for ActionResident
	DisplayName string // valid for ActionResident
}

// InterceptRequest describes one inbound platform message.
type InterceptRequest struct {
	Platform         string // "feishu" | "wecom"
	PlatformSenderID string // platform-native sender ID
	ChatID           string
	ChatType         string // direct | group
	Text             string
	DisplayName      string // platform display name, if known
	HasMedia         bool
}

// Bridge is per-channel: it owns the channel's identity resolver, while the
// warden/supervisor gate is process-wide and reference-counted.
type Bridge struct {
	resolver      *IdentityResolver
	gate          *Gate
	relay         *RelayClient
	unmappedReply string
	limiter       *rejectionLimiter
}

// NewBridge builds the channel's AEP integration. The manager is the
// resident session (identity.read); settings nil means no integration and
// callers should not build a bridge at all.
func NewBridge(manager *aep.Manager, cfg *config.Config, settings *config.AEPChannelSettings) (*Bridge, error) {
	if settings == nil {
		return nil, fmt.Errorf("aep bridge requires channel aep settings")
	}
	resolver, err := NewIdentityResolver(manager, settings.IdentitySourceID)
	if err != nil {
		return nil, err
	}
	gate, err := AcquireGate(cfg, settings.Warden)
	if err != nil {
		return nil, err
	}
	unmapped := settings.UnmappedReply
	if unmapped == "" {
		unmapped = defaultUnmappedReply
	}
	return &Bridge{
		resolver:      resolver,
		gate:          gate,
		relay:         NewRelayClient(),
		unmappedReply: unmapped,
		limiter:       newRejectionLimiter(),
	}, nil
}

// Release drops the shared-gate reference held for this channel.
func (b *Bridge) Release() {
	if b == nil {
		return
	}
	b.gate.Release()
}

// Intercept runs the identity/warden pipeline for one inbound message. The
// reply function sends text back through the channel's own connection; it
// must not block.
func (b *Bridge) Intercept(ctx context.Context, req InterceptRequest, reply func(string) error) Verdict {
	stop := Verdict{Action: ActionStop}

	aepUserID, err := b.resolver.Resolve(ctx, req.PlatformSenderID)
	if err != nil {
		// Control-plane outage: cannot decide identity → fail closed with an
		// apology (rate-limited), never a guess.
		logger.ErrorCF(req.Platform, "identity resolution failed; failing closed", map[string]any{
			"error": err.Error(),
		})
		if b.limiter.Allow(req.ChatID, req.PlatformSenderID) {
			_ = reply(unavailableReply)
		}
		return stop
	}
	if aepUserID == "" {
		if b.limiter.Allow(req.ChatID, req.PlatformSenderID) {
			_ = reply(b.unmappedReply)
		}
		return stop
	}

	displayName := req.DisplayName
	if displayName == "" {
		displayName = aepUserID
	}
	principal := &aep.Principal{UserID: aepUserID, DisplayName: displayName, Kind: "human"}

	fork, err := b.gate.Route(ctx, principal)
	if err != nil {
		logger.ErrorCF(req.Platform, "warden routing failed; failing closed", map[string]any{
			"requester": aepUserID, "error": err.Error(),
		})
		_ = reply(relayFailureReply)
		return stop
	}
	if fork == nil {
		return Verdict{Action: ActionResident, AEPUserID: aepUserID, DisplayName: displayName}
	}

	if req.HasMedia {
		// Forks relay text synchronously; media turns stay resident-side in
		// scope terms but the frozen-scope guarantee only holds via relay.
		_ = reply(forkMediaOnlyText)
		return stop
	}
	text, err := b.relay.Call(ctx, fork, RelayTurn{
		ChatID:               req.Platform + ":" + req.ChatID,
		ChatType:             req.ChatType,
		RequesterUserID:      aepUserID,
		RequesterDisplayName: displayName,
		Text:                 req.Text,
		SourceChannel:        req.Platform,
	})
	if err != nil {
		logger.ErrorCF(req.Platform, "fork relay failed", map[string]any{
			"requester": aepUserID, "error": err.Error(),
		})
		_ = reply(relayFailureReply)
		return stop
	}
	if text != "" {
		if err := reply(text); err != nil {
			logger.WarnCF(req.Platform, "fork relay reply delivery failed", map[string]any{"error": err.Error()})
		}
	}
	return stop
}

// rejectionLimiter caps rejection texts at one per chat+sender per window;
// unmapped or outage senders must not flood a group.
type rejectionLimiter struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func newRejectionLimiter() *rejectionLimiter {
	return &rejectionLimiter{last: make(map[string]time.Time)}
}

func (l *rejectionLimiter) Allow(chatID, senderID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := chatID + "\x00" + senderID
	if time.Since(l.last[key]) < rejectionRateWindow {
		return false
	}
	l.last[key] = time.Now()
	return true
}
