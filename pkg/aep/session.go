package aep

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Logger is the minimal logging surface the session manager needs. It exists
// so the package stays decoupled from any specific logger implementation.
// Nil discards output. Implementations must never receive credentials or
// tokens: only problem codes, statuses, and lifecycle events are logged.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Infof(string, ...any)  {}
func (discardLogger) Warnf(string, ...any)  {}
func (discardLogger) Errorf(string, ...any) {}

// Config describes one digital employee's AEP binding. The password lives in
// memory only and is never logged or persisted by this package.
type Config struct {
	BaseURL        string // control service, e.g. http://localhost:8080
	DeploymentID   string
	Username       string
	Password       string
	SessionID      string // optional; defaults to picoclaw-<random>
	GatewayBaseURL string // optional; defaults to metadata.modelGateway.baseUrl
	Logger         Logger
}

func (c *Config) logger() Logger {
	if c.Logger == nil {
		return discardLogger{}
	}
	return c.Logger
}

const (
	defaultHeartbeatInterval = 30 * time.Second
	minHeartbeatInterval     = 10 * time.Second
	maxHeartbeatInterval     = 5 * time.Minute
	refreshLeadFraction      = 5 // refresh once less than 1/5 of TTL remains
	modelsCacheTTL           = 10 * time.Minute
	reloginBackoff           = 30 * time.Second
)

// Manager owns the AEP session for one digital employee process: login,
// proactive refresh, heartbeat presence, and the visible model catalog. It
// is safe for concurrent use.
type Manager struct {
	cfg    Config
	client *Client
	log    Logger

	refreshMu sync.Mutex // serializes token refresh; not held while reading

	mu             sync.RWMutex
	tokens         *TokenSet
	issuedAt       time.Time
	gatewayBaseURL string
	models         []Model
	modelsAt       time.Time
	online         bool
	lastProblem    string

	// now is swappable for deterministic tests.
	now func() time.Time

	cancel   context.CancelFunc
	stopOnce sync.Once
}

// NewManager prepares a session manager. Call Start before use.
func NewManager(cfg Config) *Manager {
	if cfg.SessionID == "" {
		cfg.SessionID = "picoclaw-" + randomSuffix()
	}
	m := &Manager{
		cfg:    cfg,
		client: NewClient(cfg.BaseURL),
		log:    cfg.logger(),
		now:    time.Now,
	}
	return m
}

// randomSuffix yields 8 hex chars of process-unique session labeling.
func randomSuffix() string {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return "00000000"
	}
	return hex.EncodeToString(buf)
}

// Start logs in, resolves the gateway, fetches the model catalog, and spawns
// the refresh and heartbeat loops. It blocks until the first login succeeds
// or ctx is done; the loops then run on the returned internal context and
// are stopped by Stop or the caller cancelling ctx.
func (m *Manager) Start(ctx context.Context) error {
	loopCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	if p := m.login(loopCtx); p != nil {
		cancel()
		return p
	}
	meta, p := m.client.Metadata(loopCtx)
	if p != nil {
		m.log.Warnf("aep: metadata probe failed: %s (code=%s status=%d)", p.Title, p.Code, p.Status)
	} else if meta.ModelGateway != nil && meta.ModelGateway.BaseURL != "" {
		m.mu.Lock()
		m.gatewayBaseURL = meta.ModelGateway.BaseURL
		m.mu.Unlock()
	}
	if m.cfg.GatewayBaseURL != "" { // explicit override wins
		m.mu.Lock()
		m.gatewayBaseURL = m.cfg.GatewayBaseURL
		m.mu.Unlock()
	}
	if p := m.refreshModels(loopCtx); p != nil {
		m.log.Warnf("aep: initial model catalog fetch failed: code=%s status=%d", p.Code, p.Status)
	}
	m.log.Infof("aep: session %s established for %s (deployment %s)", m.cfg.SessionID, m.cfg.Username, m.cfg.DeploymentID)

	go m.refreshLoop(loopCtx)
	go m.heartbeatLoop(loopCtx)
	return nil
}

// Stop terminates the background loops. Start may be called again afterwards.
func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}
	})
}

// SessionID returns the stable session label used for login.
func (m *Manager) SessionID() string { return m.cfg.SessionID }

// Online reports whether the most recent heartbeat was accepted.
func (m *Manager) Online() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.online
}

// AccessToken returns a currently valid access token, refreshing first when
// the remaining lifetime drops below the refresh lead window. A refresh
// failure with lifetime still remaining returns the cached token rather than
// failing a live request.
func (m *Manager) AccessToken(ctx context.Context) (string, error) {
	return m.tokenFor(ctx, tokenKindAccess)
}

// ModelAccessToken returns a currently valid model gateway token. An account
// without model scopes yields a clear error instead of an empty token.
func (m *Manager) ModelAccessToken(ctx context.Context) (string, error) {
	return m.tokenFor(ctx, tokenKindModel)
}

type tokenKind int

const (
	tokenKindAccess tokenKind = iota
	tokenKindModel
)

func (m *Manager) tokenFor(ctx context.Context, kind tokenKind) (string, error) {
	if tok := m.cachedToken(kind); tok != "" {
		return tok, nil
	}
	if err := m.ensureFresh(ctx); err != nil {
		if tok := m.cachedToken(kind); tok != "" {
			return tok, nil
		}
		return "", err
	}
	if tok := m.cachedToken(kind); tok != "" {
		return tok, nil
	}
	if kind == tokenKindModel {
		return "", errors.New("aep: session carries no model access token (check model assignments)")
	}
	return "", errors.New("aep: no access token available")
}

// cachedToken returns the token for kind when its remaining lifetime still
// exceeds the refresh lead window.
func (m *Manager) cachedToken(kind tokenKind) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.tokens == nil {
		return ""
	}
	ttl := m.tokens.ExpiresIn
	tok := m.tokens.AccessToken
	if kind == tokenKindModel {
		ttl = m.tokens.ModelAccessExpiresIn
		tok = m.tokens.ModelAccessToken
	}
	if tok == "" {
		return ""
	}
	remaining := time.Duration(ttl)*time.Second - m.now().Sub(m.issuedAt)
	if remaining <= 0 {
		return ""
	}
	if remaining < time.Duration(ttl)*time.Second/refreshLeadFraction {
		return "" // nearly expired: caller should refresh
	}
	return tok
}

// ensureFresh refreshes tokens unless another goroutine already did.
func (m *Manager) ensureFresh(ctx context.Context) error {
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()
	// Re-check under the refresh lock: a concurrent refresher may have won.
	if tok := m.cachedToken(tokenKindAccess); tok != "" || m.hasFreshTokens() {
		return nil
	}
	if p := m.refreshOrRelogin(ctx); p != nil {
		return p
	}
	return nil
}

func (m *Manager) hasFreshTokens() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.tokens == nil {
		return false
	}
	remaining := time.Duration(m.tokens.ExpiresIn)*time.Second - m.now().Sub(m.issuedAt)
	return remaining > time.Duration(m.tokens.ExpiresIn)*time.Second/refreshLeadFraction
}

// refreshOrRelogin must be called with refreshMu held.
func (m *Manager) refreshOrRelogin(ctx context.Context) *Problem {
	m.mu.RLock()
	refreshToken := ""
	if m.tokens != nil {
		refreshToken = m.tokens.RefreshToken
	}
	m.mu.RUnlock()
	if refreshToken != "" {
		tokens, p := m.client.Refresh(ctx, refreshToken, m.cfg.SessionID)
		if p == nil {
			m.acceptTokens(tokens)
			return nil
		}
		if !isAuthProblem(p) {
			return p
		}
		m.log.Warnf("aep: refresh rejected (code=%s), falling back to password login", p.Code)
	}
	return m.login(ctx)
}

func (m *Manager) login(ctx context.Context) *Problem {
	tokens, p := m.client.PasswordLogin(ctx, m.cfg.DeploymentID, m.cfg.Username, m.cfg.Password, m.cfg.SessionID)
	if p != nil {
		m.setOffline(p)
		return p
	}
	if tokens.PasswordChangeRequired {
		m.setOffline(&Problem{Code: "PASSWORD_CHANGE_REQUIRED", Status: http.StatusForbidden, Title: "password change required"})
		return &Problem{Code: "PASSWORD_CHANGE_REQUIRED", Status: http.StatusForbidden, Title: "password change required", Detail: "digital employee password must be reset by an administrator"}
	}
	m.acceptTokens(tokens)
	return nil
}

func (m *Manager) acceptTokens(tokens *TokenSet) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokens = tokens
	m.issuedAt = m.now()
	m.lastProblem = ""
}

func (m *Manager) setOffline(p *Problem) {
	m.mu.Lock()
	m.online = false
	m.lastProblem = fmt.Sprintf("code=%s status=%d", p.Code, p.Status)
	m.mu.Unlock()
	m.log.Errorf("aep: login failed: %s", m.lastProblem)
}

func isAuthProblem(p *Problem) bool {
	return p.Status == http.StatusUnauthorized || p.Status == http.StatusForbidden
}

func (m *Manager) refreshLoop(ctx context.Context) {
	for {
		m.mu.RLock()
		ttl := 0
		if m.tokens != nil {
			ttl = m.tokens.ExpiresIn
		}
		m.mu.RUnlock()
		interval := time.Duration(ttl) * time.Second / 3
		if interval < 30*time.Second {
			interval = 30 * time.Second
		}
		if interval > 30*time.Minute {
			interval = 30 * time.Minute
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			if !m.hasFreshTokens() {
				m.refreshMu.Lock()
				if p := m.refreshOrRelogin(ctx); p != nil {
					m.log.Warnf("aep: scheduled refresh failed: code=%s status=%d", p.Code, p.Status)
				}
				m.refreshMu.Unlock()
			}
		}
	}
}

func (m *Manager) heartbeatLoop(ctx context.Context) {
	interval := defaultHeartbeatInterval
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			interval = m.beat(ctx)
		}
	}
}

// beat sends one heartbeat and returns the next interval.
func (m *Manager) beat(ctx context.Context) time.Duration {
	accessToken, err := m.AccessToken(ctx)
	if err != nil {
		m.setOnline(false, err.Error())
		return reloginBackoff
	}
	cursor := m.controlCursor()
	resp, p := m.client.Heartbeat(ctx, accessToken, cursor)
	if p != nil {
		m.setOnline(false, fmt.Sprintf("code=%s status=%d", p.Code, p.Status))
		if isAuthProblem(p) {
			m.refreshMu.Lock()
			if rp := m.refreshOrRelogin(ctx); rp != nil {
				m.log.Warnf("aep: heartbeat recovery refresh failed: code=%s status=%d", rp.Code, rp.Status)
			}
			m.refreshMu.Unlock()
		}
		return defaultHeartbeatInterval
	}
	m.setOnline(true, "")
	next := defaultHeartbeatInterval
	if resp.NextHeartbeatAfterSeconds > 0 {
		next = time.Duration(resp.NextHeartbeatAfterSeconds) * time.Second
	}
	if next < minHeartbeatInterval {
		next = minHeartbeatInterval
	}
	if next > maxHeartbeatInterval {
		next = maxHeartbeatInterval
	}
	return next
}

// controlCursor is the last consumed control-event cursor. Consumption is
// not implemented yet; empty means "nothing acknowledged".
func (m *Manager) controlCursor() string { return "" }

func (m *Manager) setOnline(ok bool, problem string) {
	m.mu.Lock()
	m.online = ok
	m.lastProblem = problem
	m.mu.Unlock()
}

// Models returns the cached visible model catalog.
func (m *Manager) Models() []Model {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Model, len(m.models))
	copy(out, m.models)
	return out
}

// RefreshModels forces a catalog reload. Expired entries are dropped; an
// empty catalog is returned with the error so callers keep the stale view.
func (m *Manager) RefreshModels(ctx context.Context) ([]Model, error) {
	if p := m.refreshModels(ctx); p != nil {
		return m.Models(), p
	}
	return m.Models(), nil
}

func (m *Manager) refreshModels(ctx context.Context) *Problem {
	accessToken, err := m.AccessToken(ctx)
	if err != nil {
		return &Problem{Title: "no access token", Detail: err.Error(), Status: http.StatusUnauthorized}
	}
	models, p := m.client.Models(ctx, accessToken)
	if p != nil {
		return p
	}
	m.mu.Lock()
	m.models = models
	m.modelsAt = m.now()
	m.mu.Unlock()
	return nil
}

// GatewayBaseURL returns the OpenAI-compatible inference gateway base URL.
func (m *Manager) GatewayBaseURL() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.gatewayBaseURL == "" {
		return "", errors.New("aep: model gateway base URL unresolved (metadata probe pending or failed)")
	}
	return m.gatewayBaseURL, nil
}
