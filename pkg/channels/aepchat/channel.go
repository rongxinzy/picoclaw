// aepchat is the web-chat channel for digital employees: authenticated AEP
// employees send messages over HTTP and poll for replies. The bearer token
// is the employee's own AEP access token — verified locally against the
// control-service JWKS — so every message carries a real enterprise identity.
// Digital-employee accounts (kind=agent) are rejected as senders, which is
// the structural bot-loop prevention for this channel.

package aepchat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sipeed/picoclaw/pkg/aep"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/identity"
	"github.com/sipeed/picoclaw/pkg/logger"
)

const (
	platformName         = "aepchat"
	pathPrefix           = "/aepchat/"
	defaultHistory       = 500
	maxTextRunes         = 16384
	maxBodyBytes         = 1 << 20
	platformSenderPrefix = platformName + ":"
)

// record is one chat message, from either the requester or the agent.
type record struct {
	Seq        int64  `json:"seq"`
	From       string `json:"from"` // "user" | "agent"
	SenderID   string `json:"senderId,omitempty"`
	SenderName string `json:"senderName,omitempty"`
	Text       string `json:"text"`
	At         string `json:"at"`
}

// Channel implements an AEP-authenticated HTTP chat channel with polling
// delivery. Conversation history is buffered in memory per chat; each chat
// gets an independent agent session (per-chat, per-sender session keys).
type Channel struct {
	*channels.BaseChannel

	deploymentID string
	auth         *aep.Authenticator
	warden       *Warden
	supervisor   *Supervisor

	mu           sync.Mutex
	history      map[string][]record
	lastSeq      map[string]int64
	historyLimit int
	messageIDs   atomic.Int64

	ctx     context.Context
	running atomic.Bool
}

// New builds the channel. It requires a configured AEP binding because the
// authenticator verifies tokens against that control service.
func New(channelName string, bc *config.Channel, settings *config.AEPChatSettings, cfg *config.Config, b *bus.MessageBus) (*Channel, error) {
	if cfg == nil || !cfg.AEP.Enabled || !cfg.AEP.IsComplete() {
		return nil, errors.New("aepchat requires a complete aep config (base_url, deployment_id, username, password)")
	}
	limit := defaultHistory
	if settings != nil && settings.HistoryLimit > 0 {
		limit = settings.HistoryLimit
	}
	allow := []string(bc.AllowFrom)
	ch := &Channel{
		BaseChannel:  channels.NewBaseChannel(channelName, bc, b, allow),
		deploymentID: cfg.AEP.DeploymentID,
		auth:         aep.NewAuthenticator(cfg.AEP.BaseURL),
		history:      make(map[string][]record),
		lastSeq:      make(map[string]int64),
		historyLimit: limit,
	}
	ch.SetOwner(ch)
	if settings != nil && settings.Warden != nil {
		supervisor, err := NewSupervisor(&cfg.AEP, settings.Warden)
		if err != nil {
			return nil, err
		}
		warden, err := NewWarden(aep.DefaultManager(), supervisor, cfg.AEP.HomeTeamID)
		if err != nil {
			supervisor.Stop()
			return nil, err
		}
		ch.supervisor = supervisor
		ch.warden = warden
	}
	return ch, nil
}

// Start marks the channel ready; it serves HTTP through the shared gateway
// mux (WebhookHandler below).
func (c *Channel) Start(ctx context.Context) error {
	c.ctx = ctx
	c.running.Store(true)
	logger.InfoCF("channels", "aepchat ready", map[string]any{
		"deployment": c.deploymentID,
		"path":       pathPrefix,
	})
	return nil
}

func (c *Channel) Stop(ctx context.Context) error {
	c.running.Store(false)
	if c.supervisor != nil {
		c.supervisor.Stop()
	}
	return nil
}

func (c *Channel) IsRunning() bool { return c.running.Load() }

// Send buffers an agent reply for polling. It is called by the channel
// manager worker; never blocks.
func (c *Channel) Send(_ context.Context, msg bus.OutboundMessage) ([]string, error) {
	if !c.running.Load() {
		return nil, channels.ErrNotRunning
	}
	if msg.ChatID == "" || msg.Content == "" {
		return nil, nil
	}
	rec := c.append(msg.ChatID, record{From: "agent", Text: msg.Content})
	return []string{fmt.Sprintf("aepchat-%d", rec.Seq)}, nil
}

// WebhookPath registers the channel subtree on the shared gateway mux.
func (c *Channel) WebhookPath() string { return pathPrefix }

// ServeHTTP self-routes the channel API:
//
//	GET  /aepchat/v1/health
//	POST /aepchat/v1/chats/{chatID}/messages   {"text": "...", "chatType": "..."}
//	GET  /aepchat/v1/chats/{chatID}/messages?after=<seq>
func (c *Channel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, pathPrefix)
	switch {
	case path == "v1/health":
		w.WriteHeader(http.StatusOK)
		return
	case strings.HasPrefix(path, "v1/chats/") && strings.HasSuffix(path, "/messages"):
		rest := strings.TrimSuffix(strings.TrimPrefix(path, "v1/chats/"), "/messages")
		chatID, err := urlPathUnescape(rest)
		if err != nil || chatID == "" || strings.Contains(chatID, "/") {
			writeError(w, http.StatusBadRequest, "INVALID_CHAT", "chat id must be a single non-empty path segment")
			return
		}
		if r.Method == http.MethodPost {
			c.handlePost(w, r, chatID)
			return
		}
		if r.Method == http.MethodGet {
			c.handleGet(w, r, chatID)
			return
		}
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "use POST or GET")
		return
	default:
		writeError(w, http.StatusNotFound, "ROUTE_NOT_FOUND", "unknown aepchat route")
	}
}

func (c *Channel) handlePost(w http.ResponseWriter, r *http.Request, chatID string) {
	if !c.running.Load() {
		writeError(w, http.StatusServiceUnavailable, "CHANNEL_NOT_RUNNING", "aepchat is not running")
		return
	}
	principal, status, problem := c.authenticate(r)
	if problem != "" {
		writeError(w, status, "UNAUTHORIZED", problem)
		return
	}
	if target, routeErr := c.route(r.Context(), principal); routeErr != nil {
		writeError(w, http.StatusServiceUnavailable, "EPHEMERAL_UNAVAILABLE", routeErr.Error())
		return
	} else if target != "" {
		c.proxyTo(w, r, target)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "unreadable body")
		return
	}
	var payload struct {
		Text     string `json:"text"`
		ChatType string `json:"chatType"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "body must be JSON with a text field")
		return
	}
	text := strings.TrimSpace(payload.Text)
	if text == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "text is required")
		return
	}
	if len([]rune(text)) > maxTextRunes {
		writeError(w, http.StatusRequestEntityTooLarge, "MESSAGE_TOO_LARGE", "text exceeds the per-message limit")
		return
	}
	chatType := strings.ToLower(payload.ChatType)
	if chatType != "group" {
		chatType = "direct"
	}
	if !c.IsAllowed(principal.UserID) {
		writeError(w, http.StatusForbidden, "SENDER_NOT_ALLOWED", "the aepchat allow_from list does not include this account")
		return
	}

	rec := c.append(chatID, record{
		From:       "user",
		SenderID:   principal.UserID,
		SenderName: principal.DisplayName,
		Text:       text,
	})

	messageID := fmt.Sprintf("aepchat-%d-%d", c.messageIDs.Add(1), rec.Seq)
	sender := bus.SenderInfo{
		Platform:    platformName,
		PlatformID:  principal.UserID,
		CanonicalID: identity.BuildCanonicalID(platformName, principal.UserID),
		Username:    principal.UserID,
		DisplayName: principal.DisplayName,
	}
	inboundCtx := bus.InboundContext{
		Channel:   c.Name(),
		ChatID:    chatID,
		ChatType:  chatType,
		SenderID:  principal.UserID,
		MessageID: messageID,
		Raw: map[string]string{
			"aep_user_id":    principal.UserID,
			"aep_session_id": principal.SessionID,
		},
	}
	c.HandleInboundContext(r.Context(), chatID, text, nil, inboundCtx, sender)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"messageId": messageID,
		"seq":       rec.Seq,
	})
}

func (c *Channel) handleGet(w http.ResponseWriter, r *http.Request, chatID string) {
	principal, status, problem := c.authenticate(r)
	if problem != "" {
		writeError(w, status, "UNAUTHORIZED", problem)
		return
	}
	if target, routeErr := c.route(r.Context(), principal); routeErr != nil {
		writeError(w, http.StatusServiceUnavailable, "EPHEMERAL_UNAVAILABLE", routeErr.Error())
		return
	} else if target != "" {
		c.proxyTo(w, r, target)
		return
	}
	after := int64(0)
	if raw := r.URL.Query().Get("after"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "after must be a non-negative integer")
			return
		}
		after = parsed
	}
	// Only the chat's participants poll: the requester must appear as the
	// sender of an earlier user record, unless the history is still empty.
	c.mu.Lock()
	all := c.history[chatID]
	c.mu.Unlock()
	if !c.participates(all, principal.UserID) {
		writeError(w, http.StatusForbidden, "NOT_PARTICIPANT", "this account has not written to the chat")
		return
	}
	var out []record
	for _, rec := range all {
		if rec.Seq > after {
			out = append(out, rec)
		}
	}
	next := after
	if n := len(all); n > 0 {
		next = all[n-1].Seq
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"messages": out, "nextAfter": next})
}

// authenticate resolves the AEP bearer principal; a rejected digital
// employee gets 403 with a stable reason.
func (c *Channel) authenticate(r *http.Request) (*aep.Principal, int, string) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return nil, http.StatusUnauthorized, "Authorization: Bearer <aep access token> is required"
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	principal, err := c.auth.Authenticate(r.Context(), c.deploymentID, token)
	if err != nil {
		if errors.Is(err, aep.ErrNotHuman) {
			return nil, http.StatusForbidden, "digital employees cannot use the chat channel"
		}
		return nil, http.StatusUnauthorized, "token verification failed"
	}
	return principal, 0, ""
}

func (c *Channel) participates(all []record, userID string) bool {
	if len(all) == 0 {
		return true // nothing secret yet; the first POST establishes the chat
	}
	for _, rec := range all {
		if rec.From == "user" && rec.SenderID == userID {
			return true
		}
	}
	return false
}

// append stores a record under a per-chat monotonic sequence and trims the
// buffer to the configured history limit.
func (c *Channel) append(chatID string, rec record) record {
	now := time.Now().UTC().Format(time.RFC3339)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastSeq[chatID]++
	rec.Seq = c.lastSeq[chatID]
	rec.At = now
	c.history[chatID] = append(c.history[chatID], rec)
	if excess := len(c.history[chatID]) - c.historyLimit; excess > 0 {
		c.history[chatID] = c.history[chatID][excess:]
	}
	return rec
}

// route applies the resident/ephemeral split when the warden is enabled.
func (c *Channel) route(ctx context.Context, principal *aep.Principal) (string, error) {
	if c.warden == nil {
		return "", nil
	}
	return c.warden.Route(ctx, principal)
}

// proxyTo forwards one chat request to an ephemeral fork verbatim: the fork
// re-authenticates the requester's token through the identical path.
func (c *Channel) proxyTo(w http.ResponseWriter, r *http.Request, target string) {
	url := target + r.URL.Path
	if r.URL.RawQuery != "" {
		url += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, url, r.Body)
	if err != nil {
		writeError(w, http.StatusBadGateway, "PROXY_FAILED", err.Error())
		return
	}
	req.Header.Set("Authorization", r.Header.Get("Authorization"))
	if contentType := r.Header.Get("Content-Type"); contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, "PROXY_FAILED", err.Error())
		return
	}
	defer resp.Body.Close()
	if contentType := resp.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func urlPathUnescape(segment string) (string, error) {
	segment = strings.ReplaceAll(segment, "%2F", "/") // reject escaped slashes
	unescaped, err := url.PathUnescape(segment)
	if err != nil {
		return "", err
	}
	return unescaped, nil
}

func writeError(w http.ResponseWriter, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "detail": detail})
}
