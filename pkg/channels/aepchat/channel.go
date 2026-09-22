// aepchat is the web-chat channel for digital employees: authenticated AEP
// employees send messages over HTTP and poll for replies. The bearer token
// is the employee's own AEP access token — verified locally against the
// control-service JWKS — so every message carries a real enterprise identity.
// Digital-employee accounts (kind=agent) are rejected as senders, which is
// the structural bot-loop prevention for this channel.

package aepchat

import (
	"context"
	"crypto/subtle"
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
	"github.com/sipeed/picoclaw/pkg/channels/aepgate"
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
	// relayTurnTimeout bounds one relayed turn; the resident turns it into an
	// apology to the requester.
	relayTurnTimeout = 120 * time.Second
	// relayMaxWaiters caps queued relay turns per chat.
	relayMaxWaiters = 16
	// SSE stream cadence and buffering.
	sseHeartbeat     = 20 * time.Second
	sseSubscriberBuf = 64
	evKindRecord     = "record"
	evKindDraft      = "draft"
	evKindReconnect  = "reconnect"
	// relayResultCache bounds how many finished relay turns (by turnId)
	// stay replayable for idempotent supervisor retries.
	relayResultCache = 64
	// relayTurnIDMax bounds a caller-supplied idempotency key.
	relayTurnIDMax = 128
	// metadataKeyOutboundKind mirrors the agent package's final marker.
	metadataKeyOutboundKind = "outbound_kind"
	outboundKindFinal       = "final"
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

// relayTurnResult is the remembered outcome of one finished relay turn,
// replayed verbatim for a repeated idempotency key.
type relayTurnResult struct {
	reply string
	seq   int64
}

// streamEvent is one SSE event delivered to live subscribers. data is
// pre-marshaled JSON so every subscriber byte-shares the payload.
type streamEvent struct {
	kind string
	data []byte
}

// Channel implements an AEP-authenticated HTTP chat channel with polling
// delivery. Conversation history is buffered in memory per chat; each chat
// gets an independent agent session (per-chat, per-sender session keys).
type Channel struct {
	*channels.BaseChannel

	deploymentID string
	auth         *aep.Authenticator
	gate         *aepgate.Gate

	mu           sync.Mutex
	history      map[string][]record
	lastSeq      map[string]int64
	historyLimit int
	messageIDs   atomic.Int64

	relaySecret  string
	relayMu      sync.Mutex
	relayWaiters map[string][]chan string // chat ID → FIFO of pending relay turns
	relaySkips   map[string]int           // finals owed by abandoned turns
	// relayResults remembers finished turns by idempotency key (FIFO
	// eviction past relayResultCache) so a supervisor retry replays the
	// outcome instead of re-executing the turn.
	relayResults     map[string]relayTurnResult
	relayResultOrder []string

	streaming   bool
	streamMu    sync.Mutex
	drafts      map[string]string             // chat ID → accumulated draft text
	subscribers map[string][]chan streamEvent // chat ID → live SSE subscribers
	// relayTimeout bounds one relayed turn; swappable in tests.
	relayTimeout time.Duration

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
		relayWaiters: make(map[string][]chan string),
		relaySkips:   make(map[string]int),
		relayResults: make(map[string]relayTurnResult),
		drafts:       make(map[string]string),
		subscribers:  make(map[string][]chan streamEvent),
		streaming:    settings != nil && settings.Streaming.Enabled,
	}
	if settings != nil {
		ch.relaySecret = settings.RelaySecret.String()
	}
	ch.SetOwner(ch)
	if settings != nil {
		gate, err := aepgate.AcquireGate(cfg, settings.Warden)
		if err != nil {
			return nil, err
		}
		ch.gate = gate
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
	c.gate.Release()
	return nil
}

func (c *Channel) IsRunning() bool { return c.running.Load() }

// Send buffers an agent reply for polling. It is called by the channel
// manager worker; never blocks. A final reply also wakes the head relay
// waiter when one is queued for the chat.
func (c *Channel) Send(_ context.Context, msg bus.OutboundMessage) ([]string, error) {
	if !c.running.Load() {
		return nil, channels.ErrNotRunning
	}
	if msg.ChatID == "" || msg.Content == "" {
		return nil, nil
	}
	rec := c.append(msg.ChatID, record{From: "agent", Text: msg.Content})
	if msg.Context.Raw[metadataKeyOutboundKind] == outboundKindFinal {
		c.deliverRelay(msg.ChatID, msg.Content)
	}
	return []string{fmt.Sprintf("aepchat-%d", rec.Seq)}, nil
}

// WebhookPath registers the channel subtree on the shared gateway mux.
func (c *Channel) WebhookPath() string { return pathPrefix }

// BeginStream implements channels.StreamingCapable: the agent loop streams
// assistant replies through bus.Streamer when the channel config enables it.
func (c *Channel) BeginStream(_ context.Context, chatID string) (channels.Streamer, error) {
	if !c.streaming {
		return nil, errors.New("aepchat streaming disabled in config")
	}
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}
	return &aepchatStreamer{channel: c, chatID: chatID}, nil
}

// aepchatStreamer feeds the SSE layer: Update publishes accumulated drafts,
// Finalize commits the authoritative record.
type aepchatStreamer struct {
	channel *Channel
	chatID  string
}

func (s *aepchatStreamer) Update(_ context.Context, accumulated string) error {
	s.channel.publishDraft(s.chatID, accumulated)
	return nil
}

// Finalize commits the final record. The channel manager suppresses the
// normal outbound Send for streamed finals, so this path must reproduce
// Send's full semantics: append the record and wake any queued relay
// waiter.
func (s *aepchatStreamer) Finalize(_ context.Context, content string) error {
	s.channel.append(s.chatID, record{From: "agent", Text: content})
	s.channel.publishDraft(s.chatID, "")
	s.channel.deliverRelay(s.chatID, content)
	return nil
}

func (s *aepchatStreamer) Cancel(_ context.Context) {
	s.channel.publishDraft(s.chatID, "")
}

// ServeHTTP self-routes the channel API:
//
//	GET  /aepchat/v1/health
//	POST /aepchat/v1/chats/{chatID}/messages   {"text": "...", "chatType": "..."}
//	GET  /aepchat/v1/chats/{chatID}/messages?after=<seq>
//	GET  /aepchat/v1/chats/{chatID}/stream?after=<seq>   (SSE)
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
	case strings.HasPrefix(path, "v1/chats/") && strings.HasSuffix(path, "/stream"):
		rest := strings.TrimSuffix(strings.TrimPrefix(path, "v1/chats/"), "/stream")
		chatID, err := urlPathUnescape(rest)
		if err != nil || chatID == "" || strings.Contains(chatID, "/") {
			writeError(w, http.StatusBadRequest, "INVALID_CHAT", "chat id must be a single non-empty path segment")
			return
		}
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "use GET")
			return
		}
		c.handleStream(w, r, chatID)
		return
	case path == "v1/relay/turns":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "use POST")
			return
		}
		c.handleRelayTurn(w, r)
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

// handleStream serves one chat as SSE: it replays records newer than the
// `after` cursor, then streams live records and assistant drafts. The
// subscription and history snapshot happen under one streamMu section so no
// event can fall between replay and live phases.
func (c *Channel) handleStream(w http.ResponseWriter, r *http.Request, chatID string) {
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
	after := int64(0)
	if raw := r.URL.Query().Get("after"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "after must be a non-negative integer")
			return
		}
		after = parsed
	}
	c.mu.Lock()
	participant := c.participates(c.history[chatID], principal.UserID)
	c.mu.Unlock()
	if !participant {
		writeError(w, http.StatusForbidden, "NOT_PARTICIPANT", "this account has not written to the chat")
		return
	}

	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	writeEvent := func(kind string, data []byte) bool {
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", kind, data); err != nil {
			return false
		}
		return rc.Flush() == nil
	}

	// Subscribe and snapshot atomically: publishers take streamMu to
	// broadcast, so everything appended after this section lands in the
	// subscriber channel, never between replay and live.
	sub := make(chan streamEvent, sseSubscriberBuf)
	c.streamMu.Lock()
	c.subscribers[chatID] = append(c.subscribers[chatID], sub)
	c.mu.Lock()
	snapshot := append([]record(nil), c.history[chatID]...)
	draft := c.drafts[chatID]
	c.mu.Unlock()
	c.streamMu.Unlock()
	defer func() {
		c.streamMu.Lock()
		subs := c.subscribers[chatID]
		for i, s := range subs {
			if s == sub {
				c.subscribers[chatID] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		c.streamMu.Unlock()
	}()

	for _, rec := range snapshot {
		if rec.Seq > after {
			if !writeEvent(evKindRecord, mustMarshal(rec)) {
				return
			}
		}
	}
	if draft != "" {
		if !writeEvent(evKindDraft, mustMarshal(map[string]string{"text": draft})) {
			return
		}
	}

	heartbeat := time.NewTicker(sseHeartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case ev := <-sub:
			if !writeEvent(ev.kind, ev.data) {
				return
			}
			if ev.kind == evKindReconnect {
				return
			}
		case <-heartbeat.C:
			if _, err := w.Write([]byte(": ping\n\n")); err != nil {
				return
			}
			if rc.Flush() != nil {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return data
}

// handleRelayTurn executes one turn on behalf of a resident gateway that
// routed a subordinate's IM message to this fork. The relay secret (not a
// human token) authenticates the caller; the requester identity arrives as
// data already resolved by the resident's identity-mapping bridge, so the
// fork's tools scope by the original requester.
//
// The reply is the turn's final outbound message. Waiters queue FIFO per
// chat; per-session turn serialization keeps finals in arrival order, so
// each waiter receives exactly its own turn's reply.
func (c *Channel) handleRelayTurn(w http.ResponseWriter, r *http.Request) {
	if !c.running.Load() {
		writeError(w, http.StatusServiceUnavailable, "CHANNEL_NOT_RUNNING", "aepchat is not running")
		return
	}
	if c.relaySecret == "" {
		writeError(w, http.StatusServiceUnavailable, "RELAY_DISABLED", "this instance accepts no relayed turns")
		return
	}
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") ||
		subtle.ConstantTimeCompare([]byte(strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))), []byte(c.relaySecret)) != 1 {
		writeError(w, http.StatusUnauthorized, "RELAY_UNAUTHORIZED", "relay token mismatch")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "unreadable body")
		return
	}
	var payload struct {
		ChatID               string `json:"chatID"`
		ChatType             string `json:"chatType"`
		RequesterUserID      string `json:"requesterUserID"`
		RequesterDisplayName string `json:"requesterDisplayName"`
		Text                 string `json:"text"`
		SourceChannel        string `json:"sourceChannel"`
		TurnID               string `json:"turnId"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "body must be a JSON relay turn")
		return
	}
	payload.ChatID = strings.TrimSpace(payload.ChatID)
	payload.RequesterUserID = strings.TrimSpace(payload.RequesterUserID)
	payload.TurnID = strings.TrimSpace(payload.TurnID)
	text := strings.TrimSpace(payload.Text)
	if payload.ChatID == "" || strings.Contains(payload.ChatID, "/") || payload.RequesterUserID == "" || text == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "chatID, requesterUserID, and text are required")
		return
	}
	if len(payload.TurnID) > relayTurnIDMax {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "turnId exceeds the length limit")
		return
	}
	if cached, ok := c.recallRelayResult(payload.TurnID); ok {
		// Idempotent retry: replay the remembered outcome, no re-execution.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"reply": cached.reply, "seq": cached.seq})
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

	waiter := c.registerRelayWaiter(payload.ChatID)
	if waiter == nil {
		writeError(w, http.StatusServiceUnavailable, "RELAY_BUSY", "too many queued relay turns for this chat")
		return
	}

	rec := c.append(payload.ChatID, record{
		From:       "user",
		SenderID:   payload.RequesterUserID,
		SenderName: payload.RequesterDisplayName,
		Text:       text,
	})
	messageID := fmt.Sprintf("aepchat-relay-%d-%d", c.messageIDs.Add(1), rec.Seq)
	displayName := payload.RequesterDisplayName
	if displayName == "" {
		displayName = payload.RequesterUserID
	}
	sender := bus.SenderInfo{
		Platform:    platformName,
		PlatformID:  payload.RequesterUserID,
		CanonicalID: identity.BuildCanonicalID(platformName, payload.RequesterUserID),
		Username:    payload.RequesterUserID,
		DisplayName: displayName,
	}
	inboundCtx := bus.InboundContext{
		Channel:   c.Name(),
		ChatID:    payload.ChatID,
		ChatType:  chatType,
		SenderID:  payload.RequesterUserID,
		MessageID: messageID,
		Raw: map[string]string{
			"aep_user_id":  payload.RequesterUserID,
			"relay":        "1",
			"relay_source": payload.SourceChannel,
		},
	}
	c.HandleInboundContext(r.Context(), payload.ChatID, text, nil, inboundCtx, sender)

	relayTimeout := relayTurnTimeout
	if c.relayTimeout > 0 {
		relayTimeout = c.relayTimeout
	}
	timer := time.NewTimer(relayTimeout)
	defer timer.Stop()
	select {
	case reply := <-waiter:
		c.rememberRelayResult(payload.TurnID, reply, rec.Seq)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"reply": reply, "seq": rec.Seq})
		return
	case <-timer.C:
	case <-r.Context().Done():
	}
	// Abandon: if the waiter is still queued, the turn's eventual final must
	// be discarded (skip counter); if delivery raced with the timeout, the
	// non-blocking read below still wins.
	c.abandonRelayWaiter(payload.ChatID, waiter)
	select {
	case reply := <-waiter:
		c.rememberRelayResult(payload.TurnID, reply, rec.Seq)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"reply": reply, "seq": rec.Seq})
		return
	default:
	}
	writeError(w, http.StatusGatewayTimeout, "RELAY_TURN_TIMEOUT", "the fork did not finish the turn in time")
}

// registerRelayWaiter appends one FIFO waiter for the chat, rejecting the
// call (nil) when the queue is saturated.
func (c *Channel) registerRelayWaiter(chatID string) chan string {
	waiter := make(chan string, 1)
	c.relayMu.Lock()
	defer c.relayMu.Unlock()
	if len(c.relayWaiters[chatID]) >= relayMaxWaiters {
		return nil
	}
	c.relayWaiters[chatID] = append(c.relayWaiters[chatID], waiter)
	return waiter
}

// recallRelayResult returns the remembered outcome of a prior relay turn,
// if any. Remembered outcomes let a supervisor retry a turn (network-level
// replay) without re-executing it.
func (c *Channel) recallRelayResult(turnID string) (relayTurnResult, bool) {
	if turnID == "" {
		return relayTurnResult{}, false
	}
	c.relayMu.Lock()
	defer c.relayMu.Unlock()
	res, ok := c.relayResults[turnID]
	return res, ok
}

// rememberRelayResult stores a finished turn's outcome, evicting the oldest
// remembered turn beyond the cache bound. Timed-out turns are never
// remembered: a retry re-executes (at-least-once on the timeout path).
func (c *Channel) rememberRelayResult(turnID, reply string, seq int64) {
	if turnID == "" {
		return
	}
	c.relayMu.Lock()
	defer c.relayMu.Unlock()
	if _, ok := c.relayResults[turnID]; ok {
		return
	}
	c.relayResults[turnID] = relayTurnResult{reply: reply, seq: seq}
	c.relayResultOrder = append(c.relayResultOrder, turnID)
	for len(c.relayResultOrder) > relayResultCache {
		oldest := c.relayResultOrder[0]
		c.relayResultOrder = c.relayResultOrder[1:]
		delete(c.relayResults, oldest)
	}
}

// abandonRelayWaiter removes a timed-out waiter; if it was still queued, the
// pending final is owed to nobody and the next final must be skipped.
func (c *Channel) abandonRelayWaiter(chatID string, waiter chan string) {
	c.relayMu.Lock()
	defer c.relayMu.Unlock()
	queue := c.relayWaiters[chatID]
	for i, queued := range queue {
		if queued == waiter {
			c.relayWaiters[chatID] = append(queue[:i], queue[i+1:]...)
			c.relaySkips[chatID]++
			return
		}
	}
}

// deliverRelay hands one final reply to the head relay waiter, consuming a
// skip first when an abandoned turn is still owed a discard.
func (c *Channel) deliverRelay(chatID, content string) {
	c.relayMu.Lock()
	if c.relaySkips[chatID] > 0 {
		c.relaySkips[chatID]--
		c.relayMu.Unlock()
		return
	}
	var waiter chan string
	if queue := c.relayWaiters[chatID]; len(queue) > 0 {
		waiter = queue[0]
		c.relayWaiters[chatID] = queue[1:]
	}
	c.relayMu.Unlock()
	if waiter == nil {
		return
	}
	select {
	case waiter <- content:
	default: // waiter abandoned between pop and send; the value is its own
	}
}

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
	c.lastSeq[chatID]++
	rec.Seq = c.lastSeq[chatID]
	rec.At = now
	c.history[chatID] = append(c.history[chatID], rec)
	if excess := len(c.history[chatID]) - c.historyLimit; excess > 0 {
		c.history[chatID] = c.history[chatID][excess:]
	}
	c.mu.Unlock()
	c.publishRecord(chatID, rec)
	return rec
}

// publishRecord broadcasts one committed record to the chat's SSE
// subscribers. Callers must not hold c.mu (lock order: c.mu is always
// released before streamMu).
func (c *Channel) publishRecord(chatID string, rec record) {
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	c.broadcast(chatID, streamEvent{kind: evKindRecord, data: data})
}

// publishDraft stores the accumulated assistant draft (empty clears it) and
// broadcasts the full-state text; clients replace, not append.
func (c *Channel) publishDraft(chatID, text string) {
	c.streamMu.Lock()
	if strings.TrimSpace(text) == "" {
		delete(c.drafts, chatID)
	} else {
		c.drafts[chatID] = text
	}
	c.streamMu.Unlock()
	data, _ := json.Marshal(map[string]string{"text": text})
	c.broadcast(chatID, streamEvent{kind: evKindDraft, data: data})
}

// broadcast delivers an event to every subscriber. Publishing is serialized
// by streamMu; a slow consumer overflows its buffer and is forced onto a
// clean reconnect instead of silently missing records.
func (c *Channel) broadcast(chatID string, ev streamEvent) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	for _, sub := range c.subscribers[chatID] {
		select {
		case sub <- ev:
		default:
			select {
			case <-sub: // drop the oldest queued event
			default:
			}
			sub <- streamEvent{kind: evKindReconnect, data: []byte("{}")}
		}
	}
}

// route applies the resident/ephemeral split when the warden is enabled.
func (c *Channel) route(ctx context.Context, principal *aep.Principal) (string, error) {
	if c.gate == nil {
		return "", nil
	}
	target, err := c.gate.Route(ctx, principal)
	if err != nil {
		return "", err
	}
	if target == nil {
		return "", nil
	}
	return target.URL(), nil
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
	for _, hk := range []string{"Cache-Control", "X-Accel-Buffering"} {
		if v := resp.Header.Get(hk); v != "" {
			w.Header().Set(hk, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		// SSE pass-through: flush every chunk so fork-streamed drafts cross
		// the resident proxy without buffering.
		rc := http.NewResponseController(w)
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				if _, werr := w.Write(buf[:n]); werr != nil {
					return
				}
				_ = rc.Flush()
			}
			if err != nil {
				return
			}
		}
	}
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
