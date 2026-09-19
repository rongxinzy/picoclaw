// Package aep integrates picoclaw with an Agent Enterprise Protocol (AEP)
// control service. A digital employee is an AEP account (kind=agent); this
// package owns the session lifecycle for one such account: password login,
// token refresh, heartbeat-driven presence, and model discovery through the
// AEP model gateway.
package aep

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// protocolVersion is sent as X-AEP-Protocol-Version on every versioned
	// request. The value is fixed by the AEP v1 contract.
	protocolVersion = "1.0"

	defaultTimeout = 30 * time.Second
	maxBodySize    = 8 << 20 // 8 MiB, generous for model catalogs
)

// TokenSet mirrors the session structure returned by password login and
// refresh. A returned refresh token replaces the previous one.
type TokenSet struct {
	AccessToken            string `json:"accessToken"`
	RefreshToken           string `json:"refreshToken"`
	ModelAccessToken       string `json:"modelAccessToken"`
	TokenType              string `json:"tokenType"`
	ExpiresIn              int    `json:"expiresIn"`
	ModelAccessExpiresIn   int    `json:"modelAccessExpiresIn"`
	DeploymentID           string `json:"deploymentId"`
	SessionID              string `json:"sessionId"`
	PasswordChangeRequired bool   `json:"passwordChangeRequired"`
}

// Model is one entry of the AEP model catalog visible to the account.
type Model struct {
	ID            string   `json:"id"`
	DisplayName   string   `json:"displayName"`
	SourceType    string   `json:"sourceType"`
	Protocol      string   `json:"protocol"`
	Endpoint      string   `json:"endpoint"`
	UpstreamModel string   `json:"upstreamModel"`
	Capabilities  []string `json:"capabilities"`
	ContextWindow int      `json:"contextWindow"`
	IsDefault     bool     `json:"isDefault"`
	Enabled       bool     `json:"enabled"`
}

// Metadata is the subset of GET /metadata the runtime depends on.
type Metadata struct {
	Service                   string        `json:"service"`
	SupportedProtocolVersions []string      `json:"supportedProtocolVersions"`
	DeploymentID              string        `json:"deploymentId"`
	ModelGateway              *ModelGateway `json:"modelGateway"`
}

// ModelGateway describes the OpenAI-compatible inference gateway.
type ModelGateway struct {
	BaseURL    string `json:"baseUrl"`
	Protocol   string `json:"protocol"`
	APIVersion string `json:"apiVersion"`
}

// HeartbeatResponse reports liveness acceptance and pending control events.
type HeartbeatResponse struct {
	ServerTime                string            `json:"serverTime"`
	ControlEvents             ControlEventProbe `json:"controlEvents"`
	NextHeartbeatAfterSeconds int               `json:"nextHeartbeatAfterSeconds"`
}

// ControlEventProbe is a discovery hint, not a correctness boundary.
type ControlEventProbe struct {
	Pending   bool   `json:"pending"`
	Watermark string `json:"watermark"`
}

// Problem is an RFC 9457 application/problem+json error body.
type Problem struct {
	Type       string `json:"type"`
	Title      string `json:"title"`
	Status     int    `json:"status"`
	Detail     string `json:"detail"`
	Code       string `json:"code"`
	RequestID  string `json:"requestId"`
	RetryAfter string `json:"retryAfter"` // Retry-After header, if present
}

// Error implements error. Code is the stable AEP problem code when present.
func (p *Problem) Error() string {
	if p.Code != "" {
		return fmt.Sprintf("aep: %s: %s (%d)", p.Code, p.Detail, p.Status)
	}
	return fmt.Sprintf("aep: %s: %s (%d)", p.Title, p.Detail, p.Status)
}

// Client is a minimal REST client for the AEP v1 API. It is safe for
// concurrent use.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient returns a client bound to a control service base URL such as
// http://localhost:8080 (no trailing slash, no /aep/v1 suffix).
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: defaultTimeout},
	}
}

// PasswordLogin exchanges account credentials for a session. The sessionID
// labels the session in the AEP admin console; it should be stable per
// process and unique per digital employee instance.
func (c *Client) PasswordLogin(ctx context.Context, deploymentID, username, password, sessionID string) (*TokenSet, *Problem) {
	body := map[string]string{
		"deploymentId": deploymentID,
		"sessionId":    sessionID,
		"username":     username,
		"password":     password,
	}
	var out TokenSet
	if p := c.post(ctx, "/aep/v1/auth/password/login", "", body, &out); p != nil {
		return nil, p
	}
	return &out, nil
}

// Refresh exchanges a refresh token for a fresh token set.
func (c *Client) Refresh(ctx context.Context, refreshToken, sessionID string) (*TokenSet, *Problem) {
	body := map[string]string{
		"refreshToken": refreshToken,
		"sessionId":    sessionID,
	}
	var out TokenSet
	if p := c.post(ctx, "/aep/v1/auth/refresh", "", body, &out); p != nil {
		return nil, p
	}
	return &out, nil
}

// Heartbeat reports liveness. cursor is the last seen control-event cursor;
// it may be empty before any event has been consumed.
func (c *Client) Heartbeat(ctx context.Context, accessToken, cursor string) (*HeartbeatResponse, *Problem) {
	body := map[string]string{
		"lastControlEventCursor": cursor,
		"status":                 "online",
	}
	var out HeartbeatResponse
	if p := c.post(ctx, "/aep/v1/user/heartbeat", accessToken, body, &out); p != nil {
		return nil, p
	}
	return &out, nil
}

// Models lists the model catalog visible to the authenticated account.
func (c *Client) Models(ctx context.Context, accessToken string) ([]Model, *Problem) {
	var out struct {
		Models []Model `json:"models"`
	}
	if p := c.get(ctx, "/aep/v1/user/models", accessToken, &out); p != nil {
		return nil, p
	}
	return out.Models, nil
}

// Metadata reads service metadata. It is the only call that works without
// a protocol-version header, so it doubles as a compatibility probe.
func (c *Client) Metadata(ctx context.Context) (*Metadata, *Problem) {
	var out Metadata
	if p := c.get(ctx, "/aep/v1/metadata", "", &out); p != nil {
		return nil, p
	}
	return &out, nil
}

// ResourceRef is one (kind, id) resource reference in a retrieval context.
type ResourceRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// RetrievalContext is the data-scope evaluation result for one user: the
// subtree of visible teams plus explicit allow/deny references. An explicit
// deny always wins over an allow.
type RetrievalContext struct {
	PrincipalID           string        `json:"principalId"`
	DeploymentID          string        `json:"deploymentId"`
	OrgScope              []string      `json:"orgScope"`
	OwnTeamIDs            []string      `json:"ownTeamIds"`
	RoleScope             []string      `json:"roleScope"`
	AllowedResources      []ResourceRef `json:"allowedResources"`
	DeniedResources       []ResourceRef `json:"deniedResources"`
	CrossDepartmentReason string        `json:"crossDepartmentReason,omitempty"`
}

// DataScopeContext resolves the retrieval context of one user. The caller
// needs the data_scope.read permission (granted to digital-employee runtime
// roles); userId may be any user of the deployment, enabling delegation:
// the runtime answers with what the requester may see, not what the agent
// itself may see.
func (c *Client) DataScopeContext(ctx context.Context, accessToken, userID string) (*RetrievalContext, *Problem) {
	var out RetrievalContext
	path := "/aep/v1/admin/data-scope/context?userId=" + url.QueryEscape(userID)
	if p := c.get(ctx, path, accessToken, &out); p != nil {
		return nil, p
	}
	return &out, nil
}

func (c *Client) post(ctx context.Context, path, accessToken string, body any, out any) *Problem {
	payload, err := json.Marshal(body)
	if err != nil {
		return &Problem{Title: "request encoding failed", Detail: err.Error(), Status: http.StatusInternalServerError}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return &Problem{Title: "request build failed", Detail: err.Error(), Status: http.StatusInternalServerError}
	}
	return c.do(req, accessToken, out)
}

func (c *Client) get(ctx context.Context, path, accessToken string, out any) *Problem {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return &Problem{Title: "request build failed", Detail: err.Error(), Status: http.StatusInternalServerError}
	}
	return c.do(req, accessToken, out)
}

func (c *Client) do(req *http.Request, accessToken string, out any) *Problem {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-AEP-Protocol-Version", protocolVersion)
	if accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+accessToken)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return &Problem{Title: "request failed", Detail: redactURL(err.Error()), Status: http.StatusBadGateway}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return parseProblem(resp)
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxBodySize))
	if err := dec.Decode(out); err != nil {
		return &Problem{Title: "response decoding failed", Detail: err.Error(), Status: http.StatusBadGateway}
	}
	return nil
}

func parseProblem(resp *http.Response) *Problem {
	p := &Problem{Status: resp.StatusCode, Title: resp.Status}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = json.Unmarshal(body, p)
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		p.RetryAfter = ra
	}
	return p
}

// redactURL strips query strings from transport errors so credentials or
// tokens echoed in URLs never reach logs.
func redactURL(msg string) string {
	if i := strings.IndexByte(msg, '?'); i >= 0 && strings.Contains(msg[:i], "://") {
		return msg[:i] + "?[redacted]"
	}
	return msg
}

// CurrentUser resolves the authenticated account's own identity.
func (c *Client) CurrentUser(ctx context.Context, accessToken string) (*Principal, *Problem) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/aep/v1/user/me", nil)
	if err != nil {
		return nil, &Problem{Title: "request build failed", Detail: err.Error(), Status: http.StatusInternalServerError}
	}
	var out currentUserResponse
	if p := c.do(req, accessToken, &out); p != nil {
		return nil, p
	}
	return &Principal{
		UserID: out.User.ID, DisplayName: out.User.DisplayName, Kind: out.User.Kind,
		DeploymentID: out.DeploymentID, Roles: out.Roles,
	}, nil
}

// EphemeralAgentInput assembles a conversation-scoped digital employee in
// one call: frozen scope snapshot from the requester plus model assignments.
type EphemeralAgentInput struct {
	Username        string   `json:"username"`
	DisplayName     string   `json:"displayName"`
	Password        string   `json:"password"`
	RoleIDs         []string `json:"roleIds"`
	TeamIDs         []string `json:"teamIds"`
	HomeTeamID      string   `json:"homeTeamId"`
	PromptSkillID   string   `json:"promptSkillId,omitempty"`
	DisplayTitle    string   `json:"displayTitle,omitempty"`
	Ephemeral       bool     `json:"ephemeral"`
	ExpiresAt       string   `json:"expiresAt"`
	ScopeFromUserID string   `json:"scopeFromUserId,omitempty"`
	ModelIDs        []string `json:"modelIds,omitempty"`
}

// CreateAgent provisions a digital employee account (admin API).
func (c *Client) CreateAgent(ctx context.Context, accessToken string, input EphemeralAgentInput) (*AgentRecord, *Problem) {
	var out AgentRecord
	if p := c.post(ctx, "/aep/v1/admin/agents", accessToken, input, &out); p != nil {
		return nil, p
	}
	return &out, nil
}

// DeleteAgent removes a digital employee account (admin API).
func (c *Client) DeleteAgent(ctx context.Context, accessToken, agentID string) *Problem {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/aep/v1/admin/agents/"+url.PathEscape(agentID), nil)
	if err != nil {
		return &Problem{Title: "request build failed", Detail: err.Error(), Status: http.StatusInternalServerError}
	}
	if p := c.do(req, accessToken, nil); p != nil {
		return p
	}
	return nil
}

// RevokeSession revokes one user session (admin API).
func (c *Client) RevokeSession(ctx context.Context, accessToken, sessionID string) *Problem {
	if p := c.post(ctx, "/aep/v1/admin/sessions/"+url.PathEscape(sessionID)+"/revoke", accessToken, struct{}{}, nil); p != nil {
		return p
	}
	return nil
}

// AgentRecord is the createAgent response.
type AgentRecord struct {
	ID           string  `json:"id"`
	Username     string  `json:"username"`
	DisplayName  string  `json:"displayName"`
	HomeTeamID   string  `json:"homeTeamId"`
	DisplayTitle string  `json:"displayTitle"`
	Ephemeral    bool    `json:"ephemeral"`
	ExpiresAt    *string `json:"expiresAt"`
}
