package aep

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Requester authentication for chat-facing surfaces: verify a human's AEP
// access token locally against the control-service JWKS, then resolve the
// authenticated principal's identity. Digital employees (kind=agent) are
// rejected so two agents can never talk each other into a loop.

// Principal is an authenticated human requester.
type Principal struct {
	UserID       string
	DisplayName  string
	Kind         string
	SessionID    string
	DeploymentID string
	Roles        []string
}

// ErrNotHuman marks a token that belongs to a digital-employee account.
var ErrNotHuman = errors.New("aep: requester is not a human account")

// Authenticator validates AEP access tokens for one control service.
// JWT signatures are verified locally against a cached JWKS; identity is
// resolved once per token via /user/me and memoized until shortly before
// the token's remaining lifetime ends.
type Authenticator struct {
	client *Client

	jwksMu   sync.RWMutex
	jwks     *JWKSet
	jwksAt   time.Time
	jwksFail time.Time

	identityMu sync.Mutex
	identity   map[string]identityEntry
}

type identityEntry struct {
	principal Principal
	expiresAt time.Time
}

// JWKSet mirrors /.well-known/jwks.json.
type JWKSet struct {
	Keys []JWK `json:"keys"`
}

// JWK is one public signing key (Ed25519, OKP key type).
type JWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Kid string `json:"kid"`
}

const (
	jwksCacheTTL   = 1 * time.Hour
	jwksRetryDelay = 30 * time.Second
	identityLead   = 2 * time.Minute // drop memoized identity before token expiry
)

// NewAuthenticator binds an authenticator to a control-service base URL.
func NewAuthenticator(baseURL string) *Authenticator {
	return &Authenticator{client: NewClient(baseURL)}
}

// Authenticate verifies the bearer token and resolves the human principal.
// It fails closed: signature, issuer, audience, deployment, expiry, and the
// human-account check must all pass.
func (a *Authenticator) Authenticate(ctx context.Context, deploymentID, accessToken string) (*Principal, error) {
	claims, err := a.verifyToken(ctx, deploymentID, accessToken)
	if err != nil {
		return nil, err
	}
	if p, ok := a.cachedIdentity(accessToken, claims); ok {
		return p, nil
	}
	principal, perr := a.resolveIdentity(ctx, accessToken, claims)
	if perr != nil {
		return nil, perr
	}
	a.cacheIdentity(accessToken, claims, principal)
	return principal, nil
}

type accessClaims struct {
	DeploymentID string   `json:"deployment_id"`
	SessionID    string   `json:"session_id"`
	Roles        []string `json:"roles"`
	TokenUse     string   `json:"token_use"`
	Issuer       string   `json:"iss"`
	Subject      string   `json:"sub"`
	Audience     []string `json:"aud"`
	ExpiresAt    int64    `json:"exp"`
	IssuedAt     int64    `json:"iat"`
	JWTID        string   `json:"jti"`
}

// The jwt/v5 Claims interface, mapped onto the plain fields above.
func (c *accessClaims) GetExpirationTime() (*jwt.NumericDate, error) {
	return jwt.NewNumericDate(time.Unix(c.ExpiresAt, 0)), nil
}
func (c *accessClaims) GetIssuedAt() (*jwt.NumericDate, error) {
	return jwt.NewNumericDate(time.Unix(c.IssuedAt, 0)), nil
}
func (c *accessClaims) GetNotBefore() (*jwt.NumericDate, error) { return nil, nil }
func (c *accessClaims) GetAudience() (jwt.ClaimStrings, error) {
	return jwt.ClaimStrings(c.Audience), nil
}
func (c *accessClaims) GetIssuer() (string, error)  { return c.Issuer, nil }
func (c *accessClaims) GetSubject() (string, error) { return c.Subject, nil }

type currentUserResponse struct {
	User struct {
		ID          string `json:"id"`
		DisplayName string `json:"displayName"`
		Kind        string `json:"kind"`
	} `json:"user"`
	DeploymentID string   `json:"deploymentId"`
	Roles        []string `json:"roles"`
}

func (a *Authenticator) resolveIdentity(ctx context.Context, accessToken string, claims *accessClaims) (*Principal, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.client.baseURL+"/aep/v1/user/me", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-AEP-Protocol-Version", protocolVersion)
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := a.client.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("aep: identity resolution failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("aep: identity resolution returned %d", resp.StatusCode)
	}
	var out currentUserResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("aep: identity decoding failed: %w", err)
	}
	if out.User.ID == "" || out.User.ID != claims.Subject {
		return nil, errors.New("aep: identity mismatch between token and session")
	}
	if out.User.Kind != "human" {
		return nil, ErrNotHuman
	}
	return &Principal{
		UserID:       out.User.ID,
		DisplayName:  out.User.DisplayName,
		Kind:         out.User.Kind,
		SessionID:    claims.SessionID,
		DeploymentID: out.DeploymentID,
		Roles:        out.Roles,
	}, nil
}

func (a *Authenticator) cachedIdentity(token string, claims *accessClaims) (*Principal, bool) {
	a.identityMu.Lock()
	defer a.identityMu.Unlock()
	entry, ok := a.identity[token]
	if !ok || time.Now().After(entry.expiresAt) {
		return nil, false
	}
	return &entry.principal, true
}

func (a *Authenticator) cacheIdentity(token string, claims *accessClaims, p *Principal) {
	expiry := time.Unix(claims.ExpiresAt, 0).Add(-identityLead)
	a.identityMu.Lock()
	defer a.identityMu.Unlock()
	if a.identity == nil {
		a.identity = make(map[string]identityEntry)
	}
	// Opportunistic cleanup keeps the map bounded under many requesters.
	if len(a.identity) > 4096 {
		a.identity = make(map[string]identityEntry)
	}
	a.identity[token] = identityEntry{principal: *p, expiresAt: expiry}
}
