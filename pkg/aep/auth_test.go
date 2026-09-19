package aep

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// authFixture serves JWKS and /user/me for one signing key.
type authFixture struct {
	server    *httptest.Server
	pub       ed25519.PublicKey
	priv      ed25519.PrivateKey
	kid       string
	identity  atomic.Value // currentUserResponse to serve
	meCalls   atomic.Int32
	jwksCalls atomic.Int32
}

func newAuthFixture(t *testing.T) *authFixture {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f := &authFixture{pub: pub, priv: priv, kid: "test-key-1"}
	f.identity.Store(map[string]any{
		"user":         map[string]any{"id": "user-1", "displayName": "Zhang San", "kind": "human"},
		"deploymentId": "demo",
		"roles":        []string{"employee"},
	})
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/jwks.json":
			f.jwksCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"keys": []map[string]any{{
					"kty": "OKP", "crv": "Ed25519", "use": "sig", "alg": "EdDSA",
					"kid": f.kid,
					"x":   base64.RawURLEncoding.EncodeToString(f.pub),
				}},
			})
		case "/aep/v1/user/me":
			f.meCalls.Add(1)
			_ = json.NewEncoder(w).Encode(f.identity.Load())
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *authFixture) signed(sub, deploymentID, audience string, expires time.Time) string {
	claims := jwt.MapClaims{
		"deployment_id": deploymentID,
		"session_id":    "session-1",
		"roles":         []string{"employee"},
		"token_use":     "aep",
		"iss":           f.server.URL,
		"sub":           sub,
		"aud":           []string{audience},
		"exp":           expires.Unix(),
		"iat":           time.Now().Add(-time.Minute).Unix(),
		"jti":           "jti-1",
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	tok.Header["kid"] = f.kid
	signed, err := tok.SignedString(f.priv)
	if err != nil {
		panic(err)
	}
	return signed
}

func TestAuthenticateResolvesHumanPrincipal(t *testing.T) {
	f := newAuthFixture(t)
	auth := NewAuthenticator(f.server.URL)
	tok := f.signed("user-1", "demo", "aep-control", time.Now().Add(time.Hour))

	p, err := auth.Authenticate(context.Background(), "demo", tok)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if p.UserID != "user-1" || p.DisplayName != "Zhang San" || p.Kind != "human" || p.DeploymentID != "demo" {
		t.Fatalf("principal = %+v", p)
	}
	// The memoized identity path skips the second /user/me call.
	if _, err = auth.Authenticate(context.Background(), "demo", tok); err != nil {
		t.Fatalf("second authenticate: %v", err)
	}
	if got := f.meCalls.Load(); got != 1 {
		t.Fatalf("/user/me calls = %d, want 1 (identity memoized)", got)
	}
}

func TestAuthenticateRejectsAgentAccounts(t *testing.T) {
	f := newAuthFixture(t)
	f.identity.Store(map[string]any{
		"user":         map[string]any{"id": "agent-1", "displayName": "Helper", "kind": "agent"},
		"deploymentId": "demo",
		"roles":        []string{"member"},
	})
	auth := NewAuthenticator(f.server.URL)
	tok := f.signed("agent-1", "demo", "aep-control", time.Now().Add(time.Hour))

	if _, err := auth.Authenticate(context.Background(), "demo", tok); err != ErrNotHuman {
		t.Fatalf("expected ErrNotHuman, got %v", err)
	}
}

func TestAuthenticateRejectsForeignDeploymentAndBadTokens(t *testing.T) {
	f := newAuthFixture(t)
	auth := NewAuthenticator(f.server.URL)

	cases := []struct {
		name  string
		token string
	}{
		{"foreign deployment", f.signed("user-1", "other", "aep-control", time.Now().Add(time.Hour))},
		{"expired", f.signed("user-1", "demo", "aep-control", time.Now().Add(-time.Minute))},
		{"wrong audience", f.signed("user-1", "demo", "somebody-else", time.Now().Add(time.Hour))},
		{"garbage", "not-a-jwt"},
	}
	for _, tc := range cases {
		if _, err := auth.Authenticate(context.Background(), "demo", tc.token); err == nil {
			t.Fatalf("%s: expected rejection", tc.name)
		}
	}
}

func TestAuthenticateRefreshesJWKSOnUnknownKid(t *testing.T) {
	f := newAuthFixture(t)
	auth := NewAuthenticator(f.server.URL)
	tok := f.signed("user-1", "demo", "aep-control", time.Now().Add(time.Hour))

	// First authentication primes the cache.
	if _, err := auth.Authenticate(context.Background(), "demo", tok); err != nil {
		t.Fatalf("prime: %v", err)
	}
	// Rotate the key: a token under a new kid must trigger a refresh.
	_, priv2, _ := ed25519.GenerateKey(rand.Reader)
	f.priv = priv2
	f.kid = "test-key-2"
	f.pub = priv2.Public().(ed25519.PublicKey)
	f.identity.Store(map[string]any{
		"user":         map[string]any{"id": "user-1", "displayName": "Zhang San", "kind": "human"},
		"deploymentId": "demo",
		"roles":        []string{"employee"},
	})
	f.meCalls.Store(0)
	rotated := f.signed("user-1", "demo", "aep-control", time.Now().Add(2*time.Hour))
	p, err := auth.Authenticate(context.Background(), "demo", rotated)
	if err != nil {
		t.Fatalf("after rotation: %v", err)
	}
	if !strings.Contains(f.server.URL, "127.0.0.1") || p == nil {
		t.Fatal("unreachable")
	}
	if got := f.jwksCalls.Load(); got < 2 {
		t.Fatalf("jwks fetches = %d, want >= 2 (rotation forced a refresh)", got)
	}
}

func TestClaimsAccessorsAndDiscardLogger(t *testing.T) {
	claims := &accessClaims{Issuer: "iss", Subject: "sub", IssuedAt: 1700000000}
	if got, err := claims.GetIssuer(); got != "iss" || err != nil {
		t.Fatalf("GetIssuer = %q %v", got, err)
	}
	if got, err := claims.GetSubject(); got != "sub" || err != nil {
		t.Fatalf("GetSubject = %q %v", got, err)
	}
	if at, err := claims.GetIssuedAt(); at == nil || err != nil {
		t.Fatalf("GetIssuedAt = %v %v", at, err)
	}
	discardLogger{}.Infof("x %d", 1)
	discardLogger{}.Warnf("x")
	discardLogger{}.Errorf("x")
	if randomSuffix() == "" || len(randomSuffix()) != 8 {
		t.Fatal("randomSuffix must yield 8 hex chars")
	}
}

func TestAuthenticatePeerChecksPermissionAndAcceptsAgents(t *testing.T) {
	f := newAuthFixture(t)
	f.identity.Store(map[string]any{
		"user":         map[string]any{"id": "agent-1", "displayName": "Peer Agent", "kind": "agent"},
		"deploymentId": "demo",
		"roles":        []string{"runner"},
		"permissions":  []string{"models.read", "agents.invoke"},
	})
	auth := NewAuthenticator(f.server.URL)
	tok := f.signed("agent-1", "demo", "aep-control", time.Now().Add(time.Hour))

	peer, err := auth.AuthenticatePeer(context.Background(), "demo", tok, "agents.invoke")
	if err != nil || peer.UserID != "agent-1" || peer.Kind != "agent" {
		t.Fatalf("AuthenticatePeer = %+v, %v", peer, err)
	}
	if len(peer.Permissions) != 2 || peer.Permissions[1] != "agents.invoke" {
		t.Fatalf("permissions = %v", peer.Permissions)
	}

	// Missing permission is a typed error carrying the peer identity.
	f.identity.Store(map[string]any{
		"user":         map[string]any{"id": "agent-2", "displayName": "Poor Peer", "kind": "agent"},
		"deploymentId": "demo",
		"roles":        []string{"runner"},
		"permissions":  []string{"models.read"},
	})
	weak := f.signed("agent-2", "demo", "aep-control", time.Now().Add(time.Hour))
	peer, err = auth.AuthenticatePeer(context.Background(), "demo", weak, "agents.invoke")
	if !errors.Is(err, ErrMissingPermission) || peer == nil || peer.UserID != "agent-2" {
		t.Fatalf("missing permission = %+v, %v", peer, err)
	}

	// Garbage tokens are rejected outright.
	if _, err := auth.AuthenticatePeer(context.Background(), "demo", "junk", "agents.invoke"); err == nil {
		t.Fatal("garbage token must fail")
	}
}
