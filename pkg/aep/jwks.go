package aep

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// verifyToken validates an AEP access token locally: EdDSA signature against
// the cached JWKS, issuer, audience, expiry, deployment binding, and token
// use. An unknown key id triggers one JWKS refresh before failing.
func (a *Authenticator) verifyToken(ctx context.Context, deploymentID, token string) (*accessClaims, error) {
	claims := &accessClaims{}
	_, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		return a.signingKey(ctx, t)
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer(a.client.baseURL), jwt.WithAudience("aep-control"))
	if err != nil {
		return nil, fmt.Errorf("aep: token verification failed: %w", err)
	}
	if claims.DeploymentID != deploymentID {
		return nil, errors.New("aep: token belongs to a different deployment")
	}
	if claims.TokenUse != "" && claims.TokenUse != "aep" {
		return nil, errors.New("aep: token is not usable as an access token")
	}
	if claims.Subject == "" {
		return nil, errors.New("aep: token carries no subject")
	}
	return claims, nil
}

func (a *Authenticator) signingKey(ctx context.Context, t *jwt.Token) (any, error) {
	kid, _ := t.Header["kid"].(string)
	key, err := a.lookupKey(ctx, kid, false)
	if err != nil {
		// One forced refresh covers key rotation before failing.
		key, err = a.lookupKey(ctx, kid, true)
		if err != nil {
			return nil, err
		}
	}
	raw, err := base64.RawURLEncoding.DecodeString(key.X)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, errors.New("aep: malformed Ed25519 key in JWKS")
	}
	return ed25519.PublicKey(raw), nil
}

func (a *Authenticator) lookupKey(ctx context.Context, kid string, force bool) (*JWK, error) {
	set := a.loadJWKS(ctx, force)
	if set == nil {
		return nil, errors.New("aep: JWKS unavailable")
	}
	for i := range set.Keys {
		key := &set.Keys[i]
		if key.Kid == kid || (kid == "" && len(set.Keys) == 1) {
			return key, nil
		}
	}
	return nil, fmt.Errorf("aep: no JWKS key matches kid %q", kid)
}

// loadJWKS returns the cached key set, fetching or refreshing when the cache
// is empty, stale, or explicitly forced. Failed fetches are retried no more
// often than jwksRetryDelay to avoid a fetch storm per request.
func (a *Authenticator) loadJWKS(ctx context.Context, force bool) *JWKSet {
	a.jwksMu.Lock()
	defer a.jwksMu.Unlock()
	fresh := a.jwks != nil && time.Since(a.jwksAt) <= jwksCacheTTL
	if !force {
		if fresh {
			return a.jwks
		}
		if !a.jwksFail.IsZero() && time.Since(a.jwksFail) < jwksRetryDelay {
			return a.jwks // possibly nil: recent failure, back off
		}
	}
	set, err := a.doFetchJWKS(ctx)
	if err != nil {
		a.jwksFail = time.Now()
		return a.jwks // best effort: keep serving the previous set, if any
	}
	a.jwks = set
	a.jwksAt = time.Now()
	a.jwksFail = time.Time{}
	return a.jwks
}

func (a *Authenticator) doFetchJWKS(ctx context.Context) (*JWKSet, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.client.baseURL+"/.well-known/jwks.json", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := a.client.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("aep: JWKS fetch failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("aep: JWKS fetch returned %d", resp.StatusCode)
	}
	var set JWKSet
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&set); err != nil {
		return nil, fmt.Errorf("aep: JWKS decoding failed: %w", err)
	}
	if len(set.Keys) == 0 {
		return nil, errors.New("aep: JWKS contains no keys")
	}
	return &set, nil
}
