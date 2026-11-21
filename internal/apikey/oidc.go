package apikey

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// Principal represents the resolved identity and permissions from either an API key or an OIDC token.
type Principal struct {
	TenantID string
	Scopes   []string
}

// OIDCMapping maps claims to tenant and scopes.
type OIDCMapping struct {
	TenantClaim   string
	ScopesClaim   string
	DefaultTenant string
}

// JWK represents a JSON Web Key.
type JWK struct {
	Kty string   `json:"kty"`
	Kid string   `json:"kid"`
	Use string   `json:"use"`
	Alg string   `json:"alg"`
	N   string   `json:"n"`
	E   string   `json:"e"`
}

// JWKS represents a JSON Web Key Set.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// JWKSCache caches fetched key sets with a bounded lifetime.
type JWKSCache struct {
	url       string
	mu        sync.RWMutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
	client    *http.Client
}

// NewJWKSCache creates a new JWKS cache.
func NewJWKSCache(url string) *JWKSCache {
	return &JWKSCache{
		url:    url,
		client: &http.Client{Timeout: 5 * time.Second},
		keys:   make(map[string]*rsa.PublicKey),
	}
}

// GetKey retrieves a public key by kid, fetching or refreshing JWKS if necessary.
func (c *JWKSCache) GetKey(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	c.mu.RLock()
	if pk, ok := c.keys[kid]; ok && time.Since(c.fetchedAt) < 15*time.Minute {
		c.mu.RUnlock()
		return pk, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if pk, ok := c.keys[kid]; ok && time.Since(c.fetchedAt) < 15*time.Minute {
		return pk, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch jwks: status %d", resp.StatusCode)
	}

	var jwks JWKS
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return nil, err
	}

	newKeys := make(map[string]*rsa.PublicKey)
	for _, k := range jwks.Keys {
		if k.Kty == "RSA" && k.N != "" && k.E != "" {
			pubKey, err := parseRSAPublicKey(k.N, k.E)
			if err == nil {
				newKeys[k.Kid] = pubKey
			}
		}
	}

	c.keys = newKeys
	c.fetchedAt = time.Now()

	if pk, ok := c.keys[kid]; ok {
		return pk, nil
	}

	return nil, errors.New("key id not found in jwks")
}

func parseRSAPublicKey(nStr, eStr string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(nStr)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(eStr)
	if err != nil {
		return nil, err
	}

	n := new(big.Int).SetBytes(nBytes)
	var e int
	for _, b := range eBytes {
		e = (e << 8) + int(b)
	}

	return &rsa.PublicKey{N: n, E: e}, nil
}
