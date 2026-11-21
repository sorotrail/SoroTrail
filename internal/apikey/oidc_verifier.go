package apikey

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors" 
	"strings"
	"time"
)

// OIDCVerifier verifies OIDC tokens and maps claims.
type OIDCVerifier struct {
	config OIDCConfig
	cache  *JWKSCache
}

// NewOIDCVerifier creates an OIDCVerifier from OIDCConfig.
func NewOIDCVerifier(cfg OIDCConfig) *OIDCVerifier {
	return &OIDCVerifier{
		config: cfg,
		cache:  NewJWKSCache(cfg.JWKSURL),
	}
}

// VerifyToken validates a raw JWT token and extracts the Principal.
func (v *OIDCVerifier) VerifyToken(ctx context.Context, tokenString string) (*Principal, error) {
	if !v.config.Enabled {
		return nil, errors.New("oidc authentication is not enabled")
	}

	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return nil, errors.New("malformed token: expected 3 parts")
	}

	// Decode header
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	UseJSON := map[string]interface{}{}
	if err := json.Unmarshal(headerBytes, &UseJSON); err != nil {
		return nil, errors.New("malformed token header")
	}

	alg, _ := UseJSON["alg"].(string)
	// Refuse unexpected algorithms (e.g. none or symmetric algorithms for asymmetric verification)
	if alg != "RS256" {
		return nil, errors.New("unsupported algorithm")
	}

	kid, _ := UseJSON["kid"].(string)
	if kid == "" {
		return nil, errors.New("missing kid in token header")
	}

	// Decode payload
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errors.New("malformed token payload")
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &claims);
	err != nil {
		return nil, errors.New("malformed token claims")
	}

	// Validate issuer
	iss, _ := claims["iss"].(string)
	if iss != v.config.Issuer {
		return nil, errors.New("invalid issuer")
	}

	// Validate audience
	aud := claims["aud"]
	validAud := false
	switch a := aud.(type) {
		case string:
			if a == v.config.Audience {
				validAud = true
			}
		case []interface{}:
			for _, item := range a {
				if s, ok := item.(string); ok && s == v.config.Audience {
					validAud = true
					break
				}
			}
	}
	if !validAud {
		return nil, errors.New("invalid audience")
	}

	now := time.Now().Unix()
	// Tolerated clock skew: 60 seconds
	const skew = 60

	if exp, ok := claims["exp"].(float64);
	ok {
		if now > int64(exp)+skew {
			return nil, errors.New("token is expired")
		}
	} else {
		return nil, errors.New("missing exp claim")
	}

	if nbf, ok := claims["nbf"].(float64);
	ok {
		if now < int64(nbf)-skew {
			return nil, errors.New("token not yet valid")
		}
	}

	// Verify signature
	pubKey, err := v.cache.GetKey(ctx, kid)
	if err != nil {
		return nil, err
	}

	signInput := parts[0] + "." + parts[1]
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, errors.New("malformed signature")
	}

	hash := sha256.Sum256([]byte(signInput))
	err = rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hash[:], sigBytes)
	if err != nil {
		return nil, errors.New("signature verification failed")
	}

	// Map claims to tenant and scopes
	tenantClaimKey := v.config.TenantClaim
	if tenantClaimKey == "" {
		tenantClaimKey = "tenant_id"
	}
	tenantID, _ := claims[tenantClaimKey].(string)
	if tenantID == "" {
		if v.config.DefaultTenant != "" {
			tenantID = v.config.DefaultTenant
		} else {
			return nil, errors.New("tenant claim missing and no default tenant configured")
		}
	}

	scopesClaimKey := v.config.ScopesClaim
	if scopesClaimKey == "" {
		scopesClaimKey = "scopes"
	}
	var scopes []string
	if scVal, ok := claims[scopesClaimKey]; ok {
		switch s := scVal.(type) {
		case string:
			scopes = strings.Fields(s)
		case []interface{}:
			for _, item := range s {
				if str, ok := item.(string); ok {
					scopes = append(scopes, str)
				}
			}
		}
	}

	return &Principal{
		TenantID: tenantID,
		Scopes:   scopes,
	}, nil
}
