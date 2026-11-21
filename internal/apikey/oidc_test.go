package apikey

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func generateTestRSAKey(t *testing.T) (*rsa.PrivateKey, string) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate rsa key: %v", err)
	}
	kid := "test-kid-1"
	return priv, kid
}

func createTestToken(t *testing.T, priv *rsa.PrivateKey, kid string, claims map[string]interface{}) string {
	header := map[string]interface{}{
		"alg": "RS256",
		"kid": kid,
		"typ": "JWT",
	}
	hBytes, _ := json.Marshal(header)
	pBytes, _ := json.Marshal(claims)

	hEnc := base64.RawURLEncoding.EncodeToString(hBytes)
	pEnc := base64.RawURLEncoding.EncodeToString(pBytes)

	msg := hEnc + "." + pEnc
	hash := sha256.Sum256([]byte(msg))
	sig, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, hash[:])
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	sEnc := base64.RawURLEncoding.EncodeToString(sig)

	return msg + "." + sEnc
}

func TestOIDCVerifierSuccess(t *testing.T) {
	priv, kid := generateTestRSAKey(t)
	pub := &priv.PublicKey

	nStr := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	eBytes := []byte{byte(pub.E >> 16), byte(pub.E >> 8), byte(pub.E)}
	if pub.E < 256 {
		eBytes = []byte{byte(pub.E)}
	} else if pub.E < 65536 {
		eBytes = []byte{byte(pub.E >> 8), byte(pub.E)}
	}
	eStr := base64.RawURLEncoding.EncodeToString(eBytes)

	jwks := JWKS{
		Keys: []JWK{
			{Kty: "RSA", Kid: kid, Use: "sig", Alg: "RS256", N: nStr, E: eStr},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(jwks)
	}))
	defer srv.Close()

	cfg := OIDCConfig{
		Enabled:     true,
		Issuer:      "https://auth.example.com",
		Audience:    "sorotrail-api",
		JWKSURL:     srv.URL,
		TenantClaim: "tenant_id",
		ScopesClaim: "scopes",
	}

	verifier := NewOIDCVerifier(cfg)

	claims := map[string]interface{}{
		"iss":       "https://auth.example.com",
		"aud":       "sorotrail-api",
		"exp":       float64(time.Now().Add(1 * time.Hour).Unix()),
		"nbf":       float64(time.Now().Add(-1 * time.Minute).Unix()),
		"tenant_id": "tenant-123",
		"scopes":    []interface{}{"read", "write"},
	}

	token := createTestToken(t, priv, kid, claims)
	principal, err := verifier.VerifyToken(context.Background(), token)
	if err != nil {
		t.Fatalf("unexpected error verifying valid token: %v", err)
	}

	if principal.TenantID != "tenant-123" {
		t.Errorf("expected tenant-123, got %s", principal.TenantID)
	}
	if len(principal.Scopes) != 2 {
		t.Errorf("expected 2 scopes, got %d", len(principal.Scopes))
	}
}

func TestOIDCVerifierDenialCases(t *testing.T) {
	priv, kid := generateTestRSAKey(t)
	pub := &priv.PublicKey
	nStr := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	eStr := base64.RawURLEncoding.EncodeToString([]byte{1, 0, 1})
	jwks := JWKS{Keys: []JWK{{Kty: "RSA", Kid: kid, N: nStr, E: eStr}}}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(jwks)
	}))
	defer srv.Close()

	cfg := OIDCConfig{
		Enabled:     true,
		Issuer:      "https://auth.example.com",
		Audience:    "sorotrail-api",
		JWKSURL:     srv.URL,
		TenantClaim: "tenant_id",
		ScopesClaim: "scopes",
	}

	verifier := NewOIDCVerifier(cfg)

	baseClaims := map[string]interface{}{
		"iss":       "https://auth.example.com",
		"aud":       "sorotrail-api",
		"exp":       float64(time.Now().Add(1 * time.Hour).Unix()),
		"tenant_id": "tenant-123",
	}

	// 1. Wrong issuer
	{
		c := map[string]interface{}{}
		for k, v := range baseClaims {
			c[k] = v
		}
		c["iss"] = "https://wrong.example.com"
		tok := createTestToken(t, priv, kid, c)
		if _, err := verifier.VerifyToken(context.Background(), tok); err == nil {
			t.Error("expected error for wrong issuer, got nil")
		}
	}

	// 2. Wrong audience
	{
		c := map[string]interface{}{}
		for k, v := range baseClaims {
			c[k] = v
		}
		c["aud"] = "wrong-aud"
		tok := createTestToken(t, priv, kid, c)
		if _, err := verifier.VerifyToken(context.Background(), tok); err == nil {
			t.Error("expected error for wrong audience, got nil")
		}
	}

	// 3. Expired token
	{
		c := map[string]interface{}{}
		for k, v := range baseClaims {
			c[k] = v
		}
		c["exp"] = float64(time.Now().Add(-2 * time.Hour).Unix())
		tok := createTestToken(t, priv, kid, c)
		if _, err := verifier.VerifyToken(context.Background(), tok); err == nil {
			t.Error("expected error for expired token, got nil")
		}
	}

	// 4. Missing tenant claim
	{
		c := map[string]interface{}{}
		for k, v := range baseClaims {
			c[k] = v
		}
		delete(c, "tenant_id")
		tok := createTestToken(t, priv, kid, c)
		if _, err := verifier.VerifyToken(context.Background(), tok); err == nil {
			t.Error("expected error for missing tenant claim, got nil")
		}
	}
}
