// Package apikey generates and validates SoroTrail API keys.
//
// A key has the form `sorotrail_<prefix>_<secret>`:
//
//   - prefix: 16 hex chars (8 random bytes), stored in plaintext and
//     indexed so a presented key resolves to its database row with a
//     single lookup (bcrypt hashes cannot be searched by value).
//   - secret: 64 hex chars (32 random bytes). Only its bcrypt hash is
//     ever persisted; the plaintext secret is shown to the caller once
//     at creation time and is otherwise unrecoverable.
package apikey

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// SchemePrefix is the human-readable prefix every SoroTrail key starts
// with, so keys are recognizable in logs and configuration.
const SchemePrefix = "sorotrail_"

// HashCost is the bcrypt cost used when hashing key secrets. The default
// cost (10) is deliberately kept: API keys are high-entropy secrets, so
// the work factor is about slowing a database thief down, not defending
// weak input.
const HashCost = bcrypt.DefaultCost

// KeyLengths describe the generated parts. The prefix is public (it only
// routes a lookup); the secret is the credential and must never be
// logged or stored in plaintext.
const (
	PrefixLength = 8  // random bytes → 16 hex chars
	SecretLength = 32 // random bytes → 64 hex chars
)

// Generate creates a new API key. It returns the full key — which is
// shown to the caller exactly once — together with the prefix (stored
// in plaintext for lookup) and the secret (stored only as a bcrypt
// hash).
func Generate() (key, prefix, secret string, err error) {
	p := make([]byte, PrefixLength)
	if _, err := rand.Read(p); err != nil {
		return "", "", "", err
	}
	s := make([]byte, SecretLength)
	if _, err := rand.Read(s); err != nil {
		return "", "", "", err
	}
	prefix = hex.EncodeToString(p)
	secret = hex.EncodeToString(s)
	return SchemePrefix + prefix + "_" + secret, prefix, secret, nil
}

// Parse splits a presented key into its prefix and secret parts. It
// returns ok=false for anything that does not exactly match the
// `sorotrail_<prefix>_<secret>` shape: the prefix is 16 hex chars and
// the secret is 64 hex chars (the lengths Generate produces). Being
// strict here means malformed or truncated keys are rejected before
// they ever touch the database.
func Parse(key string) (prefix, secret string, ok bool) {
	rest, found := strings.CutPrefix(key, SchemePrefix)
	if !found {
		return "", "", false
	}
	prefix, secret, found = strings.Cut(rest, "_")
	if !found || !isHex(prefix, 2*PrefixLength) || !isHex(secret, 2*SecretLength) {
		return "", "", false
	}
	return prefix, secret, true
}

// isHex reports whether s is exactly n lowercase hexadecimal digits
// (hex.EncodeToString output).
func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < n; i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// HashSecret returns the bcrypt hash of a key secret. Store this, never
// the secret itself.
func HashSecret(secret string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(secret), HashCost)
	if err != nil {
		return "", errors.New("hashing API key secret: " + err.Error())
	}
	return string(b), nil
}

// VerifySecret reports whether the presented secret matches the stored
// bcrypt hash. It returns false (never an error) on any mismatch or
// malformed hash, so callers can collapse "bad key" and "bad hash" into
// one unauthenticated response.
func VerifySecret(secret, hash string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(secret)) == nil
}
