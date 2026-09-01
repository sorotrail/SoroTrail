package apikey

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerate_ShapeAndUniqueness(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		key, prefix, secret, err := Generate()
		require.NoError(t, err)
		require.False(t, seen[key], "keys must be unique")
		seen[key] = true

		assert.True(t, strings.HasPrefix(key, SchemePrefix), "key carries the scheme prefix")
		assert.Len(t, prefix, 2*PrefixLength, "prefix is hex-encoded random bytes")
		assert.Len(t, secret, 2*SecretLength, "secret is hex-encoded random bytes")
		assert.Equal(t, SchemePrefix+prefix+"_"+secret, key, "key is the concatenation of its parts")
	}
}

func TestGenerate_SecretsAreRandom(t *testing.T) {
	_, _, s1, err := Generate()
	require.NoError(t, err)
	_, _, s2, err := Generate()
	require.NoError(t, err)
	assert.NotEqual(t, s1, s2, "two generated secrets must differ")
}

func TestParse(t *testing.T) {
	key, prefix, secret, err := Generate()
	require.NoError(t, err)

	t.Run("round-trips a generated key", func(t *testing.T) {
		gotPrefix, gotSecret, ok := Parse(key)
		require.True(t, ok)
		assert.Equal(t, prefix, gotPrefix)
		assert.Equal(t, secret, gotSecret)
	})

	t.Run("rejects malformed keys", func(t *testing.T) {
		for _, bad := range []string{
			"",
			"plain-token",
			SchemePrefix,                     // no parts at all
			SchemePrefix + "_secret",         // missing prefix
			SchemePrefix + "prefix_",         // missing secret
			SchemePrefix + "a_b_c",           // too many parts
			SchemePrefix + " " + "_" + "x",   // whitespace prefix
			"other_" + prefix + "_" + secret, // wrong scheme
		} {
			_, _, ok := Parse(bad)
			assert.False(t, ok, "expected %q to be rejected", bad)
		}
	})
}

func TestHashAndVerify(t *testing.T) {
	hash, err := HashSecret("deadbeef")
	require.NoError(t, err)
	assert.NotEqual(t, "deadbeef", hash, "hash must not contain the plaintext")
	assert.True(t, VerifySecret("deadbeef", hash))
	assert.False(t, VerifySecret("wrong-secret", hash), "mismatched secret fails")
	assert.False(t, VerifySecret("deadbeef", "not-a-bcrypt-hash"), "malformed hash fails")
	assert.False(t, VerifySecret("", ""), "empty inputs fail")
}

func TestHashSecret_TwoHashesDiffer(t *testing.T) {
	// bcrypt salts each hash, so the same secret produces different
	// digests — verification must go through CompareHashAndPassword.
	h1, err := HashSecret("same")
	require.NoError(t, err)
	h2, err := HashSecret("same")
	require.NoError(t, err)
	assert.NotEqual(t, h1, h2)
	assert.True(t, VerifySecret("same", h1))
	assert.True(t, VerifySecret("same", h2))
}
