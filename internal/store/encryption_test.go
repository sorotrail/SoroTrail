package store

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustRandomHexKey(t *testing.T) string {
	t.Helper()
	buf := make([]byte, 32)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	return hex.EncodeToString(buf)
}

func TestFieldEncryptor_RoundTripAndNonDeterminism(t *testing.T) {
	k1 := mustRandomHexKey(t)
	enc, err := ParseFieldEncryptor("v1:" + k1)
	require.NoError(t, err)
	require.Equal(t, "v1", enc.ActiveVersion())
	require.Equal(t, []string{"v1"}, enc.KeyVersions())

	secret := "whsec_live_super_secret_signing_key_987654321"
	c1, err := enc.EncryptString(secret)
	require.NoError(t, err)
	c2, err := enc.EncryptString(secret)
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(c1, "enc:v1:"), "ciphertext must carry version header")
	assert.True(t, strings.HasPrefix(c2, "enc:v1:"), "ciphertext must carry version header")
	assert.NotEqual(t, c1, c2, "encrypting identical plaintext twice must produce distinct ciphertexts (random nonce)")
	assert.NotContains(t, c1, secret, "ciphertext must never contain plaintext")
	assert.True(t, IsEncryptedValue(c1))

	ver, isEnc := EncryptedValueVersion(c1)
	assert.True(t, isEnc)
	assert.Equal(t, "v1", ver)

	p1, err := enc.DecryptString(c1)
	require.NoError(t, err)
	assert.Equal(t, secret, p1)

	p2, err := enc.DecryptString(c2)
	require.NoError(t, err)
	assert.Equal(t, secret, p2)

	// Empty string remains empty so optional secrets work without a dummy envelope.
	emptyCipher, err := enc.EncryptString("")
	require.NoError(t, err)
	assert.Equal(t, "", emptyCipher)
	emptyPlain, err := enc.DecryptString("")
	require.NoError(t, err)
	assert.Equal(t, "", emptyPlain)
}

func TestFieldEncryptor_WrongKeyTamperedAndCorrupted(t *testing.T) {
	k1 := mustRandomHexKey(t)
	k2 := mustRandomHexKey(t)

	enc1, err := ParseFieldEncryptor("v1:" + k1)
	require.NoError(t, err)
	encWrongSameVersion, err := ParseFieldEncryptor("v1:" + k2)
	require.NoError(t, err)
	enc2Only, err := ParseFieldEncryptor("v2:" + k2)
	require.NoError(t, err)

	secret := "whsec_do_not_leak_in_error_logs_12345"
	cipherText, err := enc1.EncryptString(secret)
	require.NoError(t, err)

	t.Run("wrong key with same version fails AEAD tag check", func(t *testing.T) {
		_, err := encWrongSameVersion.DecryptString(cipherText)
		require.ErrorIs(t, err, ErrEncryptionAuthFailed)
		assert.NotContains(t, err.Error(), secret)
		assert.NotContains(t, err.Error(), k1)
		assert.NotContains(t, err.Error(), k2)
	})

	t.Run("unknown key version fails cleanly", func(t *testing.T) {
		_, err := enc2Only.DecryptString(cipherText)
		require.ErrorIs(t, err, ErrUnknownEncryptionKeyVersion)
		assert.NotContains(t, err.Error(), secret)
	})

	t.Run("nil encryptor rejects encrypted value with ErrEncryptionKeyRequired", func(t *testing.T) {
		var nilEnc *FieldEncryptor
		_, err := nilEnc.DecryptString(cipherText)
		require.ErrorIs(t, err, ErrEncryptionKeyRequired)
	})

	t.Run("tampered version header fails AAD authentication even if keys match", func(t *testing.T) {
		// Keyring with identical key bytes under v1 and v2 to prove version is bound in AAD.
		sameKeyHex := strings.Repeat("42", 32)
		encSameKeyBothVersions, err := ParseFieldEncryptor("v2:" + sameKeyHex + ",v1:" + sameKeyHex)
		require.NoError(t, err)
		v2Cipher, err := encSameKeyBothVersions.EncryptString("bound-to-v2")
		require.NoError(t, err)
		tamperedHeader := strings.Replace(v2Cipher, "enc:v2:", "enc:v1:", 1)
		_, err = encSameKeyBothVersions.DecryptString(tamperedHeader)
		require.ErrorIs(t, err, ErrEncryptionAuthFailed)
	})

	t.Run("flipped bit in ciphertext payload fails authentication", func(t *testing.T) {
		parts := strings.SplitN(cipherText, ":", 3)
		require.Len(t, parts, 3)
		raw, err := base64.RawURLEncoding.DecodeString(parts[2])
		require.NoError(t, err)
		raw[len(raw)-1] ^= 0xff
		corrupted := "enc:v1:" + base64.RawURLEncoding.EncodeToString(raw)

		_, err = enc1.DecryptString(corrupted)
		require.ErrorIs(t, err, ErrEncryptionAuthFailed)
	})

	t.Run("truncated or malformed ciphertext fails with ErrEncryptionAuthFailed", func(t *testing.T) {
		for _, bad := range []string{
			"enc:",
			"enc:v1:",
			"enc::abc",
			"enc:v1:!!!not_base64!!!",
			"enc:v1:" + base64.RawURLEncoding.EncodeToString([]byte("short")),
		} {
			_, err := enc1.DecryptString(bad)
			require.ErrorIs(t, err, ErrEncryptionAuthFailed, "input %q", bad)
		}
	})
}

func TestSQLite_SubscriptionSecretEncryptedAtRestAndKeyRotation(t *testing.T) {
	st := newSQLiteStore(t)
	sq, ok := st.(*SQLite)
	require.True(t, ok)
	ctx := context.Background()

	k1 := mustRandomHexKey(t)
	k2 := mustRandomHexKey(t)

	// 1. Seed a legacy plaintext subscription before enabling field encryption.
	legacySub, err := sq.CreateSubscription(ctx, Subscription{
		URL: "https://hooks.example.com/legacy",
		Filters: SubscriptionFilter{
			ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		},
		Secret:  "whsec_legacy_plaintext_secret_001",
		Enabled: true,
	})
	require.NoError(t, err)

	// 2. Enable encryption with v1 and create a new subscription.
	encV1, err := ParseFieldEncryptor("v1:" + k1)
	require.NoError(t, err)
	sq.SetFieldEncryptor(encV1)
	require.NoError(t, sq.VerifyEncryptionReadiness(ctx))

	v1Sub, err := sq.CreateSubscription(ctx, Subscription{
		URL: "https://hooks.example.com/v1",
		Filters: SubscriptionFilter{
			ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		},
		Secret:  "whsec_v1_encrypted_secret_002",
		Enabled: true,
	})
	require.NoError(t, err)

	// Inspect the raw stored value in SQLite to prove it is ciphertext on disk.
	var rawStoredV1 string
	require.NoError(t, sq.db.QueryRowContext(ctx, `SELECT secret FROM subscriptions WHERE id = ?`, v1Sub.ID).Scan(&rawStoredV1))
	assert.True(t, strings.HasPrefix(rawStoredV1, "enc:v1:"), "raw DB column must store versioned ciphertext, got %q", rawStoredV1)
	assert.NotContains(t, rawStoredV1, v1Sub.Secret, "raw DB column must not contain plaintext secret")

	// Verify transparent decryption across GetSubscription, ListSubscriptions, and ListEnabledSubscriptions.
	gotV1, err := sq.GetSubscription(ctx, v1Sub.ID, SubscriptionOwner{})
	require.NoError(t, err)
	assert.Equal(t, v1Sub.Secret, gotV1.Secret)

	gotLegacy, err := sq.GetSubscription(ctx, legacySub.ID, SubscriptionOwner{})
	require.NoError(t, err)
	assert.Equal(t, legacySub.Secret, gotLegacy.Secret, "legacy plaintext row must remain readable during transition")

	allSubs, err := sq.ListSubscriptions(ctx, SubscriptionOwner{})
	require.NoError(t, err)
	require.Len(t, allSubs, 2)

	enabledSubs, err := sq.ListEnabledSubscriptions(ctx)
	require.NoError(t, err)
	require.Len(t, enabledSubs, 2)

	// 3. Startup readiness check fails fast if encryption key is removed or missing v1.
	sq.SetFieldEncryptor(nil)
	err = sq.VerifyEncryptionReadiness(ctx)
	require.ErrorIs(t, err, ErrEncryptionKeyRequired)

	encV2WithoutV1, err := ParseFieldEncryptor("v2:" + k2)
	require.NoError(t, err)
	sq.SetFieldEncryptor(encV2WithoutV1)
	err = sq.VerifyEncryptionReadiness(ctx)
	require.ErrorIs(t, err, ErrUnknownEncryptionKeyVersion)

	// 4. Configure multi-key keyring (v2 active, v1 retained) and verify readiness + reads.
	encV2AndV1, err := ParseFieldEncryptor("v2:" + k2 + ",v1:" + k1)
	require.NoError(t, err)
	sq.SetFieldEncryptor(encV2AndV1)
	require.NoError(t, sq.VerifyEncryptionReadiness(ctx))

	gotV1UnderMultiKey, err := sq.GetSubscription(ctx, v1Sub.ID, SubscriptionOwner{})
	require.NoError(t, err)
	assert.Equal(t, v1Sub.Secret, gotV1UnderMultiKey.Secret, "v1 row must decrypt transparently while v2 is active")

	// 5. Run online batched rotation to re-encrypt both legacy plaintext and v1 rows to v2.
	stats, err := sq.RotateSubscriptionsEncryption(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, 2, stats.Scanned)
	assert.Equal(t, 1, stats.EncryptedNew)
	assert.Equal(t, 1, stats.ReEncrypted)
	assert.Equal(t, 0, stats.AlreadyActive)

	// Verify both rows now have enc:v2: in the raw DB column.
	for _, id := range []int64{legacySub.ID, v1Sub.ID} {
		var raw string
		require.NoError(t, sq.db.QueryRowContext(ctx, `SELECT secret FROM subscriptions WHERE id = ?`, id).Scan(&raw))
		assert.True(t, strings.HasPrefix(raw, "enc:v2:"), "row %d must be encrypted under v2 after rotation, got %q", id, raw)
	}

	// Re-running rotation is idempotent (0 rows rotated, 2 already active).
	statsAgain, err := sq.RotateSubscriptionsEncryption(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 2, statsAgain.Scanned)
	assert.Equal(t, 0, statsAgain.EncryptedNew)
	assert.Equal(t, 0, statsAgain.ReEncrypted)
	assert.Equal(t, 2, statsAgain.AlreadyActive)

	// 6. Retire v1 from the keyring (keep only v2) and confirm readiness and reads succeed.
	sq.SetFieldEncryptor(encV2WithoutV1)
	require.NoError(t, sq.VerifyEncryptionReadiness(ctx))

	afterLegacy, err := sq.GetSubscription(ctx, legacySub.ID, SubscriptionOwner{})
	require.NoError(t, err)
	assert.Equal(t, legacySub.Secret, afterLegacy.Secret)

	afterV1, err := sq.GetSubscription(ctx, v1Sub.ID, SubscriptionOwner{})
	require.NoError(t, err)
	assert.Equal(t, v1Sub.Secret, afterV1.Secret)

	// 7. UpdateSubscription also encrypts updated secrets under active key v2.
	afterV1.Secret = "whsec_updated_under_v2_secret_003"
	updatedSub, err := sq.UpdateSubscription(ctx, afterV1, SubscriptionOwner{})
	require.NoError(t, err)
	assert.Equal(t, afterV1.Secret, updatedSub.Secret)

	var rawUpdated string
	require.NoError(t, sq.db.QueryRowContext(ctx, `SELECT secret FROM subscriptions WHERE id = ?`, afterV1.ID).Scan(&rawUpdated))
	assert.True(t, strings.HasPrefix(rawUpdated, "enc:v2:"))
	assert.NotContains(t, rawUpdated, afterV1.Secret)
	gotUpdated, err := sq.GetSubscription(ctx, afterV1.ID, SubscriptionOwner{})
	require.NoError(t, err)
	assert.Equal(t, afterV1.Secret, gotUpdated.Secret)
}

func TestSensitiveColumnInventory(t *testing.T) {
	inv := SensitiveColumnInventory()
	require.NotEmpty(t, inv)

	byTableCol := make(map[string]ColumnProtectionPolicy, len(inv))
	for _, item := range inv {
		byTableCol[item.Table+"."+item.Column] = item
		assert.NotEmpty(t, item.Rationale, "every column in inventory must document its rationale")
	}

	subSecret, ok := byTableCol["subscriptions.secret"]
	require.True(t, ok, "subscriptions.secret must be in inventory")
	assert.Equal(t, ProtectionEncryptedAEAD, subSecret.Protection)

	apiKeyHash, ok := byTableCol["api_keys.key_hash"]
	require.True(t, ok, "api_keys.key_hash must be in inventory")
	assert.Equal(t, ProtectionOneWayHashed, apiKeyHash.Protection)

	tenantKeyHash, ok := byTableCol["tenant_api_keys.key_hash"]
	require.True(t, ok, "tenant_api_keys.key_hash must be in inventory")
	assert.Equal(t, ProtectionOneWayHashed, tenantKeyHash.Protection)

	apiKeyPrefix, ok := byTableCol["api_keys.prefix"]
	require.True(t, ok, "api_keys.prefix must be in inventory")
	assert.Equal(t, ProtectionPlaintextLookupOrPublic, apiKeyPrefix.Protection)
}

func TestRotateSubscriptionsEncryption_ConcurrentSecretUpdateNotClobbered(t *testing.T) {
	st := newSQLiteStore(t)
	sq, ok := st.(*SQLite)
	require.True(t, ok)
	ctx := context.Background()

	k1 := mustRandomHexKey(t)
	k2 := mustRandomHexKey(t)

	encV1, err := ParseFieldEncryptor("v1:" + k1)
	require.NoError(t, err)
	sq.SetFieldEncryptor(encV1)

	sub, err := sq.CreateSubscription(ctx, Subscription{
		URL: "https://hooks.example.com/concurrent",
		Filters: SubscriptionFilter{
			ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		},
		Secret:  "whsec_initial_v1_secret",
		Enabled: true,
	})
	require.NoError(t, err)

	encV2AndV1, err := ParseFieldEncryptor("v2:" + k2 + ",v1:" + k1)
	require.NoError(t, err)
	sq.SetFieldEncryptor(encV2AndV1)

	concurrentNewSecret := "whsec_concurrently_updated_by_tenant_mid_batch"
	stats, err := rotateSQLSubscriptionsEncryptionWithHook(ctx, sq.db, encV2AndV1, 10, func(batchIDs []int64) {
		require.Contains(t, batchIDs, sub.ID)
		sub.Secret = concurrentNewSecret
		_, updateErr := sq.UpdateSubscription(ctx, sub, SubscriptionOwner{})
		require.NoError(t, updateErr)
	})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Scanned)
	assert.Equal(t, 0, stats.EncryptedNew)
	assert.Equal(t, 0, stats.ReEncrypted)
	assert.Equal(t, 1, stats.SkippedConcurrentUpdates, "concurrently updated row must be skipped by WHERE id = ? AND secret = ? guard")

	got, err := sq.GetSubscription(ctx, sub.ID, SubscriptionOwner{})
	require.NoError(t, err)
	assert.Equal(t, concurrentNewSecret, got.Secret, "concurrent tenant secret update must never be clobbered by rotation")
}

func TestCompactUniqueStringsAndReadinessHelpers(t *testing.T) {
	versions := []string{"v3", "v1", "v3", "v2", "v1"}
	sort.Strings(versions)
	assert.Equal(t, []string{"v1", "v2", "v3"}, compactUniqueStrings(versions))
}
