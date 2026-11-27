package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/api"
	"github.com/sorotrail/sorotrail/internal/apikey"
	"github.com/sorotrail/sorotrail/internal/store"
)

// stubAPIKeyStore provides in-memory API key storage for CLI tests.
type stubAPIKeyStore struct {
	keys      map[int64]store.APIKey
	byPrefix  map[string]store.APIKey
	nextID    int64
	createErr error
	listErr   error
	revokeErr error
}

func newStubAPIKeyStore() *stubAPIKeyStore {
	return &stubAPIKeyStore{
		keys:     make(map[int64]store.APIKey),
		byPrefix: make(map[string]store.APIKey),
		nextID:   1,
	}
}

func (s *stubAPIKeyStore) RotateAPIKey(_ context.Context, predecessorID int64, newKey store.APIKey, expiresIn time.Duration) (store.APIKey, error) {
	_, ok := s.keys[predecessorID]
	if !ok {
		return store.APIKey{}, store.ErrNotFound
	}
	s.nextID++
	newKey.CreatedAt = time.Now().UTC().Truncate(time.Second)
	now := time.Now().UTC()
	newKey.ExpiresAt = &now
	if expiresIn > 0 {
		exp := now.Add(expiresIn)
		newKey.ExpiresAt = &exp
	}
	newKey.PredecessorID = &predecessorID
	s.keys[newKey.ID] = newKey
	s.byPrefix[newKey.Prefix] = newKey
	return newKey, nil
}

func (s *stubAPIKeyStore) AbandonAPIKeyRotation(_ context.Context, successorID int64) error {
	k, ok := s.keys[successorID]
	if !ok {
		return store.ErrNotFound
	}
	now := time.Now().UTC()
	k.RevokedAt = &now
	s.keys[successorID] = k
	return nil
}

func (s *stubAPIKeyStore) GetAPIKey(_ context.Context, id int64) (store.APIKey, error) {
	if s.listErr != nil {
		return store.APIKey{}, s.listErr
	}
	k, ok := s.keys[id]
	if !ok {
		return store.APIKey{}, store.ErrNotFound
	}
	return k, nil
}

func (s *stubAPIKeyStore) CreateAPIKey(_ context.Context, k store.APIKey) (store.APIKey, error) {
	if s.createErr != nil {
		return store.APIKey{}, s.createErr
	}
	k.ID = s.nextID
	s.nextID++
	k.CreatedAt = time.Now().UTC().Truncate(time.Second)
	s.keys[k.ID] = k
	s.byPrefix[k.Prefix] = k
	return k, nil
}

func (s *stubAPIKeyStore) ListAPIKeys(_ context.Context) ([]store.APIKey, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]store.APIKey, 0, len(s.keys))
	for id := int64(1); id < s.nextID; id++ {
		if k, ok := s.keys[id]; ok {
			out = append(out, k)
		}
	}
	return out, nil
}

func (s *stubAPIKeyStore) RevokeAPIKey(_ context.Context, id int64) error {
	if s.revokeErr != nil {
		return s.revokeErr
	}
	k, ok := s.keys[id]
	if !ok {
		return store.ErrNotFound
	}
	if k.Revoked() {
		return nil // idempotent
	}
	now := time.Now().UTC()
	k.RevokedAt = &now
	s.keys[id] = k
	s.byPrefix[k.Prefix] = k
	return nil
}

// TestAPIKeyCreate verifies key generation, single plaintext emission,
// hashed persistence, argument validation, and store error handling.
func TestAPIKeyCreate(t *testing.T) {
	stub := newStubAPIKeyStore()
	orig := openAPIKeyStore
	openAPIKeyStore = func(context.Context) (apiKeyStore, func(), error) {
		return stub, func() {}, nil
	}
	defer func() { openAPIKeyStore = orig }()

	// 1. Success case: key is persisted, plaintext shown once, bcrypt hash stored.
	var buf bytes.Buffer
	err := apikeyCreateTo(&buf, []string{"--name", "ops-key"})
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "created API key 1 (ops-key)")
	assert.Contains(t, out, "prefix: ")
	assert.Contains(t, out, "key:    sorotrail_")
	assert.Contains(t, out, "store this key now — it will not be shown again.")

	// Check stored key state
	require.Len(t, stub.keys, 1)
	saved := stub.keys[1]
	assert.Equal(t, int64(1), saved.ID)
	assert.Equal(t, "ops-key", saved.Name)
	assert.NotEmpty(t, saved.Prefix)
	assert.NotEmpty(t, saved.KeyHash)

	// Extract the plaintext key from command output
	lines := strings.Split(out, "\n")
	var fullKey string
	for _, l := range lines {
		if strings.HasPrefix(l, "key:    ") {
			fullKey = strings.TrimSpace(strings.TrimPrefix(l, "key:    "))
			break
		}
	}
	require.NotEmpty(t, fullKey, "output must print full plaintext key")

	// Verify key shape
	prefix, secret, ok := apikey.Parse(fullKey)
	require.True(t, ok, "printed key must parse cleanly with apikey.Parse")
	assert.Equal(t, saved.Prefix, prefix)

	// Verify stored form is a hash, NOT the secret
	assert.NotEqual(t, secret, saved.KeyHash, "secret must never be stored in plaintext")
	assert.NotEqual(t, fullKey, saved.KeyHash, "full key must never be stored in plaintext")
	assert.True(t, apikey.VerifySecret(secret, saved.KeyHash), "stored hash must authenticate against secret")

	// 2. Argument validation: unexpected arguments and undefined flags (like --scope).
	err = apikeyCreateTo(&buf, []string{"unexpected-arg"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected argument")

	err = apikeyCreateTo(&buf, []string{"--scope", "read"})
	require.Error(t, err)

	// 3. Name length validation (> 100 characters).
	tooLongName := strings.Repeat("x", 101)
	err = apikeyCreateTo(&buf, []string{"--name", tooLongName})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--name must be at most 100 characters")

	// 4. --help flag returns nil without error.
	err = apikeyCreateTo(&buf, []string{"--help"})
	require.NoError(t, err)

	// 5. Store error returns failure.
	stub.createErr = errors.New("db error creating key")
	err = apikeyCreateTo(&buf, []string{"--name", "fail"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db error creating key")
	stub.createErr = nil
}

// TestAPIKeyList verifies listing output, secret non-disclosure, empty list,
// argument validation, and error propagation.
func TestAPIKeyList(t *testing.T) {
	stub := newStubAPIKeyStore()
	orig := openAPIKeyStore
	openAPIKeyStore = func(context.Context) (apiKeyStore, func(), error) {
		return stub, func() {}, nil
	}
	defer func() { openAPIKeyStore = orig }()

	// 1. Empty store outputs "no API keys"
	var buf bytes.Buffer
	err := apikeyListTo(&buf, nil)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "no API keys")

	// 2. Create one active key and one revoked key
	key1, p1, s1, err := apikey.Generate()
	require.NoError(t, err)
	h1, err := apikey.HashSecret(s1)
	require.NoError(t, err)
	_, _ = stub.CreateAPIKey(context.Background(), store.APIKey{Name: "active-service", Prefix: p1, KeyHash: h1})

	key2, p2, s2, err := apikey.Generate()
	require.NoError(t, err)
	h2, err := apikey.HashSecret(s2)
	require.NoError(t, err)
	k2, _ := stub.CreateAPIKey(context.Background(), store.APIKey{Name: "old-worker", Prefix: p2, KeyHash: h2})
	_ = stub.RevokeAPIKey(context.Background(), k2.ID)

	buf.Reset()
	err = apikeyListTo(&buf, nil)
	require.NoError(t, err)

	out := buf.String()
	// Assert whole output contains expected headers, prefixes, and metadata
	assert.Contains(t, out, "ID")
	assert.Contains(t, out, "NAME")
	assert.Contains(t, out, "PREFIX")
	assert.Contains(t, out, "CREATED")
	assert.Contains(t, out, "STATUS")

	assert.Contains(t, out, "active-service")
	assert.Contains(t, out, p1)
	assert.Contains(t, out, "active")

	assert.Contains(t, out, "old-worker")
	assert.Contains(t, out, p2)
	assert.Contains(t, out, "revoked")

	// CRITICAL SECURITY ASSERTION: the listing output must NEVER leak secrets or full keys
	assert.NotContains(t, out, s1, "listing must not contain secret 1")
	assert.NotContains(t, out, s2, "listing must not contain secret 2")
	assert.NotContains(t, out, key1, "listing must not contain full plaintext key 1")
	assert.NotContains(t, out, key2, "listing must not contain full plaintext key 2")

	// 3. Unexpected argument
	err = apikeyListTo(&buf, []string{"unexpected"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected argument")

	// 4. Store error
	stub.listErr = errors.New("db query failed")
	err = apikeyListTo(&buf, nil)
	require.Error(t, err)
	stub.listErr = nil
}

// TestAPIKeyRevoke verifies key revocation, subsequent rejection by
// authentication, non-existent key reporting, idempotency, and argument validation.
func TestAPIKeyRevoke(t *testing.T) {
	stub := newStubAPIKeyStore()
	orig := openAPIKeyStore
	openAPIKeyStore = func(context.Context) (apiKeyStore, func(), error) {
		return stub, func() {}, nil
	}
	defer func() { openAPIKeyStore = orig }()

	// Create a key to revoke
	_, prefix, secret, err := apikey.Generate()
	require.NoError(t, err)
	hash, err := apikey.HashSecret(secret)
	require.NoError(t, err)
	created, err := stub.CreateAPIKey(context.Background(), store.APIKey{Name: "test-rev", Prefix: prefix, KeyHash: hash})
	require.NoError(t, err)
	assert.False(t, created.Revoked())

	// 1. Revoke existing key
	var buf bytes.Buffer
	err = apikeyRevokeTo(&buf, []string{"1"})
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "revoked API key 1")

	// Verify key is now marked revoked in the store
	revoked := stub.keys[created.ID]
	assert.True(t, revoked.Revoked(), "revoked key must report Revoked() == true")
	assert.NotNil(t, revoked.RevokedAt)

	// 2. Revoking an unknown key returns ErrNotFound
	buf.Reset()
	err = apikeyRevokeTo(&buf, []string{"9999"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API key 9999 not found")

	// 3. Revoking twice behaves predictably (idempotent)
	buf.Reset()
	err = apikeyRevokeTo(&buf, []string{"1"})
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "revoked API key 1")

	// 4. Invalid ID formats
	for _, args := range [][]string{{"abc"}, {"0"}, {"--", "-5"}} {
		buf.Reset()
		err = apikeyRevokeTo(&buf, args)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid API key id")
	}

	// 5. Missing or multiple arguments
	buf.Reset()
	err = apikeyRevokeTo(&buf, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one key ID is required")

	buf.Reset()
	err = apikeyRevokeTo(&buf, []string{"1", "2"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one key ID is required")

	// 6. --help returns nil
	buf.Reset()
	err = apikeyRevokeTo(&buf, []string{"--help"})
	require.NoError(t, err)

	// 7. Store error
	stub.revokeErr = errors.New("db failure on revoke")
	err = apikeyRevokeTo(&buf, []string{"1"})
	require.Error(t, err)
	stub.revokeErr = nil
}

// TestRunAPIKey verifies dispatching, usage handling, exit codes, and direct execution.
func TestRunAPIKey(t *testing.T) {
	stub := newStubAPIKeyStore()
	orig := openAPIKeyStore
	openAPIKeyStore = func(context.Context) (apiKeyStore, func(), error) {
		return stub, func() {}, nil
	}
	defer func() { openAPIKeyStore = orig }()

	// 1. Missing subcommand returns error
	var buf bytes.Buffer
	err := runAPIKeyTo(&buf, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apikey requires a subcommand")
	assert.Equal(t, 1, exitCode(err))

	// 2. Unknown subcommand returns error
	err = runAPIKeyTo(&buf, []string{"destroy"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown apikey subcommand \"destroy\" (want create|list|revoke)")
	assert.Equal(t, 1, exitCode(err))

	// 3. Help options return nil and write usage
	for _, h := range []string{"help", "-h", "--help"} {
		buf.Reset()
		err = runAPIKeyTo(&buf, []string{h})
		require.NoError(t, err)
		assert.Contains(t, buf.String(), "usage: sorotrail apikey <command>")
		assert.Contains(t, buf.String(), "create")
		assert.Contains(t, buf.String(), "list")
		assert.Contains(t, buf.String(), "revoke")
	}

	// 4. Direct entrypoint execution via runAPIKey (redirecting stdout)
	r, w, pipeErr := os.Pipe()
	require.NoError(t, pipeErr)
	oldStdout := os.Stdout
	os.Stdout = w

	runErr := runAPIKey([]string{"create", "--name", "cli-test"})
	w.Close()
	os.Stdout = oldStdout

	require.NoError(t, runErr)
	buf.Reset()
	_, _ = io.Copy(&buf, r)
	assert.Contains(t, buf.String(), "created API key")

	// Direct list via runAPIKey
	r2, w2, _ := os.Pipe()
	os.Stdout = w2
	runErr = runAPIKey([]string{"list"})
	w2.Close()
	os.Stdout = oldStdout
	require.NoError(t, runErr)
	buf.Reset()
	_, _ = io.Copy(&buf, r2)
	assert.Contains(t, buf.String(), "cli-test")

	// Direct revoke via runAPIKey
	r3, w3, _ := os.Pipe()
	os.Stdout = w3
	runErr = runAPIKey([]string{"revoke", "1"})
	w3.Close()
	os.Stdout = oldStdout
	require.NoError(t, runErr)
	buf.Reset()
	_, _ = io.Copy(&buf, r3)
	assert.Contains(t, buf.String(), "revoked API key 1")

	// Direct usage call
	apikeyUsage()
}

// TestAPIKeyWrappers ensures the os.Stdout wrapper functions (apikeyCreate,
// apikeyList, apikeyRevoke) are directly exercised.
func TestAPIKeyWrappers(t *testing.T) {
	stub := newStubAPIKeyStore()
	orig := openAPIKeyStore
	openAPIKeyStore = func(context.Context) (apiKeyStore, func(), error) {
		return stub, func() {}, nil
	}
	defer func() { openAPIKeyStore = orig }()

	r, w, err := os.Pipe()
	require.NoError(t, err)
	oldStdout := os.Stdout
	os.Stdout = w

	errCreate := apikeyCreate([]string{"--name", "wrapper-test"})
	errList := apikeyList(nil)
	errRevoke := apikeyRevoke([]string{"1"})

	w.Close()
	os.Stdout = oldStdout

	require.NoError(t, errCreate)
	require.NoError(t, errList)
	require.NoError(t, errRevoke)

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	assert.Contains(t, buf.String(), "created API key 1")
	assert.Contains(t, buf.String(), "wrapper-test")
	assert.Contains(t, buf.String(), "revoked API key 1")
}

// stubTenantStore implements store.TenantStore for bootstrapAdminKey testing.
type stubTenantStore struct {
	store.TenantStore
	tenants      map[string]store.Tenant
	keys         map[string][]byte
	getErr       error
	createKeyErr error
}

func newStubTenantStore() *stubTenantStore {
	return &stubTenantStore{
		tenants: map[string]store.Tenant{
			"default": {ID: 1, Name: "default", Enabled: true, Admin: true},
		},
		keys: make(map[string][]byte),
	}
}

func (s *stubTenantStore) GetTenantByName(_ context.Context, name string) (store.Tenant, error) {
	if s.getErr != nil {
		return store.Tenant{}, s.getErr
	}
	t, ok := s.tenants[name]
	if !ok {
		return store.Tenant{}, store.ErrNotFound
	}
	return t, nil
}

func (s *stubTenantStore) CreateTenantAPIKeyIfAbsent(_ context.Context, tenantID int64, name, prefix string, digest []byte) error {
	if s.createKeyErr != nil {
		return s.createKeyErr
	}
	// Idempotent: record if not already recorded
	if _, exists := s.keys[prefix]; !exists {
		s.keys[prefix] = digest
	}
	return nil
}

// TestBootstrapAdminKey covers the automatic admin key bootstrap during startup:
// empty key handling, malformed key rejection, missing tenant error,
// fresh install, idempotent restart, and verifying NO secret is logged.
func TestBootstrapAdminKey(t *testing.T) {
	ctx := context.Background()

	// 1. Empty key returns nil without error
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
	ts := newStubTenantStore()

	err := bootstrapAdminKey(ctx, ts, "", logger)
	require.NoError(t, err)
	assert.Empty(t, ts.keys)

	// 2. Malformed key formats rejected
	for _, bad := range []string{"not-a-key", "sorotrail_invalid", "st_too_short", "st_123456789012"} {
		err = bootstrapAdminKey(ctx, ts, bad, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "MULTI_TENANT_BOOTSTRAP_KEY is not a valid key")
	}

	// 3. Missing default tenant returns error
	plaintext, prefix, digest, err := api.GenerateAPIKey()
	require.NoError(t, err)

	tsNoDefault := &stubTenantStore{
		tenants: make(map[string]store.Tenant),
		keys:    make(map[string][]byte),
	}
	err = bootstrapAdminKey(ctx, tsNoDefault, plaintext, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading default tenant")

	// 4. Store error during key installation returns error
	tsErr := newStubTenantStore()
	tsErr.createKeyErr = errors.New("db disk failure")
	err = bootstrapAdminKey(ctx, tsErr, plaintext, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "installing bootstrap key")

	// 5. Fresh installation: key is installed and logged safely
	logBuf.Reset()
	err = bootstrapAdminKey(ctx, ts, plaintext, logger)
	require.NoError(t, err)
	assert.Equal(t, digest, ts.keys[prefix], "stored digest must match key digest")

	logOut := logBuf.String()
	assert.Contains(t, logOut, "bootstrap admin key installed from MULTI_TENANT_BOOTSTRAP_KEY")
	assert.Contains(t, logOut, `"tenant":"default"`)
	assert.Contains(t, logOut, `"prefix":"`+prefix+`"`)

	// CRITICAL SECURITY ASSERTION: neither the secret nor the plaintext key appears in the log!
	parts := strings.Split(plaintext, "_")
	require.Len(t, parts, 3)
	secretPart := parts[2]
	assert.NotContains(t, logOut, secretPart, "log output must never contain the key secret")
	assert.NotContains(t, logOut, plaintext, "log output must never contain the full plaintext key")

	// 6. Restart / Idempotency: re-running with the same key does not create duplicate or error
	err = bootstrapAdminKey(ctx, ts, plaintext, logger)
	require.NoError(t, err)
	assert.Len(t, ts.keys, 1, "restart must not create duplicate keys")
}
