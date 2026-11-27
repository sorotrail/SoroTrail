package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/apikey"
)

func actionAuditTestBackends(t *testing.T) map[string]ActionAuditStore {
	t.Helper()
	sqlStore, ok := newSQLiteStore(t).(ActionAuditStore)
	require.True(t, ok)

	traced := NewTracingStore(newSQLiteStore(t), nil).(ActionAuditStore)
	guarded := NewGuardedStore(newSQLiteStore(t), GuardedStoreOptions{}).(ActionAuditStore)

	return map[string]ActionAuditStore{
		"memory":  NewMemoryActionAuditStore(),
		"sqlite":  sqlStore,
		"traced":  traced,
		"guarded": guarded,
	}
}

func TestSanitizeActorIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		// Happy path: identifiers without secret material pass through, with
		// surrounding whitespace trimmed.
		{name: "plain identifier is preserved", input: "operator@example.com", want: "operator@example.com"},
		{name: "surrounding whitespace is trimmed", input: "  ci-deployer\t", want: "ci-deployer"},
		{name: "empty identifier stays empty", input: "   ", want: ""},
		{name: "already redacted key is unchanged", input: "sorotrail_0123456789abcdef", want: "sorotrail_0123456789abcdef"},

		// Error path: a secret-bearing actor ID must never be returned raw.
		{name: "full api key keeps only public prefix", input: "sorotrail_0123456789abcdef_supersecret", want: "sorotrail_0123456789abcdef"},
		{name: "tenant api key keeps only public prefix", input: "st_9988776655443322_supersecret", want: "st_9988776655443322"},
		{name: "bearer api key keeps only public prefix", input: "Bearer sorotrail_0123456789abcdef_supersecret", want: "sorotrail_0123456789abcdef"},
		{name: "opaque bearer token is fully redacted", input: "Bearer opaque-token-value", want: "bearer-redacted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeActorIdentifier(tt.input)

			assert.Equal(t, tt.want, got)
			assert.NotContains(t, got, "supersecret", "raw key secret must never be returned")
			assert.NotContains(t, got, "opaque-token-value", "raw bearer token must never be returned")
		})
	}
}

func TestSanitizeActionAuditMetadata(t *testing.T) {
	t.Run("nil and empty metadata return an empty map", func(t *testing.T) {
		assert.Empty(t, SanitizeActionAuditMetadata(nil))
		assert.Empty(t, SanitizeActionAuditMetadata(map[string]any{}))
	})

	t.Run("non-secret values are preserved", func(t *testing.T) {
		in := map[string]any{
			"contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
			"attempt":     2,
		}

		got := SanitizeActionAuditMetadata(in)

		assert.Equal(t, in, got)
		assert.NotSame(t, &in, &got, "sanitizer must return a copy")
	})

	t.Run("keys are trimmed and blank keys dropped", func(t *testing.T) {
		got := SanitizeActionAuditMetadata(map[string]any{"  note  ": "ok", "   ": "dropped"})

		assert.Equal(t, map[string]any{"note": "ok"}, got)
	})

	t.Run("nested maps are sanitized recursively", func(t *testing.T) {
		in := map[string]any{
			"outer": map[string]any{"api_key": "sorotrail_0123456789abcdef_supersecret", "keep": "yes"},
		}

		got := SanitizeActionAuditMetadata(in)

		inner, ok := got["outer"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "yes", inner["keep"])
		assert.NotContains(t, inner, "api_key")
		assert.Equal(t, "sorotrail_0123456789abcdef", inner["key_prefix"])
	})

	t.Run("secret values are replaced by a public key prefix", func(t *testing.T) {
		got := SanitizeActionAuditMetadata(map[string]any{
			"key":    "sorotrail_0123456789abcdef_supersecret",
			"token":  "opaque-token-value",
			"secret": "hunter2",
		})

		assert.Equal(t, "sorotrail_0123456789abcdef", got["key_prefix"])
		assert.NotContains(t, got, "key")
		assert.NotContains(t, got, "token")
		assert.NotContains(t, got, "secret")
		assert.NotContains(t, got, "opaque-token-value")
		assert.NotContains(t, got, "hunter2")
	})
}

func TestActionAuditStore_RecordQueryAndHashChain(t *testing.T) {
	for name, st := range actionAuditTestBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			baseTime := time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC)

			tenantA := int64(101)
			tenantB := int64(202)

			rec1, err := st.RecordActionAudit(ctx, ActionAuditRecord{
				ActorID:      "sorotrail_0123456789abcdef",
				ActorType:    ActorTypePerson,
				Action:       "watched_contract.add",
				ResourceType: "watched_contract",
				ResourceID:   "CCONTRACT1",
				Method:       "POST",
				Route:        "/watched-contracts",
				RemoteAddr:   "192.0.2.10:54321",
				RequestID:    "req-1",
				Status:       ActionAuditStatusSucceeded,
				HTTPStatus:   201,
				Metadata:     map[string]any{"label": "treasury"},
				CreatedAt:    baseTime,
			})
			require.NoError(t, err)
			assert.Equal(t, int64(1), rec1.ID)
			assert.Empty(t, rec1.PrevHash)
			assert.NotEmpty(t, rec1.EntryHash)

			rec2, err := st.RecordActionAudit(ctx, ActionAuditRecord{
				ActorID:      "st_tenantaprefix",
				ActorType:    ActorTypeAutomation,
				TenantID:     &tenantA,
				Action:       "subscription.create",
				ResourceType: "subscription",
				ResourceID:   "sub-101",
				Method:       "POST",
				Route:        "/subscriptions",
				RemoteAddr:   "198.51.100.7:12345",
				RequestID:    "req-2",
				Status:       ActionAuditStatusSucceeded,
				HTTPStatus:   201,
				Metadata:     map[string]any{"url": "https://example.com/hook"},
				CreatedAt:    baseTime.Add(time.Minute),
			})
			require.NoError(t, err)
			assert.Equal(t, rec1.EntryHash, rec2.PrevHash, "second record must chain to first record's entry_hash")
			assert.NotEmpty(t, rec2.EntryHash)

			rec3, err := st.RecordActionAudit(ctx, ActionAuditRecord{
				ActorID:      "st_tenantbprefix",
				ActorType:    ActorTypePerson,
				TenantID:     &tenantB,
				Action:       "subscription.delete",
				ResourceType: "subscription",
				ResourceID:   "sub-101",
				Method:       "DELETE",
				Route:        "/subscriptions/{id}",
				RemoteAddr:   "203.0.113.9:44321",
				RequestID:    "req-3",
				Status:       ActionAuditStatusFailed,
				HTTPStatus:   404,
				ErrorCode:    "not_found",
				CreatedAt:    baseTime.Add(2 * time.Minute),
			})
			require.NoError(t, err)
			assert.Equal(t, rec2.EntryHash, rec3.PrevHash)

			// Operator scope sees all 3 records in descending ID order.
			all, err := st.QueryActionAuditLogs(ctx, ActionAuditFilter{
				Scope: OperatorActionAuditScope(),
			})
			require.NoError(t, err)
			require.Len(t, all, 3)
			assert.Equal(t, rec3.ID, all[0].ID)
			assert.Equal(t, rec2.ID, all[1].ID)
			assert.Equal(t, rec1.ID, all[2].ID)

			// Hash chain verification succeeds on untampered records.
			require.NoError(t, VerifyActionAuditChain(all))

			// Tampering with any field or deleting an intermediate entry breaks the chain.
			tampered := make([]ActionAuditRecord, len(all))
			copy(tampered, all)
			tampered[1].ResourceID = "sub-999"
			require.ErrorIs(t, VerifyActionAuditChain(tampered), ErrActionAuditChainBroken)

			missingMiddle := []ActionAuditRecord{all[0], all[2]}
			require.ErrorIs(t, VerifyActionAuditChain(missingMiddle), ErrActionAuditChainBroken)

			// Tenant A scope sees only Tenant A's record (never Operator's or Tenant B's).
			onlyTenantA, err := st.QueryActionAuditLogs(ctx, ActionAuditFilter{
				Scope: TenantActionAuditScope(tenantA),
			})
			require.NoError(t, err)
			require.Len(t, onlyTenantA, 1)
			assert.Equal(t, rec2.ID, onlyTenantA[0].ID)
			require.NotNil(t, onlyTenantA[0].TenantID)
			assert.Equal(t, tenantA, *onlyTenantA[0].TenantID)

			// Tenant B scope sees only Tenant B's failed attempt.
			onlyTenantB, err := st.QueryActionAuditLogs(ctx, ActionAuditFilter{
				Scope: TenantActionAuditScope(tenantB),
			})
			require.NoError(t, err)
			require.Len(t, onlyTenantB, 1)
			assert.Equal(t, rec3.ID, onlyTenantB[0].ID)
			assert.Equal(t, ActionAuditStatusFailed, onlyTenantB[0].Status)

			// Zero-value scope fails closed with ErrTenantScopeRequired.
			_, err = st.QueryActionAuditLogs(ctx, ActionAuditFilter{})
			require.ErrorIs(t, err, ErrTenantScopeRequired)
			_, err = st.QueryActionAuditLogs(ctx, ActionAuditFilter{
				Scope: TenantActionAuditScope(0),
			})
			require.ErrorIs(t, err, ErrTenantScopeRequired)
		})
	}
}

func TestActionAuditStore_NeverStoresSecretMaterial(t *testing.T) {
	fullKey, prefix, secret, err := apikey.Generate()
	require.NoError(t, err)

	tenantFullKey := "st_9988776655443322_deadbeefcafebabe1122334455667788"
	webhookSecret := "whsec_ultra_sensitive_signing_secret"

	st := newSQLiteStore(t).(ActionAuditStore)
	ctx := context.Background()

	rec, err := st.RecordActionAudit(ctx, ActionAuditRecord{
		ActorID:      "Bearer " + fullKey,
		ActorType:    ActorTypePerson,
		Action:       "apikey.create",
		ResourceType: "apikey",
		ResourceID:   tenantFullKey,
		Method:       "POST",
		Route:        "/apikeys",
		RemoteAddr:   "127.0.0.1:8080",
		RequestID:    "req-sec",
		Status:       ActionAuditStatusSucceeded,
		HTTPStatus:   201,
		Metadata: map[string]any{
			"key":            fullKey,
			"secret":         webhookSecret,
			"webhook_secret": webhookSecret,
			"password":       "hunter2",
			"token":          "raw-token-value",
			"label":          "ci-deployer",
		},
	})
	require.NoError(t, err)

	assert.Equal(t, "sorotrail_"+prefix, rec.ActorID, "actor_id must keep only the public key prefix")
	assert.NotContains(t, rec.ActorID, secret)
	assert.Equal(t, "st_9988776655443322", rec.ResourceID, "resource_id must redact secret suffix")
	assert.NotContains(t, rec.ResourceID, "deadbeefcafebabe1122334455667788")

	assert.Equal(t, "sorotrail_"+prefix, rec.Metadata["key_prefix"])
	assert.Equal(t, "ci-deployer", rec.Metadata["label"])
	assert.NotContains(t, rec.Metadata, "key")
	assert.NotContains(t, rec.Metadata, "secret")
	assert.NotContains(t, rec.Metadata, "webhook_secret")
	assert.NotContains(t, rec.Metadata, "password")
	assert.NotContains(t, rec.Metadata, "token")

	stored, err := st.QueryActionAuditLogs(ctx, ActionAuditFilter{
		Scope: OperatorActionAuditScope(),
	})
	require.NoError(t, err)
	require.Len(t, stored, 1)
	assert.Equal(t, "sorotrail_"+prefix, stored[0].ActorID)
	assert.Equal(t, "sorotrail_"+prefix, stored[0].Metadata["key_prefix"])
	assert.NotContains(t, stored[0].Metadata, "secret")
}

func TestActionAuditRetention_SeparateFromEventRetention(t *testing.T) {
	rawStore := newSQLiteStore(t)
	auditStore := rawStore.(ActionAuditStore)
	ctx := context.Background()

	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	// Seed an old event (30 days old) and an old audit log (30 days old) plus
	// a very old audit log (400 days old).
	_, err := rawStore.UpsertEvents(ctx, []Event{
		{
			Network:    "testnet",
			ID:         "0000000000000000100-0000000001",
			ContractID: "CCONTRACT1",
			Ledger:     100,
			Type:       "contract",
			TxHash:     "tx1",
			Topics:     json.RawMessage("[]"),
			Value:      json.RawMessage("{}"),
			CreatedAt:  now.Add(-30 * 24 * time.Hour),
		},
	})
	require.NoError(t, err)

	oldAudit400d, err := auditStore.RecordActionAudit(ctx, ActionAuditRecord{
		ActorID:      "sorotrail_0123456789abcdef",
		ActorType:    ActorTypePerson,
		Action:       "apikey.create",
		ResourceType: "apikey",
		ResourceID:   "1",
		Status:       ActionAuditStatusSucceeded,
		CreatedAt:    now.Add(-400 * 24 * time.Hour),
	})
	require.NoError(t, err)

	recentAudit30d, err := auditStore.RecordActionAudit(ctx, ActionAuditRecord{
		ActorID:      "sorotrail_0123456789abcdef",
		ActorType:    ActorTypePerson,
		Action:       "watched_contract.add",
		ResourceType: "watched_contract",
		ResourceID:   "CCONTRACT1",
		Status:       ActionAuditStatusSucceeded,
		CreatedAt:    now.Add(-30 * 24 * time.Hour),
	})
	require.NoError(t, err)

	// 1. Running event retention (deleting events before ledger 200) deletes the
	// event row but MUST NOT touch the 30-day-old or 400-day-old administrative action audit records.
	deletedEvents, err := rawStore.DeleteEventsBefore(ctx, 200, now.Add(-7*24*time.Hour), 1000)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deletedEvents)

	logsAfterEventPrune, err := auditStore.QueryActionAuditLogs(ctx, ActionAuditFilter{
		Scope: OperatorActionAuditScope(),
	})
	require.NoError(t, err)
	require.Len(t, logsAfterEventPrune, 2, "event retention pruning must never delete administrative action audit records")

	// 2. Running the separate action audit retention policy (default 365 days)
	// prunes the 400-day-old audit record while preserving the 30-day-old audit record.
	prunedAudits, err := PruneActionAuditLogs(ctx, auditStore, ActionAuditRetentionOptions{
		Age: DefaultActionAuditRetentionAge,
		Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), prunedAudits)

	remainingAudits, err := auditStore.QueryActionAuditLogs(ctx, ActionAuditFilter{
		Scope: OperatorActionAuditScope(),
	})
	require.NoError(t, err)
	require.Len(t, remainingAudits, 1)
	assert.Equal(t, recentAudit30d.ID, remainingAudits[0].ID)
	assert.NotEqual(t, oldAudit400d.ID, remainingAudits[0].ID)
}

func TestNormalizeActionAuditRecord(t *testing.T) {
	t.Run("explicit values are kept, trimmed, and converted to UTC", func(t *testing.T) {
		created := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.FixedZone("UTC+2", 2*60*60))

		got := NormalizeActionAuditRecord(ActionAuditRecord{
			ActorID:      "  operator@example.com ",
			ActorType:    " PERSON ",
			Action:       " watched_contract.add ",
			ResourceType: " watched_contract ",
			ResourceID:   " CCONTRACT1 ",
			Method:       " POST ",
			Route:        " /v1/admin/watched-contracts ",
			RemoteAddr:   " 127.0.0.1:5432 ",
			RequestID:    " req-1 ",
			Status:       " SUCCEEDED ",
			ErrorCode:    " none ",
			Metadata:     map[string]any{"contract_id": "CCONTRACT1"},
			CreatedAt:    created,
		})

		assert.Equal(t, "operator@example.com", got.ActorID)
		assert.Equal(t, ActorTypePerson, got.ActorType)
		assert.Equal(t, "watched_contract.add", got.Action)
		assert.Equal(t, "watched_contract", got.ResourceType)
		assert.Equal(t, "CCONTRACT1", got.ResourceID)
		assert.Equal(t, "POST", got.Method)
		assert.Equal(t, "/v1/admin/watched-contracts", got.Route)
		assert.Equal(t, "127.0.0.1:5432", got.RemoteAddr)
		assert.Equal(t, "req-1", got.RequestID)
		assert.Equal(t, ActionAuditStatusSucceeded, got.Status)
		assert.Equal(t, "none", got.ErrorCode)
		assert.Equal(t, map[string]any{"contract_id": "CCONTRACT1"}, got.Metadata)

		// A caller-supplied timestamp is preserved to the microsecond, but the
		// stored value is always UTC.
		assert.Equal(t, time.UTC, got.CreatedAt.Location())
		assert.True(t, created.Truncate(time.Microsecond).Equal(got.CreatedAt), "got %s", got.CreatedAt)
	})

	t.Run("an empty record gets safe defaults", func(t *testing.T) {
		before := time.Now().UTC()
		got := NormalizeActionAuditRecord(ActionAuditRecord{})

		assert.Equal(t, "anonymous", got.ActorID)
		assert.Equal(t, ActorTypePerson, got.ActorType)
		assert.Equal(t, ActionAuditStatusSucceeded, got.Status)
		assert.Empty(t, got.Metadata)
		assert.Equal(t, time.UTC, got.CreatedAt.Location())
		assert.False(t, got.CreatedAt.Before(before.Truncate(time.Microsecond)), "got %s", got.CreatedAt)
		assert.WithinDuration(t, time.Now().UTC(), got.CreatedAt, time.Minute)
	})

	t.Run("actor type and status normalize from their known synonyms", func(t *testing.T) {
		tests := []struct {
			name          string
			actorType     string
			status        string
			wantActorType string
			wantStatus    string
		}{
			{
				name:          "person stays a person",
				actorType:     ActorTypePerson,
				status:        ActionAuditStatusSucceeded,
				wantActorType: ActorTypePerson,
				wantStatus:    ActionAuditStatusSucceeded,
			},
			{
				name:          "bot collapses to automation",
				actorType:     "bot",
				status:        ActionAuditStatusFailed,
				wantActorType: ActorTypeAutomation,
				wantStatus:    ActionAuditStatusFailed,
			},
			{
				name:          "scheduled job collapses to automation",
				actorType:     " SCHEDULED_JOB ",
				status:        " FAILED ",
				wantActorType: ActorTypeAutomation,
				wantStatus:    ActionAuditStatusFailed,
			},
			{
				// The non-happy path: unrecognized values must never be stored
				// verbatim, they fall back to the conservative defaults.
				name:          "unknown actor type falls back to person",
				actorType:     "alien",
				status:        ActionAuditStatusSucceeded,
				wantActorType: ActorTypePerson,
				wantStatus:    ActionAuditStatusSucceeded,
			},
			{
				name:          "unknown status falls back to succeeded",
				actorType:     ActorTypePerson,
				status:        "bogus",
				wantActorType: ActorTypePerson,
				wantStatus:    ActionAuditStatusSucceeded,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := NormalizeActionAuditRecord(ActionAuditRecord{
					ActorType: tt.actorType,
					Status:    tt.status,
				})

				assert.Equal(t, tt.wantActorType, got.ActorType)
				assert.Equal(t, tt.wantStatus, got.Status)
			})
		}
	})

	t.Run("secret material is redacted before it can be persisted", func(t *testing.T) {
		got := NormalizeActionAuditRecord(ActionAuditRecord{
			ActorID:    "Bearer sorotrail_0123456789abcdef_supersecret",
			ResourceID: "st_9988776655443322_supersecret",
			Metadata: map[string]any{
				"api_key": "sorotrail_0123456789abcdef_supersecret",
				"note":    "kept",
			},
		})

		assert.Equal(t, "sorotrail_0123456789abcdef", got.ActorID)
		assert.Equal(t, "st_9988776655443322", got.ResourceID)
		assert.NotContains(t, got.Metadata, "api_key")
		assert.Equal(t, "sorotrail_0123456789abcdef", got.Metadata["key_prefix"])
		assert.Equal(t, "kept", got.Metadata["note"])

		// No field of the normalized record may carry the raw secret, including
		// metadata values nested under non-secret keys.
		raw, err := json.Marshal(got)
		require.NoError(t, err)
		assert.NotContains(t, string(raw), "supersecret", "raw secret must never reach the audit record")
	})
}
