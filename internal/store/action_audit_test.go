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
