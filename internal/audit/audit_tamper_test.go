//go:build integration

package audit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/testdb"
)

func TestAuditChainIntegrityAndScenarios(t *testing.T) {
	db := testdb.Setup(t, nil)
	if db == nil {
		return
	}
	ctx := context.Background()

	tenant1 := int64(100)
	tenant2 := int64(200)

	t1 := time.Now().UTC()
	r1 := Record{
		Sequence:  1,
		Timestamp: t1,
		TenantID:  &tenant1,
		Action:    "KEY_CREATE",
		Actor:     "admin",
		Target:    "key_1",
		PrevHash:  "",
	}
	h1, err := ComputeHash(r1)
	require.NoError(t, err)
	r1.Hash = h1

	_, err = db.Exec(ctx, `INSERT INTO audit_records (sequence, timestamp, tenant_id, action, actor, target, prev_hash, hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, r1.Sequence, r1.Timestamp, r1.TenantID, r1.Action, r1.Actor, r1.Target, r1.PrevHash, r1.Hash)
	require.NoError(t, err)

	r2 := Record{
		Sequence:  2,
		Timestamp: t1.Add(time.Second),
		TenantID:  &tenant2,
		Action:    "TENANT_CHANGE",
		Actor:     "admin",
		Target:    "tenant_2",
		PrevHash:  h1,
	}
	h2, err := ComputeHash(r2)
	require.NoError(t, err)
	r2.Hash = h2

	_, err = db.Exec(ctx, `INSERT INTO audit_records (sequence, timestamp, tenant_id, action, actor, target, prev_hash, hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, r2.Sequence, r2.Timestamp, r2.TenantID, r2.Action, r2.Actor, r2.Target, r2.PrevHash, r2.Hash)
	require.NoError(t, err)

	v := NewVerifier(db)

	// 1. Intact chain global verification
	last, err := v.VerifyChain(ctx, 1, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), last)

	// Multi-tenant visibility: verifying tenant1 should only see/verify tenant1 entries
	lastT1, err := v.VerifyChain(ctx, 1, &tenant1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), lastT1)

	// Checkpoint emission
	cp, err := EmitCheckpoint(ctx, db)
	assert.NoError(t, err)
	assert.NotNil(t, cp)
	assert.Equal(t, int64(2), cp.Sequence)

	// 2. Retention sweep test with checkpointing
	// Perform checkpoint before pruning historical records
	_, err = EmitCheckpointForRange(ctx, db, 1)
	assert.NoError(t, err)

	// Simulate retention sweep pruning sequence 1 after checkpointing
	_, err = db.Exec(ctx, `DELETE FROM audit_records WHERE sequence = 1`)
	require.NoError(t, err)

	// Verification starting from remaining sequence 2 should succeed because a checkpoint exists for prior state
	lastRet, err := v.VerifyChain(ctx, 2, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), lastRet)

	// Restore table for next tests
	_, err = db.Exec(ctx, `INSERT INTO audit_records (sequence, timestamp, tenant_id, action, actor, target, prev_hash, hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, r1.Sequence, r1.Timestamp, r1.TenantID, r1.Action, r1.Actor, r1.Target, r1.PrevHash, r1.Hash)
	require.NoError(t, err)

	// 3. Truncated table / missing records simulation (without checkpoint covering gap)
	_, err = db.Exec(ctx, `DELETE FROM audit_records WHERE sequence = 2`)
	require.NoError(t, err)
	_, err = v.VerifyChain(ctx, 1, nil)
	assert.Error(t, err, "truncated table must fail verification")

	// Restore row 2
	_, err = db.Exec(ctx, `INSERT INTO audit_records (sequence, timestamp, tenant_id, action, actor, target, prev_hash, hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, r2.Sequence, r2.Timestamp, r2.TenantID, r2.Action, r2.Actor, r2.Target, r2.PrevHash, r2.Hash)
	require.NoError(t, err)

	// 4. Altered record verification failure
	_, err = db.Exec(ctx, `UPDATE audit_records SET hash = 'tampered' WHERE sequence = 1`)
	require.NoError(t, err)
	_, err = v.VerifyChain(ctx, 1, nil)
	assert.Error(t, err, "altered record must fail verification")
}
