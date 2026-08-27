//go:build integration

package store

// Coverage for migration 0022_event_quick_filter_indexes: the composite and
// partial indexes added for the frequent "contract_id [+ ledger range]" and
// "contract_id ORDER BY id" query shapes that /events and
// /contracts/{id}/events issue.
//
// Two layers of coverage. The catalog layer asserts the index set landed
// with the exact column order and partial predicate the queries need (the
// same catalog-driven pattern as TestTxHashIndex and
// TestPartialIndexForSuccessfulCalls). The planner layer seeds enough rows
// that the planner deterministically prefers an index over a seq scan, and
// asserts the targeted query shapes are answered from the index — including,
// where the index's column order matches the ORDER BY, that no separate
// Sort node appears.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	idxCLI  = "idx_events_contract_ledger_id"
	idxCI   = "idx_events_contract_id_id"
	idxCLIS = "idx_events_contract_ledger_id_successful"
)

// TestQuickFilterIndexes_Exist reads the events table's indexes back from
// the catalog and asserts the migration landed the shape the query paths
// depend on: the widened composite, the id-ordering composite, the partial
// successful-calls variant, and that the superseded single-column index was
// dropped.
func TestQuickFilterIndexes_Exist(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	// The three indexes 0022 creates must exist on the events parent.
	for _, name := range []string{idxCLI, idxCI, idxCLIS} {
		t.Run(name, func(t *testing.T) {
			var tableName string
			err := st.pool.QueryRow(ctx,
				`SELECT tablename FROM pg_indexes WHERE indexname = $1`, name,
			).Scan(&tableName)
			require.NoError(t, err, "index %s should exist after migrations", name)
			assert.Equal(t, "events", tableName)
		})
	}

	// Column order matters for btree prefix seeks, so assert it directly.
	var cliDef, ciDef string
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT indexdef FROM pg_indexes WHERE indexname = $1`, idxCLI).Scan(&cliDef))
	require.NoError(t, st.pool.QueryRow(ctx,
		`SELECT indexdef FROM pg_indexes WHERE indexname = $1`, idxCI).Scan(&ciDef))

	assert.Contains(t, cliDef, "(contract_id, ledger, id)",
		"the range+id ordering index must be (contract_id, ledger, id)")
	assert.Contains(t, ciDef, "(contract_id, id)",
		"the default-order index must be (contract_id, id)")

	// The partial variant must still be partial (predicate on the catalog
	// pg_index.indpred column, not merely the definition string).
	var isPartial bool
	require.NoError(t, st.pool.QueryRow(ctx, `
		SELECT i.indpred IS NOT NULL
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = $1`, idxCLIS).Scan(&isPartial))
	assert.True(t, isPartial, "%s should have a partial predicate", idxCLIS)

	// 0022 drops the indexes it supersedes.
	assert.False(t, indexExists(ctx, st.pool, "idx_events_contract_id"))
	assert.False(t, indexExists(ctx, st.pool, "idx_events_contract_ledger"))
	assert.False(t, indexExists(ctx, st.pool, "idx_events_contract_ledger_successful"))
}

// seedQuickFilterRows writes a wide table: many contracts' rows fill one
// partition while the target contract owns a small slice, spread across the
// full ledger window, with a successful/failed split to exercise the partial
// index. The volume makes the always-index-backed claim in the default-mode
// EXPLAIN subtest hold (a narrow predicate never degrades to a Seq Scan).
func seedQuickFilterRows(t *testing.T, st *Postgres, targetContract string) {
	t.Helper()
	ctx := context.Background()

	const rows = 30_000
	events := make([]Event, 0, rows)
	for i := 0; i < rows; i++ {
		contract := otherContract(i)
		if i%50 == 0 {
			contract = targetContract
		}
		ledger := int64(1 + i%200)

		e := testEvent(eventID(i), ledger, contract)
		// Give the target contract a successful/failed split so the
		// partial-index predicate is exercised too.
		if i%50 == 0 && (i/50)%2 == 1 {
			e.InSuccessfulCall = false
		}
		events = append(events, e)
	}
	_, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	// Fresh planner statistics after a bulk load.
	_, err = st.pool.Exec(ctx, `ANALYZE events`)
	require.NoError(t, err)
}

// otherContract returns a distinct, well-formed contract strkey for a row
// index, spread so no single contract dominates the table.
func otherContract(i int) string {
	return fmt.Sprintf("C%036d%018d", (i*7919)%1_000_000, 0)
}

// explain returns the textual plan for a query. When forceIndex is true it
// disables both seq scans and bitmap scans for the session (standard planner
// testing to prove an index CAN drive a given ordering); with those two
// disabled the planner is forced to use a plain index scan, and only the
// composite index whose column order matches the ORDER BY can do so without a
// Sort. On a small seeded table a seq scan is genuinely cheapest, so the
// default-mode run cannot be trusted to pick any particular index — that is
// exactly the flakiness the repo documents in TestTxHashIndex. The forcing
// mode sidesteps it deterministically.
func explain(t *testing.T, st *Postgres, forceIndex bool, query string, args ...any) string {
	t.Helper()
	ctx := context.Background()
	tx, err := st.pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	if forceIndex {
		_, err = tx.Exec(ctx, `SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off`)
		require.NoError(t, err)
	}
	rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+query, args...)
	require.NoError(t, err)
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	require.NoError(t, rows.Err())
	return sb.String()
}

// The frequent /events and /contracts/{id}/events query shapes must be served
// from the new indexes. A wide seeded table keeps the planner assertions
// deterministic: the target contract owns a small slice, so on a plain
// index-scan path (bitmap scans disabled) the planner must pick the composite
// index whose column order matches the requested ORDER BY and emit no Sort.
func TestQuickFilterIndexes_EnableIndexPlans(t *testing.T) {
	st := testStore(t)
	seedQuickFilterRows(t, st, contractA)

	t.Run("contract_id + ledger range is index-backed, not a seq scan", func(t *testing.T) {
		// Default planner run: the exact predicate shape must never degrade
		// to a full-partition sequential scan.
		plan := explain(t, st, false,
			`SELECT id FROM events WHERE contract_id = $1 AND ledger >= $2 AND ledger <= $3 ORDER BY id`,
			contractA, 1, 200)
		assert.NotContains(t, plan, "Seq Scan",
			"selection must come from an index, not a sequential scan of the partition")
		assert.Contains(t, plan, "Index Scan",
			"row selection must come from an index")
	})

	t.Run("contract_id + ledger range ordered by ledger, id needs no sort", func(t *testing.T) {
		// With seq/bitmap scans forced off, the only way to avoid a Sort for
		// ORDER BY ledger, id is an index scan on (contract_id, ledger, id) —
		// every other index yields a different order and forces a Sort. The
		// plan reports the partition-cloned index name (events_<range>_...),
		// so assert the shared column-pair part rather than the parent name.
		plan := explain(t, st, true,
			`SELECT id FROM events WHERE contract_id = $1 AND ledger >= $2 AND ledger <= $3 ORDER BY ledger, id`,
			contractA, 1, 200)
		assert.Contains(t, plan, "contract_id_ledger_id",
			"the range+ledger ordering must be driven by the (contract_id, ledger, id) index")
		assert.NotContains(t, plan, "Seq Scan", "must not fall back to a sequential scan")
		assert.NotContains(t, plan, "Sort",
			"index order matches ORDER BY ledger, id, so no Sort node is needed")
	})

	t.Run("contract_id alone ordered by id needs no sort", func(t *testing.T) {
		// With seq/bitmap scans forced off, ORDER BY id on a single contract
		// can only avoid a Sort via the (contract_id, id) index.
		plan := explain(t, st, true,
			`SELECT id FROM events WHERE contract_id = $1 ORDER BY id`, contractA)
		assert.Contains(t, plan, "contract_id_id",
			"the default per-contract order must scan the (contract_id, id) index")
		assert.NotContains(t, plan, "Seq Scan")
		assert.NotContains(t, plan, "Sort",
			"index order matches ORDER BY id, so no Sort node is needed")
	})

	t.Run("successful-call shape needs no sort", func(t *testing.T) {
		plan := explain(t, st, true, // forceIndex: prove the partial index serves this shape without a Sort
			`SELECT id FROM events WHERE contract_id = $1 AND in_successful_call = true AND ledger >= $2 AND ledger <= $3 ORDER BY ledger, id`,
			contractA, 1, 200)
		assert.Contains(t, plan, "contract_id_ledger_id",
			"the successful-calls shape must scan the partial (contract_id, ledger, id) index")
		assert.NotContains(t, plan, "Seq Scan")
		assert.NotContains(t, plan, "Sort")
	})
}
