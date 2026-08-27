# Events-table indexing

This document records *why* the `events` table carries the indexes it does.
The events table is the hot read surface for `/events` and
`/contracts/{id}/events`: the bounded-latency read path used by dashboards,
explorers, and analytical jobs. Every index on it is paid for on every
write, so each one earns its place by matching a query shape the endpoints
actually issue.

The table is partitioned by `ledger` (`PARTITION BY RANGE (ledger)`, see
`0008_partition_events`). Indexes are declared on the partitioned parent and
PostgreSQL clones them onto every child partition (both `events_default`
and the runtime-created `events_<from>_<to>` children), so partition
creation adds no index bookkeeping.

## Query shapes → indexes

The event read path (`buildEventWhereClause` + `QueryEvents`) filters by
`contract_id` (equality, `= ANY`, or prefix), optionally bounds `ledger` and
`created_at`, optionally narrows to successful calls
(`in_successful_call = true`), and orders by `id` (the default),
`ledger`, or `created_at` — each with `id` appended for a total order.
Pagination is keyset: a cursor is a `(sort_column, id)` row comparison.

The current contract-access indexes (after `0022_event_quick_filter_indexes`):

| Index | Columns / predicate | Serves |
| --- | --- | --- |
| `idx_events_contract_ledger_id` | `(contract_id, ledger, id)` | `WHERE contract_id = $1 [AND ledger BETWEEN ...] ORDER BY ledger, id` — returns rows in index order, no Sort |
| `idx_events_contract_id_id` | `(contract_id, id)` | `WHERE contract_id = $1 ORDER BY id` (the default keyset page) — chronological order with no Sort |
| `idx_events_contract_ledger_id_successful` | `(contract_id, ledger, id) WHERE in_successful_call = true` | the most common read narrowed to successful calls, serving `(ledger, id)` order for that subset |
| `idx_events_contract_id_created_at` | `(contract_id, created_at)` | `ORDER BY created_at` per contract (with a small Sort over `id` ties, from `0009`) |
| `idx_events_ledger` | `(ledger)` | ledger-range scans and `min(ledger)`/partition-pruning support |
| `idx_events_network_ledger` | `(network, ledger)` | multi-network repair/sweep ranges |
| `idx_events_tx_hash` | `(tx_hash)` | `GetEventsByTxHash` and `EventFilter.TxHash` |
| `idx_events_id` | `(id)` | reverse PK lookups (single event, existence probe) |
| `idx_events_topics` | GIN `(topics)` | `topics @> ...` containment, and `topic0..3` expression indexes |

A btree over `(a, b, c)` serves every prefix seek the narrower `(a, b)`
served, so the composite indexes *replace* the older single `(contract_id)`
and `(contract_id, ledger)` indexes rather than adding to them.

## Why the id tiebreaker is included (0022)

Before 0022 the contract access indexes were `(contract_id)`,
`(contract_id, ledger)`, a partial `(contract_id, ledger) WHERE
in_successful_call = true`, and `(contract_id, created_at)`. None led into
`id`. As a result two of the hottest shapes paid a Sort on every request:

- `WHERE contract_id = $1 ORDER BY id` — Postgres scanned `(contract_id,
  …)` and re-sorted by `id`.
- `WHERE contract_id = $1 AND ledger BETWEEN … ORDER BY id` — same Sort,
  because `id` is not globally ordered within a contract’s ledger sub-range.

0022 widened both composites to end in `id` (and narrowed the partial one to
the same three columns) and added the dedicated `(contract_id, id)` index
for the default path, dropping the now-redundant single-column index. The
index *count* on the table is unchanged (4 → 4), so the per-row write cost
stays equivalent — wider pages on two indexes, one redundant index gone.

## The proof that the composites remove the Sort

`postgres_index_test.go` (`TestQuickFilterIndexes_EnableIndexPlans`) seeds a
partition and runs `EXPLAIN`. On the small seeded table a sequential scan is
genuinely cheapest (the same flakiness `TestTxHashIndex` documents), so the
planner assertions force the index path with `SET LOCAL enable_seqscan =
off; enable_bitmapscan = off`. Under that — the large-table regime, where an
index is cheaper than a seq scan — the planner must pick the composite whose
column order matches the `ORDER BY`, and emits **no `Sort` node**:

- `ORDER BY ledger, id` over a `contract_id`+range filter → driven by the
  `(contract_id, ledger, id)` index, no Sort.
- `ORDER BY id` on a single contract → driven by `(contract_id, id)`, no Sort.
- successful-calls shape → driven by the partial index, no Sort.
- the shape never degrades to a `Seq Scan` on default planner settings.

A no-Sort plan under the forced regime is conclusive: no other index can
produce `(ledger, id)` or `(id)` order from a `contract_id` predicate
without sorting.

## Write throughput

Index maintenance is additive to every `INSERT`. The design keeps the index
count flat and prunes a redundant index so a write does not pay to maintain
an index no read uses. `BenchmarkUpsertEvents_Batch*` in
`postgres_bench_test.go` provides the throughput baseline; re-run after any
index change (`go test -tags=integration -bench=BenchmarkUpsert -benchmem
./internal/store/`).