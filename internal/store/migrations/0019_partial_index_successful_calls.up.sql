-- A partial index over the hot-path (contract_id, ledger) access pattern,
-- restricted to events emitted inside a successful contract call.
--
-- Most read traffic only cares about events from successful calls
-- (in_successful_call = true); failed-call events are a minority kept for
-- completeness and auditing. Scoping the index with the partial predicate
-- keeps it smaller than the full idx_events_contract_ledger and lets the
-- planner drive the common
--   WHERE contract_id = $1 [AND ledger BETWEEN ...] AND in_successful_call = true
-- query shape straight off the index without visiting failed-call rows. It
-- mirrors the existing idx_events_contract_ledger composite so it serves the
-- same ordering/range needs, just narrowed to the successful subset.
--
-- IF NOT EXISTS keeps this migration idempotent across the rupture scenario
-- in TestMigrate_UpgradesLegacyEventsTable, which forces schema_migrations
-- back to an earlier version and re-applies every later migration (0004,
-- 0006, 0007, 0008, and 0010 use the same guard for the same reason).
--
-- events is partitioned (PARTITION BY RANGE (ledger)), so this creates a
-- partitioned index on the parent that Postgres cascades to every child
-- partition, including events_default.
CREATE INDEX IF NOT EXISTS idx_events_contract_ledger_successful
    ON events (contract_id, ledger)
    WHERE in_successful_call = true;
