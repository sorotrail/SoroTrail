-- 0022_event_quick_filter_indexes
--
-- Analysis
-- --------
-- The /events and /contracts/{id}/events endpoints build WHERE clauses from
-- contract_id (+ optional ledger range, + in_successful_call) and order by
-- either id (the default), ledger, or created_at. The pagination cursor is a
-- (sort_column, id) row comparison, so the most valuable ordering the query
-- wants is "contract_id first, then the sort column, then id".
--
--   WHERE contract_id = $1 [AND ledger BETWEEN $2 AND $3]
--   ORDER BY id                      -- default keyset page
--   ORDER BY ledger, id              -- order_by=ledger
--
-- Before this migration the events table carried four contract-access
-- indexes built from the pre-partition era:
--
--   idx_events_contract_id             (contract_id)
--   idx_events_contract_ledger         (contract_id, ledger)
--   idx_events_contract_ledger_successful  (contract_id, ledger) WHERE in_successful_call = true
--   idx_events_contract_id_created_at  (contract_id, created_at)
--
-- None begins with the tiebreaker id. A `WHERE contract_id = $1 ORDER BY id`
-- query therefore had to scan `(contract_id, ...)` and re-sort by id, and
-- `WHERE contract_id = $1 AND ledger BETWEEN ... ORDER BY id` had to sort too
-- (id is not globally ordered within a ledger sub-range of one contract).
--
-- What this migration changes
-- --------------------------
-- 1. UPGRADE idx_events_contract_ledger (contract_id, ledger) to
--    idx_events_contract_ledger_id (contract_id, ledger, id). A btree over
--    (a, b, c) serves every prefix seek the two-column index served (a, and
--    a, b) and additionally returns rows already ordered by (a, b, c) — so the
--    common contract_id + ledger-range filter can stream its result in the
--    requested (ledger, id) / (id-after-range) order without a Sort node.
-- 2. UPGRADE the partial index to (contract_id, ledger, id) restricted to
--    in_successful_call = true, mirroring the change above for the most
--    common read (successful-call events only).
-- 3. ADD idx_events_contract_id_id (contract_id, id) — the index the default
--    `WHERE contract_id = $1 ORDER BY id` path needs, so a contract's events
--    are read in chronological (id) order directly.
-- 4. DROP idx_events_contract_id (contract_id), which is now fully redundant:
--    the three indexes above each lead with contract_id, so a bare
--    `contract_id = $1` predicate is served by any of them via a prefix seek.
--
-- Write-throughput note
-- ---------------------
-- The number of indexes on the events table is unchanged (4 → 4), so the
-- per-row write cost of maintaining them stays equivalent; the widest two
-- gained one trailing id column apiece in exchange for dropping a redundant
-- single-column index entirely. The trade is a slightly larger index page
-- against eliminating a Sort on the two hottest reads.

-- Drop the superseded indexes first: the (contract_id, ...) names conflict
-- with nothing after this point, and creating the new wider indexes is a
-- clean follow-on. IF NOT EXISTS / IF EXISTS keep this migration safe to
-- re-apply (e.g. the rupture scenario in
-- TestMigrate_UpgradesLegacyEventsTable, which rolls schema_migrations back
-- and replays every later migration).
DROP INDEX IF EXISTS idx_events_contract_id;
DROP INDEX IF EXISTS idx_events_contract_ledger;
DROP INDEX IF EXISTS idx_events_contract_ledger_successful;

CREATE INDEX IF NOT EXISTS idx_events_contract_ledger_id
    ON events (contract_id, ledger, id);

CREATE INDEX IF NOT EXISTS idx_events_contract_id_id
    ON events (contract_id, id);

CREATE INDEX IF NOT EXISTS idx_events_contract_ledger_id_successful
    ON events (contract_id, ledger, id)
    WHERE in_successful_call = true;