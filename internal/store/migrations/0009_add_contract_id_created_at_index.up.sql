-- Composite index to speed up recent-events-per-contract queries that filter
-- by contract_id and order/sort by created_at. A btree on (contract_id, created_at)
-- covers both the WHERE clause and the ORDER BY in a single index scan, avoiding
-- a separate sort step.
--
-- IF NOT EXISTS keeps this safe across partial re-applies (same pattern as
-- 0004_add_created_at_index and the CREATE INDEX statements in 0008).
CREATE INDEX IF NOT EXISTS idx_events_contract_id_created_at
    ON events (contract_id, created_at);
