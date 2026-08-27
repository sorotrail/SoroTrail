-- 0022_event_quick_filter_indexes (down)
--
-- Restore the pre-0022 index set: drop the widened composite/partial indexes
-- and recreate idx_events_contract_id and the two two-column variants. Each
-- statement is guarded so an already-partial rollback (e.g. a hand-applied
-- down in a test environment) does not error.

DROP INDEX IF EXISTS idx_events_contract_ledger_id;
DROP INDEX IF EXISTS idx_events_contract_id_id;
DROP INDEX IF EXISTS idx_events_contract_ledger_id_successful;

CREATE INDEX IF NOT EXISTS idx_events_contract_id ON events (contract_id);
CREATE INDEX IF NOT EXISTS idx_events_contract_ledger ON events (contract_id, ledger);
CREATE INDEX IF NOT EXISTS idx_events_contract_ledger_successful
    ON events (contract_id, ledger)
    WHERE in_successful_call = true;