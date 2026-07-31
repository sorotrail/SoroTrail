-- Revert multi-network migration.
-- This is destructive: all network-scoped data collapses to a single
-- network (the first alphabetically). Only run when absolutely necessary.

-- 1. Revert watched_contracts to single PK
ALTER TABLE watched_contracts DROP CONSTRAINT IF EXISTS watched_contracts_pkey;
ALTER TABLE watched_contracts ADD PRIMARY KEY (contract_id);
ALTER TABLE watched_contracts DROP COLUMN IF EXISTS network;

-- 2. Revert audit_findings
ALTER TABLE audit_findings DROP COLUMN IF EXISTS network;

-- 3. Revert audit_state to singleton
ALTER TABLE audit_state DROP CONSTRAINT IF EXISTS audit_state_pkey;
ALTER TABLE audit_state ADD PRIMARY KEY (id);
ALTER TABLE audit_state DROP COLUMN IF EXISTS network;

-- 4. Revert ingestion_state to singleton
ALTER TABLE ingestion_state DROP CONSTRAINT IF EXISTS ingestion_state_pkey;
ALTER TABLE ingestion_state ADD PRIMARY KEY (id);
ALTER TABLE ingestion_state DROP COLUMN IF EXISTS network;

-- 5. Revert events PK and indexes
ALTER TABLE events DROP CONSTRAINT IF EXISTS events_pkey;
ALTER TABLE events ADD PRIMARY KEY (ledger, id);
ALTER TABLE events DROP COLUMN IF EXISTS network;

DROP INDEX IF EXISTS idx_events_network_contract_id;
DROP INDEX IF EXISTS idx_events_network_ledger;
DROP INDEX IF EXISTS idx_events_network_contract_ledger;
DROP INDEX IF EXISTS idx_events_network_created_at;
-- Recreate old indexes
CREATE INDEX IF NOT EXISTS idx_events_contract_id ON events (contract_id);
CREATE INDEX IF NOT EXISTS idx_events_ledger ON events (ledger);
CREATE INDEX IF NOT EXISTS idx_events_contract_ledger ON events (contract_id, ledger);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at);
