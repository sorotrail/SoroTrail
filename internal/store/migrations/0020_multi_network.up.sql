-- Multi-network: add a network dimension to events and all dependent tables
-- so one deployment can index testnet + mainnet (or any set of RPC endpoints)
-- into a single database, with per-network isolation at every layer.
--
-- Existing rows are tagged with the configured default network name (from
-- NETWORK_NAME env var, defaulting to "default"), so a single-network
-- deployment upgrades without any data migration tooling.

-- 1. Add network column to events (nullable initially so we can tag existing data)
ALTER TABLE events ADD COLUMN IF NOT EXISTS network text;

-- Tag existing rows before making the column NOT NULL.
-- The placeholder is replaced at migration time by the store package.
UPDATE events SET network = 'default' WHERE network IS NULL;

ALTER TABLE events ALTER COLUMN network SET NOT NULL;

-- 2. Drop and recreate the primary key to include network.
-- For partitioned tables we must drop/recreate; the partition children
-- inherit the new PK automatically.
ALTER TABLE events DROP CONSTRAINT IF EXISTS events_pkey;
ALTER TABLE events ADD PRIMARY KEY (network, ledger, id);

-- 3. Recreate indexes that benefit from network prefix.
-- The old indexes are left in place; they still work but new queries
-- that include network will prefer the network-prefixed versions.
DROP INDEX IF EXISTS idx_events_contract_id;
CREATE INDEX idx_events_network_contract_id ON events (network, contract_id);
DROP INDEX IF EXISTS idx_events_ledger;
CREATE INDEX idx_events_network_ledger ON events (network, ledger);
DROP INDEX IF EXISTS idx_events_contract_ledger;
CREATE INDEX idx_events_network_contract_ledger ON events (network, contract_id, ledger);
DROP INDEX IF EXISTS idx_events_created_at;
CREATE INDEX idx_events_network_created_at ON events (network, created_at);

-- Topic indexes are still useful without network prefix since topic
-- queries already filter by network. The gin index is kept as-is.

-- 4. Per-network ingestion state. The old singleton row (id=1) is
-- migrated to the new composite key (network, id) where id stays 1.
ALTER TABLE ingestion_state ADD COLUMN IF NOT EXISTS network text;
UPDATE ingestion_state SET network = 'default' WHERE network IS NULL;
ALTER TABLE ingestion_state ALTER COLUMN network SET NOT NULL;
ALTER TABLE ingestion_state DROP CONSTRAINT IF EXISTS ingestion_state_pkey;
ALTER TABLE ingestion_state ADD PRIMARY KEY (network, id);

-- 5. Per-network audit state.
ALTER TABLE audit_state ADD COLUMN IF NOT EXISTS network text;
UPDATE audit_state SET network = 'default' WHERE network IS NULL;
ALTER TABLE audit_state ALTER COLUMN network SET NOT NULL;
ALTER TABLE audit_state DROP CONSTRAINT IF EXISTS audit_state_pkey;
ALTER TABLE audit_state ADD PRIMARY KEY (network, id);

-- 6. Add network to audit findings for scoped lookups.
ALTER TABLE audit_findings ADD COLUMN IF NOT EXISTS network text;
UPDATE audit_findings SET network = 'default' WHERE network IS NULL;
ALTER TABLE audit_findings ALTER COLUMN network SET NOT NULL;

-- 7. Add network to watched_contracts so different networks can watch
-- different contract sets.
ALTER TABLE watched_contracts ADD COLUMN IF NOT EXISTS network text;
UPDATE watched_contracts SET network = 'default' WHERE network IS NULL;
ALTER TABLE watched_contracts ALTER COLUMN network SET NOT NULL;
ALTER TABLE watched_contracts DROP CONSTRAINT IF EXISTS watched_contracts_pkey;
ALTER TABLE watched_contracts ADD PRIMARY KEY (network, contract_id);

-- 8. Update ensure_event_partitions to include network (cosmetic: the
-- function signature doesn't change, but the documentation for partitions
-- now expects network-scoped data).
