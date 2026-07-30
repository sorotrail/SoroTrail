BEGIN;

-- Add network column to events table. The default empty string is a marker:
-- existing single-network rows will be tagged by the migration logic below.
-- We can't make it NOT NULL with a default during the ALTER because the PK
-- constraint would fail on the existing non-nullable ledger+id compound key.
-- Instead we add the column as nullable, fill it, then set NOT NULL.

ALTER TABLE events ADD COLUMN network text;

-- Tag all existing rows with the default network by reading the singleton
-- ingestion_state row's implied network. For single-network deployments the
-- migration's Go code (or a manual UPDATE) will set this to the configured
-- default. Since we don't have the config value in SQL, we leave it to the
-- application to tag rows on startup if network is empty.
--
-- The application handles the tagging: on first startup after this migration,
-- if any events have network='', the app runs:
--   UPDATE events SET network = '<default_network>' WHERE network = '' OR network IS NULL;
-- This is safe because the migration itself only adds the column.

-- Make network NOT NULL (after the app's startup tagger runs, or here we
-- provide a temporary default that gets fixed up on startup).
UPDATE events SET network = '' WHERE network IS NULL;
ALTER TABLE events ALTER COLUMN network SET NOT NULL;
ALTER TABLE events ALTER COLUMN network SET DEFAULT '';

-- Drop old PK and indexes, recreate with network
ALTER TABLE events DROP CONSTRAINT events_pkey CASCADE;

ALTER TABLE events ADD PRIMARY KEY (network, ledger, id);

-- Recreate indexes with network
CREATE INDEX idx_events_id ON events (network, id);
CREATE INDEX idx_events_contract_id ON events (network, contract_id);
CREATE INDEX idx_events_ledger ON events (network, ledger);
CREATE INDEX idx_events_contract_ledger ON events (network, contract_id, ledger);
-- The GIN index on topics stays network-qualified for partition pruning
CREATE INDEX idx_events_topics ON events USING gin (network, topics);
CREATE INDEX idx_events_created_at ON events (network, created_at);

-- ── ingestion_state ──────────────────────────────────────────────────────
-- Drop the singleton-row constraint; key by network instead.
ALTER TABLE ingestion_state DROP CONSTRAINT ingestion_state_pkey CASCADE;
ALTER TABLE ingestion_state ADD COLUMN network text;
UPDATE ingestion_state SET network = '' WHERE network IS NULL;
ALTER TABLE ingestion_state ALTER COLUMN network SET NOT NULL;
ALTER TABLE ingestion_state ADD PRIMARY KEY (network);
ALTER TABLE ingestion_state DROP COLUMN id;

-- ── audit_state ──────────────────────────────────────────────────────────
ALTER TABLE audit_state DROP CONSTRAINT audit_state_pkey CASCADE;
ALTER TABLE audit_state ADD COLUMN network text;
UPDATE audit_state SET network = '' WHERE network IS NULL;
ALTER TABLE audit_state ALTER COLUMN network SET NOT NULL;
ALTER TABLE audit_state ADD PRIMARY KEY (network);
ALTER TABLE audit_state DROP COLUMN id;

-- ── subscriptions ────────────────────────────────────────────────────────
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS network text NOT NULL DEFAULT '';

-- ── watched_contracts ────────────────────────────────────────────────────
-- watched_contracts is shared across networks (what contracts to watch).
-- No network column needed here.

-- ── ensure_event_partitions ──────────────────────────────────────────────
-- Update the partition function to include network in partition key? No,
-- partitions are still by ledger range. The network column is part of the
-- PK but not the partition key (ledger still drives partitioning).

COMMIT;
