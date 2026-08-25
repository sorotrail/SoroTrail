-- Adds the network dimension the store layer already reads and writes.
--
-- This runs after 0008_partition_events, not before it, and must stay
-- there: 0008 renames events to events_legacy and creates a fresh,
-- partitioned events table from an explicit column list. Anything that
-- adds a column to events at a lower version is silently discarded by
-- that rebuild, which is exactly how an earlier draft of this migration
-- passed the unit tests and then failed every integration test with
-- "column \"network\" does not exist".
--
-- This replaces two competing, mutually incompatible drafts of the same
-- change (0005_add_network_column and 0005_multi_network) that both landed
-- under version 0005 during a batch of merges. Neither matched the Go code:
-- one dropped ingestion_state.id, the other re-keyed it, and both break the
-- singleton upsert in Postgres.SaveIngestionState. This migration adds only
-- what internal/store/postgres.go actually requires.

-- events.network is read by DeleteEventRange (WHERE network = $1) but is not
-- listed in the UpsertEvents column list, so it needs a default.
ALTER TABLE events ADD COLUMN IF NOT EXISTS network text NOT NULL DEFAULT 'default';
CREATE INDEX IF NOT EXISTS idx_events_network_ledger ON events (network, ledger);

-- audit_state is keyed by network: GetAuditState selects WHERE network = $1
-- and SaveAuditState upserts ON CONFLICT (network), which requires network
-- to be unique on its own. The original singleton id (CHECK (id = 1)) has no
-- default, so it would reject those inserts outright and is dropped here.
ALTER TABLE audit_state ADD COLUMN IF NOT EXISTS network text NOT NULL DEFAULT 'default';
ALTER TABLE audit_state DROP CONSTRAINT IF EXISTS audit_state_pkey CASCADE;
ALTER TABLE audit_state DROP COLUMN IF EXISTS id;
ALTER TABLE audit_state ADD PRIMARY KEY (network);

-- audit_findings.network is written explicitly by RecordAuditFinding.
ALTER TABLE audit_findings ADD COLUMN IF NOT EXISTS network text NOT NULL DEFAULT 'default';

-- ingestion_state and watched_contracts are deliberately untouched: the
-- store still treats ingestion_state as a singleton (id = 1) and
-- watched_contracts as network-agnostic (ON CONFLICT (contract_id)).
