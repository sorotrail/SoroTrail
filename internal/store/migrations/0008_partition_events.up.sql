BEGIN;

ALTER TABLE events RENAME TO events_legacy;

-- The legacy events table (post-rename) keeps its PK constraint
-- (events_pkey) and the secondary indexes (idx_events_*) the previous
-- migrations created. Their names are global to the schema. Free them
-- BEFORE creating the new partitioned events table below — otherwise
-- this migration cannot run twice in a row on a DB whose events was
-- hand-de-partitioned: the second run fails with "relation
-- 'idx_events_contract_id' already exists". events_legacy is dropped at
-- the end of this BEGIN block, so the loss of its indexes is transient
-- — the INSERT ... SELECT below reads it sequentially anyway.
ALTER TABLE events_legacy DROP CONSTRAINT IF EXISTS events_pkey CASCADE;
DROP INDEX IF EXISTS idx_events_id;
DROP INDEX IF EXISTS idx_events_contract_id;
DROP INDEX IF EXISTS idx_events_ledger;
DROP INDEX IF EXISTS idx_events_contract_ledger;
DROP INDEX IF EXISTS idx_events_topics;
DROP INDEX IF EXISTS idx_events_created_at;

CREATE TABLE events (
    id                 text NOT NULL,
    contract_id        text NOT NULL,
    ledger             bigint NOT NULL,
    type               text NOT NULL,
    tx_hash            text NOT NULL,
    tx_index           int NOT NULL DEFAULT 0,
    op_index           int NOT NULL DEFAULT 0,
    in_successful_call boolean NOT NULL DEFAULT true,
    topics             jsonb NOT NULL DEFAULT '[]'::jsonb,
    value              jsonb,
    created_at         timestamptz NOT NULL DEFAULT now(),
    raw_topic_xdr      text[],
    raw_value_xdr      text,
    PRIMARY KEY (ledger, id)
) PARTITION BY RANGE (ledger);

CREATE INDEX idx_events_id ON events (id);
CREATE INDEX idx_events_contract_id ON events (contract_id);
CREATE INDEX idx_events_ledger ON events (ledger);
CREATE INDEX idx_events_contract_ledger ON events (contract_id, ledger);
CREATE INDEX idx_events_topics ON events USING gin (topics);
CREATE INDEX idx_events_created_at ON events (created_at);
-- Recreate the positional-topic indexes from 0003_topic_position_indexes;
-- they lived on the events we just renamed to events_legacy and would
-- otherwise die with it. IF NOT EXISTS keeps this idempotent against
-- the rupture scenario in TestMigrate_UpgradesLegacyEventsTable.
CREATE INDEX IF NOT EXISTS idx_events_topic0 ON events ((topics->0));
CREATE INDEX IF NOT EXISTS idx_events_topic1 ON events ((topics->1));
CREATE INDEX IF NOT EXISTS idx_events_topic2 ON events ((topics->2));
CREATE INDEX IF NOT EXISTS idx_events_topic3 ON events ((topics->3));

CREATE OR REPLACE FUNCTION ensure_event_partitions(from_ledger bigint, to_ledger bigint, partition_span bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    partition_from bigint;
    partition_to   bigint;
    partition_name text;
BEGIN
    IF from_ledger IS NULL OR to_ledger IS NULL OR from_ledger > to_ledger THEN
        RETURN;
    END IF;
    IF partition_span <= 0 THEN
        RAISE EXCEPTION 'partition_span must be positive';
    END IF;

    partition_from := (from_ledger / partition_span) * partition_span;
    WHILE partition_from <= to_ledger LOOP
        partition_to := partition_from + partition_span;
        partition_name := format('events_%s_%s', partition_from, partition_to - 1);
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF events FOR VALUES FROM (%s) TO (%s)',
            partition_name,
            partition_from,
            partition_to
        );
        partition_from := partition_to;
    END LOOP;
END;
$$;

-- Use a DEFAULT partition as the catch-all for migration-time bulk
-- inserts: rather than pre-creating one (or several) range partitions
-- with the production span=120960 (which would later overlap the
-- narrow events_X_Y children the runtime ensure_event_partitions
-- creates with whatever smaller span the operator/test configured),
-- route the migration's INSERT into a single DEFAULT child. Range
-- partitions and a DEFAULT partition are siblings, never overlapping,
-- so this avoids the "partition would overlap partition" cascade on
-- the second run and on partition-span changes post-migration.
--
-- The ensure_event_partitions function is still CREATED here for
-- runtime use; nothing in this migration calls it with a hard-coded
-- span any more.
--
-- Mid-flight idempotency: the migration isn't always followed by a
-- rupture before being applied again. PostgreSQL's CREATE TABLE
-- PARTITION OF refuses IF NOT EXISTS, but DROP TABLE IF EXISTS does
-- drop a child partition (auto-detaching it first), so this single
-- line is safe across the common scenarios:
--
--   fresh DB              — events_default doesn't exist → no-op
--   post-rupture re-apply — rupture's DROP TABLE events_partitioned
--                           CASCADE already dropped events_default → no-op
--   no-rupture re-apply   — drops a stale events_default so the new
--                           CREATE TABLE PARTITION OF below can attach
--                           the same name to the new events parent
--
-- The no-rupture re-apply path is a known data-loss corner: prior 0008
-- data was stored as rows inside events_default (parent events is empty
-- in PostgreSQL partitioning), DROP TABLE drops those rows, and the
-- subsequent INSERT INTO events SELECT FROM events_legacy inserts 0 rows
-- because the renamed parent itself holds no rows (only its children do).
-- This is reachable only via deliberate operator intervention (a manual
-- schema_migrations rollback, since golang-migrate itself sits at
-- version=8 after a clean run) — the operator should `pg_dump` events
-- before re-running 0008 if recovery matters.
DROP TABLE IF EXISTS events_default;

CREATE TABLE events_default PARTITION OF events DEFAULT;

-- Note on partition pruning: rows INSERTed into the partitioned events
-- while only events_default exists (the migration's bulk copy below)
-- live in events_default until runtime ensure_event_partitions
-- creates narrow range children for those ledger ranges. PostgreSQL
-- ≥ 11 can prune events_default out of a query plan when range
-- children provably cover the queried ledger band; ensure
-- runtime callers produce a complete ledger coverage if they want
-- queries to skip events_default entirely.

INSERT INTO events (
    id, contract_id, ledger, type, tx_hash, tx_index, op_index,
    in_successful_call, topics, value, created_at, raw_topic_xdr, raw_value_xdr
)
SELECT
    id, contract_id, ledger, type, tx_hash, tx_index, op_index,
    in_successful_call, topics, value, created_at, raw_topic_xdr, raw_value_xdr
FROM events_legacy
ORDER BY ledger, id;

DROP TABLE events_legacy;

COMMIT;