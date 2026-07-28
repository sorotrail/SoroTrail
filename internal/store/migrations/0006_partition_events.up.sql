BEGIN;

ALTER TABLE events RENAME TO events_legacy;

-- Drop legacy indexes whose names would collide with new indexes on the
-- replacement events table. PostgreSQL enforces unique index names per
-- schema, so the renamed table's indexes (now on events_legacy) prevent
-- creating fresh ones with the same names.
DROP INDEX IF EXISTS idx_events_contract_id;
DROP INDEX IF EXISTS idx_events_ledger;
DROP INDEX IF EXISTS idx_events_contract_ledger;
DROP INDEX IF EXISTS idx_events_topics;
DROP INDEX IF EXISTS idx_events_created_at;
-- Also drop the old primary key constraint so the new table's PRIMARY KEY
-- (which gets the same implicit constraint name "events_pkey") does not
-- collide with the constraint inherited by events_legacy.
ALTER TABLE events_legacy DROP CONSTRAINT IF EXISTS events_pkey;

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

SELECT ensure_event_partitions((SELECT min(ledger) FROM events_legacy), (SELECT max(ledger) FROM events_legacy), 120960);

-- Ensure raw-XDR columns exist on the legacy table; they were absent in
-- very early schemas (before migration 7 / old 0004_raw_xdr_and_replay).
ALTER TABLE events_legacy
    ADD COLUMN IF NOT EXISTS raw_topic_xdr text[],
    ADD COLUMN IF NOT EXISTS raw_value_xdr text;

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