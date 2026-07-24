BEGIN;

ALTER TABLE events RENAME TO events_legacy;

-- Renaming a table does NOT rename its indexes or constraints, so every
-- index name from 0001_init is still attached to events_legacy and would
-- collide with the identically-named indexes created below. Free the names
-- before recreating them on the partitioned table; events_legacy is dropped
-- at the end of this migration, so nothing is lost.
-- The new partitioned table's primary key will be auto-named events_pkey,
-- so free that name if the legacy table holds it. Guarded, because a legacy
-- table that arrived by some other route may have a differently-named key.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'events_pkey' AND conrelid = 'events_legacy'::regclass
    ) THEN
        ALTER TABLE events_legacy RENAME CONSTRAINT events_pkey TO events_legacy_pkey;
    END IF;
END $$;
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