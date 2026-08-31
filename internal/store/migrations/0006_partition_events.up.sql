BEGIN;

ALTER TABLE events RENAME TO events_legacy;

-- Index names are schema-wide in Postgres, and the renamed legacy table
-- still owns the indexes created by earlier migrations (idx_events_*, the
-- created_at index, and the positional topic indexes). Drop them so the
-- fresh names can be recreated on the partitioned table below; the legacy
-- table is dropped at the end of this migration anyway.
DROP INDEX IF EXISTS idx_events_id;
DROP INDEX IF EXISTS idx_events_contract_id;
DROP INDEX IF EXISTS idx_events_ledger;
DROP INDEX IF EXISTS idx_events_contract_ledger;
DROP INDEX IF EXISTS idx_events_topics;
DROP INDEX IF EXISTS idx_events_created_at;
DROP INDEX IF EXISTS idx_events_topic0;
DROP INDEX IF EXISTS idx_events_topic1;
DROP INDEX IF EXISTS idx_events_topic2;
DROP INDEX IF EXISTS idx_events_topic3;

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
    topics_xdr         jsonb CHECK (topics_xdr IS NULL OR jsonb_typeof(topics_xdr) = 'array'),
    value_xdr          text,
    PRIMARY KEY (ledger, id)
) PARTITION BY RANGE (ledger);

CREATE INDEX idx_events_id ON events (id);
CREATE INDEX idx_events_contract_id ON events (contract_id);
CREATE INDEX idx_events_ledger ON events (ledger);
CREATE INDEX idx_events_contract_ledger ON events (contract_id, ledger);
CREATE INDEX idx_events_topics ON events USING gin (topics);
CREATE INDEX idx_events_created_at ON events (created_at);
-- Positional topic filters (topics->0 .. topics->3) were indexed before
-- partitioning; recreate them on the partitioned table so those queries
-- keep their index acceleration (the GIN index only serves @> containment).
CREATE INDEX idx_events_topic0 ON events ((topics->0));
CREATE INDEX idx_events_topic1 ON events ((topics->1));
CREATE INDEX idx_events_topic2 ON events ((topics->2));
CREATE INDEX idx_events_topic3 ON events ((topics->3));

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
    in_successful_call, topics, value, created_at, topics_xdr, value_xdr
)
SELECT
    id, contract_id, ledger, type, tx_hash, tx_index, op_index,
    in_successful_call, topics, value, created_at, topics_xdr, value_xdr
FROM events_legacy
ORDER BY ledger, id;

DROP TABLE events_legacy;

COMMIT;
