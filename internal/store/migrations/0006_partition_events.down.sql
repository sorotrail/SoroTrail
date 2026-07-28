BEGIN;

ALTER TABLE events RENAME TO events_partitioned;

-- Drop partitioned-table indexes whose names would collide with new
-- indexes on the replacement events table.
DROP INDEX IF EXISTS idx_events_id;
DROP INDEX IF EXISTS idx_events_contract_id;
DROP INDEX IF EXISTS idx_events_ledger;
DROP INDEX IF EXISTS idx_events_contract_ledger;
DROP INDEX IF EXISTS idx_events_topics;
DROP INDEX IF EXISTS idx_events_created_at;
-- Drop the partitioned table's primary key constraint before creating the
-- replacement plain table, whose PRIMARY KEY would otherwise collide with
-- the implicit "events_pkey" inherited by events_partitioned.
ALTER TABLE events_partitioned DROP CONSTRAINT IF EXISTS events_pkey;

CREATE TABLE events (
    id                 text PRIMARY KEY,
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
    raw_value_xdr      text
);

CREATE INDEX idx_events_contract_id ON events (contract_id);
CREATE INDEX idx_events_ledger ON events (ledger);
CREATE INDEX idx_events_contract_ledger ON events (contract_id, ledger);
CREATE INDEX idx_events_topics ON events USING gin (topics);
CREATE INDEX idx_events_created_at ON events (created_at);

INSERT INTO events (
    id, contract_id, ledger, type, tx_hash, tx_index, op_index,
    in_successful_call, topics, value, created_at, raw_topic_xdr, raw_value_xdr
)
SELECT
    id, contract_id, ledger, type, tx_hash, tx_index, op_index,
    in_successful_call, topics, value, created_at, raw_topic_xdr, raw_value_xdr
FROM events_partitioned
ORDER BY ledger, id;

DROP TABLE events_partitioned CASCADE;

DROP FUNCTION IF EXISTS ensure_event_partitions(bigint, bigint, bigint);

COMMIT;