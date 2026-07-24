BEGIN;

ALTER TABLE events RENAME TO events_partitioned;

-- Symmetric fix to the up migration: user-named indexes on
-- events_partitioned keep their original names after the rename and
-- would conflict with the CREATE INDEX statements below on the
-- freshly-created unpartitioned events table. Drop them here before
-- recreating.
DROP INDEX IF EXISTS idx_events_contract_id;
DROP INDEX IF EXISTS idx_events_ledger;
DROP INDEX IF EXISTS idx_events_contract_ledger;
DROP INDEX IF EXISTS idx_events_topics;
DROP INDEX IF EXISTS idx_events_topic0;
DROP INDEX IF EXISTS idx_events_topic1;
DROP INDEX IF EXISTS idx_events_topic2;
DROP INDEX IF EXISTS idx_events_topic3;
DROP INDEX IF EXISTS idx_events_created_at;

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
    topics_xdr         jsonb CHECK (topics_xdr IS NULL OR jsonb_typeof(topics_xdr) = 'array'),
    value_xdr          text
);

CREATE INDEX idx_events_contract_id ON events (contract_id);
CREATE INDEX idx_events_ledger ON events (ledger);
CREATE INDEX idx_events_contract_ledger ON events (contract_id, ledger);
CREATE INDEX idx_events_topics ON events USING gin (topics);
CREATE INDEX idx_events_created_at ON events (created_at);

INSERT INTO events (
    id, contract_id, ledger, type, tx_hash, tx_index, op_index,
    in_successful_call, topics, value, created_at, topics_xdr, value_xdr
)
SELECT
    id, contract_id, ledger, type, tx_hash, tx_index, op_index,
    in_successful_call, topics, value, created_at, topics_xdr, value_xdr
FROM events_partitioned
ORDER BY ledger, id;

DROP TABLE events_partitioned CASCADE;

DROP FUNCTION IF EXISTS ensure_event_partitions(bigint, bigint, bigint);

COMMIT;
