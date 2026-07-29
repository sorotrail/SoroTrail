BEGIN;

ALTER TABLE events RENAME TO events_partitioned;

-- The partitioned events (post-rename) keeps its PK constraint
-- (events_pkey) and the secondary indexes (idx_events_*) the up
-- migration created. Their names are global to the schema. Free them
-- BEFORE creating the new non-partitioned events table below — same
-- rationale as the up migration; otherwise re-running down fails with
-- "relation ... already exists". events_partitioned is dropped at the
-- end of this BEGIN block, so the loss of its indexes is transient.
ALTER TABLE events_partitioned DROP CONSTRAINT IF EXISTS events_pkey CASCADE;
DROP INDEX IF EXISTS idx_events_id;
DROP INDEX IF EXISTS idx_events_contract_id;
DROP INDEX IF EXISTS idx_events_ledger;
DROP INDEX IF EXISTS idx_events_contract_ledger;
DROP INDEX IF EXISTS idx_events_topics;
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
-- Recreate the positional-topic indexes from 0003_topic_position_indexes;
-- events_partitioned (renamed from the partitioned events) is dropped at
-- the end of this BEGIN block, so without these the rolled-back table
-- loses the topic-position index plan that 0003 put in place.
CREATE INDEX IF NOT EXISTS idx_events_topic0 ON events ((topics->0));
CREATE INDEX IF NOT EXISTS idx_events_topic1 ON events ((topics->1));
CREATE INDEX IF NOT EXISTS idx_events_topic2 ON events ((topics->2));
CREATE INDEX IF NOT EXISTS idx_events_topic3 ON events ((topics->3));

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
