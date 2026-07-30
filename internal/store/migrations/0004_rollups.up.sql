-- Incrementally maintained rollups for time-series analytics.
-- Updated transactionally with event inserts so analytics are never stale
-- and never require a full re-scan of the events table.
--
-- Bucket granularity is hourly in storage; daily buckets are derived by
-- aggregation at query time.

-- Per-bucket event counts keyed by (bucket_start, contract_id, type).
-- Count represents the total number of events in that bucket.
CREATE TABLE rollup_events (
    bucket_start timestamptz NOT NULL,
    contract_id  text NOT NULL,
    type         text NOT NULL,
    count        bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, contract_id, type)
);

CREATE INDEX idx_rollup_events_bucket ON rollup_events (bucket_start);
CREATE INDEX idx_rollup_events_contract ON rollup_events (contract_id, bucket_start);
CREATE INDEX idx_rollup_events_type ON rollup_events (type, bucket_start);

-- Per-bucket token transfer volume and unique-address counts.
-- Populated by the same delta-application point as rollup_events
-- when events carry transfer-shaped topics/value (SEP-41 / #1).
-- Volume is NUMERIC to safely hold i128 amounts that overflow bigint.
CREATE TABLE rollup_token_volume (
    bucket_start          timestamptz NOT NULL,
    contract_id           text NOT NULL,
    volume                numeric NOT NULL DEFAULT 0,
    unique_address_count  bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, contract_id)
);

CREATE INDEX idx_rollup_volume_bucket ON rollup_token_volume (bucket_start);
CREATE INDEX idx_rollup_volume_contract ON rollup_token_volume (contract_id, bucket_start);
