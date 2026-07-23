DROP TABLE IF EXISTS replay_state;

ALTER TABLE events
    DROP COLUMN IF EXISTS raw_topic_xdr,
    DROP COLUMN IF EXISTS raw_value_xdr;
