DROP TABLE IF EXISTS replay_state;

ALTER TABLE events
    DROP COLUMN IF EXISTS topics_xdr,
    DROP COLUMN IF EXISTS value_xdr;
