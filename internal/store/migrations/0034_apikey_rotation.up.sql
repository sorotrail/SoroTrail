-- API key rotation support: adds expiry, last-use tracking, and predecessor
-- link so a key created by the rotation endpoint can reference the key it
-- replaces. All new columns are nullable so existing rows are unaffected.

ALTER TABLE api_keys
    ADD COLUMN predecessor_id bigint REFERENCES api_keys(id),
    ADD COLUMN expires_at     timestamptz,
    ADD COLUMN last_used_at   timestamptz;
