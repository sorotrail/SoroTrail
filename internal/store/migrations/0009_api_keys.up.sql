-- API keys for authenticating requests against the HTTP API.
--
-- Only a bcrypt hash of the key secret is stored; the plaintext secret
-- is shown once at creation and is otherwise unrecoverable. The prefix
-- (first 16 hex chars of the key, after `sorotrail_`) is stored in
-- plaintext so a presented key resolves to its row with one indexed
-- lookup — bcrypt hashes cannot be searched by value.
--
-- Revocation is a soft delete (revoked_at); the row is kept so key IDs
-- stay stable for audit and CLI bookkeeping.
CREATE TABLE api_keys (
    id         bigserial PRIMARY KEY,
    -- name is a human-readable label for the key's owner/purpose.
    name       text NOT NULL DEFAULT '',
    -- prefix uniquely identifies the key inside the full token.
    prefix     text NOT NULL UNIQUE,
    -- key_hash is the bcrypt hash of the secret portion of the key.
    key_hash   text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    -- revoked_at is NULL while the key is active; any non-NULL value
    -- invalidates the key immediately.
    revoked_at timestamptz
);

-- Active-key lookups filter on revoked_at; the partial index keeps the
-- common path (validate a presented key) narrow.
CREATE INDEX idx_api_keys_active_prefix ON api_keys (prefix)
    WHERE revoked_at IS NULL;
