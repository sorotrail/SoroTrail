-- Tenant-scoped idempotency keys for write endpoints (#1268), mirroring
-- the Postgres series' 0030_idempotency_keys.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id       TEXT    NOT NULL,
    idempotency_key TEXT    NOT NULL,
    request_hash    TEXT    NOT NULL,
    method          TEXT    NOT NULL,
    path            TEXT    NOT NULL,
    status_code     INTEGER NOT NULL DEFAULT 0,
    content_type    TEXT    NOT NULL DEFAULT '',
    response_body   BLOB    NOT NULL DEFAULT X'',
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    expires_at      TEXT    NOT NULL,
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_expires_at
    ON idempotency_keys (expires_at);
