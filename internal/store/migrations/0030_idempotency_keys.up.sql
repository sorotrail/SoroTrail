-- Tenant-scoped idempotency keys for write endpoints (#1268).
-- Keyed by (tenant_id, idempotency_key) so one tenant's key cannot collide
-- with or read another tenant's stored response, and concurrent duplicate
-- requests resolve to one operation via the primary key uniqueness constraint.
-- Only a SHA-256 hash of the request payload is stored (never raw request
-- bodies). Expired rows are swept via idx_idempotency_keys_expires_at.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id       text        NOT NULL,
    idempotency_key text        NOT NULL,
    request_hash    text        NOT NULL,
    method          text        NOT NULL,
    path            text        NOT NULL,
    status_code     int         NOT NULL DEFAULT 0,
    content_type    text        NOT NULL DEFAULT '',
    response_body   bytea       NOT NULL DEFAULT ''::bytea,
    created_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_expires_at
    ON idempotency_keys (expires_at);
