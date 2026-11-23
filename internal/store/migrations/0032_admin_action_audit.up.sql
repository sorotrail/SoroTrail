-- Durable audit log for administrative actions (#1277).
-- Every administrative mutation (API key creation/revocation, watched-contract
-- changes, contract spec overrides, webhook subscription changes, tenant and
-- grant mutations, dead-letter deletion, bulk event deletion, and backfill or
-- replay runs) writes an immutable row here, including rejected attempts.
-- Secret material (raw API keys, webhook secrets, bearer tokens) is never
-- stored; only public identifiers (key id and prefix) are recorded.
-- Each row stores prev_hash and entry_hash (SHA-256 chain) for tamper evidence.
CREATE TABLE IF NOT EXISTS admin_action_audit (
    id            bigserial   PRIMARY KEY,
    actor_id      text        NOT NULL,
    actor_type    text        NOT NULL,
    tenant_id     bigint,
    action        text        NOT NULL,
    resource_type text        NOT NULL,
    resource_id   text        NOT NULL DEFAULT '',
    method        text        NOT NULL DEFAULT '',
    route         text        NOT NULL DEFAULT '',
    remote_addr   text        NOT NULL DEFAULT '',
    request_id    text        NOT NULL DEFAULT '',
    status        text        NOT NULL,
    http_status   int         NOT NULL DEFAULT 0,
    error_code    text        NOT NULL DEFAULT '',
    metadata      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    prev_hash     text        NOT NULL DEFAULT '',
    entry_hash    text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_admin_action_audit_tenant_id
    ON admin_action_audit (tenant_id, id DESC);

CREATE INDEX IF NOT EXISTS idx_admin_action_audit_action
    ON admin_action_audit (action, id DESC);

CREATE INDEX IF NOT EXISTS idx_admin_action_audit_created_at
    ON admin_action_audit (created_at);
