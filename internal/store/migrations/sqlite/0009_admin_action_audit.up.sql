-- Durable audit log for administrative actions (#1277), mirroring
-- the Postgres series' 0032_admin_action_audit.
CREATE TABLE IF NOT EXISTS admin_action_audit (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id      TEXT    NOT NULL,
    actor_type    TEXT    NOT NULL,
    tenant_id     INTEGER,
    action        TEXT    NOT NULL,
    resource_type TEXT    NOT NULL,
    resource_id   TEXT    NOT NULL DEFAULT '',
    method        TEXT    NOT NULL DEFAULT '',
    route         TEXT    NOT NULL DEFAULT '',
    remote_addr   TEXT    NOT NULL DEFAULT '',
    request_id    TEXT    NOT NULL DEFAULT '',
    status        TEXT    NOT NULL,
    http_status   INTEGER NOT NULL DEFAULT 0,
    error_code    TEXT    NOT NULL DEFAULT '',
    metadata      TEXT    NOT NULL DEFAULT '{}',
    prev_hash     TEXT    NOT NULL DEFAULT '',
    entry_hash    TEXT    NOT NULL,
    created_at    TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_admin_action_audit_tenant_id
    ON admin_action_audit (tenant_id, id DESC);

CREATE INDEX IF NOT EXISTS idx_admin_action_audit_action
    ON admin_action_audit (action, id DESC);

CREATE INDEX IF NOT EXISTS idx_admin_action_audit_created_at
    ON admin_action_audit (created_at);
