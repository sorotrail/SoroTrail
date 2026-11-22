CREATE TABLE IF NOT EXISTS audit_records (
    sequence BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tenant_id BIGINT,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    target TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_checkpoints (
    sequence BIGINT PRIMARY KEY,
    hash TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_tenant ON audit_records(tenant_id);
CREATE INDEX IF NOT EXISTS idx_audit_sequence ON audit_records(sequence);

REVOKE UPDATE, DELETE ON audit_records FROM PUBLIC;
REVOKE UPDATE, DELETE ON audit_records FROM CURRENT_USER;
