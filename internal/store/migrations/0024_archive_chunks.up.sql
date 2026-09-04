CREATE TABLE IF NOT EXISTS archive_chunks (
    ledger_start    BIGINT PRIMARY KEY,
    ledger_end      BIGINT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    object_uri      TEXT,
    row_count       BIGINT,
    manifest_sha256 TEXT,
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    verified_at     TIMESTAMPTZ,
    closed_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_archive_chunks_status ON archive_chunks (status);
