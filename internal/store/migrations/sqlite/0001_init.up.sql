CREATE TABLE IF NOT EXISTS events (
    id                 TEXT NOT NULL PRIMARY KEY,
    contract_id        TEXT NOT NULL,
    ledger             INTEGER NOT NULL,
    type               TEXT NOT NULL,
    tx_hash            TEXT NOT NULL,
    tx_index           INTEGER NOT NULL DEFAULT 0,
    op_index           INTEGER NOT NULL DEFAULT 0,
    in_successful_call INTEGER NOT NULL DEFAULT 1,
    topics             TEXT NOT NULL DEFAULT '[]',
    value              TEXT,
    created_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    topics_xdr         TEXT,
    value_xdr          TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_id ON events (id);
CREATE INDEX IF NOT EXISTS idx_events_contract_id ON events (contract_id);
CREATE INDEX IF NOT EXISTS idx_events_ledger ON events (ledger);
CREATE INDEX IF NOT EXISTS idx_events_contract_ledger ON events (contract_id, ledger);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at);

CREATE TABLE IF NOT EXISTS ingestion_state (
    id                   INTEGER PRIMARY KEY CHECK (id = 1),
    last_ingested_ledger INTEGER NOT NULL DEFAULT 0,
    last_cursor          TEXT NOT NULL DEFAULT '',
    updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS watched_contracts (
    contract_id TEXT PRIMARY KEY,
    added_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS audit_state (
    id                       INTEGER PRIMARY KEY CHECK (id = 1),
    verified_through_ledger  INTEGER NOT NULL DEFAULT 0,
    updated_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS audit_findings (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    from_ledger       INTEGER NOT NULL,
    to_ledger         INTEGER NOT NULL,
    expected_count    INTEGER NOT NULL,
    actual_count      INTEGER NOT NULL,
    missing_ids       TEXT NOT NULL DEFAULT '[]',
    status            TEXT NOT NULL DEFAULT 'open',
    attempts          INTEGER NOT NULL DEFAULT 0,
    last_attempted_at TEXT,
    last_error        TEXT,
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_audit_findings_status ON audit_findings (status);
CREATE INDEX IF NOT EXISTS idx_audit_findings_open_range
    ON audit_findings (from_ledger, to_ledger)
    WHERE status = 'open';

CREATE TABLE IF NOT EXISTS replay_state (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    from_ledger   INTEGER NOT NULL DEFAULT 0,
    to_ledger     INTEGER NOT NULL DEFAULT 0,
    last_event_id TEXT NOT NULL DEFAULT '',
    processed     INTEGER NOT NULL DEFAULT 0,
    changed       INTEGER NOT NULL DEFAULT 0,
    skipped       INTEGER NOT NULL DEFAULT 0,
    started_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    completed_at  TEXT
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    url           TEXT NOT NULL,
    filters       TEXT NOT NULL DEFAULT '{}',
    secret        TEXT NOT NULL,
    enabled       INTEGER NOT NULL DEFAULT 1,
    failure_count INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_enabled ON subscriptions (enabled) WHERE enabled = 1;

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    event_id        TEXT NOT NULL,
    status          TEXT NOT NULL,
    response_code   INTEGER NOT NULL DEFAULT 0,
    duration_ms     INTEGER NOT NULL DEFAULT 0,
    error           TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_subscription
    ON delivery_attempts (subscription_id, created_at DESC);

CREATE TABLE IF NOT EXISTS contract_specs (
    wasm_hash   TEXT PRIMARY KEY,
    contract_id TEXT NOT NULL,
    spec_json   TEXT NOT NULL DEFAULT '[]',
    fetched_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_contract_specs_contract_id ON contract_specs (contract_id);
