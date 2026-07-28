-- Singleton row tracking how far the auditor has verified stored data
-- against the RPC. Zero means "no ledger has been verified yet".
CREATE TABLE audit_state (
    id                       int PRIMARY KEY CHECK (id = 1),
    verified_through_ledger  bigint NOT NULL DEFAULT 0,
    updated_at               timestamptz NOT NULL DEFAULT now()
);

-- One row per ledger-range finding recorded by the auditor. The range is
-- bounded (see audit_finding_max_ledgers in main) so repair attempts
-- re-fetch a tractable window. status transitions:
--   on insert:     'open'
--   on repair ok:  'repaired'
--   after max attempts:          'unrecoverable' (kept open, surfaced via API)
--   if range aged out of RPC:    'unverifiable'
CREATE TABLE audit_findings (
    id                bigserial PRIMARY KEY,
    from_ledger       bigint NOT NULL,
    to_ledger         bigint NOT NULL,
    expected_count    int    NOT NULL,         -- what the RPC returned for [from..to]
    actual_count      int    NOT NULL,         -- what the store held before repair
    missing_ids       text[] NOT NULL DEFAULT '{}',
    status            text   NOT NULL DEFAULT 'open',
    attempts          int    NOT NULL DEFAULT 0,
    last_attempted_at timestamptz,
    last_error        text,
    created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_findings_status ON audit_findings (status);
-- Speeds up "are there open findings in [from..to]?" look-ups by the auditor.
CREATE INDEX idx_audit_findings_open_range
    ON audit_findings (from_ledger, to_ledger)
    WHERE status = 'open';
