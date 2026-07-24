-- Originally numbered 0002_ in an earlier draft; renumbered to 0004_ to avoid
-- a prefix collision with 0002_audit. IF NOT EXISTS keeps the migration
-- safe for environments where the index may already exist (e.g. applied
-- out-of-band, or via a prior broken state).
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at);