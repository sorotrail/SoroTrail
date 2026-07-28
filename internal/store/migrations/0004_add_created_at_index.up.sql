-- IF NOT EXISTS keeps the migration safe for environments where the index
-- may already exist (e.g., applied out-of-band or via a prior state).
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at);