-- Originally applied as a simple CREATE INDEX; IF NOT EXISTS keeps it
-- safe across partially-applied environments (e.g. someone wired the
-- index up by hand before the migration landed).
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at);
