-- Raw XDR retention: keep the bytes the RPC gave us alongside the decoded
-- JSON so a future decoder can re-derive topics/value without re-querying an
-- RPC that has long since dropped the ledger (see `sorotrail replay`).
--
-- Nullable on purpose: rows ingested before this migration have no raw XDR,
-- and events delivered via xdrFormat "json" never had any. Replay skips and
-- counts those rows rather than failing.
-- IF NOT EXISTS: TestMigrate_UpgradesLegacyEventsTable rewinds
-- schema_migrations to legacySchemaMigrationsVersion and re-applies this
-- migration on top of a DB that already has these columns/table (only the
-- events table itself was reverted to its legacy shape).
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS raw_topic_xdr text[],
    ADD COLUMN IF NOT EXISTS raw_value_xdr text;

-- Single-row progress marker for the replay tool, mirroring ingestion_state.
-- Updated in the same transaction as each batch's rewrites, so an interrupted
-- replay resumes exactly where it committed.
CREATE TABLE IF NOT EXISTS replay_state (
    id            int PRIMARY KEY CHECK (id = 1),
    from_ledger   bigint NOT NULL DEFAULT 0,
    to_ledger     bigint NOT NULL DEFAULT 0,
    -- Last event ID whose rewrite committed; '' means "start of range".
    -- Events are keyed by zero-padded TOIDs, so id > cursor walks the range
    -- in chronological order.
    last_event_id text NOT NULL DEFAULT '',
    processed     bigint NOT NULL DEFAULT 0,
    changed       bigint NOT NULL DEFAULT 0,
    skipped       bigint NOT NULL DEFAULT 0,
    started_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    completed_at  timestamptz
);
