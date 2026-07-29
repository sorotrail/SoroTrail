-- IF NOT EXISTS keeps this migration idempotent across the rupture
-- scenario in TestMigrate_UpgradesLegacyEventsTable: the second Migrate
-- call re-applies every migration whose version is > the forced
-- schema_migrations version. Without IF NOT EXISTS Postgres errors
-- with "relation backfill_state already exists" and leaves the DB
-- dirty. Migrations 0006 and 0007 already use IF NOT EXISTS for the
-- same reason; 0010 was added later and missed the pattern.
CREATE TABLE IF NOT EXISTS backfill_state (
    id                  int PRIMARY KEY CHECK (id = 1),
    contract_id         text NOT NULL,
    from_ledger         bigint NOT NULL,
    to_ledger           bigint NOT NULL,
    last_ledger         bigint NOT NULL DEFAULT 0,
    started_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    completed_at        timestamptz
);