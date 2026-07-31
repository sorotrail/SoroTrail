-- IF NOT EXISTS: the store's integration tests each call Migrate(), and
-- parallel subtests can race to apply a newly added version. Making the
-- statement idempotent keeps the loser of that race from marking the schema
-- dirty, which fails every test in the package at once.
ALTER TABLE ingestion_state
    ADD COLUMN IF NOT EXISTS last_successful_poll timestamptz;
