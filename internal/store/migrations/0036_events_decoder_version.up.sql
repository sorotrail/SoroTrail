-- Adds decoder_version to events so replay can skip rows that are already
-- at or above a given decoder version. Defaults to 0 (pre-versioning era),
-- meaning all existing rows will be replayed on the next targeted pass.
--
-- The column is added to the parent partitioned table; PostgreSQL propagates
-- it to all existing and future partitions automatically.
ALTER TABLE events ADD COLUMN IF NOT EXISTS decoder_version integer NOT NULL DEFAULT 0;
