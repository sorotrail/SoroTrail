-- Per-range backfill progress persistence (#1237).
-- Stores the serialized slice of BackfillRange checkpoints so interrupted
-- or partially failed parallel backfills resume only incomplete or failed
-- ledger sub-ranges without re-fetching completed ranges.
ALTER TABLE backfill_state
    ADD COLUMN IF NOT EXISTS ranges_json jsonb NOT NULL DEFAULT '[]'::jsonb;
