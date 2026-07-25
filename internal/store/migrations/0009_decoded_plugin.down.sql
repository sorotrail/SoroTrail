-- Reverse 0002.
DROP INDEX IF EXISTS idx_events_decoded_payload;
ALTER TABLE events
  DROP COLUMN IF EXISTS decoded_by,
  DROP COLUMN IF EXISTS decoded_payload;
