-- 0002: protocol-specific decoded event payloads from .wasm plugins.
ALTER TABLE events
  ADD COLUMN IF NOT EXISTS decoded_payload jsonb,
  ADD COLUMN IF NOT EXISTS decoded_by      text;
CREATE INDEX IF NOT EXISTS idx_events_decoded_payload ON events USING gin (decoded_payload);
