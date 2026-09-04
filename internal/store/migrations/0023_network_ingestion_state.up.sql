-- Namespace ingestion cursors and event identity by network.
ALTER TABLE ingestion_state ADD COLUMN IF NOT EXISTS network text;
UPDATE ingestion_state SET network = 'default' WHERE network IS NULL;
ALTER TABLE ingestion_state ALTER COLUMN network SET NOT NULL;
ALTER TABLE ingestion_state DROP CONSTRAINT IF EXISTS ingestion_state_pkey;
ALTER TABLE ingestion_state DROP CONSTRAINT IF EXISTS ingestion_state_id_check;
ALTER TABLE ingestion_state DROP COLUMN IF EXISTS id;
ALTER TABLE ingestion_state ADD PRIMARY KEY (network);

ALTER TABLE events DROP CONSTRAINT IF EXISTS events_pkey;
ALTER TABLE events ADD PRIMARY KEY (network, ledger, id);
ALTER TABLE events DROP CONSTRAINT IF EXISTS events_id_key;
CREATE INDEX IF NOT EXISTS idx_events_network_id ON events (network, id);
