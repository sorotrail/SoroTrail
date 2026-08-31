ALTER TABLE events DROP CONSTRAINT IF EXISTS events_pkey;
ALTER TABLE events ADD PRIMARY KEY (ledger, id);
DROP INDEX IF EXISTS idx_events_network_id;

ALTER TABLE ingestion_state DROP CONSTRAINT IF EXISTS ingestion_state_pkey;
ALTER TABLE ingestion_state ADD COLUMN id int;
UPDATE ingestion_state SET id = 1 WHERE network = 'default';
ALTER TABLE ingestion_state DROP COLUMN network;
ALTER TABLE ingestion_state ADD PRIMARY KEY (id);
ALTER TABLE ingestion_state ADD CONSTRAINT ingestion_state_id_check CHECK (id = 1);
