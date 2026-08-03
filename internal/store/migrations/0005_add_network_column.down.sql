BEGIN;

-- events: drop network from PK and indexes, then drop the column
ALTER TABLE events DROP CONSTRAINT events_pkey CASCADE;
ALTER TABLE events ADD PRIMARY KEY (ledger, id);
CREATE INDEX idx_events_id ON events (id);
CREATE INDEX idx_events_contract_id ON events (contract_id);
CREATE INDEX idx_events_ledger ON events (ledger);
CREATE INDEX idx_events_contract_ledger ON events (contract_id, ledger);
CREATE INDEX idx_events_topics ON events USING gin (topics);
CREATE INDEX idx_events_created_at ON events (created_at);
ALTER TABLE events DROP COLUMN network;

-- ingestion_state: add back the singleton row
ALTER TABLE ingestion_state DROP CONSTRAINT ingestion_state_pkey CASCADE;
ALTER TABLE ingestion_state ADD COLUMN id int;
UPDATE ingestion_state SET id = 1;
ALTER TABLE ingestion_state ALTER COLUMN id SET NOT NULL;
ALTER TABLE ingestion_state ADD PRIMARY KEY (id);
ALTER TABLE ingestion_state ADD CONSTRAINT ingestion_state_id_check CHECK (id = 1);
ALTER TABLE ingestion_state DROP COLUMN network;

-- audit_state: add back the singleton row
ALTER TABLE audit_state DROP CONSTRAINT audit_state_pkey CASCADE;
ALTER TABLE audit_state ADD COLUMN id int;
UPDATE audit_state SET id = 1;
ALTER TABLE audit_state ALTER COLUMN id SET NOT NULL;
ALTER TABLE audit_state ADD PRIMARY KEY (id);
ALTER TABLE audit_state ADD CONSTRAINT audit_state_id_check CHECK (id = 1);
ALTER TABLE audit_state DROP COLUMN network;

-- subscriptions: drop network column
ALTER TABLE subscriptions DROP COLUMN IF EXISTS network;

COMMIT;
