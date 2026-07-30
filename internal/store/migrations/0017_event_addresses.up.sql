-- event_addresses: inverted index from address → events.
--
-- Every G... or C... address found in an event's decoded topics or value JSON
-- gets a row here. The index is populated during ingestion (in the same
-- transaction as UpsertEvents) so it is never behind stored events.
--
-- role records where the address appeared: "topic[N]" for the Nth topic
-- (N is 0-based), "value" for the value body, or "topic" when only the
-- topic array index isn't known (fallback).
--
-- Coordinates with #38 (partitioning): if this table partitions by the same
-- scheme as events, the partition key is event_id's leading ledger component.
-- The composite PK already makes address=$1 ORDER BY event_id an index-only
-- scan — verify with EXPLAIN after creation.
CREATE TABLE IF NOT EXISTS event_addresses (
    address   TEXT    NOT NULL,
    event_id  TEXT    NOT NULL,
    role      TEXT    NOT NULL,
    PRIMARY KEY (address, event_id, role)
);

-- The index serves cursor pagination on (address, event_id) without a sort
-- node. Postgres can satisfy ORDER BY event_id from the PK's b-tree directly
-- because the PK is already a covering index for the query.
CREATE INDEX IF NOT EXISTS idx_event_addresses_event_id
    ON event_addresses (event_id);

COMMENT ON TABLE event_addresses IS
    'Inverted index mapping addresses to events. Populated during ingestion.';
COMMENT ON COLUMN event_addresses.address IS
    'Stellar address (G... or C... strkey) found in the event.';
COMMENT ON COLUMN event_addresses.event_id IS
    'Event ID (TOID format) this address appears in.';
COMMENT ON COLUMN event_addresses.role IS
    'Where the address appeared: topic[N], value, or topic.';
