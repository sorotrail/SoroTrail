-- Dead-letter queue for events the ingester could not persist (decode
-- failures, constraint violations, future schema mismatches). Each row
-- carries the raw RPC payload + error context so an operator can
-- hand-replay the event later and DELETE the row once it's handled.
--
-- Primary key is a fresh bigserial so the same event ID can be
-- dead-lettered more than once across runs (a buggy decoder today, a
-- fixed decoder tomorrow — the second attempt produces a new row
-- instead of clashing on the constraint). contract_id is indexed for
-- the per-contract listing case; ledger for the chronological scan
-- an operator would run while triaging.
--
-- IF NOT EXISTS keeps this migration idempotent across the rupture
-- scenario in TestMigrate_UpgradesLegacyEventsTable, mirroring the
-- pattern used by 0006/0007/0010.
CREATE TABLE IF NOT EXISTS dead_letters (
    id            bigserial PRIMARY KEY,
    event_id      text NOT NULL,
    contract_id   text NOT NULL,
    ledger        bigint NOT NULL,
    type          text NOT NULL,
    tx_hash       text NOT NULL DEFAULT '',
    topic_xdr     text[],
    value_xdr     text,
    error         text NOT NULL,
    attempts      int NOT NULL DEFAULT 1,
    last_attempt  timestamptz NOT NULL DEFAULT now(),
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- UNIQUE(event_id) is what the DeadLetterEvent ON CONFLICT (event_id)
-- clause resolves against; without it, Postgres rejects the upsert
-- with 42P10 ("no unique or exclusion constraint matching the ON
-- CONFLICT specification"). The same constraint also serves as the
-- lookup index for the retry path.
CREATE UNIQUE INDEX IF NOT EXISTS uniq_dead_letters_event_id ON dead_letters (event_id);
CREATE INDEX IF NOT EXISTS idx_dead_letters_contract_id ON dead_letters (contract_id);
CREATE INDEX IF NOT EXISTS idx_dead_letters_ledger ON dead_letters (ledger);
