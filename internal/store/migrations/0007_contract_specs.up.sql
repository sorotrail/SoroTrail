-- Contract specs: parsed ScSpecEntry arrays extracted from deployed Wasm blobs.
-- Each row is keyed by the Wasm blob hash (SHA-256), so contract upgrades
-- (which change the Wasm hash) automatically create a fresh spec row.
--
-- The contract_id column is a convenience lookup; the cache is keyed by
-- wasm_hash because the spec is a property of the code, not the contract.
-- Multiple contracts may share the same Wasm hash (same code, different
-- instance storage), and they share one spec row.
-- IF NOT EXISTS: TestMigrate_UpgradesLegacyEventsTable rewinds
-- schema_migrations and re-applies this migration on top of a DB where
-- this table already exists (only events was reverted to its legacy
-- shape).
CREATE TABLE IF NOT EXISTS contract_specs (
    wasm_hash   text PRIMARY KEY,
    contract_id text NOT NULL,
    spec_json   jsonb NOT NULL DEFAULT '[]'::jsonb,
    fetched_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contract_specs_contract_id ON contract_specs (contract_id);
