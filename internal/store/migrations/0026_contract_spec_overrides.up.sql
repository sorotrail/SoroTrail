-- User-supplied contract spec overrides (task: user-supplied contract spec).
-- Some contracts do not expose a fetchable spec (no contractspecv0 section,
-- unreachable wasm, etc.). Operators can upload a spec JSON per contract_id;
-- the enricher prefers this override over the RPC-fetched spec.
--
-- Keyed by contract_id (not wasm_hash): the override describes the contract's
-- event shapes regardless of which code version is deployed, and survives
-- contract upgrades the same way a fetched spec would not.
--
-- spec_json shape (validated on upload by the API layer):
-- {
--   "events": [
--     {
--       "name": "transfer",                      // required, non-empty
--       "doc": "optional documentation",          // optional
--       "topic_specs": [ {"name":"to","type":"address"} ],   // optional
--       "value_spec":  {"name":"amount","type":"i128"}       // optional
--     }
--   ]
-- }
CREATE TABLE IF NOT EXISTS contract_spec_overrides (
    contract_id  text PRIMARY KEY,
    spec_json    jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
