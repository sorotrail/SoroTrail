-- Multi-tenancy (#48): tenants own API keys, contract grants, watch-list
-- entries and quotas.
--
-- Every statement here is idempotent (IF NOT EXISTS / ON CONFLICT). That is
-- a requirement in this repo, not a stylistic choice:
-- TestMigrate_UpgradesLegacyEventsTable forces schema_migrations back to an
-- earlier version and re-runs Migrate, so the whole chain above it is
-- replayed against a database where it has already been applied. A plain
-- CREATE TABLE fails there, and a failed migration leaves the database
-- marked dirty — which then fails every later test in the package with
-- "Dirty database version", not just the one that replayed.
--
-- Upgrade path: every existing deployment is single-tenant. The seeded
-- "default" tenant is a wildcard admin, and MULTI_TENANT=false makes every
-- request run as it, so an upgraded instance behaves exactly as before.
-- Turning MULTI_TENANT=true is what starts enforcing the boundary.

CREATE TABLE IF NOT EXISTS tenants (
    id          bigserial PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    -- wildcard tenants read every contract, including ones ingested but
    -- granted to nobody. This is the legacy/admin posture; a normal tenant
    -- has wildcard=false and reads exactly its rows in
    -- tenant_contract_grants.
    wildcard    boolean NOT NULL DEFAULT false,
    -- is_admin gates /admin/*: tenant and key management, and reading any
    -- tenant's usage. Orthogonal to wildcard on purpose — an auditor tenant
    -- can be given wildcard reads without the ability to mint keys.
    is_admin    boolean NOT NULL DEFAULT false,
    -- A disabled tenant's keys stop authenticating without being destroyed,
    -- so suspending for non-payment is reversible and does not invalidate
    -- the customer's stored credentials.
    enabled     boolean NOT NULL DEFAULT true,
    -- NULL means "inherit the instance-wide RATE_LIMIT_* setting". 0 is a
    -- distinct, meaningful value (deny) so these cannot be NOT NULL DEFAULT 0.
    rate_limit_rps        double precision,
    rate_limit_burst      int,
    -- NULL means unlimited, subject to the instance-wide cap.
    max_watched_contracts int,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- A tenant reads exactly the contracts listed here (unless wildcard).
CREATE TABLE IF NOT EXISTS tenant_contract_grants (
    tenant_id   bigint NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    contract_id text NOT NULL,
    granted_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, contract_id)
);

-- Reverse lookup: "who can see this contract", used when revoking.
CREATE INDEX IF NOT EXISTS idx_tenant_grants_contract ON tenant_contract_grants (contract_id);

-- Contracts a tenant has asked to have indexed. Ingestion watches the UNION
-- of this table and the global watched_contracts table, so two tenants
-- wanting the same contract share one set of rows and removal by one of
-- them does not stop ingestion for the other. Refcounting is implicit: the
-- union recomputes on read, so a contract stays watched exactly as long as
-- at least one row anywhere still names it.
CREATE TABLE IF NOT EXISTS tenant_watched_contracts (
    tenant_id   bigint NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    contract_id text NOT NULL,
    added_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, contract_id)
);

CREATE INDEX IF NOT EXISTS idx_tenant_watched_contract ON tenant_watched_contracts (contract_id);

-- API keys (#17). Only the SHA-256 of the key is stored, so a database
-- disclosure does not yield usable credentials. prefix is the non-secret
-- lookup handle: it selects the candidate row by index, and the secret half
-- is then compared in constant time.
CREATE TABLE IF NOT EXISTS api_keys (
    id           bigserial PRIMARY KEY,
    tenant_id    bigint NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name         text NOT NULL DEFAULT '',
    prefix       text NOT NULL UNIQUE,
    key_hash     bytea NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    -- Revocation is a tombstone rather than a delete so an audit of "which
    -- key served this request" survives the key being turned off.
    revoked_at   timestamptz
);

CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys (tenant_id) WHERE revoked_at IS NULL;

-- Usage is aggregated per tenant per UTC day. Per-request rows would grow
-- without bound and buy nothing: billing and quota questions are asked by
-- day, and the API server accumulates in memory and flushes on a ticker,
-- so a busy tenant costs one UPSERT per flush rather than one per request.
CREATE TABLE IF NOT EXISTS tenant_usage (
    tenant_id      bigint NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    day            date NOT NULL,
    requests       bigint NOT NULL DEFAULT 0,
    events_served  bigint NOT NULL DEFAULT 0,
    -- Stream time is accounted in seconds; a stream that outlives a day
    -- boundary is attributed to the day its accounting flush lands in.
    stream_seconds bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, day)
);

-- Webhook subscriptions belong to a tenant too. A subscription is a read
-- path that delivers to an external URL, so an unowned one would be the
-- easiest way out of the boundary: subscribe to a contract you cannot read
-- and have the events posted to a server you control.
--
-- NULL means operator-owned — the posture of every subscription created
-- before this column existed, and of any created in single-tenant mode.
ALTER TABLE subscriptions
    ADD COLUMN IF NOT EXISTS tenant_id bigint REFERENCES tenants(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_subscriptions_tenant ON subscriptions (tenant_id);

-- The tenant every pre-#48 deployment implicitly ran as. Wildcard + admin so
-- that an upgraded instance that later flips MULTI_TENANT=true still has a
-- way in.
INSERT INTO tenants (name, wildcard, is_admin, enabled)
VALUES ('default', true, true, true)
ON CONFLICT (name) DO NOTHING;
