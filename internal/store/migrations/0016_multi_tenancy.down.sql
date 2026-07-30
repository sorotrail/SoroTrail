DROP INDEX IF EXISTS idx_subscriptions_tenant;
ALTER TABLE subscriptions DROP COLUMN IF EXISTS tenant_id;

DROP TABLE IF EXISTS tenant_usage;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS tenant_watched_contracts;
DROP TABLE IF EXISTS tenant_contract_grants;
DROP TABLE IF EXISTS tenants;
