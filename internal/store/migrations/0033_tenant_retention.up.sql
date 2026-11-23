-- Per-tenant retention (#1249).
--
-- NULL means "inherit the instance-wide horizon" (RETENTION_MAX_AGE), which
-- is what every existing tenant gets, so an upgraded instance retains
-- exactly as before. A value is the tenant's own horizon in seconds; zero
-- and negative values are rejected because "no age limit" is expressed by
-- the instance default, not by a tenant override.
--
-- The pruner keeps a contract's events for the longest horizon among the
-- tenants entitled to read it (see internal/store/tenant_retention.go), so
-- this column never causes a row another tenant still needs to be deleted.
--
-- Idempotent (IF NOT EXISTS) for the same reason 0016 is: the migration
-- tests replay the chain against an already-migrated database.
ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS retention_max_age_seconds bigint
        CHECK (retention_max_age_seconds IS NULL OR retention_max_age_seconds > 0);
