-- Webhook subscriptions: consumers register callback URLs with event
-- filters. Ingested events that match a subscription's filters are POSTed to
-- its URL asynchronously.
-- IF NOT EXISTS throughout: TestMigrate_UpgradesLegacyEventsTable rewinds
-- schema_migrations and re-applies this migration on top of a DB where
-- these tables already exist (only events was reverted to its legacy
-- shape).
CREATE TABLE IF NOT EXISTS subscriptions (
    id            bigserial PRIMARY KEY,
    url           text NOT NULL,
    -- filters uses the same shape as GET /events query parameters:
    -- {"contract_id":"C...", "type":"contract", "topic":{"symbol":"transfer"},
    --  "from_ledger":250000, "to_ledger":260000}
    -- An empty object {} matches every event.
    filters       jsonb NOT NULL DEFAULT '{}'::jsonb,
    -- secret is the HMAC-SHA256 signing key shared with the subscriber.
    secret        text NOT NULL,
    enabled       boolean NOT NULL DEFAULT true,
    -- failure_count tracks consecutive delivery failures; the worker
    -- auto-disables a subscription after MaxDeliveryAttempts consecutive
    -- failures and resets it to 0 on any successful delivery.
    failure_count int NOT NULL DEFAULT 0,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_enabled ON subscriptions (enabled)
    WHERE enabled = true;

-- One row per delivery attempt. A subscription for an event may have
-- multiple rows here (initial attempt + retries).
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id              bigserial PRIMARY KEY,
    subscription_id bigint NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    event_id        text NOT NULL,
    -- status: "success" | "failed"
    status          text NOT NULL,
    -- response_code is the HTTP status from the subscriber's server, or 0
    -- for network-level failures (DNS, timeout, connection refused).
    response_code   int NOT NULL DEFAULT 0,
    -- duration_ms is the round-trip time in milliseconds.
    duration_ms     int NOT NULL DEFAULT 0,
    -- error is a human-readable description of what went wrong (empty on
    -- success).
    error           text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_subscription
    ON delivery_attempts (subscription_id, created_at DESC);
