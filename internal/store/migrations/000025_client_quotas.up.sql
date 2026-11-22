CREATE TABLE IF NOT EXISTS client_quotas (
    client_key VARCHAR(255) PRIMARY KEY,
    hour_start TIMESTAMPTZ NOT NULL,
    day_start TIMESTAMPTZ NOT NULL,
    hour_used BIGINT NOT NULL,
    day_used BIGINT NOT NULL
);
