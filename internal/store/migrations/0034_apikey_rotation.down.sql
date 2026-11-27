ALTER TABLE api_keys
    DROP COLUMN IF EXISTS last_used_at,
    DROP COLUMN IF EXISTS expires_at,
    DROP COLUMN IF EXISTS predecessor_id;
