ALTER TABLE api_keys
    DROP INDEX idx_api_keys_retire,
    DROP COLUMN retire_at;
