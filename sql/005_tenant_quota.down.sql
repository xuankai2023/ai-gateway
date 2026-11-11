-- 回滚租户限流配额列
ALTER TABLE tenants
    DROP COLUMN concurrency_limit,
    DROP COLUMN tpm_limit,
    DROP COLUMN rps_limit;
