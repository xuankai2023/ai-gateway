-- ============================================================================
-- 租户限流配额：RPS / TPM / 并发数三列
--
-- NULL = 未配置，回落 gateway.default_rps_limit / default_tpm_limit /
-- default_concurrency_limit 全局默认（应用层合并，不写库内默认值，
-- 避免后续调全局默认值时存量行不生效）。
-- ============================================================================

ALTER TABLE tenants
    ADD COLUMN rps_limit         INT NULL AFTER budget_monthly,
    ADD COLUMN tpm_limit         INT NULL AFTER rps_limit,
    ADD COLUMN concurrency_limit INT NULL AFTER tpm_limit;
