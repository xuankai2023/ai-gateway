-- ============================================================================
-- 012_int_amounts 回滚：BIGINT μUSD → DECIMAL USD
-- ============================================================================

DROP TABLE IF EXISTS credit_lots;

-- --- usage_logs ---
ALTER TABLE usage_logs
    ADD COLUMN cost_usd          DECIMAL(10,6) NOT NULL DEFAULT 0 AFTER output_tokens,
    ADD COLUMN reserved_cost_usd DECIMAL(10,6) NULL DEFAULT NULL AFTER output_tokens,
    ADD COLUMN final_cost_usd    DECIMAL(10,6) NULL DEFAULT NULL AFTER output_tokens;

UPDATE usage_logs
SET cost_usd          = cost / 1000000,
    reserved_cost_usd = CASE WHEN reserved_cost IS NULL THEN NULL ELSE reserved_cost / 1000000 END,
    final_cost_usd    = CASE WHEN final_cost IS NULL THEN NULL ELSE final_cost / 1000000 END;

ALTER TABLE usage_logs
    DROP COLUMN cost,
    DROP COLUMN reserved_cost,
    DROP COLUMN final_cost,
    DROP COLUMN price_version,
    CHANGE COLUMN cost_usd          cost          DECIMAL(10,6) NOT NULL DEFAULT 0,
    CHANGE COLUMN reserved_cost_usd reserved_cost DECIMAL(10,6) NULL DEFAULT NULL,
    CHANGE COLUMN final_cost_usd    final_cost    DECIMAL(10,6) NULL DEFAULT NULL;

-- --- ledger_entries ---
ALTER TABLE ledger_entries
    ADD COLUMN delta_usd DECIMAL(12,4) NOT NULL DEFAULT 0 AFTER tenant_id;

UPDATE ledger_entries SET delta_usd = delta / 1000000;

ALTER TABLE ledger_entries
    DROP COLUMN delta,
    CHANGE COLUMN delta_usd delta DECIMAL(12,4) NOT NULL DEFAULT 0;

-- --- tenants ---
ALTER TABLE tenants
    ADD COLUMN balance_usd     DECIMAL(12,4) NOT NULL DEFAULT 0 AFTER budget_monthly,
    ADD COLUMN reserved_usd    DECIMAL(12,4) NOT NULL DEFAULT 0 AFTER budget_monthly,
    ADD COLUMN trust_quota_usd DECIMAL(12,4) NOT NULL DEFAULT 10 AFTER budget_monthly;

UPDATE tenants
SET balance_usd     = balance / 1000000,
    reserved_usd    = reserved / 1000000,
    trust_quota_usd = trust_quota / 1000000;

ALTER TABLE tenants
    DROP COLUMN balance,
    DROP COLUMN reserved,
    DROP COLUMN trust_quota,
    DROP COLUMN budget_micro,
    CHANGE COLUMN balance_usd     balance     DECIMAL(12,4) NOT NULL DEFAULT 0,
    CHANGE COLUMN reserved_usd    reserved    DECIMAL(12,4) NOT NULL DEFAULT 0,
    CHANGE COLUMN trust_quota_usd trust_quota DECIMAL(12,4) NOT NULL DEFAULT 10;

-- --- models ---
ALTER TABLE models
    ADD COLUMN input_price_usd  DECIMAL(10,6) NOT NULL DEFAULT 0 AFTER output_price_1k,
    ADD COLUMN output_price_usd DECIMAL(10,6) NOT NULL DEFAULT 0 AFTER output_price_1k;

UPDATE models
SET input_price_usd  = input_price_1k / 1000000,
    output_price_usd = output_price_1k / 1000000;

ALTER TABLE models
    DROP COLUMN input_price_1k,
    DROP COLUMN output_price_1k,
    DROP COLUMN pricing_model,
    DROP COLUMN pricing_config,
    DROP COLUMN price_version,
    DROP COLUMN price_updated_at,
    CHANGE COLUMN input_price_usd  input_price_1k  DECIMAL(10,6) NOT NULL DEFAULT 0,
    CHANGE COLUMN output_price_usd output_price_1k DECIMAL(10,6) NOT NULL DEFAULT 0;
