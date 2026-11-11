-- ============================================================================
-- 012_int_amounts：金额全链路切换为 MicroUSD（BIGINT，1 USD = 1,000,000 μUSD）
--
-- 背景：DECIMAL / float64 在预占与结算之间会产生微小误差，且多档定价
--       （阶梯 / 包块 / 按次）无法用单一每 1K 单价表达。
--
-- 变更：
--   1. models：价格列转 BIGINT μUSD + pricing_model / pricing_config / price_version
--   2. tenants / ledger_entries / usage_logs：金额列转 BIGINT μUSD
--   3. credit_lots：积分批次（过期作废 / 先到期先扣）
--
-- 注意：执行前请停止写入（网关滚动更新期间新旧代码不兼容金额单位）。
-- ============================================================================

-- --- models：定价模型 / 版本 / 整数价格 ---
ALTER TABLE models
    ADD COLUMN pricing_model    VARCHAR(16)  NOT NULL DEFAULT 'linear'
        COMMENT 'linear | graduated | package | per_unit' AFTER is_active,
    ADD COLUMN pricing_config   JSON         NULL
        COMMENT '计价档位 / 包块 / 按次配置（金额单位 μUSD）' AFTER is_active,
    ADD COLUMN price_version    INT          NOT NULL DEFAULT 1
        COMMENT '价格版本（每次改价 +1，usage_logs 记录）' AFTER is_active,
    ADD COLUMN price_updated_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3) AFTER is_active,
    ADD COLUMN input_price_1k_micro  BIGINT  NOT NULL DEFAULT 0 AFTER output_price_1k,
    ADD COLUMN output_price_1k_micro BIGINT  NOT NULL DEFAULT 0 AFTER output_price_1k;

UPDATE models
SET input_price_1k_micro  = CAST(ROUND(input_price_1k * 1000000) AS SIGNED),
    output_price_1k_micro = CAST(ROUND(output_price_1k * 1000000) AS SIGNED);

ALTER TABLE models
    DROP COLUMN input_price_1k,
    DROP COLUMN output_price_1k,
    CHANGE COLUMN input_price_1k_micro  input_price_1k  BIGINT NOT NULL DEFAULT 0 COMMENT '输入单价：μUSD / 1K tokens',
    CHANGE COLUMN output_price_1k_micro output_price_1k BIGINT NOT NULL DEFAULT 0 COMMENT '输出单价：μUSD / 1K tokens';

-- --- tenants：余额 / 预占 / 信任额度转 BIGINT μUSD ---
ALTER TABLE tenants
    ADD COLUMN balance_micro     BIGINT NOT NULL DEFAULT 0 AFTER budget_monthly,
    ADD COLUMN reserved_micro    BIGINT NOT NULL DEFAULT 0 AFTER budget_monthly,
    ADD COLUMN trust_quota_micro BIGINT NOT NULL DEFAULT 10000000 AFTER budget_monthly;

UPDATE tenants
SET balance_micro     = CAST(ROUND(balance * 1000000) AS SIGNED),
    reserved_micro    = CAST(ROUND(reserved * 1000000) AS SIGNED),
    trust_quota_micro = CAST(ROUND(trust_quota * 1000000) AS SIGNED);

ALTER TABLE tenants
    DROP COLUMN balance,
    DROP COLUMN reserved,
    DROP COLUMN trust_quota,
    CHANGE COLUMN balance_micro     balance     BIGINT NOT NULL DEFAULT 0 COMMENT '余额（μUSD，Redis 快照）',
    CHANGE COLUMN reserved_micro    reserved    BIGINT NOT NULL DEFAULT 0 COMMENT '预占（μUSD，Redis 快照）',
    CHANGE COLUMN trust_quota_micro trust_quota BIGINT NOT NULL DEFAULT 10000000 COMMENT '免预占信任额度（μUSD）';

-- --- ledger_entries：delta 转 BIGINT μUSD ---
ALTER TABLE ledger_entries
    ADD COLUMN delta_micro BIGINT NOT NULL DEFAULT 0 AFTER tenant_id;

UPDATE ledger_entries
SET delta_micro = CAST(ROUND(delta * 1000000) AS SIGNED);

ALTER TABLE ledger_entries
    DROP COLUMN delta,
    CHANGE COLUMN delta_micro delta BIGINT NOT NULL DEFAULT 0 COMMENT 'Δ balance（μUSD）';

-- --- usage_logs：成本列转 BIGINT μUSD + price_version ---
ALTER TABLE usage_logs
    ADD COLUMN cost_micro          BIGINT NOT NULL DEFAULT 0 AFTER output_tokens,
    ADD COLUMN reserved_cost_micro BIGINT NULL DEFAULT NULL AFTER output_tokens,
    ADD COLUMN final_cost_micro    BIGINT NULL DEFAULT NULL AFTER output_tokens,
    ADD COLUMN price_version       INT NOT NULL DEFAULT 1 AFTER output_tokens;

UPDATE usage_logs
SET cost_micro          = CAST(ROUND(cost * 1000000) AS SIGNED),
    reserved_cost_micro = CASE WHEN reserved_cost IS NULL THEN NULL
                               ELSE CAST(ROUND(reserved_cost * 1000000) AS SIGNED) END,
    final_cost_micro    = CASE WHEN final_cost IS NULL THEN NULL
                               ELSE CAST(ROUND(final_cost * 1000000) AS SIGNED) END;

ALTER TABLE usage_logs
    DROP COLUMN cost,
    DROP COLUMN reserved_cost,
    DROP COLUMN final_cost,
    CHANGE COLUMN cost_micro          cost          BIGINT NOT NULL DEFAULT 0 COMMENT '本次成本（μUSD）',
    CHANGE COLUMN reserved_cost_micro reserved_cost BIGINT NULL DEFAULT NULL COMMENT '预占金额（μUSD）',
    CHANGE COLUMN final_cost_micro    final_cost    BIGINT NULL DEFAULT NULL COMMENT '最终结算金额（μUSD）';

-- --- 积分批次：过期作废 / 先到期先扣 ---
CREATE TABLE IF NOT EXISTS credit_lots (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    amount      BIGINT NOT NULL COMMENT '批次总额（μUSD）',
    remaining   BIGINT NOT NULL COMMENT '剩余额度（μUSD）',
    source      VARCHAR(16) NOT NULL DEFAULT 'admin_grant' COMMENT 'topup | promo | subscription | admin_grant',
    source_ref  VARCHAR(128) NULL,
    expires_at  DATETIME(3) NULL COMMENT 'NULL = 永久有效',
    created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_lot_tenant_expiry (tenant_id, expires_at ASC),
    CONSTRAINT fk_lot_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --- 月度预算：保留 DECIMAL 展示字段，同时新增精确 μUSD 存储 ---
ALTER TABLE tenants ADD COLUMN budget_micro BIGINT NULL COMMENT '月度预算 μUSD（NULL 无限）';
UPDATE tenants SET budget_micro = CAST(ROUND(budget_monthly * 1000000) AS SIGNED) WHERE budget_monthly > 0;
