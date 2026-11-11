-- ============================================================================
-- 账本（ledger）三件套：租户余额 / 预占 / 预占时间戳 +  ledger_entries 流水
--
-- 设计要点：
--  1. balance 由每次 Settles 时 HINCRBYFLOAT 增减（Settles 内同时核销 reserved）
--  2. reserved 在 Reserves 时 +delta，Settles 时一次性清 0
--  3. trust_quota 为免预占额度（租户余额 <= trust_quota 时跳过 Reserves）
--  4. reserved_at 用于 reconciler 扫 reserved > 0 且 1h 未动的租户做冲正
--  5. ledger_entries 为 append-only 流水表，按 tenant_id + created_at 倒序建索引
-- ============================================================================

-- --- tenants 扩展：余额 / 预占 / 免预占额度 ---
ALTER TABLE tenants
    ADD COLUMN balance     DECIMAL(12,4) NOT NULL DEFAULT 0  AFTER budget_monthly,
    ADD COLUMN reserved    DECIMAL(12,4) NOT NULL DEFAULT 0  AFTER balance,
    ADD COLUMN trust_quota DECIMAL(12,4) NOT NULL DEFAULT 10 AFTER reserved;

-- --- 流水表 ledger_entries ---
CREATE TABLE ledger_entries (
    id         BIGINT        AUTO_INCREMENT PRIMARY KEY,
    request_id CHAR(36)      NOT NULL,
    tenant_id  BIGINT        NOT NULL,
    delta      DECIMAL(12,4) NOT NULL COMMENT '对 balance 的影响（负数=扣，正数=退）',
    kind       VARCHAR(16)   NOT NULL COMMENT 'reserve | settle | refund | topup',
    created_at DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX  idx_ledger_tenant_time (tenant_id, created_at DESC),
    CONSTRAINT fk_ledger_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --- usage_logs 扩展：预占 / 最终成本 ---
ALTER TABLE usage_logs
    ADD COLUMN reserved_cost DECIMAL(10,6) NULL DEFAULT NULL AFTER cost,
    ADD COLUMN final_cost    DECIMAL(10,6) NULL DEFAULT NULL AFTER reserved_cost;
