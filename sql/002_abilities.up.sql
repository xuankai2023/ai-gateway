-- ============================================================================
-- 路由调度 + 多渠道路由表（abilities 倒排索引）
--
-- 核心设计：abilities 表是"谁能服务这个模型"的倒排索引
--
--  正排 models 表：tenant → [model_name → provider + provider_model]（一条记录）
--  倒排 abilities 表：model + priority → [channel1, channel2, ...]（多条记录）
--
-- 查询优势：
--  正排查路由：全表扫描 O(渠道数 × 模型数)
--  倒排查路由：(tenant_id, model_name, priority) 复合索引直接命中 O(1)
--
-- retry 复用设计：每轮重试 = 换一个渠道 + 自动降一层优先级
--  priorities = [100, 80, 50, 0]   （100=最高优先级）
--  retry 0 → priority 100 的渠道
--  retry 1 → priority 80 的渠道
--  retry 2 → priority 50 的渠道
--
-- 同一优先级多渠道：weight 加权随机（权重 0 视为 1，防饿死）
-- ============================================================================

CREATE TABLE abilities (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT       NOT NULL,
    model_name      VARCHAR(64)  NOT NULL,           -- 业务方模型名，如 "demo-chat"
    provider        VARCHAR(32)  NOT NULL,           -- 必须与 config.providers[].type 一致
    provider_model  VARCHAR(128) NOT NULL,           -- 供应商真实模型名
    priority        INT          NOT NULL DEFAULT 100, -- 优先级：100=最高，数字越小越往后
    weight          INT          NOT NULL DEFAULT 1,  -- 同层多渠道时的权重
    input_price_1k   DECIMAL(10,6) NOT NULL DEFAULT 0, -- 本渠道输入 token 单价（每 1000）
    output_price_1k  DECIMAL(10,6) NOT NULL DEFAULT 0, -- 本渠道输出 token 单价（每 1000）
    enabled         TINYINT(1)   NOT NULL DEFAULT 1,   -- 是否启用（失败时异步禁用）
    fallback_model  VARCHAR(128),                      -- 熔断时的兜底模型（可选）
    created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    -- 🔴 核心复合索引：一次查询命中 "tenant + model + 某优先级的启用渠道"
    INDEX idx_abilities_lookup (tenant_id, model_name, priority, enabled),

    INDEX idx_abilities_provider (provider),
    CONSTRAINT fk_abilities_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 示例种子（可选，部署时按实际模型配置插入）：
--
-- INSERT INTO abilities (tenant_id, model_name, provider, provider_model, priority, weight, enabled) VALUES
-- (1, 'demo-chat', 'openai',       'gpt-4o-mini',       100, 2, 1),
-- (1, 'demo-chat', 'anthropic',    'claude-3-haiku',    80,  1, 1),
-- (1, 'demo-chat', 'volcengine',   'doubao-pro-32k',    50,  1, 1);
