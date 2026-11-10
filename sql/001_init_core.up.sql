-- ============================================================================
-- P0 核心表结构（MySQL 8.0+）
--
--   tenants       租户
--   api_keys      API Key（存 SHA256 哈希，不存明文）
--   models        模型路由（租户可用模型 + 供应商映射 + 价格）
--   usage_logs    用量流水（每次调用一条，P0 同步写）
-- ============================================================================

CREATE TABLE tenants (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    name            VARCHAR(128)  NOT NULL UNIQUE,
    tier            VARCHAR(32)   NOT NULL DEFAULT 'free',
    budget_monthly  DECIMAL(12,2) NOT NULL DEFAULT 0,  -- 0 = 无限
    status          VARCHAR(16)   NOT NULL DEFAULT 'active',
    created_at      DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at      DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    INDEX idx_tenants_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE api_keys (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    hash_key    VARCHAR(256) NOT NULL,
    name        VARCHAR(64)  NOT NULL,
    expires_at  DATETIME(3),
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY idx_api_keys_hash (hash_key),
    INDEX idx_api_keys_tenant (tenant_id),
    CONSTRAINT fk_api_keys_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE models (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id           BIGINT NOT NULL,
    model_name          VARCHAR(64) NOT NULL,           -- 业务方用的名字：如 "demo-chat"
    provider            VARCHAR(32) NOT NULL,           -- 必须与 config.providers[].type 一致
    provider_model      VARCHAR(128) NOT NULL,          -- 供应商真实模型名：如 "gpt-4o-mini"
    is_active           TINYINT(1) NOT NULL DEFAULT 1,
    input_price_1k      DECIMAL(10,6) NOT NULL DEFAULT 0,
    output_price_1k     DECIMAL(10,6) NOT NULL DEFAULT 0,
    UNIQUE KEY idx_models_tenant_name (tenant_id, model_name),
    CONSTRAINT fk_models_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE usage_logs (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    request_id      CHAR(36) NOT NULL,                -- UUID 字符串
    tenant_id       BIGINT NOT NULL,
    api_key_id      BIGINT,
    model           VARCHAR(64) NOT NULL,
    provider        VARCHAR(32) NOT NULL,
    input_tokens    INT NOT NULL,
    output_tokens   INT NOT NULL,
    cost            DECIMAL(10,6) NOT NULL,
    latency_ms      INT NOT NULL,
    status          VARCHAR(16) NOT NULL,             -- success / error
    error_message   TEXT,
    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY idx_usage_request_id (request_id),
    INDEX idx_usage_tenant_time (tenant_id, created_at DESC),
    INDEX idx_usage_model (model),
    CONSTRAINT fk_usage_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id),
    CONSTRAINT fk_usage_apikey FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
