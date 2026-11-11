-- ============================================================================
-- 渠道级多 Key 池：channel_keys
--
-- 一个渠道（abilities 一条记录）可以配置多把 key，每把 key 是一个独立的上游凭证。
-- 网关按权重轮询选 key；同 key 连续失败 3 次进入 60s 冷却（本地熔断级隔离，不扩散到整个渠道）。
--
-- key_ref 只存"凭证引用"，不落明文：
--   env://OPENAI_API_KEY_1   → 启动时 os.Getenv("OPENAI_API_KEY_1")
--   vault:///ai-gateway/key-1 → 后续接入 Vault 时按路径读取
-- 阶段 16 会接入 Vault，当前实现先支持 env:// 前缀。
-- ============================================================================

CREATE TABLE channel_keys (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    channel_id          BIGINT        NOT NULL,           -- abilities.id
    key_ref             VARCHAR(256)  NOT NULL,           -- 凭证引用（env:// / vault://），不落明文
    weight              INT           NOT NULL DEFAULT 1,  -- 同渠道多 key 的权重（0 视为 1）
    enabled             TINYINT(1)    NOT NULL DEFAULT 1,   -- 是否启用（Admin 侧可禁用，阶段 15）
    consecutive_failures INT         NOT NULL DEFAULT 0,   -- 连续失败计数（KeyPool 本地熔断用）
    disabled_until      DATETIME(3),                       -- 冷却截止时间（NULL=正常）
    disabled_reason     VARCHAR(255),                      -- 禁用/冷却原因（调试用）
    created_at          DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at          DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    INDEX idx_channel_keys_lookup (channel_id, enabled, disabled_until),
    INDEX idx_channel_keys_enabled (enabled),
    CONSTRAINT fk_channel_keys_channel FOREIGN KEY (channel_id) REFERENCES abilities(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================================
-- 幂等种子脚本示例（部署时按需插入）：
--   INSERT INTO channel_keys (channel_id, key_ref, weight) VALUES
--     (1, 'env://OPENAI_API_KEY_1', 2),
--     (1, 'env://OPENAI_API_KEY_2', 1),
--     (1, 'env://OPENAI_API_KEY_3', 1);
-- ============================================================================
