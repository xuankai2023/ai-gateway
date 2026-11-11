-- ============================================================================
-- 语义缓存租户开关（阶段 12）
--
-- cache_enabled 默认 0（opt-in）：LLM 响应非确定性，默认不缓存更安全；
-- 租户显式开启后，非流式请求走 Redis 精确哈希 +（可选）Qdrant 向量相似缓存。
-- ============================================================================

ALTER TABLE tenants
    ADD COLUMN cache_enabled TINYINT(1) NOT NULL DEFAULT 0 AFTER trust_quota;
