-- ============================================================================
-- API Key 绑定项目：鉴权通过后把 project_id 写入 gin.Context
--
-- 允许 NULL：存量 Key 不受影响，RAG 落地前不做强制校验。
-- 不建 projects 表 / 外键：项目实体等 RAG 阶段再补，避免本轮改动扩散。
-- ============================================================================

ALTER TABLE api_keys
    ADD COLUMN project_id BIGINT NULL AFTER tenant_id,
    ADD INDEX idx_api_keys_project (project_id);
