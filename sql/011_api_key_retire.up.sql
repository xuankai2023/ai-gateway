-- ============================================================================
-- 阶段 16：API Key 轮换宽限期
--
-- retire_at：旧 key 的退役截止时间（轮换时由 Admin API 设置）。
-- 共存期内（now < retire_at）旧 key 仍可鉴权；过宽限期（now >= retire_at）旧 key 401。
-- NULL = 未进入退役流程，长期有效（仍受 expires_at 约束）。
-- ============================================================================

ALTER TABLE api_keys
    ADD COLUMN retire_at DATETIME(3) NULL AFTER expires_at,
    ADD INDEX idx_api_keys_retire (retire_at);
