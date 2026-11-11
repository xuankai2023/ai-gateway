-- ============================================================================
-- 渠道健康状态：连续失败计数 + 冷却截止时间 + 禁用原因
--
-- 与 enabled 的分工：
--   enabled = 0            永久禁用（401/403 鉴权失败、上游模型不存在）
--   disabled_until         临时冷却（连续失败达到阈值后自动恢复）
--   consecutive_failures   连续失败次数（成功即清零）
-- ============================================================================

ALTER TABLE abilities
    ADD COLUMN consecutive_failures INT NOT NULL DEFAULT 0 AFTER enabled,
    ADD COLUMN disabled_until DATETIME(3) NULL AFTER consecutive_failures,
    ADD COLUMN disabled_reason VARCHAR(255) NULL AFTER disabled_until;
