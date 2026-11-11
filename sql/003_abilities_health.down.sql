-- 回滚渠道健康状态字段
ALTER TABLE abilities
    DROP COLUMN disabled_reason,
    DROP COLUMN disabled_until,
    DROP COLUMN consecutive_failures;
