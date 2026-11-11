-- ============================================================================
-- 阶段 15：管理面操作审计表
--
-- 所有 /admin/v1 写操作（tenants/api_keys/models/abilities/channel_keys/
-- gray_rules/config 变更）落一条审计记录：谁（actor）在什么时间（created_at）
-- 做了什么动作（action）带了什么参数（payload，密钥类字段已脱敏）。
-- ============================================================================

CREATE TABLE admin_audit_logs (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    actor       VARCHAR(128) NOT NULL,                -- 操作者（X-Admin-Actor 头，缺省 "admin"）
    action      VARCHAR(128) NOT NULL,                -- 如 tenant.create / api_key.revoke / config.update
    payload     TEXT,                                 -- 请求参数快照（JSON，密钥明文不落）
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_admin_audit_time (created_at DESC),
    INDEX idx_admin_audit_action (action)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
