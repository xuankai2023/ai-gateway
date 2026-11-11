-- 阶段 13：模型灰度规则表
-- 每个租户每个模型至多一条规则（uk_tenant_model），weight 为灰度流量百分比（0~100）
CREATE TABLE IF NOT EXISTS gray_rules (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id    BIGINT       NOT NULL,
    model_name   VARCHAR(128) NOT NULL COMMENT '原模型名（客户端请求名）',
    target_model VARCHAR(128) NOT NULL COMMENT '灰度目标模型名（路由解析用）',
    weight       INT          NOT NULL DEFAULT 0 COMMENT '灰度流量百分比 0~100',
    enabled      TINYINT(1)   NOT NULL DEFAULT 1,
    created_at   TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at   TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_tenant_model (tenant_id, model_name)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT ='模型灰度规则（阶段 13）';
