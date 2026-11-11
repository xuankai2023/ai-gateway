-- ============================================================================
-- 工程化配套：请求级诊断包（对照 production-engineering-improvement-plan.md 2.4）
--
-- 采样落库每次请求的路由决策链路（attempts JSON）：每个渠道的
-- channel_id / provider / base_url / 状态 / 错误归类 / 耗时 / 事前熔断状态。
-- 错误请求（5xx / 上游耗尽）100% 采样，正常请求按 diagnostics.sample_rate（默认 1%）。
--
-- 纪律：request_digest 只存 BodyDigest 输出（len + sha256 前缀），不落明文；
--      attempts[].error_message 写入前经 observability.RedactText 脱敏。
-- ============================================================================

CREATE TABLE request_diagnostics (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    trace_id       VARCHAR(64)  NOT NULL,                -- 请求级 trace_id（中间件注入）
    tenant_id      BIGINT       NOT NULL,
    model          VARCHAR(128) NOT NULL,                -- 客户端请求模型
    stream         BOOLEAN      NOT NULL DEFAULT FALSE,
    request_digest VARCHAR(128) NOT NULL,                -- BodyDigest 输出（len=.. sha256=..）
    attempts       JSON         NOT NULL,                -- 路由决策链路（见表头注释）
    status_code    INT          NOT NULL,                -- 最终返回给客户端的 HTTP 状态码
    error_type     VARCHAR(64),                          -- 错误归类（auth/rate_limit/server/timeout/overloaded/unknown）
    error_message  TEXT,                                 -- 已脱敏
    total_cost     BIGINT       NOT NULL DEFAULT 0,      -- 最终成本（μUSD，与 012_int_amounts 口径一致）
    duration_ms    INT          NOT NULL,                -- 请求总耗时
    created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_diag_trace_id (trace_id),
    INDEX idx_diag_tenant_created (tenant_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
