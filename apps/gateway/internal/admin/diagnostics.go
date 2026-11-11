// 诊断包查询（Admin 面入口）
//
// GET /admin/v1/diagnostics?trace_id=xxx          按 trace_id 精确查
// GET /admin/v1/diagnostics?tenant_id=1&from=&to=&limit=100  按租户 + 时间范围
//
// 响应返回前对 attempts[].error_message / error_message 再过一次 RedactText
// （写入时已脱敏，读取再脱敏一次是幂等兜底，防历史脏数据）。
package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
)

// diagnosticsMaxLimit 单次查询上限（诊断包用于排障，不做全量导出）
const diagnosticsMaxLimit = 200

// diagnosticsTimeLayout from/to 查询参数时间格式（RFC3339 或 2006-01-02）
var diagnosticsTimeLayouts = []string{time.RFC3339, "2006-01-02"}

// listDiagnostics 诊断包查询
func listDiagnostics(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()

		if traceID := c.Query("trace_id"); traceID != "" {
			queryByTraceID(c, ctx, pool, traceID)
			return
		}
		queryByTenant(c, ctx, pool)
	}
}

// queryByTraceID 按 trace_id 精确查询（可能多条：同一 trace 的重试 / 重复上报）
func queryByTraceID(c *gin.Context, ctx context.Context, pool *sql.DB, traceID string) {
	rows, err := pool.QueryContext(ctx, `
		SELECT id, trace_id, tenant_id, model, stream, request_digest, attempts,
		       status_code, error_type, error_message, total_cost, duration_ms, created_at
		FROM request_diagnostics WHERE trace_id = ? ORDER BY id DESC LIMIT 20`, traceID)
	if err != nil {
		apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_query_failed", err.Error())
		return
	}
	defer rows.Close()
	c.JSON(http.StatusOK, gin.H{"data": scanDiagnostics(rows)})
}

// queryByTenant 按租户 + 时间范围查询（from/to 缺省 = 最近 1 小时）
func queryByTenant(c *gin.Context, ctx context.Context, pool *sql.DB) {
	tenantID, err := strconv.ParseInt(c.Query("tenant_id"), 10, 64)
	if err != nil || tenantID <= 0 {
		apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_tenant_id",
			"tenant_id is required and must be a positive integer (or use trace_id)")
		return
	}

	to := time.Now()
	from := to.Add(-time.Hour)
	if v := c.Query("from"); v != "" {
		if t, err := parseDiagnosticsTime(v); err == nil {
			from = t
		} else {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_from",
				"from must be RFC3339 or YYYY-MM-DD")
			return
		}
	}
	if v := c.Query("to"); v != "" {
		if t, err := parseDiagnosticsTime(v); err == nil {
			to = t
		} else {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_to",
				"to must be RFC3339 or YYYY-MM-DD")
			return
		}
	}

	limit := 100
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= diagnosticsMaxLimit {
			limit = n
		}
	}

	rows, err := pool.QueryContext(ctx, `
		SELECT id, trace_id, tenant_id, model, stream, request_digest, attempts,
		       status_code, error_type, error_message, total_cost, duration_ms, created_at
		FROM request_diagnostics
		WHERE tenant_id = ? AND created_at >= ? AND created_at <= ?
		ORDER BY id DESC LIMIT ?`, tenantID, from, to, limit)
	if err != nil {
		apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_query_failed", err.Error())
		return
	}
	defer rows.Close()
	c.JSON(http.StatusOK, gin.H{"data": scanDiagnostics(rows)})
}

// parseDiagnosticsTime 解析 from/to（支持 RFC3339 与 YYYY-MM-DD）
func parseDiagnosticsTime(v string) (time.Time, error) {
	for _, layout := range diagnosticsTimeLayouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t, nil
		}
	}
	return time.Time{}, strconv.ErrSyntax
}

// scanDiagnostics 扫描结果集并对 error_message 做兜底脱敏
func scanDiagnostics(rows *sql.Rows) []map[string]any {
	out := []map[string]any{}
	for rows.Next() {
		var (
			id, tenantID            int64
			traceID, model, digest  string
			stream                  bool
			attemptsRaw             []byte
			statusCode              int
			errorType, errorMessage *string
			totalCost, durationMs   int64
			createdAt               *string
		)
		if err := rows.Scan(&id, &traceID, &tenantID, &model, &stream, &digest, &attemptsRaw,
			&statusCode, &errorType, &errorMessage, &totalCost, &durationMs, &createdAt); err != nil {
			continue // 单行扫描失败跳过该行，不中断整体查询
		}

		// attempts JSON 反序列化 → 逐条脱敏 error_message（幂等兜底）
		var attempts []map[string]any
		if err := json.Unmarshal(attemptsRaw, &attempts); err != nil {
			attempts = []map[string]any{}
		}
		for _, att := range attempts {
			if msg, ok := att["error_message"].(string); ok && msg != "" {
				att["error_message"] = observability.RedactText(msg)
			}
		}

		entry := map[string]any{
			"id":             id,
			"trace_id":       traceID,
			"tenant_id":      tenantID,
			"model":          model,
			"stream":         stream,
			"request_digest": digest,
			"attempts":       attempts,
			"status_code":    statusCode,
			"error_type":     errorType,
			"total_cost":     totalCost,
			"duration_ms":    durationMs,
			"created_at":     createdAt,
		}
		if errorMessage != nil {
			entry["error_message"] = observability.RedactText(*errorMessage)
		} else {
			entry["error_message"] = nil
		}
		out = append(out, entry)
	}
	return out
}
