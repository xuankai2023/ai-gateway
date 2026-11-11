// 配置热更新（Admin 面入口）+ 审计查询
package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// ConfigChannel Redis Pub/Sub 频道：热更新推送（JSON map）
// main 订阅它把变更应用到本节点；Admin PUT /config 应用到本节点并发布到该频道
const ConfigChannel = "gw:config"

// getConfig 当前热更新值快照
func getConfig(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		if a.Hot == nil {
			apperr.Abort(c, http.StatusServiceUnavailable, "internal_error", "hot_config_unavailable", "hot config unavailable")
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": a.Hot.Snapshot()})
	}
}

// updateConfig 热更新运行期配置：本节点立即生效 + 发布 gw:config 推送其他节点（阶段 15）
// 仅接受热更新白名单 key，非法项整体拒绝（400）
func updateConfig(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body map[string]any
		if !bindJSON(c, &body) {
			return
		}
		if len(body) == 0 {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "empty_config", "no config keys provided")
			return
		}
		if a.Hot == nil {
			apperr.Abort(c, http.StatusServiceUnavailable, "internal_error", "hot_config_unavailable", "hot config unavailable")
			return
		}
		applied, err := a.Hot.ApplyMap(body)
		if err != nil {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_config", err.Error())
			return
		}

		// 推送其他节点（best-effort；发布失败只影响传播速度，本节点已生效）
		if a.Redis != nil {
			raw, err := json.Marshal(body)
			if err == nil {
				pubCtx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
				if err := a.Redis.Publish(pubCtx, ConfigChannel, raw).Err(); err != nil {
					observability.L().Warn("admin: publish config hot-update failed", zap.Error(err))
				}
				cancel()
			}
		}

		writeAudit(a, actorOf(c), "config.update", body)
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"applied": applied}})
	}
}

// listAuditLogs 审计查询：按时间倒序，limit<=200
func listAuditLogs(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		limit := 100
		if v := c.Query("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 200 {
				limit = n
			}
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		rows, err := pool.QueryContext(ctx,
			`SELECT id, actor, action, payload, created_at FROM admin_audit_logs ORDER BY id DESC LIMIT ?`, limit)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_query_failed", err.Error())
			return
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var (
				id        int64
				actor     string
				action    string
				payload   *string
				createdAt *string
			)
			if err := rows.Scan(&id, &actor, &action, &payload, &createdAt); err != nil {
				apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_scan_failed", err.Error())
				return
			}
			out = append(out, map[string]any{
				"id": id, "actor": actor, "action": action, "payload": payload, "created_at": createdAt,
			})
		}
		c.JSON(http.StatusOK, gin.H{"data": out})
	}
}
