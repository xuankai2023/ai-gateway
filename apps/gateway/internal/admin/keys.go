// api_keys 管理：签发 / 吊销 / 轮换（阶段 16 双 key 共存宽限期）
package admin

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
)

// defaultRotateGrace 轮换默认宽限期（24h）：共存期内新旧 key 均可用
const defaultRotateGrace = 24 * time.Hour

// apiKeyRow 管理面视图（永不包含 hash_key 与明文）
type apiKeyRow struct {
	ID        int64      `json:"id"`
	TenantID  int64      `json:"tenant_id"`
	Name      string     `json:"name"`
	ExpiresAt *time.Time `json:"expires_at"`
	RetireAt  *time.Time `json:"retire_at"`
	CreatedAt *time.Time `json:"created_at"`
}

func listAPIKeys(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		rows, err := pool.QueryContext(ctx,
			`SELECT id, tenant_id, name, expires_at, retire_at, created_at FROM api_keys WHERE tenant_id = ? ORDER BY id`, id)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_query_failed", err.Error())
			return
		}
		defer rows.Close()
		out := []apiKeyRow{}
		for rows.Next() {
			var k apiKeyRow
			if err := rows.Scan(&k.ID, &k.TenantID, &k.Name, &k.ExpiresAt, &k.RetireAt, &k.CreatedAt); err != nil {
				apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_scan_failed", err.Error())
				return
			}
			out = append(out, k)
		}
		c.JSON(http.StatusOK, gin.H{"data": out})
	}
}

// createAPIKeyReq 签发入参；expires_in_days<=0 或缺省 = 永不过期
type createAPIKeyReq struct {
	Name          string `json:"name" binding:"required"`
	ExpiresInDays int    `json:"expires_in_days"`
}

func createAPIKey(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		tenantID, ok := pathID(c)
		if !ok {
			return
		}
		var req createAPIKeyReq
		if !bindJSON(c, &req) {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		var expiresAt *time.Time
		if req.ExpiresInDays > 0 {
			t := time.Now().Add(time.Duration(req.ExpiresInDays) * 24 * time.Hour)
			expiresAt = &t
		}
		plain, hash := newAPIKey()
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx,
			`INSERT INTO api_keys (tenant_id, hash_key, name, expires_at) VALUES (?, ?, ?, ?)`,
			tenantID, hash, req.Name, expiresAt)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_insert_failed", err.Error())
			return
		}
		id, _ := res.LastInsertId()
		// 审计只记哈希前缀，明文不落盘
		afterWrite(c, a, "api_key.create", map[string]any{
			"id": id, "tenant_id": tenantID, "name": req.Name, "key_redacted": observability.RedactKey(plain),
		})
		// 明文只在本次响应返回一次
		c.JSON(http.StatusCreated, gin.H{"data": gin.H{
			"id": id, "tenant_id": tenantID, "name": req.Name, "api_key": plain, "expires_at": expiresAt,
		}})
	}
}

// revokeAPIKey 吊销：物理删除（usage_logs.api_key_id 保留历史，FK 约束下删除失败时退化为立即过期）
// 失效 auth 缓存并广播 → 全节点 100ms 内 401
func revokeAPIKey(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx, `DELETE FROM api_keys WHERE id = ?`, id)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_delete_failed", err.Error())
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			apperr.Abort(c, http.StatusNotFound, "not_found", "api_key_not_found", "api key not found")
			return
		}
		afterWrite(c, a, "api_key.revoke", map[string]any{"id": id}, "auth")
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": id, "revoked": true}})
	}
}

// rotateAPIKeyReq 轮换入参；grace_period_seconds<=0 或缺省 = 默认 24h
type rotateAPIKeyReq struct {
	GracePeriodSeconds int64 `json:"grace_period_seconds"`
}

// rotateAPIKey 轮换（阶段 16）：
//  1. 签发新 key（同 tenant 同 name 后缀 -rotated）
//  2. 旧 key retire_at = now + grace（共存期内新旧均可用；过宽限期旧 key 401）
//  3. 失效 auth 缓存并广播
//
// 响应只在本次返回新 key 明文
func rotateAPIKey(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		var req rotateAPIKeyReq
		if !bindJSON(c, &req) {
			return
		}
		grace := defaultRotateGrace
		if req.GracePeriodSeconds > 0 {
			grace = time.Duration(req.GracePeriodSeconds) * time.Second
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()

		// 1. 查旧 key 归属
		var tenantID int64
		var name string
		err := pool.QueryRowContext(ctx,
			`SELECT tenant_id, name FROM api_keys WHERE id = ?`, id).Scan(&tenantID, &name)
		if err == sql.ErrNoRows {
			apperr.Abort(c, http.StatusNotFound, "not_found", "api_key_not_found", "api key not found")
			return
		}
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_query_failed", err.Error())
			return
		}

		// 2. 签发新 key
		plain, hash := newAPIKey()
		res, err := pool.ExecContext(ctx,
			`INSERT INTO api_keys (tenant_id, hash_key, name) VALUES (?, ?, ?)`,
			tenantID, hash, name+"-rotated")
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_insert_failed", err.Error())
			return
		}
		newID, _ := res.LastInsertId()

		// 3. 旧 key 进入退役宽限期
		retireAt := time.Now().Add(grace)
		if _, err := pool.ExecContext(ctx,
			`UPDATE api_keys SET retire_at = ? WHERE id = ?`, retireAt, id); err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_update_failed", err.Error())
			return
		}

		afterWrite(c, a, "api_key.rotate", map[string]any{
			"old_id": id, "new_id": newID, "tenant_id": tenantID,
			"retire_at": retireAt, "key_redacted": observability.RedactKey(plain),
		}, "auth")
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"old_id": id, "new_id": newID, "tenant_id": tenantID,
			"api_key": plain, "retire_at": retireAt, "grace_period_seconds": int64(grace.Seconds()),
		}})
	}
}
