// tenants 管理：CRUD + 配额调整 + 启停
package admin

import (
	"context"
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/apperr"
	"github.com/gin-gonic/gin"
)

const (
	// tenantPageDefault 列表默认页大小
	tenantPageDefault = 50
	// tenantPageMax 列表最大页大小
	tenantPageMax = 200
)

// tenantRow tenants 表行（管理面视图）
type tenantRow struct {
	ID               int64      `json:"id"`
	Name             string     `json:"name"`
	Tier             string     `json:"tier"`
	BudgetMonthly    float64    `json:"budget_monthly"`
	Status           string     `json:"status"`
	RPSLimit         *int64     `json:"rps_limit"`
	TPMLimit         *int64     `json:"tpm_limit"`
	ConcurrencyLimit *int64     `json:"concurrency_limit"`
	CacheEnabled     bool       `json:"cache_enabled"`
	CreatedAt        *time.Time `json:"created_at"`
}

const tenantCols = `id, name, tier, budget_monthly, status, rps_limit, tpm_limit, concurrency_limit, cache_enabled, created_at`

func scanTenant(row interface{ Scan(...any) error }) (tenantRow, error) {
	var t tenantRow
	err := row.Scan(&t.ID, &t.Name, &t.Tier, &t.BudgetMonthly, &t.Status,
		&t.RPSLimit, &t.TPMLimit, &t.ConcurrencyLimit, &t.CacheEnabled, &t.CreatedAt)
	return t, err
}

// listTenants 游标分页列表：cursor 为上一页最后一条 id；支持 q 名称模糊与 status 精确过滤
func listTenants(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}

		limit := tenantPageDefault
		if v := c.Query("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_limit", "limit must be a positive integer")
				return
			}
			if n > tenantPageMax {
				n = tenantPageMax
			}
			limit = n
		}

		cursor, _ := strconv.ParseInt(c.Query("cursor"), 10, 64)

		where := make([]string, 0, 3)
		args := make([]any, 0, 4)
		if cursor > 0 {
			where = append(where, "id > ?")
			args = append(args, cursor)
		}
		if q := strings.TrimSpace(c.Query("q")); q != "" {
			where = append(where, "name LIKE ?")
			args = append(args, "%"+q+"%")
		}
		if status := strings.TrimSpace(c.Query("status")); status != "" {
			if status != "active" && status != "disabled" {
				apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_status", "status must be active or disabled")
				return
			}
			where = append(where, "status = ?")
			args = append(args, status)
		}

		query := `SELECT ` + tenantCols + ` FROM tenants`
		if len(where) > 0 {
			query += ` WHERE ` + strings.Join(where, " AND ")
		}
		query += ` ORDER BY id LIMIT ?`
		args = append(args, limit+1)

		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		rows, err := pool.QueryContext(ctx, query, args...)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_query_failed", err.Error())
			return
		}
		defer rows.Close()
		out := []tenantRow{}
		for rows.Next() {
			t, err := scanTenant(rows)
			if err != nil {
				apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_scan_failed", err.Error())
				return
			}
			out = append(out, t)
		}

		var nextCursor any
		if len(out) > limit {
			out = out[:limit]
			nextCursor = out[len(out)-1].ID
		}
		c.JSON(http.StatusOK, gin.H{"data": out, "next_cursor": nextCursor})
	}
}

func getTenant(a *app.App) gin.HandlerFunc {
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
		t, err := scanTenant(pool.QueryRowContext(ctx, `SELECT `+tenantCols+` FROM tenants WHERE id = ?`, id))
		if err == sql.ErrNoRows {
			apperr.Abort(c, http.StatusNotFound, "not_found", "tenant_not_found", "tenant not found")
			return
		}
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_query_failed", err.Error())
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": t})
	}
}

// createTenantReq 创建租户入参
type createTenantReq struct {
	Name             string   `json:"name" binding:"required"`
	Tier             string   `json:"tier"`
	BudgetMonthly    *float64 `json:"budget_monthly"`
	RPSLimit         *int64   `json:"rps_limit"`
	TPMLimit         *int64   `json:"tpm_limit"`
	ConcurrencyLimit *int64   `json:"concurrency_limit"`
}

func createTenant(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req createTenantReq
		if !bindJSON(c, &req) {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		tier := req.Tier
		if tier == "" {
			tier = "free"
		}
		budget := 0.0
		if req.BudgetMonthly != nil {
			budget = *req.BudgetMonthly
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		// budget_micro 与 budget_monthly 同步写入：前者是精确 μUSD 存储（NULL = 无限）
		res, err := pool.ExecContext(ctx,
			`INSERT INTO tenants (name, tier, budget_monthly, budget_micro, rps_limit, tpm_limit, concurrency_limit) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			req.Name, tier, budget, budgetMicroArg(budget), req.RPSLimit, req.TPMLimit, req.ConcurrencyLimit)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_insert_failed", err.Error())
			return
		}
		id, _ := res.LastInsertId()
		afterWrite(c, a, "tenant.create", map[string]any{"id": id, "name": req.Name})
		c.JSON(http.StatusCreated, gin.H{"data": gin.H{"id": id, "name": req.Name, "tier": tier, "status": "active"}})
	}
}

// updateTenant 可调字段：name/tier/budget_monthly/status/三配额（null=回落默认）/cache_enabled
// 状态与配额变更 → 失效 auth + quota 缓存并广播（吊销全节点 100ms 生效的关键路径）
func updateTenant(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := pathID(c)
		if !ok {
			return
		}
		var body map[string]any
		if !bindJSON(c, &body) {
			return
		}
		pool := dbOr503(c, a)
		if pool == nil {
			return
		}
		// 月度预算同步维护 budget_micro（精确 μUSD），避免两列长期不一致
		if v, ok := body["budget_monthly"].(float64); ok {
			body["budget_micro"] = budgetMicroArg(v)
		}
		setSQL, args, err := buildUpdate("tenants", body, map[string]func(any) (any, error){
			"name":              convString,
			"tier":              convString,
			"budget_monthly":    convFloat,
			"budget_micro":      convNullMicro,
			"status":            convString,
			"rps_limit":         convNullInt,
			"tpm_limit":         convNullInt,
			"concurrency_limit": convNullInt,
			"cache_enabled":     convBool,
		})
		if err != nil {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_update_fields", err.Error())
			return
		}
		args = append(args, id)
		ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
		defer cancel()
		res, err := pool.ExecContext(ctx, setSQL, args...)
		if err != nil {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_update_failed", err.Error())
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			apperr.Abort(c, http.StatusNotFound, "not_found", "tenant_not_found", "tenant not found")
			return
		}
		// 租户状态影响鉴权结果、配额影响限流：两个命名空间都失效
		afterWrite(c, a, "tenant.update", map[string]any{"id": id, "fields": sortedKeys(body)}, "auth", "quota")
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": id, "updated": sortedKeys(body)}})
	}
}
