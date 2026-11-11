// Package admin 管理面 API（阶段 15）
//
// 与业务面隔离：
//   - 独立监听端口（默认 :8082，main 单独起 http.Server；生产经 NetworkPolicy 仅内网可达）
//   - 独立鉴权：X-Admin-Token 头（常量时间比对），与业务 api_keys 体系完全无关
//
// 写操作闭环：
//  1. 写 MySQL（唯一事实源）
//  2. 本节点缓存失效（a.InvalidateCache）
//  3. 广播 cache_invalidate（其他节点 100ms 级同步；channel_keys 变更附带 KeyPool 重载）
//  4. 异步写 admin_audit_logs 审计（who/action/payload；密钥明文脱敏不落盘）
package admin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/broadcast"
	"ai-gateway/pkg/ledger"
	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// queryTimeout 管理面单次 SQL 超时
const queryTimeout = 5 * time.Second

// NewRouter 管理面路由（token 为空时返回 nil，main 不启动管理面）
func NewRouter(a *app.App, token string) *gin.Engine {
	if token == "" {
		return nil
	}
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(adminAuth(token))

	v1 := r.Group("/admin/v1")
	{
		v1.GET("/tenants", listTenants(a))
		v1.POST("/tenants", createTenant(a))
		v1.GET("/tenants/:id", getTenant(a))
		v1.PATCH("/tenants/:id", updateTenant(a))

		v1.GET("/tenants/:id/keys", listAPIKeys(a))
		v1.POST("/tenants/:id/keys", createAPIKey(a))
		v1.DELETE("/keys/:id", revokeAPIKey(a))
		v1.POST("/keys/:id/rotate", rotateAPIKey(a))

		v1.GET("/models", listModels(a))
		v1.POST("/models", createModel(a))
		v1.PATCH("/models/:id", updateModel(a))
		v1.DELETE("/models/:id", deleteModel(a))

		v1.GET("/abilities", listAbilities(a))
		v1.POST("/abilities", createAbility(a))
		v1.PATCH("/abilities/:id", updateAbility(a))
		v1.DELETE("/abilities/:id", deleteAbility(a))

		v1.GET("/channel_keys", listChannelKeys(a))
		v1.POST("/channel_keys", createChannelKey(a))
		v1.PATCH("/channel_keys/:id", updateChannelKey(a))
		v1.DELETE("/channel_keys/:id", deleteChannelKey(a))

		v1.GET("/gray_rules", listGrayRules(a))
		v1.POST("/gray_rules", createGrayRule(a))
		v1.PATCH("/gray_rules/:id", updateGrayRule(a))
		v1.DELETE("/gray_rules/:id", deleteGrayRule(a))

		v1.GET("/config", getConfig(a))
		v1.PUT("/config", updateConfig(a))

		v1.GET("/audit_logs", listAuditLogs(a))

		v1.GET("/diagnostics", listDiagnostics(a))
	}
	return r
}

// adminAuth X-Admin-Token 常量时间比对（与业务 key 体系隔离）
func adminAuth(token string) gin.HandlerFunc {
	expected := []byte(token)
	return func(c *gin.Context) {
		got := []byte(c.GetHeader("X-Admin-Token"))
		if subtle.ConstantTimeCompare(got, expected) != 1 {
			apperr.Abort(c, http.StatusUnauthorized, "authentication_error", "invalid_admin_token", "invalid admin token")
			return
		}
		c.Next()
	}
}

// --- 写操作闭环辅助 ---

// actorOf 操作者标识（X-Admin-Actor 头，缺省 "admin"）
func actorOf(c *gin.Context) string {
	if v := strings.TrimSpace(c.GetHeader("X-Admin-Actor")); v != "" {
		return v
	}
	return "admin"
}

// afterWrite 写操作后闭环：本节点缓存失效 → 跨节点广播（每命名空间一条事件）→ 异步审计
// ns：失效的缓存命名空间清单（"channel_keys" 额外触发本节点 KeyPool 重载 + abilities 失效）
func afterWrite(c *gin.Context, a *app.App, action string, payload any, ns ...string) {
	for _, ns := range ns {
		if ns == "channel_keys" {
			if err := a.Providers.ReloadChannelKeys(c.Request.Context(), a.DB); err != nil {
				observability.L().Warn("admin: reload channel_keys failed", zap.Error(err))
			}
			a.InvalidateCache("abilities")
		} else {
			a.InvalidateCache(ns)
		}
		if a.Bus != nil {
			ev := invalidationEvent(c, ns)
			a.Async.Go(func() {
				ctx, cancel := asyncCtx()
				defer cancel()
				if err := a.Bus.Publish(ctx, ev); err != nil {
					observability.L().Warn("admin: publish cache_invalidate failed", zap.String("ns", ns), zap.Error(err))
				}
			})
		}
	}

	writeAudit(a, actorOf(c), action, payload)
}

// invalidationEvent 构造 cache_invalidate 广播事件
func invalidationEvent(c *gin.Context, ns string) broadcast.Event {
	return broadcast.Event{Type: broadcast.TypeCacheInvalidate, CachePrefix: ns, TraceID: c.GetString("trace_id")}
}

// writeAudit 异步落审计（best-effort：DB 不可用只告警，不影响已成功的写操作）
func writeAudit(a *app.App, actor, action string, payload any) {
	if a.DB == nil {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		raw = []byte(`"(unmarshalable)"`)
	}
	a.Async.Go(func() {
		ctx, cancel := asyncCtx()
		defer cancel()
		if _, err := a.DB.ExecContext(ctx,
			`INSERT INTO admin_audit_logs (actor, action, payload) VALUES (?, ?, ?)`,
			actor, action, string(raw)); err != nil {
			observability.L().Warn("admin: audit write failed",
				zap.String("action", action), zap.Error(err))
		}
	})
}

func asyncCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), queryTimeout)
}

// --- 通用辅助 ---

// pathID 解析路径参数 :id（>0 的 int64）
func pathID(c *gin.Context) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_id", "id must be a positive integer")
		return 0, false
	}
	return id, true
}

// bindJSON 绑定请求体；失败写 400 并返回 false
func bindJSON(c *gin.Context, dst any) bool {
	if err := c.ShouldBindJSON(dst); err != nil {
		apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_request_body", "invalid request body: "+err.Error())
		return false
	}
	return true
}

// dbOr503 取 DB 连接池；nil 写 503 并返回 nil
func dbOr503(c *gin.Context, a *app.App) *sql.DB {
	if a.DB == nil {
		apperr.Abort(c, http.StatusServiceUnavailable, "internal_error", "db_unavailable", "db unavailable")
		return nil
	}
	return a.DB
}

// newAPIKey 生成业务 API key 明文（sk- + 48 hex）；只在签发响应中返回一次
func newAPIKey() (plain, hash string) {
	var buf [24]byte
	_, _ = rand.Read(buf[:])
	plain = "sk-" + hex.EncodeToString(buf[:])
	sum := sha256.Sum256([]byte(plain))
	return plain, hex.EncodeToString(sum[:])
}

// sortedKeys map 键排序（动态 UPDATE 的列序确定性，便于测试匹配）
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func errBadField(col string) error {
	return fmt.Errorf("unsupported or unknown field: %s", col)
}

func errNoFields() error {
	return errors.New("no updatable fields provided")
}

// convString / convInt / convFloat / convBool / convNullString 动态 UPDATE 的字段转换器
func convString(v any) (any, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("must be string, got %v", v)
	}
	return s, nil
}

func convInt(v any) (any, error) {
	f, ok := v.(float64)
	if !ok {
		return nil, fmt.Errorf("must be number, got %v", v)
	}
	return int64(f), nil
}

// convNullInt 可空整数：JSON null → nil（DB NULL，回落全局默认）
func convNullInt(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	return convInt(v)
}

// budgetMicroArg 美元预算 → budget_micro 参数（<=0 视为无限，写 NULL）
func budgetMicroArg(usd float64) any {
	if usd <= 0 {
		return nil
	}
	return int64(ledger.FromDollars(usd))
}

// convNullMicro budget_micro 字段转换器（int64 μUSD / nil）
func convNullMicro(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	m, ok := v.(int64)
	if !ok {
		return nil, fmt.Errorf("must be integer, got %v", v)
	}
	return m, nil
}

func convFloat(v any) (any, error) {
	f, ok := v.(float64)
	if !ok {
		return nil, fmt.Errorf("must be number, got %v", v)
	}
	return f, nil
}

func convBool(v any) (any, error) {
	b, ok := v.(bool)
	if !ok {
		return nil, fmt.Errorf("must be boolean, got %v", v)
	}
	if b {
		return 1, nil
	}
	return 0, nil
}

// buildUpdate 按白名单动态拼 UPDATE SET 子句；fields 为允许更新的列
// 返回 set 子句与参数；无任何有效字段时 err 非空
func buildUpdate(table string, body map[string]any, fields map[string]func(any) (any, error)) (string, []any, error) {
	parts := make([]string, 0, len(body))
	args := make([]any, 0, len(body))
	for _, col := range sortedKeys(body) {
		conv, ok := fields[col]
		if !ok {
			return "", nil, errBadField(col)
		}
		v, err := conv(body[col])
		if err != nil {
			return "", nil, fmt.Errorf("%s %w", col, err)
		}
		parts = append(parts, col+" = ?")
		args = append(args, v)
	}
	if len(parts) == 0 {
		return "", nil, errNoFields()
	}
	return "UPDATE " + table + " SET " + strings.Join(parts, ", ") + " WHERE id = ?", args, nil
}
