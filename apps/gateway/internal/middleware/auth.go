// AuthMiddleware API Key 鉴权（本地缓存版：热数据不再每请求打 MySQL）
//
// 位置：第 4 层（在 Logger 之后，业务 Handler 之前）
//
// 流程：
//  1. 从 Header Authorization: Bearer <key> 取明文 key
//  2. SHA256 哈希（不存明文）
//  3. 先查本地缓存 auth:{sha256(key)}（TTL 60s）；命中且有效直接放行
//  4. miss → 查 api_keys 表 → 查 tenants 表 → 全部通过后回填缓存
//  5. ctx.Set("tenant_id", ...) / ctx.Set("api_key_id", ...) / ctx.Set("tenant_status", ...)
//     project_id 非 NULL 时额外写入 ctx.Set("project_id", ...)
//
// Abort 条件：
//   - 无 Authorization header → 401
//   - key 无效（SHA256 在 api_keys 找不到） → 401
//   - key 过期（expires_at < now） → 401
//   - 租户停用（tenants.status != 'active'） → 403
//
// 缓存纪律：
//   - 只缓存全部校验通过的肯定结果；key 无效/过期、租户停用等否定结果不缓存（防错杀延迟感知）
//   - 命中时仍会重检 expires_at 与 tenant_status；缓存条目失效即清除并回源 DB
package middleware

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/cache"
	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// authTTL 鉴权缓存条目的过期时间
const authTTL = 60 * time.Second

// authEntry 鉴权缓存值（命名空间 auth:{sha256(key)}）
type authEntry struct {
	tenantID     int64
	apiKeyID     int64
	projectID    sql.NullInt64
	expiresAt    *time.Time
	retireAt     *time.Time // 阶段 16：轮换宽限期截止（nil = 未进入退役流程）
	tenantStatus string
}

// valid 缓存命中后仍要做的轻量校验：key 未过期且未过退役宽限期且租户为 active
func (e authEntry) valid() bool {
	if e.tenantStatus != "active" {
		return false
	}
	if e.expiresAt != nil && !time.Now().Before(*e.expiresAt) {
		return false
	}
	// 轮换宽限期：now >= retire_at 时旧 key 立即失效（401）
	if e.retireAt != nil && !time.Now().Before(*e.retireAt) {
		return false
	}
	return true
}

// Auth API Key 鉴权中间件（阶段 14：func(app) 形式，缓存实例级、随 App 生命周期关闭）
func Auth(a *app.App) gin.HandlerFunc {
	// authCache 鉴权本地缓存：key = sha256(api key)，TTL 60s
	authCache := cache.New[authEntry](cache.Options{
		Name:       "auth",
		MaxEntries: 10000,
		DefaultTTL: authTTL,
	})
	observability.RegisterCache("auth", authCache)
	a.AddCloser(authCache.Close)
	// Admin 吊销/轮换 key、启停租户 → 广播 cache_invalidate{prefix=auth} 全节点立即生效（阶段 15）
	a.RegisterInvalidator("auth", func() { authCache.InvalidatePrefix("") })

	return func(c *gin.Context) {
		// 1. 取 Authorization header
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			apperr.Abort(c, http.StatusUnauthorized, "authentication_error", "missing_api_key", "missing api key")
			return
		}

		// 2. 解析 Bearer token
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			apperr.Abort(c, http.StatusUnauthorized, "authentication_error", "invalid_authorization_format", "invalid authorization format, expected 'Bearer <key>'")
			return
		}
		plainKey := strings.TrimSpace(parts[1])
		if plainKey == "" {
			apperr.Abort(c, http.StatusUnauthorized, "authentication_error", "empty_api_key", "empty api key")
			return
		}

		// 3. SHA256 哈希
		hash := sha256Hash(plainKey)

		// 4. 先查本地缓存；命中且条目仍有效则直接放行
		if entry, ok := authCache.Get(hash); ok {
			if entry.valid() {
				setAuthContext(c, entry)
				c.Next()
				return
			}
			// 缓存条目已失效（如 key 到期）：清除后回源 DB，以 DB 最新状态为准
			authCache.Invalidate(hash)
		}

		// 5. 回源 MySQL 查 api_keys 表
		pool := a.DB
		if pool == nil {
			observability.L().Error("auth: db pool not initialized",
				zap.String("trace_id", c.GetString("trace_id")),
			)
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "db_unavailable", "internal error: db unavailable")
			return
		}

		queryCtx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		var keyID int64
		var tenantID int64
		var projectID sql.NullInt64
		var expiresAt *time.Time
		var retireAt *time.Time
		err := pool.QueryRowContext(queryCtx, `
			SELECT id, tenant_id, project_id, expires_at, retire_at
			FROM api_keys
			WHERE hash_key = ?
		`, hash).Scan(&keyID, &tenantID, &projectID, &expiresAt, &retireAt)

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				observability.L().Info("auth: api key not found",
					zap.String("trace_id", c.GetString("trace_id")),
				)
			} else {
				observability.L().Warn("auth: db query error",
					zap.String("trace_id", c.GetString("trace_id")),
					zap.Error(err),
				)
			}
			apperr.Abort(c, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "invalid api key")
			return
		}

		// 5. 检查过期与退役宽限期（阶段 16 轮换：过 retire_at 的旧 key 401）
		if expiresAt != nil && time.Now().After(*expiresAt) {
			apperr.Abort(c, http.StatusUnauthorized, "authentication_error", "api_key_expired", "api key expired")
			return
		}
		if retireAt != nil && !time.Now().Before(*retireAt) {
			apperr.Abort(c, http.StatusUnauthorized, "authentication_error", "api_key_retired", "api key retired (rotation grace period over)")
			return
		}

		// 6. 查租户状态
		var tenantStatus string
		err = pool.QueryRowContext(queryCtx, `
			SELECT status FROM tenants WHERE id = ?
		`, tenantID).Scan(&tenantStatus)

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				observability.L().Warn("auth: tenant not found for key",
					zap.String("trace_id", c.GetString("trace_id")),
					zap.Int64("tenant_id", tenantID),
				)
			} else {
				observability.L().Warn("auth: tenant query db error",
					zap.String("trace_id", c.GetString("trace_id")),
					zap.Error(err),
				)
			}
			apperr.Abort(c, http.StatusUnauthorized, "authentication_error", "tenant_not_found", "invalid api key (tenant not found)")
			return
		}

		if tenantStatus != "active" {
			apperr.Abort(c, http.StatusForbidden, "forbidden", "tenant_inactive", "tenant is not active")
			return
		}

		// 7. 全部通过 → 回填缓存（只缓存肯定结果）+ 塞 context
		entry := authEntry{
			tenantID:     tenantID,
			apiKeyID:     keyID,
			projectID:    projectID,
			expiresAt:    expiresAt,
			retireAt:     retireAt,
			tenantStatus: tenantStatus,
		}
		authCache.Set(hash, entry)
		setAuthContext(c, entry)

		c.Next()
	}
}

// setAuthContext 把鉴权结果写入 gin context（缓存命中与回源路径共用）
func setAuthContext(c *gin.Context, entry authEntry) {
	c.Set("tenant_id", entry.tenantID)
	c.Set("api_key_id", entry.apiKeyID)
	c.Set("tenant_status", entry.tenantStatus)
	if entry.projectID.Valid {
		c.Set("project_id", entry.projectID.Int64)
	}
}

// sha256Hash 计算字符串的 SHA256 十六进制哈希
func sha256Hash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
