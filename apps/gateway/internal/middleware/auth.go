// AuthMiddleware API Key 鉴权（P0 版：直连 MySQL，不接 Redis）
//
// 位置：第 4 层（在 Logger 之后，业务 Handler 之前）
//
// 流程：
//  1. 从 Header Authorization: Bearer <key> 取明文 key
//  2. SHA256 哈希（不存明文）
//  3. 查 api_keys 表 → 拿到 tenant_id / expires_at
//  4. 查 tenants 表 → 拿到 status
//  5. ctx.Set("tenant_id", ...) / ctx.Set("api_key_id", ...) / ctx.Set("tenant_status", ...)
//
// Abort 条件：
//   - 无 Authorization header → 401
//   - key 无效（SHA256 在 api_keys 找不到） → 401
//   - key 过期（expires_at < now） → 401
//   - 租户停用（tenants.status != 'active'） → 403
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

	"ai-gateway/pkg/db"
	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// dbPool 测试时可替换，生产默认 db.GetPool
var dbPool = db.GetPool

// Auth API Key 鉴权中间件
func Auth() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. 取 Authorization header
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"message": "missing api key",
					"type":    "authentication_error",
				},
			})
			return
		}

		// 2. 解析 Bearer token
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"message": "invalid authorization format, expected 'Bearer <key>'",
					"type":    "authentication_error",
				},
			})
			return
		}
		plainKey := strings.TrimSpace(parts[1])
		if plainKey == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"message": "empty api key",
					"type":    "authentication_error",
				},
			})
			return
		}

		// 3. SHA256 哈希
		hash := sha256Hash(plainKey)

		// 4. 直连 MySQL 查 api_keys 表
		pool := dbPool()
		if pool == nil {
			observability.L().Error("auth: db pool not initialized",
				zap.String("trace_id", c.GetString("trace_id")),
			)
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error": gin.H{
					"message": "internal error: db unavailable",
					"type":    "internal_error",
				},
			})
			return
		}

		queryCtx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		var keyID int64
		var tenantID int64
		var expiresAt *time.Time
		err := pool.QueryRowContext(queryCtx, `
			SELECT id, tenant_id, expires_at
			FROM api_keys
			WHERE hash_key = ?
		`, hash).Scan(&keyID, &tenantID, &expiresAt)

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
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"message": "invalid api key",
					"type":    "authentication_error",
				},
			})
			return
		}

		// 5. 检查过期
		if expiresAt != nil && time.Now().After(*expiresAt) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"message": "api key expired",
					"type":    "authentication_error",
				},
			})
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
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"message": "invalid api key (tenant not found)",
					"type":    "authentication_error",
				},
			})
			return
		}

		if tenantStatus != "active" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": gin.H{
					"message": "tenant is not active",
					"type":    "forbidden",
				},
			})
			return
		}

		// 7. 全部通过 → 塞 context
		c.Set("tenant_id", tenantID)
		c.Set("api_key_id", keyID)
		c.Set("tenant_status", tenantStatus)

		c.Next()
	}
}

// sha256Hash 计算字符串的 SHA256 十六进制哈希
func sha256Hash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
