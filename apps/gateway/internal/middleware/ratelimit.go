// RateLimitMiddleware 租户限流准入（RPS / TPM / 并发数三合一）
//
// 位置：第 5 层（Auth 之后、relay 之前）——缓存命中也要先过限流，防止绕过。
//
// 流程：
//  1. 取 Auth 写入 context 的 tenant_id
//  2. 解析租户配额：本地缓存（quota:{tenant_id}, TTL 60s）→ miss 查 tenants 表
//     （rps_limit / tpm_limit / concurrency_limit，NULL 列回落全局默认）
//  3. RPS  令牌桶（Redis Lua，key=rl:rps:{tenant_id}）
//  4. TPM  滑动窗口（Redis Lua，key=rl:tpm:{tenant_id}，token 数为请求体估算值）
//  5. 并发数 INCR（key=rl:conc:{tenant_id}，120s TTL 兜底）→ defer DECR
//
// Abort 条件：任一项超限 → 429 + Retry-After + apperr 统一格式（type=rate_limit_error），
// 并计 gateway_rate_limited_total{tenant,kind}。
//
// 故障策略：Redis 不可用 / DB 配额查询失败 → 告警日志 + 放行（fail-open）。
// 限流是防护层不是账房：抖动期宁可临时不限，也不把正常流量全挡死；
// 并发数硬上限由 load_shed（阶段 5）在最外层兜底。
//
// TPM 口径：准入只估输入 token（输出未知），结算整单重算在阶段 10 账本落地。
package middleware

import (
	"context"
	"database/sql"
	"net/http"
	"strconv"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/cache"
	"ai-gateway/pkg/observability"
	"ai-gateway/pkg/ratelimit"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// quotaTTL 租户配额本地缓存过期时间
const quotaTTL = 60 * time.Second

// 内置兜底默认值（配置缺省/非法值时使用）
const (
	fallbackRPS         = 10
	fallbackTPM         = 10000
	fallbackConcurrency = 50
)

// quotaEntry 解析后的租户配额（默认值已合并）
type quotaEntry struct {
	rps         int
	tpm         int
	concurrency int
}

// RateLimit 租户限流中间件（阶段 14：func(app) 形式；默认值取自 a.Hot 热更新值，阶段 15）
func RateLimit(a *app.App) gin.HandlerFunc {
	// defaultQuota 每请求读取热更新值（Admin 改默认限额 / SIGHUP 不重进程生效）；
	// a.Hot 缺失或零值时回落静态配置，再回落内置兜底
	defaultQuota := func() quotaEntry {
		rps, tpm, conc := 0, 0, 0
		if a.Hot != nil {
			rps, tpm, conc = a.Hot.DefaultRPSLimit(), a.Hot.DefaultTPMLimit(), a.Hot.DefaultConcurrencyLimit()
		}
		if rps <= 0 && a.Config != nil {
			rps = a.Config.Gateway.DefaultRPSLimit
		}
		if tpm <= 0 && a.Config != nil {
			tpm = a.Config.Gateway.DefaultTPMLimit
		}
		if conc <= 0 && a.Config != nil {
			conc = a.Config.Gateway.DefaultConcurrencyLimit
		}
		if rps <= 0 {
			rps = fallbackRPS
		}
		if tpm <= 0 {
			tpm = fallbackTPM
		}
		if conc <= 0 {
			conc = fallbackConcurrency
		}
		return quotaEntry{rps: rps, tpm: tpm, concurrency: conc}
	}

	// quotaCache 租户配额本地缓存：key = tenant_id，TTL 60s（实例级，随 App 生命周期关闭）
	quotaCache := cache.New[quotaEntry](cache.Options{
		Name:       "quota",
		MaxEntries: 10000,
		DefaultTTL: quotaTTL,
	})
	observability.RegisterCache("quota", quotaCache)
	a.AddCloser(quotaCache.Close)
	// Admin 调整租户配额 / 启停租户 → 广播 cache_invalidate{prefix=quota} 全节点立即生效（阶段 15）
	a.RegisterInvalidator("quota", func() { quotaCache.InvalidatePrefix("") })

	// resolveQuota 解析租户配额：本地缓存优先，miss 查 tenants 表；NULL 列回落全局默认
	resolveQuota := func(c *gin.Context, tenantID int64) quotaEntry {
		key := strconv.FormatInt(tenantID, 10)
		if entry, ok := quotaCache.Get(key); ok {
			return entry
		}

		pool := a.DB
		if pool == nil {
			return defaultQuota()
		}

		queryCtx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		var rps, tpm, conc sql.NullInt64
		err := pool.QueryRowContext(queryCtx, `
			SELECT rps_limit, tpm_limit, concurrency_limit
			FROM tenants
			WHERE id = ?
		`, tenantID).Scan(&rps, &tpm, &conc)
		if err != nil {
			// 查询失败（含行不存在）：fail-open 用全局默认，不缓存（下次重查）
			observability.L().Warn("ratelimit: quota query failed, fallback to defaults",
				zap.String("trace_id", c.GetString("trace_id")),
				zap.Int64("tenant_id", tenantID),
				zap.Error(err),
			)
			return defaultQuota()
		}

		defaults := defaultQuota()
		entry := quotaEntry{
			rps:         nullIntOr(rps, defaults.rps),
			tpm:         nullIntOr(tpm, defaults.tpm),
			concurrency: nullIntOr(conc, defaults.concurrency),
		}
		quotaCache.Set(key, entry)
		return entry
	}

	return func(c *gin.Context) {
		tenantID := c.GetInt64("tenant_id")
		if tenantID <= 0 {
			// Auth 必写 tenant_id；走到这里说明中间件链被错误裁剪
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "tenant_missing", "internal error: tenant context missing")
			return
		}

		cli := a.Redis
		if cli == nil {
			observability.L().Warn("ratelimit: redis client not initialized, fail-open",
				zap.String("trace_id", c.GetString("trace_id")),
			)
			c.Next()
			return
		}
		limiter := ratelimit.New(cli)
		quota := resolveQuota(c, tenantID)

		// 1. RPS 令牌桶
		allowed, retryAfter, err := limiter.AllowRPS(c.Request.Context(), tenantID, quota.rps)
		if err != nil {
			logLimitError(c, "rps", err)
		} else if !allowed {
			rejectRateLimited(c, tenantID, "rps", retryAfter)
			return
		}

		// 2. TPM 滑动窗口（token 数为请求体估算值；读完回填 body 供 handler 使用）
		tokens, err := estimateRequestTokens(c)
		if err != nil {
			// bodyLimit 已用 MaxBytesReader 兜底，这里只会是截断读错误 → 400 早拦
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "request_body_read_failed", "failed to read request body")
			return
		}
		allowed, retryAfter, err = limiter.AllowTPM(c.Request.Context(), tenantID, quota.tpm, tokens, c.GetString("trace_id"))
		if err != nil {
			logLimitError(c, "tpm", err)
		} else if !allowed {
			rejectRateLimited(c, tenantID, "tpm", retryAfter)
			return
		}

		// 3. 并发数（占用成功才放行，defer 释放）
		allowed, err = limiter.AcquireConcurrency(c.Request.Context(), tenantID, quota.concurrency)
		if err != nil {
			logLimitError(c, "concurrency", err)
		} else if !allowed {
			rejectRateLimited(c, tenantID, "concurrency", 0)
			return
		} else {
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := limiter.ReleaseConcurrency(ctx, tenantID); err != nil {
					observability.L().Warn("ratelimit: release concurrency failed",
						zap.String("trace_id", c.GetString("trace_id")),
						zap.Int64("tenant_id", tenantID),
						zap.Error(err),
					)
				}
			}()
		}

		c.Next()
	}
}

// nullIntOr NULL 列回落默认值；非正数视为未配置
func nullIntOr(v sql.NullInt64, fallback int) int {
	if !v.Valid || v.Int64 <= 0 {
		return fallback
	}
	return int(v.Int64)
}

// estimateRequestTokens 读取请求体估算输入 token，并把 body 回填供下游 handler 复用
// （bodyLimit 中间件已用 MaxBytesReader 限制最大读取量，这里不会读爆内存）
func estimateRequestTokens(c *gin.Context) (int, error) {
	body, err := readAndRestoreBody(c)
	if err != nil {
		return 0, err
	}
	return ratelimit.EstimateTokens(body), nil
}

// rejectRateLimited 429 统一出口：Retry-After 头 + apperr 契约 + 指标
func rejectRateLimited(c *gin.Context, tenantID int64, kind string, retryAfter time.Duration) {
	secs := int(retryAfter.Seconds() + 0.5) // 向上取整
	if secs < 1 {
		secs = 1
	}
	c.Header("Retry-After", strconv.Itoa(secs))
	observability.RecordRateLimited(strconv.FormatInt(tenantID, 10), kind)
	apperr.Abort(c, http.StatusTooManyRequests, "rate_limit_error", kind+"_limit_exceeded", kind+" rate limit exceeded, please retry later")
}

// logLimitError Redis 抖动时告警并放行（fail-open）
func logLimitError(c *gin.Context, kind string, err error) {
	observability.L().Warn("ratelimit: redis error, fail-open",
		zap.String("trace_id", c.GetString("trace_id")),
		zap.String("kind", kind),
		zap.Error(err),
	)
}
