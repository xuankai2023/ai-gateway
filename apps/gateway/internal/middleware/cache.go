// SemanticCacheMiddleware 语义缓存（阶段 12）
//
// 位置：RateLimit 之后、Ledger 之前。
// 命中直接返回缓存响应并 Abort 短路，跳过 relay 与账本（cost=0，不产生预占/结算）；
// 未命中放行，handler 返回后异步写回精确层（+ 可选向量层）。
//
// 两级查找：Redis 精确哈希 →（可选）Qdrant 向量相似 ≥ 0.95。
// 隔离：Redis key 带 tenant + project + 知识库版本(epoch)，Qdrant 每租户每项目独立 collection。
//
// 开关：tenants.cache_enabled（本地缓存 60s）；查询失败按关闭处理（宁可不用缓存也不吐旧数据）。
// 知识库版本：projects.index_epoch（本地缓存 60s），文档入库 +1 → 旧答案 60s 内失效。
// 跳过：stream=true（SSE 不缓存）、无 messages、非法请求体（交给 handler 报 400）。
package middleware

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	semcache "ai-gateway/apps/gateway/internal/cache"
	"ai-gateway/apps/gateway/internal/usagelog"
	lru "ai-gateway/pkg/cache"
	"ai-gateway/pkg/observability"
	"ai-gateway/pkg/requestid"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// tenantCacheTTL 租户缓存开关 / 项目知识库版本本地缓存过期时间
const tenantCacheTTL = 60 * time.Second

// SemanticCache 语义缓存中间件（阶段 14：func(app) 形式；a.SemanticCache 为 nil 时直接放行）
func SemanticCache(a *app.App) gin.HandlerFunc {
	// tenantCacheSwitch 租户 cache_enabled 本地缓存：key = tenant_id（实例级，随 App 生命周期关闭）
	tenantCacheSwitch := lru.New[bool](lru.Options{
		Name:       "semcache_switch",
		MaxEntries: 10000,
		DefaultTTL: tenantCacheTTL,
	})
	observability.RegisterCache("semcache_switch", tenantCacheSwitch)
	a.AddCloser(tenantCacheSwitch.Close)

	// projectEpoch 项目知识库版本本地缓存：key = project_id（文档入库 / 删除后 +1）
	projectEpoch := lru.New[int64](lru.Options{
		Name:       "project_epoch",
		MaxEntries: 10000,
		DefaultTTL: tenantCacheTTL,
	})
	observability.RegisterCache("project_epoch", projectEpoch)
	a.AddCloser(projectEpoch.Close)

	inst := a.SemanticCache

	// resolveTenantCacheEnabled 租户缓存开关：本地缓存优先，miss 查 tenants.cache_enabled
	resolveTenantCacheEnabled := func(c *gin.Context, tenantID int64) bool {
		key := strconv.FormatInt(tenantID, 10)
		if v, ok := tenantCacheSwitch.Get(key); ok {
			return v
		}

		pool := a.DB
		if pool == nil {
			return false
		}
		queryCtx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		var enabled sql.NullBool
		if err := pool.QueryRowContext(queryCtx,
			`SELECT cache_enabled FROM tenants WHERE id = ?`, tenantID).Scan(&enabled); err != nil {
			// 查询失败不缓存否定结果：下次重查；本请求按关闭处理（不吐旧数据）
			observability.L().Warn("semantic cache: tenant switch query failed, treat as disabled",
				zap.String("trace_id", c.GetString("trace_id")),
				zap.Int64("tenant_id", tenantID),
				zap.Error(err),
			)
			return false
		}
		v := enabled.Valid && enabled.Bool
		tenantCacheSwitch.Set(key, v)
		return v
	}

	// resolveProjectEpoch 项目知识库版本：本地缓存优先，miss 查 projects.index_epoch
	// 项目缺失 / 查询失败返回 0（照常缓存，但项目间仍通过 key 前缀隔离）
	resolveProjectEpoch := func(c *gin.Context, projectID int64) int64 {
		if projectID <= 0 {
			return 0
		}
		key := strconv.FormatInt(projectID, 10)
		if v, ok := projectEpoch.Get(key); ok {
			return v
		}
		pool := a.DB
		if pool == nil {
			return 0
		}
		queryCtx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		var epoch int64
		if err := pool.QueryRowContext(queryCtx,
			`SELECT index_epoch FROM projects WHERE id = ?`, projectID).Scan(&epoch); err != nil {
			observability.L().Warn("semantic cache: project epoch query failed, use 0",
				zap.String("trace_id", c.GetString("trace_id")),
				zap.Int64("project_id", projectID),
				zap.Error(err),
			)
			return 0
		}
		projectEpoch.Set(key, epoch)
		return epoch
	}

	return func(c *gin.Context) {
		if inst == nil {
			c.Next()
			return
		}

		tenantID := c.GetInt64("tenant_id")
		if tenantID <= 0 {
			c.Next() // 缺失 tenant 由后续 Ledger 统一报 500，缓存层不重复裁决
			return
		}
		if !resolveTenantCacheEnabled(c, tenantID) {
			c.Next()
			return
		}

		// 读取并回填请求体（stream / model / messages 判定）
		body, err := readAndRestoreBody(c)
		if err != nil {
			c.Next() // bodyLimit 已兜底，读取失败交给 handler
			return
		}

		var probe struct {
			Model    string             `json:"model"`
			Messages []semcache.Message `json:"messages"`
			Stream   bool               `json:"stream"`
		}
		if err := json.Unmarshal(body, &probe); err != nil {
			c.Next() // 非法 JSON：交给 handler 返回 400
			return
		}
		if probe.Stream || probe.Model == "" || len(probe.Messages) == 0 {
			c.Next()
			return
		}

		projectID := c.GetInt64("project_id")
		epoch := resolveProjectEpoch(c, projectID)

		lookupCtx, lookupCancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		entry, layer, err := inst.Lookup(lookupCtx, tenantID, projectID, epoch, probe.Model, probe.Messages)
		lookupCancel()
		if err != nil {
			observability.L().Warn("semantic cache: lookup failed, fail-open",
				zap.String("trace_id", c.GetString("trace_id")),
				zap.Int64("tenant_id", tenantID),
				zap.Int64("project_id", projectID),
				zap.Error(err),
			)
			entry = nil
		}

		if entry != nil {
			serveCacheHit(a, c, tenantID, projectID, probe.Model, entry, layer)
			return
		}
		observability.RecordSemanticCacheMiss()

		// 未命中：捕获响应，2xx 异步写回
		capture := newResponseCapture(c.Writer)
		c.Writer = capture
		c.Next()

		if c.Writer.Status() < 200 || c.Writer.Status() >= 300 {
			return
		}
		respBody := capture.Bytes()
		if len(respBody) == 0 {
			return
		}
		contentType := c.Writer.Header().Get("Content-Type")
		if contentType == "" {
			contentType = "application/json"
		}
		now := time.Now()
		cached := &semcache.Entry{
			Body:        append([]byte(nil), respBody...),
			ContentType: contentType,
			CreatedAt:   now.Unix(),
			ExpiresAt:   now.Add(inst.TTL()).Unix(),
		}

		a.Async.Go(func() {
			storeCtx, storeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer storeCancel()
			if err := inst.Store(storeCtx, tenantID, projectID, epoch, probe.Model, probe.Messages, cached); err != nil {
				observability.L().Warn("semantic cache: async store failed",
					zap.Int64("tenant_id", tenantID),
					zap.Int64("project_id", projectID),
					zap.String("model", probe.Model),
					zap.Error(err),
				)
			}
		})
	}
}

// serveCacheHit 命中：写指标/日志/用量（cost=0）并短路返回
func serveCacheHit(a *app.App, c *gin.Context, tenantID, projectID int64, model string, entry *semcache.Entry, layer string) {
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = requestid.New()
		c.Set("request_id", requestID)
	}

	if a.Usage != nil {
		a.Usage.Add(usagelog.Entry{
			RequestID: requestID,
			TenantID:  tenantID,
			APIKeyID:  c.GetInt64("api_key_id"),
			ProjectID: projectID,
			Model:     model,
			Provider:  "cache",
			Cost:      0,
			Status:    "success",
		})
	}
	observability.RecordSemanticCacheHit(layer)
	observability.L().Info("semantic cache hit",
		zap.String("trace_id", c.GetString("trace_id")),
		zap.Int64("tenant_id", tenantID),
		zap.String("model", model),
		zap.String("layer", layer),
		zap.Bool("cache_hit", true),
	)

	c.Header("X-Cache", "HIT")
	c.Header("X-Cache-Layer", layer)
	c.Data(http.StatusOK, entry.ContentType, entry.Body)
	c.Abort()
}
