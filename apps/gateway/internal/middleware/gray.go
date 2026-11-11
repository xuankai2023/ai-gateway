// GrayMiddleware 模型灰度引擎（阶段 13）
//
// 位置：SemanticCache 之后、Ledger 之前。
// 为什么在缓存之后？缓存命中直接 Abort 短路返回，不需要处理灰度。
//
// 流程：
//  1. 取 Auth 写入 context 的 tenant_id，探读 body 的 model 字段（读完回填）
//  2. 查灰度规则：本地缓存（gray:{tenant_id}:{model}, TTL 30s）→ miss 查 gray_rules 表
//  3. 流量切分：hash(trace_id) % 100 < weight → ctx.Set("target_model", rule.target_model)
//     + ctx.Set("is_gray", true)；handler 用 target_model 替代请求模型做路由解析
//  4. 未命中规则或 hash 落灰度区间外 → 原模型直通
//
// 缓存纪律：肯定/否定结果都缓存（found 标志），规则变更经 a.InvalidateCache("gray")
// 立即失效（广播 cache_invalidate 通道联动）；DB 查询失败不缓存，下次重查。
//
// 埋点：gateway_gray_requests_total{model,target}；灰度命中时记 info 日志 is_gray=true。
//
// A/B 对比 sidecar 模式本期不做，留作后续扩展（主链路走原模型、异步灰度对比）。
package middleware

import (
	"context"
	"database/sql"
	"encoding/json"
	"hash/crc32"
	"strconv"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/cache"
	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// grayTTL 灰度规则本地缓存过期时间
const grayTTL = 30 * time.Second

// grayEntry 灰度规则缓存值（命名空间 gray:{tenant_id}:{model}）
// found=false 也缓存（无规则的 tenant+model 不再每请求空查 DB）
type grayEntry struct {
	targetModel string
	weight      int
	found       bool
}

// Gray 模型灰度中间件（阶段 14：func(app) 形式；缓存实例级，随 App 生命周期关闭）
func Gray(a *app.App) gin.HandlerFunc {
	// grayCache 灰度规则本地缓存：key = {tenant_id}:{model}，TTL 30s
	grayCache := cache.New[grayEntry](cache.Options{
		Name:       "gray",
		MaxEntries: 10000,
		DefaultTTL: grayTTL,
	})
	observability.RegisterCache("gray", grayCache)
	a.AddCloser(grayCache.Close)
	// 规则变更（Admin 写库 / 广播 cache_invalidate）→ 下一次请求即按最新规则判定
	a.RegisterInvalidator("gray", func() { grayCache.InvalidatePrefix("") })

	resolveRule := func(c *gin.Context, tenantID int64, model string) grayEntry {
		key := strconv.FormatInt(tenantID, 10) + ":" + model
		if entry, ok := grayCache.Get(key); ok {
			return entry
		}

		pool := a.DB
		if pool == nil {
			return grayEntry{}
		}

		queryCtx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		var entry grayEntry
		err := pool.QueryRowContext(queryCtx, `
			SELECT target_model, weight
			FROM gray_rules
			WHERE tenant_id = ? AND model_name = ? AND enabled = 1
		`, tenantID, model).Scan(&entry.targetModel, &entry.weight)

		if err != nil {
			if err == sql.ErrNoRows {
				entry = grayEntry{found: false}
				grayCache.Set(key, entry)
				return entry
			}
			// 查询失败不缓存：下次重查；本请求按无规则直通
			observability.L().Warn("gray: rule query failed, pass through",
				zap.String("trace_id", c.GetString("trace_id")),
				zap.Int64("tenant_id", tenantID),
				zap.String("model", model),
				zap.Error(err),
			)
			return grayEntry{}
		}

		entry.found = true
		grayCache.Set(key, entry)
		return entry
	}

	return func(c *gin.Context) {
		tenantID := c.GetInt64("tenant_id")
		if tenantID <= 0 {
			c.Next() // 缺失 tenant 由后续中间件统一裁决，灰度层不重复报错
			return
		}

		// 探读请求体拿 model（读完回填，下游绑定不受影响）
		model := peekRequestModel(c)
		if model == "" {
			c.Next() // 非 JSON / 无 model：交给 handler 报 400，灰度层不裁决
			return
		}

		entry := resolveRule(c, tenantID, model)
		if !entry.found || entry.weight <= 0 || entry.targetModel == "" {
			c.Next()
			return
		}

		// 流量切分：按请求级稳定 ID（trace_id）哈希，同一请求多次判定结果一致
		bucket := crc32.ChecksumIEEE([]byte(c.GetString("trace_id"))) % 100
		if int(bucket) >= entry.weight {
			c.Next()
			return
		}

		c.Set("target_model", entry.targetModel)
		c.Set("is_gray", true)
		observability.RecordGrayRequest(model, entry.targetModel)
		observability.L().Info("gray routing applied",
			zap.String("trace_id", c.GetString("trace_id")),
			zap.Int64("tenant_id", tenantID),
			zap.String("model", model),
			zap.String("target_model", entry.targetModel),
			zap.Int("weight", entry.weight),
			zap.Bool("is_gray", true),
		)
		c.Next()
	}
}

// peekRequestModel 读取并回填请求体，提取 model 字段；非法 JSON / 缺失返回 ""
func peekRequestModel(c *gin.Context) string {
	body, err := readAndRestoreBody(c)
	if err != nil {
		return "" // bodyLimit 已兜底；读失败交给下游报错
	}

	var probe struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return ""
	}
	return probe.Model
}
