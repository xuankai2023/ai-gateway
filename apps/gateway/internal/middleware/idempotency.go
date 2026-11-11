// IdempotencyMiddleware 幂等键（阶段 11）
//
// 位置：RateLimit 之后、SemanticCache / Ledger 之前。
// 早于 Ledger 的意义：重复 key 回放与 409 冲突都不再预占/结算，账本只记录真实一次上游调用。
//
// 流程（仅非流式）：
//  1. 取请求头 Idempotency-Key（缺省直接放行，不启用幂等）
//  2. Redis SETNX idem:{tenant}:{key}（TTL 24h）占位
//  3. 抢占成功 → 捕获响应体；2xx 且非空 → 写 idem:resp:{tenant}:{key} 快照；
//     失败（非 2xx / 空响应）→ 释放占位，允许客户端重试
//  4. 抢占失败 → 有快照则回放（200 + Idempotent-Replay: true）；无快照 → 409 idempotency_conflict
//  5. stream=true + 幂等键 → 400（流式不做响应快照）
//
// 故障策略：Redis 不可用 → 503 + Retry-After（幂等语义要求强一致，宁可拒绝也不重复计费）。
package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/idempotency"
	"ai-gateway/pkg/observability"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const (
	// idempotencyHeader 客户端幂等键请求头
	idempotencyHeader = "Idempotency-Key"
	// idempotencyMaxKeyLen 幂等键最大长度
	idempotencyMaxKeyLen = 255
	// idempotencyStoreTimeout 占位 / 快照读写超时
	idempotencyStoreTimeout = time.Second
)

// Idempotency 幂等键中间件（阶段 14：func(app) 形式）
func Idempotency(a *app.App) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.GetHeader(idempotencyHeader)
		if key == "" {
			c.Next()
			return
		}
		if len(key) > idempotencyMaxKeyLen {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "invalid_idempotency_key",
				"Idempotency-Key too long (max 255)")
			return
		}

		tenantID := c.GetInt64("tenant_id")
		if tenantID <= 0 {
			apperr.Abort(c, http.StatusInternalServerError, "internal_error", "tenant_missing", "internal error: tenant context missing")
			return
		}

		// 流式请求不支持幂等快照：提前拒绝（避免占位后才发现无法捕获 SSE）
		stream, err := peekStreamFlag(c)
		if err != nil {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "request_body_read_failed", "failed to read request body")
			return
		}
		if stream {
			apperr.Abort(c, http.StatusBadRequest, "invalid_request_error", "idempotency_stream_unsupported",
				"Idempotency-Key is only supported for non-streaming requests")
			return
		}

		cli := a.Redis
		if cli == nil {
			rejectIdempotencyUnavailable(c)
			return
		}
		store := idempotency.New(cli)

		acquireCtx, cancel := context.WithTimeout(c.Request.Context(), idempotencyStoreTimeout)
		acquired, err := store.Acquire(acquireCtx, tenantID, key)
		cancel()
		if err != nil {
			observability.L().Warn("idempotency: acquire failed",
				zap.String("trace_id", c.GetString("trace_id")),
				zap.Int64("tenant_id", tenantID),
				zap.Error(err),
			)
			rejectIdempotencyUnavailable(c)
			return
		}

		if !acquired {
			handleDuplicate(c, store, tenantID, key)
			return
		}

		// 首个请求：捕获响应快照
		capture := newResponseCapture(c.Writer)
		c.Writer = capture
		c.Next()

		status := c.Writer.Status()
		body := capture.Bytes()
		finishCtx, finishCancel := context.WithTimeout(context.Background(), idempotencyStoreTimeout)
		defer finishCancel()

		if status >= 200 && status < 300 && len(body) > 0 {
			contentType := c.Writer.Header().Get("Content-Type")
			if contentType == "" {
				contentType = "application/json"
			}
			if err := store.Save(finishCtx, tenantID, key, &idempotency.Snapshot{
				Status:      status,
				ContentType: contentType,
				Body:        append([]byte(nil), body...),
			}); err != nil {
				observability.L().Warn("idempotency: save snapshot failed, releasing placeholder",
					zap.String("trace_id", c.GetString("trace_id")),
					zap.Int64("tenant_id", tenantID),
					zap.Error(err),
				)
				_ = store.Release(finishCtx, tenantID, key)
			}
			return
		}

		// 失败 / 空响应：释放占位，允许客户端用同 key 重试
		if err := store.Release(finishCtx, tenantID, key); err != nil {
			observability.L().Warn("idempotency: release placeholder failed",
				zap.String("trace_id", c.GetString("trace_id")),
				zap.Int64("tenant_id", tenantID),
				zap.Error(err),
			)
		}
	}
}

// handleDuplicate 占位已被占：有快照则回放，无快照说明首个请求仍在处理 → 409
func handleDuplicate(c *gin.Context, store *idempotency.Store, tenantID int64, key string) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), idempotencyStoreTimeout)
	defer cancel()

	snap, ok, err := store.Snapshot(ctx, tenantID, key)
	if err != nil {
		observability.L().Warn("idempotency: snapshot lookup failed",
			zap.String("trace_id", c.GetString("trace_id")),
			zap.Int64("tenant_id", tenantID),
			zap.Error(err),
		)
		rejectIdempotencyUnavailable(c)
		return
	}
	if !ok {
		apperr.Abort(c, http.StatusConflict, "idempotency_error", "idempotency_conflict",
			"a request with the same Idempotency-Key is still in progress")
		return
	}

	c.Header("Idempotent-Replay", "true")
	c.Data(snap.Status, snap.ContentType, snap.Body)
	c.Abort()
	observability.L().Info("idempotency: replay snapshot",
		zap.String("trace_id", c.GetString("trace_id")),
		zap.Int64("tenant_id", tenantID),
	)
}

// peekStreamFlag 读取并回填请求体，判断是否流式请求；非法 JSON 视为非流式（交给 handler 报 400）
func peekStreamFlag(c *gin.Context) (bool, error) {
	body, err := readAndRestoreBody(c)
	if err != nil {
		return false, err
	}

	var probe struct {
		Stream bool `json:"stream"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return false, nil
	}
	return probe.Stream, nil
}

// rejectIdempotencyUnavailable Redis 不可用：幂等请求快速拒绝（不冒重复计费风险）
func rejectIdempotencyUnavailable(c *gin.Context) {
	c.Header("Retry-After", "1")
	apperr.Abort(c, http.StatusServiceUnavailable, "idempotency_error", "idempotency_unavailable",
		"idempotency store unavailable, please retry later")
}
