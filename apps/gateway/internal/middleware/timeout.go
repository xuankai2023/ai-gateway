// TimeoutMiddleware 请求级超时兜底
//
// 位置：v1 组内（Auth 之后、RateLimit 之前）——Auth 的 DB 查询自带 3s ctx 超时，
// 本层把 RateLimit（Redis 调用）与 relay（上游调用）整体包进统一时限。
//
// 分层超时体系（阶段 4）：
//   - 非流式请求：整链路上限 request_timeout（默认 60s，含全部渠道重试）
//   - 流式 chat 请求：整链路上限 stream_max_duration（默认 300s），
//     长回答不被 request_timeout 误杀；识别方式是探读 body 的 stream 字段（读完回填）
//   - 单次渠道 attempt 上限 attempt_timeout 在 relay 层，不在本层
//
// 本层只包装 context，不写任何响应：下游（relay / handler）检测到 ctx 超时后
// 按错误契约返回 504。
package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"ai-gateway/apps/gateway/internal/app"
)

// 内置兜底默认值（配置缺省/非法值时使用，与 pkg/config 默认值一致）
const (
	fallbackRequestTimeout    = 60 * time.Second
	fallbackStreamMaxDuration = 300 * time.Second
)

// TimeoutOptions 超时配置（来自 gateway.request_timeout / stream_max_duration）
type TimeoutOptions struct {
	RequestTimeout    time.Duration
	StreamMaxDuration time.Duration
}

// Timeout 请求级超时中间件（阶段 14：func(app) 形式）
// 阶段 15：时限每请求读取 a.Hot 热更新值（Admin/SIGHUP 改 request_timeout / stream_max_duration
// 不重进程生效）；a.Hot 缺失或零值时回落静态配置，再回落内置兜底
func Timeout(a *app.App) gin.HandlerFunc {
	current := func() TimeoutOptions {
		var opts TimeoutOptions
		if a.Hot != nil {
			opts.RequestTimeout = a.Hot.RequestTimeout()
			opts.StreamMaxDuration = a.Hot.StreamMaxDuration()
		}
		if (opts.RequestTimeout <= 0 || opts.StreamMaxDuration <= 0) && a.Config != nil {
			req, _, streamMax := a.Config.Gateway.TimeoutDurations()
			if opts.RequestTimeout <= 0 {
				opts.RequestTimeout = req
			}
			if opts.StreamMaxDuration <= 0 {
				opts.StreamMaxDuration = streamMax
			}
		}
		if opts.RequestTimeout <= 0 {
			opts.RequestTimeout = fallbackRequestTimeout
		}
		if opts.StreamMaxDuration <= 0 {
			opts.StreamMaxDuration = fallbackStreamMaxDuration
		}
		return opts
	}

	return func(c *gin.Context) {
		opts := current()
		d := opts.RequestTimeout
		if isStreamChatRequest(c) {
			d = opts.StreamMaxDuration
		}

		// 优先基于 Trace 注入的 otel_ctx 派生（保留 span），并回写两份，
		// 保证 handler 的 requestContext() 与 c.Request.Context() 都带超时
		base := c.Request.Context()
		if v, ok := c.Get("otel_ctx"); ok {
			if octx, ok := v.(context.Context); ok {
				base = octx
			}
		}
		ctx, cancel := context.WithTimeout(base, d)
		defer cancel()

		c.Set("otel_ctx", ctx)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

// isStreamChatRequest 识别流式 chat 请求（POST .../chat/completions 且 body {"stream":true}）
// 读取后完整回填 body，下游 RateLimit 估算与 handler 绑定不受影响
func isStreamChatRequest(c *gin.Context) bool {
	if c.Request.Method != http.MethodPost || c.Request.Body == nil {
		return false
	}
	if !strings.HasSuffix(c.Request.URL.Path, "/chat/completions") {
		return false
	}
	body, err := readAndRestoreBody(c)
	if err != nil {
		return false // bodyLimit 已用 MaxBytesReader 兜底；读失败交给下游报错
	}

	var probe struct {
		Stream bool `json:"stream"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return false // 非法 JSON 交给 handler 返回 400，按非流式处理
	}
	return probe.Stream
}
