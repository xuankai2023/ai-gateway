// Package diagnostics 请求级诊断包（排障快照）
//
// 每次请求在 relay 层记录路由决策链路（attempts），handler 收尾时按采样策略落库：
//   - 错误请求（status >= 500 或所有渠道耗尽）100% 采样
//   - 正常请求按 SampleRate（默认 1%）
//
// 写入异步进行（App.Async 执行器），不阻塞响应路径；
// error_message 落库前经 observability.RedactText 脱敏。
package diagnostics

import (
	"context"
	"database/sql"
	"encoding/json"
	"math/rand"
	"time"

	"ai-gateway/pkg/async"
	"ai-gateway/pkg/observability"
	"go.uber.org/zap"
)

// DefaultSampleRate 默认采样率（1%）
const DefaultSampleRate = 0.01

// writeTimeout 单次诊断写入超时
const writeTimeout = 3 * time.Second

// Attempt 一次渠道尝试的快照（attempts JSON 数组的元素）
type Attempt struct {
	Attempt            int    `json:"attempt"`              // 第几次尝试（1 起）
	ChannelID          int64  `json:"channel_id"`           // abilities 渠道 ID（0 = models 表兜底）
	Provider           string `json:"provider"`             // provider 类型
	BaseURL            string `json:"base_url"`             // 上游地址（无 key）
	Status             string `json:"status"`               // success / failed
	ErrorKind          string `json:"error_kind,omitempty"` // auth/rate_limit/server/timeout/overloaded/unknown
	ErrorMessage       string `json:"error_message,omitempty"`
	DurationMs         int64  `json:"duration_ms"`
	BreakerStateBefore string `json:"breaker_state_before"` // closed / open / half-open
}

// Record 一条请求诊断记录（request_diagnostics 表的一行）
type Record struct {
	TraceID       string
	TenantID      int64
	Model         string
	Stream        bool
	RequestDigest string // observability.BodyDigest 输出
	Attempts      []Attempt
	StatusCode    int
	ErrorType     string // 最后一跳的 ErrorKind；无错误为空
	ErrorMessage  string // 已脱敏
	TotalCost     int64  // μUSD
	DurationMs    int64
}

// Recorder 诊断记录器：采样判定 + 异步落库
type Recorder struct {
	db         *sql.DB
	async      *async.Executor
	sampleRate float64
	rand       *rand.Rand
}

// New 创建记录器；sampleRate <= 0 回落 DefaultSampleRate；db 或 async 为 nil 时 Record 静默跳过
func New(db *sql.DB, asyncExec *async.Executor, sampleRate float64) *Recorder {
	if sampleRate <= 0 {
		sampleRate = DefaultSampleRate
	}
	return &Recorder{
		db:         db,
		async:      asyncExec,
		sampleRate: sampleRate,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SampleRate 当前生效的采样率（测试用）
func (r *Recorder) SampleRate() float64 { return r.sampleRate }

// shouldSample 采样判定：错误请求（5xx / 有错误类型）100%，其余按采样率
func (r *Recorder) shouldSample(rec *Record) bool {
	if rec.StatusCode >= 500 || rec.ErrorType != "" {
		return true
	}
	return r.rand.Float64() < r.sampleRate
}

// Record 采样判定后异步落库；未命中采样直接返回
func (r *Recorder) Record(rec Record) {
	if r.db == nil || r.async == nil {
		return
	}
	if !r.shouldSample(&rec) {
		return
	}
	// 脱敏在写入前完成（attempts 内每条 error_message + 顶层 error_message）
	for i := range rec.Attempts {
		rec.Attempts[i].ErrorMessage = observability.RedactText(rec.Attempts[i].ErrorMessage)
	}
	rec.ErrorMessage = observability.RedactText(rec.ErrorMessage)

	attemptsJSON, err := json.Marshal(rec.Attempts)
	if err != nil {
		attemptsJSON = []byte("[]")
	}

	r.async.Go(func() {
		ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
		defer cancel()
		var errType, errMsg any
		if rec.ErrorType != "" {
			errType = rec.ErrorType
		}
		if rec.ErrorMessage != "" {
			errMsg = rec.ErrorMessage
		}
		if _, err := r.db.ExecContext(ctx, `
			INSERT INTO request_diagnostics
				(trace_id, tenant_id, model, stream, request_digest, attempts,
				 status_code, error_type, error_message, total_cost, duration_ms)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			rec.TraceID, rec.TenantID, rec.Model, rec.Stream, rec.RequestDigest, string(attemptsJSON),
			rec.StatusCode, errType, errMsg, rec.TotalCost, rec.DurationMs); err != nil {
			observability.L().Warn("diagnostics: insert failed",
				zap.String("trace_id", rec.TraceID), zap.Error(err))
		}
	})
}
