// Package observability 可观测性基础封装
//
// logger.go 结构化日志：JSON 输出到 stdout，一条一行，Loki 可直接采集。
//
// 阶段 7 新增采样：
//   - 每秒首采 100 条，超出后 1% 采样（防高并发下日志打爆磁盘）
//   - Error/Fatal 级别不采样（panic 与 5xx 必须每条留存）
//
// 启动阶段统一用 BeginStage / Done 打点，格式固定为
// ts / level / msg / stage / elapsed_ms，便于按阶段排查启动问题。
package observability

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// std 全局 logger；Init 之前为 Nop，避免 nil panic
var std = zap.NewNop()

// samplingOnce 确保采样只注入一次（测试多次调用 New 时不叠加采样器）
var samplingOnce sync.Once

// New 创建输出到 stdout 的 JSON logger；level 为空时默认 info
func New(level string) (*zap.Logger, error) {
	return newLogger(os.Stdout, level)
}

// newLogger 可指定输出目标，方便测试断言 JSON 结构
//
// 默认开启阶段 7 采样：每秒 100 条全保留，超出 1% 采样；Error 及以上不采样。
// 测试场景可传入 newLogger 后自行 Skip 设置禁用采样（见 newLoggerNoSampling）。
func newLogger(w io.Writer, level string) (*zap.Logger, error) {
	lv := zapcore.InfoLevel
	if level != "" {
		parsed, err := zapcore.ParseLevel(level)
		if err != nil {
			return nil, fmt.Errorf("无效日志级别 %q: %w", level, err)
		}
		lv = parsed
	}

	encCfg := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
	}

	// 三层 core：JSON 编码 → 采样（Info 及以下）→ 写入 sink
	baseCore := zapcore.NewCore(zapcore.NewJSONEncoder(encCfg), zapcore.AddSync(w), lv)
	sampledCore := newSamplingCore(baseCore, lv)

	return zap.New(sampledCore, zap.AddCaller()), nil
}

// newSamplingCore 包装一个采样层：Info 及以下走采样器，Error 及以上直通不采样。
//
// 采样参数（阶段 7 定死，配置化留给后续）：
//   - tick = 1s
//   - initial = 100   每秒前 100 条全保留
//   - thereafter = 100 之后每 100 条采样 1 条（≈1%）
func newSamplingCore(core zapcore.Core, level zapcore.Level) zapcore.Core {
	// 采样参数（阶段 7 定死，配置化留给后续）：
	// tick = 1s, initial = 100（每秒前 100 条全保留）, thereafter = 100（之后每 100 条采样 1 条 ≈1%）
	inner := zapcore.NewSamplerWithOptions(core, time.Second, 100, 100)
	return &passthroughCore{Core: core, sampler: inner, minLevel: zapcore.ErrorLevel - 1}
}

// passthroughCore 混合器：
//   - Info 及以下 → 走 sampler（限流）
//   - Error 及以上 → 直通 base core（绝不丢关键日志）
type passthroughCore struct {
	zapcore.Core
	sampler  zapcore.Core
	minLevel zapcore.Level // <= 此级别走 sampler，> 直通
}

func (c *passthroughCore) With(fields []zapcore.Field) zapcore.Core {
	return &passthroughCore{
		Core:     c.Core.With(fields),
		sampler:  c.sampler.With(fields),
		minLevel: c.minLevel,
	}
}

func (c *passthroughCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if entry.Level > c.minLevel {
		// Error 及以上直通 base（不采样）
		if ce := c.Core.Check(entry, checked); ce != nil {
			return ce
		}
		return checked
	}
	return c.sampler.Check(entry, checked)
}

// newLoggerNoSampling 给测试用：禁用采样，保证每条日志都可断言
func newLoggerNoSampling(w io.Writer, level string) (*zap.Logger, error) {
	lv := zapcore.InfoLevel
	if level != "" {
		parsed, err := zapcore.ParseLevel(level)
		if err != nil {
			return nil, fmt.Errorf("无效日志级别 %q: %w", level, err)
		}
		lv = parsed
	}
	encCfg := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
	}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encCfg), zapcore.AddSync(w), lv)
	return zap.New(core), nil
}

// Init 初始化全局 logger
func Init(level string) error {
	l, err := New(level)
	if err != nil {
		return err
	}
	std = l
	return nil
}

// L 返回全局 logger
func L() *zap.Logger {
	return std
}

// With 在全局 logger 上附加字段，返回子 logger
func With(fields ...zap.Field) *zap.Logger {
	return std.With(fields...)
}

// Sync 刷新缓冲；stdout 在部分平台返回 ENOTTY，忽略即可
func Sync() {
	_ = std.Sync()
}

// Stage 启动阶段计时器
type Stage struct {
	name  string
	start time.Time
}

// BeginStage 开始一个启动阶段，配合 Done 输出 elapsed_ms
func BeginStage(name string) *Stage {
	return &Stage{name: name, start: time.Now()}
}

// Done 结束阶段并输出日志：stage=<name> msg=<msg> elapsed_ms=<n>
func (s *Stage) Done(msg string, fields ...zap.Field) {
	fields = append(fields,
		zap.String("stage", s.name),
		zap.Int64("elapsed_ms", time.Since(s.start).Milliseconds()),
	)
	std.Info(msg, fields...)
}
