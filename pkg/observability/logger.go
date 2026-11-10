// Package observability 可观测性基础封装
//
// 本阶段只实现结构化日志：JSON 输出到 stdout，一条一行，Loki 可直接采集。
// 启动阶段统一用 BeginStage / Done 打点，格式固定为
// ts / level / msg / stage / elapsed_ms，便于按阶段排查启动问题。
package observability

import (
	"fmt"
	"io"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// std 全局 logger；Init 之前为 Nop，避免 nil panic
var std = zap.NewNop()

// New 创建输出到 stdout 的 JSON logger；level 为空时默认 info
func New(level string) (*zap.Logger, error) {
	return newLogger(os.Stdout, level)
}

// newLogger 可指定输出目标，方便测试断言 JSON 结构
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
