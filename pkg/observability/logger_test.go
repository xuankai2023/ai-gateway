package observability

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// swapStd 用 observer 替换全局 logger，测试结束后恢复
func swapStd(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.InfoLevel)
	old := std
	std = zap.New(core)
	t.Cleanup(func() { std = old })
	return logs
}

func TestNewLoggerOutputsJSON(t *testing.T) {
	var buf bytes.Buffer
	l, err := newLogger(&buf, "info")
	if err != nil {
		t.Fatalf("newLogger 失败: %v", err)
	}

	l.Info("loaded", zap.String("stage", "config"), zap.Int64("elapsed_ms", 3))

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("输出不是合法 JSON: %v, raw=%q", err, buf.String())
	}
	if got := line["level"]; got != "info" {
		t.Errorf("level = %v, want info", got)
	}
	if got := line["msg"]; got != "loaded" {
		t.Errorf("msg = %v, want loaded", got)
	}
	if got := line["stage"]; got != "config" {
		t.Errorf("stage = %v, want config", got)
	}
	if _, ok := line["ts"]; !ok {
		t.Errorf("缺少 ts 字段: %v", line)
	}
	if got := line["elapsed_ms"]; got != float64(3) {
		t.Errorf("elapsed_ms = %v, want 3", got)
	}
	if bytes.Count(buf.Bytes(), []byte("\n")) != 1 {
		t.Errorf("期望一条一行，实际输出: %q", buf.String())
	}
}

func TestNewLoggerRejectsInvalidLevel(t *testing.T) {
	if _, err := newLogger(&bytes.Buffer{}, "verbose"); err == nil {
		t.Fatal("期望无效日志级别报错，实际为 nil")
	}
}

func TestWithAddsFields(t *testing.T) {
	logs := swapStd(t)

	With(zap.String("trace_id", "abc123")).Info("request_start")

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("期望 1 条日志，实际 %d 条", len(entries))
	}
	if got := entries[0].ContextMap()["trace_id"]; got != "abc123" {
		t.Errorf("trace_id = %v, want abc123", got)
	}
}

func TestStageLogsNameAndElapsed(t *testing.T) {
	logs := swapStd(t)

	stage := BeginStage("mysql")
	stage.Done("db connected")

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("期望 1 条日志，实际 %d 条", len(entries))
	}
	entry := entries[0]
	if entry.Message != "db connected" {
		t.Errorf("msg = %q, want db connected", entry.Message)
	}
	fields := entry.ContextMap()
	if got := fields["stage"]; got != "mysql" {
		t.Errorf("stage = %v, want mysql", got)
	}
	elapsed, ok := fields["elapsed_ms"].(int64)
	if !ok {
		t.Fatalf("elapsed_ms 类型错误: %T", fields["elapsed_ms"])
	}
	if elapsed < 0 {
		t.Errorf("elapsed_ms 不应为负: %d", elapsed)
	}
}

// --- 阶段 7：采样测试 ---

func TestPassthroughCore_InfoGoesThroughSampler(t *testing.T) {
	// 用 observer 做 sampler 的下游，看 passthroughCore 对 Info 和 Error 的行为
	baseCore, logs := observer.New(zapcore.InfoLevel)
	pc := &passthroughCore{Core: baseCore, sampler: baseCore, minLevel: zapcore.ErrorLevel - 1}

	// Info 级别 → Check 应该委托给 sampler（这里用同一个 baseCore 所以都会放行）
	infoEntry := zapcore.Entry{Level: zapcore.InfoLevel, Message: "info-msg"}
	ce := pc.Check(infoEntry, nil)
	if ce == nil {
		t.Error("Info 级别应该能通过 passthroughCore")
	}

	// Error 级别 → Check 应该直通 baseCore（不走 sampler）
	errEntry := zapcore.Entry{Level: zapcore.ErrorLevel, Message: "err-msg"}
	ce2 := pc.Check(errEntry, nil)
	if ce2 == nil {
		t.Error("Error 级别应该能通过 passthroughCore")
	}

	_ = logs
}

func TestPassthroughCore_ErrorSkipsSampler(t *testing.T) {
	// 构造一个 "拒绝一切" 的 mock sampler，验证 Error 级别完全绕过它
	baseCore, _ := observer.New(zapcore.InfoLevel)
	rejectingCore := &rejectCore{}
	pc := &passthroughCore{Core: baseCore, sampler: rejectingCore, minLevel: zapcore.ErrorLevel - 1}

	// Info 走 rejectingCore → 应该被拒绝（Check 返回 nil）
	infoEntry := zapcore.Entry{Level: zapcore.InfoLevel, Message: "info-msg"}
	ce := pc.Check(infoEntry, nil)
	if ce != nil {
		t.Error("Info 级别应被 rejecting sampler 拒绝（Check 返回 nil）")
	}

	// Debug 走 rejectingCore → 也应该被拒绝
	debugEntry := zapcore.Entry{Level: zapcore.DebugLevel, Message: "debug-msg"}
	ceDebug := pc.Check(debugEntry, nil)
	if ceDebug != nil {
		t.Error("Debug 级别应被 rejecting sampler 拒绝（Check 返回 nil）")
	}

	// Error / DPanic / Panic / Fatal → 全部直通 baseCore，不走 sampler
	for _, lvl := range []zapcore.Level{zapcore.ErrorLevel, zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel} {
		e := zapcore.Entry{Level: lvl, Message: "msg"}
		ce := pc.Check(e, nil)
		if ce == nil {
			t.Errorf("Level %v 应直通 baseCore，不应被 sampler 拒绝", lvl)
		}
	}
}

// rejectCore 一个 mock zapcore.Core：Check 永远返回 nil（拒绝一切）
type rejectCore struct{}

func (c *rejectCore) Enabled(zapcore.Level) bool    { return true }
func (c *rejectCore) With([]zap.Field) zapcore.Core { return c }
func (c *rejectCore) Check(_ zapcore.Entry, _ *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return nil // 拒绝
}
func (c *rejectCore) Write(zapcore.Entry, []zap.Field) error { return nil }
func (c *rejectCore) Sync() error                            { return nil }

func TestNewLoggerNoSampling_Disabled(t *testing.T) {
	var buf bytes.Buffer
	l, err := newLoggerNoSampling(&buf, "info")
	if err != nil {
		t.Fatal(err)
	}

	// 连续写 200 条 Info → 全部应被保留（无采样）
	for i := 0; i < 200; i++ {
		l.Info("msg")
	}
	lines := bytes.Count(buf.Bytes(), []byte("\n"))
	if lines != 200 {
		t.Errorf("newLoggerNoSampling 期望 200 条全部保留，实际 %d 条", lines)
	}
}

// --- 阶段 7：TTFT 埋点 ---

func TestRecordTTFT(t *testing.T) {
	// RecordTTFT 直接增加 Histogram 计数；无法在测试里直接读 Prometheus registry，
	// 但可以验证它不 panic、ttftMs 非法值（负）也不 panic
	assert.NotPanics(t, func() { RecordTTFT("openai", 350) })
	assert.NotPanics(t, func() { RecordTTFT("openai", 0) })
	assert.NotPanics(t, func() { RecordTTFT("openai", -1) })
	assert.NotPanics(t, func() { RecordTTFT("", 100) })
}
