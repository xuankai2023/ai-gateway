package observability

import (
	"bytes"
	"encoding/json"
	"testing"

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
