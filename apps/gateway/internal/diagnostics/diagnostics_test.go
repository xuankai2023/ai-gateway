package diagnostics

import (
	"context"
	"strings"
	"testing"
	"time"

	"ai-gateway/pkg/async"
	"ai-gateway/pkg/dbtest"
	"ai-gateway/pkg/observability"
)

// newTestRecorder 构造 recorder 并返回 (recorder, fake, executor)
func newTestRecorder(t *testing.T, sampleRate float64) (*Recorder, *dbtest.FakeDB, *async.Executor) {
	t.Helper()
	db, fake := dbtest.New(t)
	exec := &async.Executor{}
	return New(db, exec, sampleRate), fake, exec
}

// waitExec 等待异步任务完成（测试同步点）
func waitExec(t *testing.T, exec *async.Executor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := exec.Wait(ctx); err != nil {
		t.Fatalf("async wait: %v", err)
	}
}

// TestShouldSample_ErrorAlways 错误请求 100% 采样
func TestShouldSample_ErrorAlways(t *testing.T) {
	r := New(nil, nil, 0.01) // db/async 为 nil 不影呐采样判定
	for i := 0; i < 100; i++ {
		if !r.shouldSample(&Record{StatusCode: 502}) {
			t.Fatal("5xx 必须 100% 采样")
		}
		if !r.shouldSample(&Record{StatusCode: 200, ErrorType: "rate_limit"}) {
			t.Fatal("有 error_type 必须 100% 采样")
		}
	}
}

// TestShouldSample_RateZero 采样率 0% 时正常请求全部跳过
func TestShouldSample_RateZero(t *testing.T) {
	r := New(nil, nil, 0.01)
	r.sampleRate = 0 // 直接覆盖内部字段：0% 采样
	for i := 0; i < 100; i++ {
		if r.shouldSample(&Record{StatusCode: 200}) {
			t.Fatal("0% 采样率下正常请求应全部跳过")
		}
	}
}

// TestShouldSample_RateHundred 采样率 100% 时正常请求全部命中
func TestShouldSample_RateHundred(t *testing.T) {
	r := New(nil, nil, 0.01)
	r.sampleRate = 1.0
	for i := 0; i < 100; i++ {
		if !r.shouldSample(&Record{StatusCode: 200}) {
			t.Fatal("100% 采样率下正常请求应全部命中")
		}
	}
}

// TestRecord_NilDeps 依赖为 nil 时静默跳过（不 panic、不写库）
func TestRecord_NilDeps(t *testing.T) {
	r := New(nil, nil, 1.0)
	r.Record(Record{TraceID: "t1", StatusCode: 500, ErrorType: "server"}) // 不应 panic
}

// TestRecord_NotSampled 未命中采样不写库
func TestRecord_NotSampled(t *testing.T) {
	r, fake, exec := newTestRecorder(t, 0.01)
	r.sampleRate = 0 // 0% 采样
	r.Record(Record{TraceID: "t-skip", StatusCode: 200})
	waitExec(t, exec)
	for _, call := range fake.Calls() {
		t.Errorf("未命中采样不应有 DB 调用: %s", call.SQL)
	}
}

// TestRecord_ErrorInserts 错误请求命中采样 → 异步 INSERT，attempts 脱敏
func TestRecord_ErrorInserts(t *testing.T) {
	r, fake, exec := newTestRecorder(t, 0.01)
	fake.ExpectExec("INSERT INTO request_diagnostics").WillReturnResult(1, 1)

	r.Record(Record{
		TraceID:       "trace-err",
		TenantID:      42,
		Model:         "gpt-4o",
		Stream:        true,
		RequestDigest: "len=100 sha256=abcd",
		Attempts: []Attempt{
			{Attempt: 1, ChannelID: 101, Provider: "openai", BaseURL: "https://api.openai.com",
				Status: "failed", ErrorKind: "rate_limit",
				ErrorMessage: "429 quota: Authorization: Bearer sk-leakedkey123", DurationMs: 120,
				BreakerStateBefore: "closed"},
			{Attempt: 2, ChannelID: 102, Provider: "anthropic", BaseURL: "https://api.anthropic.com",
				Status: "success", DurationMs: 890, BreakerStateBefore: "closed"},
		},
		StatusCode:   502,
		ErrorType:    "rate_limit",
		ErrorMessage: "all attempts exhausted: Authorization: Bearer sk-leakedkey123",
		TotalCost:    0,
		DurationMs:   1010,
	})
	waitExec(t, exec)
	fake.AssertExpectations(t)

	calls := fake.Calls()
	if len(calls) != 1 {
		t.Fatalf("期望 1 次 INSERT, got %d", len(calls))
	}
	args := calls[0].Args
	// args[5] = attempts JSON; args[8] = error_message
	attemptsJSON, _ := args[5].Value.(string)
	if strings.Contains(attemptsJSON, "sk-leakedkey123") {
		t.Errorf("attempts error_message 未脱敏: %s", attemptsJSON)
	}
	if !strings.Contains(attemptsJSON, observability.Redacted) {
		t.Errorf("attempts 应含脱敏占位符: %s", attemptsJSON)
	}
	errMsg, _ := args[8].Value.(string)
	if strings.Contains(errMsg, "sk-leakedkey123") {
		t.Errorf("顶层 error_message 未脱敏: %s", errMsg)
	}
	if !strings.Contains(errMsg, observability.Redacted) {
		t.Errorf("顶层 error_message 应含脱敏占位符: %s", errMsg)
	}
}

// TestRecord_NormalSampled 采样率 100% 时正常请求也落库（空 error_type / error_message 写 NULL）
func TestRecord_NormalSampled(t *testing.T) {
	r, fake, exec := newTestRecorder(t, 1.0)
	fake.ExpectExec("INSERT INTO request_diagnostics").WillReturnResult(1, 1)

	r.Record(Record{
		TraceID: "trace-ok", TenantID: 1, Model: "gpt-4o-mini",
		Attempts: []Attempt{
			{Attempt: 1, ChannelID: 7, Provider: "openai", Status: "success",
				DurationMs: 200, BreakerStateBefore: "closed"},
		},
		StatusCode: 200, DurationMs: 210,
	})
	waitExec(t, exec)
	fake.AssertExpectations(t)

	args := fake.Calls()[0].Args
	// error_type / error_message 应为 nil（SQL NULL）
	if args[7].Value != nil {
		t.Errorf("error_type 应为 NULL, got %v", args[7].Value)
	}
	if args[8].Value != nil {
		t.Errorf("error_message 应为 NULL, got %v", args[8].Value)
	}
}

// TestRecord_SampleRateStatistical 1% 采样率的统计验证（10000 次期望 60~160 命中）
func TestRecord_SampleRateStatistical(t *testing.T) {
	r := New(nil, nil, 0.01)
	hits := 0
	const n = 10000
	for i := 0; i < n; i++ {
		if r.shouldSample(&Record{StatusCode: 200}) {
			hits++
		}
	}
	if hits < 60 || hits > 160 {
		t.Errorf("1%% 采样率 10000 次命中 %d，期望 60~160", hits)
	}
}

// TestNew_DefaultSampleRate 采样率 <= 0 回落默认值
func TestNew_DefaultSampleRate(t *testing.T) {
	if r := New(nil, nil, 0); r.SampleRate() != DefaultSampleRate {
		t.Errorf("默认采样率 = %v, want %v", r.SampleRate(), DefaultSampleRate)
	}
	if r := New(nil, nil, -1); r.SampleRate() != DefaultSampleRate {
		t.Errorf("负采样率应回落默认 = %v", r.SampleRate())
	}
}
