package usagelog

import (
	"context"
	"strings"
	"testing"
	"time"

	"ai-gateway/pkg/dbtest"
	"ai-gateway/pkg/observability"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func testEntry(i int) Entry {
	return Entry{
		RequestID:    "req-" + strings.Repeat("0", 3) + string(rune('a'+i)),
		TenantID:     1,
		APIKeyID:     2,
		Model:        "test-model",
		Provider:     "openai",
		InputTokens:  10,
		OutputTokens: 20,
		Cost:         1000,
		PriceVersion: 1,
		LatencyMs:    100,
		Status:       "success",
	}
}

// TestWriterCloseDrainsQueue 优雅退出零丢失：Close 拒绝新投递后排空并写完最后一批
func TestWriterCloseDrainsQueue(t *testing.T) {
	db, fake := dbtest.New(t)
	fake.ExpectExec("INSERT INTO usage_logs").WillReturnResult(0, 3)

	// FlushInterval 拉到 1h：不依赖定时 flush，全部靠 Close 排空
	w := New(db, Options{BatchSize: 200, FlushInterval: time.Hour})
	for i := 0; i < 3; i++ {
		if !w.Add(testEntry(i)) {
			t.Fatalf("Add #%d 应成功", i)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close 返回错误: %v", err)
	}

	calls := fake.Calls()
	if len(calls) != 1 {
		t.Fatalf("exec 次数 = %d, want 1（3 条合并为一批）", len(calls))
	}
	// 3 行 × 18 列 = 54 个参数；VALUES 占位符组数 = 3
	if got, want := len(calls[0].Args), 54; got != want {
		t.Errorf("批量参数数 = %d, want %d", got, want)
	}
	if got, want := strings.Count(calls[0].SQL, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"), 3; got != want {
		t.Errorf("VALUES 组数 = %d, want %d, sql=%s", got, want, calls[0].SQL)
	}
	fake.AssertExpectations(t)
}

// TestWriterFlushOnBatchSize 攒满 BatchSize 立即 flush，不等定时器
func TestWriterFlushOnBatchSize(t *testing.T) {
	db, fake := dbtest.New(t)
	fake.ExpectExec("INSERT INTO usage_logs").WillReturnResult(0, 2)

	w := New(db, Options{BatchSize: 2, FlushInterval: time.Hour})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = w.Close(ctx)
	}()

	w.Add(testEntry(0))
	w.Add(testEntry(1))

	waitForCalls(t, fake, 1)
	fake.AssertExpectations(t)
}

// TestWriterFlushOnInterval 不足一批时按 FlushInterval 定时 flush
func TestWriterFlushOnInterval(t *testing.T) {
	db, fake := dbtest.New(t)
	fake.ExpectExec("INSERT INTO usage_logs").WillReturnResult(0, 1)

	w := New(db, Options{BatchSize: 100, FlushInterval: 20 * time.Millisecond})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = w.Close(ctx)
	}()

	w.Add(testEntry(0))

	waitForCalls(t, fake, 1)
	fake.AssertExpectations(t)
}

// TestWriterDropsWhenQueueFull 队列满：丢弃并计 gateway_usagelog_dropped_total（宁丢不堵）
func TestWriterDropsWhenQueueFull(t *testing.T) {
	before := testutil.ToFloat64(observability.GatewayUsageLogDroppedTotal)

	// 白盒构造：不启动后台 goroutine，队列必然打满
	w := &Writer{
		opts: Options{QueueSize: 1, BatchSize: 100, FlushInterval: time.Hour, ExecTimeout: time.Second},
		ch:   make(chan Entry, 1),
		done: make(chan struct{}),
	}
	if !w.Add(testEntry(0)) {
		t.Fatal("第一条应入队成功")
	}
	if w.Add(testEntry(1)) {
		t.Fatal("队列满时第二条应被丢弃")
	}

	after := testutil.ToFloat64(observability.GatewayUsageLogDroppedTotal)
	if after-before != 1 {
		t.Errorf("dropped 指标增量 = %v, want 1", after-before)
	}
}

// TestWriterAddAfterClose Close 后拒绝新投递
func TestWriterAddAfterClose(t *testing.T) {
	w := New(nil, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close 返回错误: %v", err)
	}
	if w.Add(testEntry(0)) {
		t.Fatal("Close 后 Add 应返回 false")
	}
	// 幂等 Close
	if err := w.Close(ctx); err != nil {
		t.Fatalf("重复 Close 应返回 nil: %v", err)
	}
}

// waitForCalls 轮询等待异步写库调用发生（flush 在后台 goroutine 执行）
func waitForCalls(t *testing.T, fake *dbtest.FakeDB, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(fake.Calls()) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待 %d 次写库调用超时，实际 %d 次", want, len(fake.Calls()))
}
