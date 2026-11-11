// 磁盘 Outbox 测试（usagelog 零丢失降级）
package usagelog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"ai-gateway/pkg/dbtest"
)

// outboxEntry 测试用 entry
func outboxEntry(i int) Entry {
	return Entry{
		RequestID: fmt.Sprintf("req-%d", i), TenantID: 1, APIKeyID: 2,
		Model: "gpt-4o", Provider: "openai", InputTokens: 10, OutputTokens: 20,
		Status: "success", LatencyMs: 100,
	}
}

// TestOutbox_AddDegradesWhenFull 队列满时降级写 WAL（返回 true，不丢弃）
func TestOutbox_AddDegradesWhenFull(t *testing.T) {
	db, _ := dbtest.New(t)
	dir := t.TempDir()

	// 队列容量 1，flush 周期拉长：第一条进队列，后续全部满
	w := New(db, Options{
		QueueSize: 1, BatchSize: 1000,
		FlushInterval: time.Hour, OutboxDir: dir,
	})
	defer func() { _ = w.Close(context.Background()) }()

	if !w.Add(outboxEntry(0)) {
		t.Fatal("第一条应进队列")
	}
	// 队列已满，接下来的 10 条应全部降级到 WAL
	for i := 1; i <= 10; i++ {
		if !w.Add(outboxEntry(i)) {
			t.Fatalf("第 %d 条应降级 WAL 返回 true", i)
		}
	}
	if size := w.outbox.Size(); size <= 0 {
		t.Fatalf("WAL 应有内容, size = %d", size)
	}
}

// TestOutbox_InsertFailDegrades insert 失败整批降级 WAL
func TestOutbox_InsertFailDegrades(t *testing.T) {
	db, fake := dbtest.New(t)
	fake.ExpectExec("INSERT INTO usage_logs").WillReturnError(errors.New("db down"))

	dir := t.TempDir()
	w := New(db, Options{OutboxDir: dir})
	// 直接调用 insert（绕过异步时序）
	w.insert([]Entry{outboxEntry(1), outboxEntry(2), outboxEntry(3)})
	fake.AssertExpectations(t)

	if w.outbox.Size() <= 0 {
		t.Fatal("insert 失败应降级 WAL")
	}
	// 3 行 JSON Lines
	data := readAll(t, w.outbox)
	if len(data) != 3 {
		t.Errorf("WAL 应有 3 条, got %d", len(data))
	}
}

// TestOutbox_FlushReplaysAndTruncates DB 恢复后 Flush 成功并截断 WAL，SQL 含幂等子句
func TestOutbox_FlushReplaysAndTruncates(t *testing.T) {
	db, fake := dbtest.New(t)
	dir := t.TempDir()
	w := New(db, Options{OutboxDir: dir})

	// 写入 3 条到 WAL
	for i := 0; i < 3; i++ {
		if err := w.outbox.Append(outboxEntry(i)); err != nil {
			t.Fatal(err)
		}
	}

	// DB 恢复：期望 1 次幂等批量 INSERT
	fake.ExpectExec("ON DUPLICATE KEY UPDATE").WillReturnResult(0, 3)
	n, err := w.outbox.Flush(context.Background(), w)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n != 3 {
		t.Errorf("flushed = %d, want 3", n)
	}
	fake.AssertExpectations(t)

	// SQL 必须含幂等子句
	calls := fake.Calls()
	if len(calls) != 1 || !strings.Contains(calls[0].SQL, "ON DUPLICATE KEY UPDATE") {
		t.Errorf("flush SQL 缺幂等子句: %v", calls)
	}

	// WAL 已截断
	if size := w.outbox.Size(); size != 0 {
		t.Errorf("flush 成功后 WAL 应截断, size = %d", size)
	}
}

// TestOutbox_FlushFailureKeepsWAL flush 失败保留 WAL 供下次重试
func TestOutbox_FlushFailureKeepsWAL(t *testing.T) {
	db, fake := dbtest.New(t)
	dir := t.TempDir()
	w := New(db, Options{OutboxDir: dir})

	_ = w.outbox.Append(outboxEntry(1))
	_ = w.outbox.Append(outboxEntry(2))

	fake.ExpectExec("INSERT INTO usage_logs").WillReturnError(errors.New("still down"))
	n, err := w.outbox.Flush(context.Background(), w)
	if err == nil {
		t.Fatal("DB 不可用应返回错误")
	}
	if n != 0 {
		t.Errorf("失败时 flushed = %d, want 0", n)
	}
	if w.outbox.Size() <= 0 {
		t.Error("flush 失败应保留 WAL")
	}
}

// TestOutbox_ZeroLoss1000 DB 不可用期间 1000 条全部进 WAL；恢复后恰好 flush 1000 条且无重复
//
// 时序设计（确定性）：
//  1. 不给 fake 任何 expectation → 所有 INSERT 失败 → run loop 每批都降级 WAL
//  2. 轮询等 WAL 攒满 1000 条（队列 + 内存批次最终都会落入 WAL）
//  3. Close 后 DB 恢复（补 expectation）→ Flush 恰好 1000 条，request_id 无重复
func TestOutbox_ZeroLoss1000(t *testing.T) {
	db, fake := dbtest.New(t)
	dir := t.TempDir()
	w := New(db, Options{
		QueueSize: 100, BatchSize: 200,
		FlushInterval: 10 * time.Millisecond, OutboxDir: dir,
	})

	// 1. 投递 1000 条（DB 全灭：无 expectation → ExecContext 报错 → 降级 WAL）
	for i := 0; i < 1000; i++ {
		if !w.Add(outboxEntry(i)) {
			t.Fatalf("第 %d 条丢失", i)
		}
	}

	// 2. 等 WAL 攒满 1000（race 模式下 fake 驱动慢，超时放宽到 60s）
	deadline := time.Now().Add(60 * time.Second)
	for {
		if n := len(readAll(t, w.outbox)); n >= 1000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("WAL 未攒满 1000: got %d", len(readAll(t, w.outbox)))
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 3. Close（最后 flush 仍失败 → 保留 WAL）
	_ = w.Close(context.Background())

	// 4. DB 恢复：1000 条 / BatchSize 200 = 5 批幂等 INSERT
	//    先快照 Calls（宕机期的失败 flush 尝试也含幂等子句，需排除）
	callsBefore := len(fake.Calls())
	for i := 0; i < 5; i++ {
		fake.ExpectExec("ON DUPLICATE KEY UPDATE").WillReturnResult(0, 200)
	}
	// 重新打开 outbox 句柄（Close 已关闭）做恢复 flush
	ob, err := NewDiskOutbox(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ob.Close()

	w2 := New(db, Options{BatchSize: 200}) // 借用 w2 的 db / opts 执行 insertIdempotent
	defer func() { _ = w2.Close(context.Background()) }()
	n, err := ob.Flush(context.Background(), w2)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n != 1000 {
		t.Fatalf("flushed = %d, want 1000", n)
	}
	fake.AssertExpectations(t)

	// 5. 恰好 1000 条且 request_id 无重复（只统计恢复后的新调用）
	seen := map[string]bool{}
	total := 0
	flushCalls := 0
	for _, call := range fake.Calls()[callsBefore:] {
		if !strings.Contains(call.SQL, "ON DUPLICATE KEY UPDATE") {
			continue
		}
		flushCalls++
		// 每行 18 个参数，第一个是 request_id
		for i := 0; i+17 < len(call.Args); i += 18 {
			rid, _ := call.Args[i].Value.(string)
			if seen[rid] {
				t.Errorf("重复 request_id: %s", rid)
			}
			seen[rid] = true
			total++
		}
	}
	if flushCalls != 5 {
		t.Errorf("幂等 flush 调用数 = %d, want 5", flushCalls)
	}
	if total != 1000 {
		t.Errorf("总行数 = %d, want 1000", total)
	}
	if len(seen) != 1000 {
		t.Errorf("唯一 request_id = %d, want 1000", len(seen))
	}

	// 6. WAL 截断
	if ob.Size() != 0 {
		t.Errorf("flush 后 WAL 应截断, size = %d", ob.Size())
	}
}

// TestOutbox_NoOutboxKeepsDropBehavior OutboxDir 为空时保持原丢弃行为
func TestOutbox_NoOutboxKeepsDropBehavior(t *testing.T) {
	db, _ := dbtest.New(t)
	w := New(db, Options{QueueSize: 1, FlushInterval: time.Hour})
	defer func() { _ = w.Close(context.Background()) }()

	_ = w.Add(outboxEntry(0))
	if w.Add(outboxEntry(1)) {
		t.Error("无 outbox 时队列满应丢弃（返回 false）")
	}
	if w.outbox != nil {
		t.Error("OutboxDir 为空不应创建 outbox")
	}
}

// TestOutbox_BadDir 初始化失败降级为丢弃策略（不 panic）
func TestOutbox_BadDir(t *testing.T) {
	db, _ := dbtest.New(t)
	// /dev/null/x 不可创建目录
	w := New(db, Options{QueueSize: 1, FlushInterval: time.Hour, OutboxDir: "/dev/null/x"})
	defer func() { _ = w.Close(context.Background()) }()

	_ = w.Add(outboxEntry(0))
	if w.Add(outboxEntry(1)) {
		t.Error("outbox 初始化失败应回落丢弃")
	}
}

// readAll 测试辅助：读 WAL 全部 entry
func readAll(t *testing.T, o *DiskOutbox) []Entry {
	t.Helper()
	entries, err := o.readAll()
	if err != nil {
		t.Fatalf("readAll: %v", err)
	}
	return entries
}
