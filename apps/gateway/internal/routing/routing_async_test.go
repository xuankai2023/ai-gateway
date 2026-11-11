package routing

import (
	"context"
	"testing"
	"time"

	"ai-gateway/pkg/async"
	"ai-gateway/pkg/dbtest"
)

// TestAsyncDisableChannelGoesThroughExecutor 渠道禁用任务注册到 Router 的异步执行器，
// 优雅退出时可通过 exec.Wait 排空（关停不留裸奔 goroutine）
func TestAsyncDisableChannelGoesThroughExecutor(t *testing.T) {
	db, fake := dbtest.New(t)
	exec := &async.Executor{}
	r := NewRouter(db, nil, nil, exec)
	t.Cleanup(r.Close)

	fake.ExpectExec("UPDATE abilities SET enabled = 0").WillReturnResult(0, 1)

	r.AsyncDisableChannel(123, "test reason")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := exec.Wait(ctx); err != nil {
		t.Fatalf("Wait 返回错误: %v（异步任务未被排空）", err)
	}
	fake.AssertExpectations(t)
}

// TestUpdateAbilityAsyncNilPool DB 未初始化时不投递任务（不产生 goroutine）
func TestUpdateAbilityAsyncNilPool(t *testing.T) {
	exec := &async.Executor{}
	r := NewRouter(nil, nil, nil, exec)
	t.Cleanup(r.Close)

	r.AsyncRecordSuccess(456)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := exec.Wait(ctx); err != nil {
		t.Fatalf("Wait 应立即返回: %v", err)
	}
}
