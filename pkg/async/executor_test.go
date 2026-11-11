package async

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecutorGoRunsTask(t *testing.T) {
	var e Executor
	var ran atomic.Bool
	e.Go(func() { ran.Store(true) })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := e.Wait(ctx); err != nil {
		t.Fatalf("Wait 返回错误: %v", err)
	}
	if !ran.Load() {
		t.Error("任务未执行")
	}
}

func TestExecutorWaitTimeout(t *testing.T) {
	var e Executor
	release := make(chan struct{})
	defer close(release)
	e.Go(func() { <-release })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := e.Wait(ctx); err == nil {
		t.Fatal("任务未结束时 Wait 应返回超时错误")
	}
}

func TestExecutorPanicRecovered(t *testing.T) {
	var e Executor
	e.Go(func() { panic("boom") })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// panic 被兜底：Wait 正常返回，进程不崩
	if err := e.Wait(ctx); err != nil {
		t.Fatalf("Wait 返回错误: %v", err)
	}
}

func TestExecutorConcurrentTasks(t *testing.T) {
	var e Executor
	var done atomic.Int64
	for i := 0; i < 100; i++ {
		e.Go(func() { done.Add(1) })
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := e.Wait(ctx); err != nil {
		t.Fatalf("Wait 返回错误: %v", err)
	}
	if got := done.Load(); got != 100 {
		t.Errorf("完成任务数 = %d, want 100", got)
	}
}
