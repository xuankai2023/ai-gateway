// Package async 统一异步执行器：收敛 fire-and-forget 后台任务，替代裸 go func
//
// 设计要点：
//   - 每个任务进 WaitGroup，优雅退出时 Wait 有界排空（关停不留裸奔 goroutine）
//   - 任务 panic 兜底恢复 + 日志，防后台任务打崩进程
package async

import (
	"context"
	"sync"

	"ai-gateway/pkg/observability"
	"go.uber.org/zap"
)

// Executor 异步任务执行器；零值可用，并发安全
type Executor struct {
	wg sync.WaitGroup
}

// Default 全局默认执行器（main 关停序列中 Wait 排空）
var Default = &Executor{}

// Go 注册并执行一个异步任务
func (e *Executor) Go(fn func()) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				observability.L().Error("async task panicked", zap.Any("panic", r))
			}
		}()
		fn()
	}()
}

// Wait 阻塞等待全部已注册任务完成；ctx 超时返回 ctx.Err()
func (e *Executor) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
