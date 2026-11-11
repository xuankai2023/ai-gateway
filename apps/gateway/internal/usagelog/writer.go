// Package usagelog 用量日志异步批量写入器
//
// 响应路径只向内存队列投递（非阻塞），后台 goroutine 批量写 MySQL：
// 攒满 BatchSize 条或每 FlushInterval 触发一次多行 INSERT。
//
// 丢弃策略：队列满时丢弃并计 gateway_usagelog_dropped_total（宁丢不堵响应路径）。
// 优雅退出：Close 拒绝新投递后排空队列、写完最后一批才返回（关停零丢失）。
package usagelog

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"

	"ai-gateway/pkg/ledger"
	"ai-gateway/pkg/observability"
	"go.uber.org/zap"
)

// 默认参数（main 使用，与生产加固计划口径一致）
const (
	DefaultQueueSize     = 10000           // 队列容量
	DefaultBatchSize     = 200             // 单批条数
	DefaultFlushInterval = time.Second     // 定时 flush 周期
	DefaultExecTimeout   = 5 * time.Second // 单批写库超时
)

// insertColumns usage_logs 写入列（批量 INSERT 共用）
// reserved_cost / final_cost 由 007_ledger migration 扩展，project/assistant/retrieval 由 013 扩展；
// 未跑 migration 时 INSERT 会失败（writer 丢本批，fail-open）
const insertColumns = `(request_id, tenant_id, api_key_id, project_id, assistant_id, retrieved_count, retrieval_ms, model, provider, input_tokens, output_tokens, cost, reserved_cost, final_cost, price_version, latency_ms, status, error_message)`

// Entry 一条用量日志（usage_logs 表的一行）
type Entry struct {
	RequestID    string
	TenantID     int64
	APIKeyID     int64
	ProjectID    int64 // 项目维度（0 写 NULL）
	AssistantID  int64 // 助手维度（0 写 NULL）
	Model        string
	Provider     string
	InputTokens  int64
	OutputTokens int64
	Cost         ledger.MicroUSD  // 本次成本（μUSD）
	ReservedCost *ledger.MicroUSD // 账本预占押金（μUSD；nil 写 NULL）
	FinalCost    *ledger.MicroUSD // 账本最终结算成本（μUSD；nil 写 NULL）
	PriceVersion int              // 定价版本快照（A8）
	LatencyMs    int64
	Status       string
	ErrorMessage string
	// RAG 检索维度
	RetrievedCount int
	RetrievalMs    int
}

// Options 写入器参数；零值字段回落默认
type Options struct {
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
	ExecTimeout   time.Duration

	// OutboxDir 磁盘 outbox 目录；非空时启用零丢失降级：
	//   - Add 队列满 → 追加 WAL（不丢弃）
	//   - insert 批量失败 → 整批追加 WAL
	//   - 后台每个 FlushInterval 尝试 outbox.Flush（幂等防重）
	// 为空时保持原行为（满丢弃 + 失败丢弃）
	OutboxDir string
}

func (o *Options) defaults() {
	if o.QueueSize <= 0 {
		o.QueueSize = DefaultQueueSize
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBatchSize
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = DefaultFlushInterval
	}
	if o.ExecTimeout <= 0 {
		o.ExecTimeout = DefaultExecTimeout
	}
}

// Writer 异步批量写入器；New 即启动后台 goroutine，Close 前可并发 Add
type Writer struct {
	db   *sql.DB
	opts Options
	ch   chan Entry
	done chan struct{}

	// outbox 磁盘降级（opts.OutboxDir 非空时启用）；nil 保持原丢弃策略
	outbox *DiskOutbox

	// Add 持 RLock 投递、Close 持 Lock 标记关闭：
	// Close 拿到写锁即保证没有在途投递，排空是确定性的
	mu     sync.RWMutex
	closed bool
	runWg  sync.WaitGroup
}

// New 创建写入器并启动后台批量 flush goroutine
func New(db *sql.DB, opts Options) *Writer {
	opts.defaults()
	w := &Writer{
		db:   db,
		opts: opts,
		ch:   make(chan Entry, opts.QueueSize),
		done: make(chan struct{}),
	}
	if opts.OutboxDir != "" {
		ob, err := NewDiskOutbox(opts.OutboxDir)
		if err != nil {
			// outbox 初始化失败降级为丢弃策略（不阻塞启动）
			observability.L().Error("usagelog: outbox init failed, fallback to drop-on-full",
				zap.String("dir", opts.OutboxDir), zap.Error(err))
		} else {
			w.outbox = ob
		}
	}
	w.runWg.Add(1)
	go w.run()
	return w
}

// Add 非阻塞投递一条日志
//
// 队列满：有 outbox 时降级写 WAL（返回 true，零丢失），无 outbox 时丢弃并计
// gateway_usagelog_dropped_total（宁丢不堵响应路径）；已关闭返回 false
func (w *Writer) Add(e Entry) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return false
	}
	select {
	case w.ch <- e:
		return true
	default:
		if w.outbox != nil {
			if err := w.outbox.Append(e); err == nil {
				return true
			}
			// WAL 写失败回落丢弃（不堵响应路径）
		}
		observability.RecordUsageLogDropped()
		return false
	}
}

// Close 停止接收新投递，排空队列并写完最后一批后返回；ctx 超时返回错误
func (w *Writer) Close(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()

	close(w.done)
	finished := make(chan struct{})
	go func() {
		w.runWg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run 后台批量 flush 循环；收到 done 后排空队列做最终 flush
func (w *Writer) run() {
	defer w.runWg.Done()
	ticker := time.NewTicker(w.opts.FlushInterval)
	defer ticker.Stop()

	batch := make([]Entry, 0, w.opts.BatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		w.insert(batch)
		batch = batch[:0]
	}

	for {
		select {
		case e := <-w.ch:
			batch = append(batch, e)
			if len(batch) >= w.opts.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
			// outbox 周期 flush：DB 恢复后自动补写（幂等防重）
			if w.outbox != nil && w.outbox.Size() > 0 {
				if n, err := w.outbox.Flush(context.Background(), w); err != nil {
					observability.L().Warn("usagelog: outbox flush failed, will retry",
						zap.Error(err))
				} else if n > 0 {
					observability.L().Info("usagelog: outbox replayed", zap.Int("count", n))
				}
			}
		case <-w.done:
			for {
				select {
				case e := <-w.ch:
					batch = append(batch, e)
					if len(batch) >= w.opts.BatchSize {
						flush()
					}
				default:
					flush()
					// 关停兜底：把 outbox 中残留尽力 flush 后关闭句柄
					if w.outbox != nil {
						ctx, cancel := context.WithTimeout(context.Background(), w.opts.ExecTimeout)
						_, _ = w.outbox.Flush(ctx, w)
						cancel()
						_ = w.outbox.Close()
					}
					return
				}
			}
		}
	}
}

// insert 一批日志一次多行 INSERT；失败时：有 outbox 整批降级 WAL（零丢失），无 outbox 丢弃
func (w *Writer) insert(batch []Entry) {
	if w.db == nil || len(batch) == 0 {
		return
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO usage_logs ")
	sb.WriteString(insertColumns)
	sb.WriteString(" VALUES ")
	args := make([]interface{}, 0, len(batch)*18)
	for i := range batch {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		e := &batch[i]
		var reserved, final any
		if e.ReservedCost != nil {
			reserved = int64(*e.ReservedCost)
		}
		if e.FinalCost != nil {
			final = int64(*e.FinalCost)
		}
		var projectID, assistantID any
		if e.ProjectID > 0 {
			projectID = e.ProjectID
		}
		if e.AssistantID > 0 {
			assistantID = e.AssistantID
		}
		args = append(args, e.RequestID, e.TenantID, e.APIKeyID, projectID, assistantID,
			e.RetrievedCount, e.RetrievalMs, e.Model, e.Provider,
			e.InputTokens, e.OutputTokens, int64(e.Cost), reserved, final,
			e.PriceVersion, e.LatencyMs, e.Status, e.ErrorMessage)
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.opts.ExecTimeout)
	defer cancel()
	if _, err := w.db.ExecContext(ctx, sb.String(), args...); err != nil {
		if w.outbox != nil {
			if appendErr := w.outbox.AppendBatch(batch); appendErr == nil {
				observability.L().Warn("usagelog: batch insert failed, degraded to outbox",
					zap.Int("batch", len(batch)), zap.Error(err))
				return
			}
		}
		observability.L().Error("usagelog: batch insert failed, entries lost",
			zap.Int("batch", len(batch)),
			zap.Error(err),
		)
	}
}
