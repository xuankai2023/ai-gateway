// 磁盘 Outbox：usagelog 零丢失降级
//
// 触发点：
//  1. Add() 队列满 → 不丢弃，追加本地 WAL（JSON Lines）
//  2. insert() 批量写库失败 → 整批追加 WAL
//
// 后台 Flush 周期读取 WAL，批量 INSERT ... ON DUPLICATE KEY UPDATE
// （request_id 有唯一键，幂等防重），全部成功后截断 WAL。
// 适用场景：压测打满队列 / DB 短暂不可用时的用量日志零丢失。
package usagelog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"ai-gateway/pkg/observability"
	"go.uber.org/zap"
)

// DiskOutbox 本地 WAL outbox（单文件 JSON Lines，进程级互斥写）
type DiskOutbox struct {
	dir  string
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
}

// NewDiskOutbox 打开（或创建）WAL 文件；dir 不存在自动创建（0700）
func NewDiskOutbox(dir string) (*DiskOutbox, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("outbox mkdir: %w", err)
	}
	f, err := os.OpenFile(walPath(dir), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("outbox open: %w", err)
	}
	return &DiskOutbox{dir: dir, file: f, enc: json.NewEncoder(f)}, nil
}

func walPath(dir string) string { return filepath.Join(dir, "usagelog_outbox.wal") }

// Append 追加一条 entry 到 WAL（JSON Lines；fsync 留给 OS flush，进程崩溃不丢已 write 数据）
func (o *DiskOutbox) Append(e Entry) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file == nil {
		return fmt.Errorf("outbox closed")
	}
	return o.enc.Encode(e)
}

// AppendBatch 批量追加（insert 失败时整批降级）
func (o *DiskOutbox) AppendBatch(batch []Entry) error {
	for _, e := range batch {
		if err := o.Append(e); err != nil {
			return err
		}
	}
	return nil
}

// Size WAL 当前字节数（监控 / 测试用）
func (o *DiskOutbox) Size() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file == nil {
		return 0
	}
	fi, err := o.file.Stat()
	if err != nil {
		return 0
	}
	return fi.Size()
}

// Close 关闭 WAL 文件句柄
func (o *DiskOutbox) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file == nil {
		return nil
	}
	err := o.file.Close()
	o.file = nil
	return err
}

// Flush 读取 WAL 全部 entry，幂等批量写入 DB，全部成功后截断 WAL。
// 返回 flush 成功的条数；任一批失败时保留 WAL（含未写成功的行），下次 Flush 重试。
//
// 幂等：usage_logs.request_id 有唯一键，用 INSERT ... ON DUPLICATE KEY UPDATE
// request_id=request_id（no-op 更新）实现"恰好一次"语义。
func (o *DiskOutbox) Flush(ctx context.Context, w *Writer) (flushed int, err error) {
	entries, err := o.readAll()
	if err != nil {
		return 0, err
	}
	if len(entries) == 0 {
		return 0, nil
	}

	// 分批写入（复用 BatchSize 口径）
	batchSize := w.opts.BatchSize
	for i := 0; i < len(entries); i += batchSize {
		end := i + batchSize
		if end > len(entries) {
			end = len(entries)
		}
		if err := w.insertIdempotent(ctx, entries[i:end]); err != nil {
			return flushed, fmt.Errorf("outbox flush batch failed at %d/%d: %w", i, len(entries), err)
		}
		flushed += end - i
	}

	// 全部成功 → 截断 WAL（先关闭再以 O_TRUNC 重开）
	if err := o.truncate(); err != nil {
		return flushed, fmt.Errorf("outbox truncate: %w", err)
	}
	observability.L().Info("usagelog outbox: flushed", zap.Int("count", flushed))
	return flushed, nil
}

// readAll 读取 WAL 全部行（坏行跳过不阻塞整体 flush）
func (o *DiskOutbox) readAll() ([]Entry, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file == nil {
		return nil, nil
	}
	if err := o.file.Sync(); err != nil {
		return nil, err
	}

	data, err := os.ReadFile(walPath(o.dir))
	if err != nil {
		return nil, err
	}
	var entries []Entry
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var e Entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			observability.L().Warn("usagelog outbox: skip corrupt line", zap.Error(err))
			continue
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// truncate 截断 WAL（调用方已确认全部 flush 成功）
func (o *DiskOutbox) truncate() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file == nil {
		return nil
	}
	if err := o.file.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(walPath(o.dir), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	o.file = f
	o.enc = json.NewEncoder(f)
	return nil
}

// insertIdempotent 幂等批量写入（ON DUPLICATE KEY UPDATE request_id=request_id 去重）
func (w *Writer) insertIdempotent(ctx context.Context, batch []Entry) error {
	if w.db == nil || len(batch) == 0 {
		return nil
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
	// 幂等：重复 request_id 时 no-op（保留首条写入，防 outbox 重试产生重复行）
	sb.WriteString(" ON DUPLICATE KEY UPDATE request_id = request_id")

	ctx, cancel := context.WithTimeout(ctx, w.opts.ExecTimeout)
	defer cancel()
	_, err := w.db.ExecContext(ctx, sb.String(), args...)
	return err
}
