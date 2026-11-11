// Package creditlots 积分批次（credit_lots）扣减与过期清理。
//
// 设计（方案 B）：Redis 账本仍是余额的唯一权威；credit_lots 仅用于「批次剩余额度」
// 与「过期作废」展示。结算成功后按 SQL 从最早到期批次开始扣 remaining（A10），
// Reconciler 定时把已过期批次 remaining 清零（A9）。
package creditlots

import (
	"context"
	"database/sql"
	"fmt"

	"ai-gateway/pkg/ledger"
)

// DeductResult 批次扣减结果。
type DeductResult struct {
	Deducted ledger.MicroUSD
	Lots     int
}

// Deduct 从租户未过期批次中扣减 amount，顺序：
//
//	expires_at IS NULL ASC  → 永久额度排最后
//	expires_at ASC          → 先到期先扣（A10）
//	id ASC                  → 同到期时间按创建顺序
//
// 单条 UPDATE 带 remaining >= take 条件，天然并发安全；无需跨语句事务。
func Deduct(ctx context.Context, db *sql.DB, tenantID int64, amount ledger.MicroUSD) (DeductResult, error) {
	var out DeductResult
	if db == nil || amount <= 0 {
		return out, nil
	}

	rows, err := db.QueryContext(ctx, `
SELECT id, remaining FROM credit_lots
WHERE tenant_id = ?
  AND remaining > 0
  AND (expires_at IS NULL OR expires_at >= NOW(3))
ORDER BY expires_at IS NULL ASC, expires_at ASC, id ASC`, tenantID)
	if err != nil {
		return out, fmt.Errorf("creditlots: query lots: %w", err)
	}
	type lotRow struct {
		id        int64
		remaining ledger.MicroUSD
	}
	var lots []lotRow
	for rows.Next() {
		var (
			id        int64
			remaining int64
		)
		if err := rows.Scan(&id, &remaining); err != nil {
			rows.Close()
			return out, fmt.Errorf("creditlots: scan lot: %w", err)
		}
		lots = append(lots, lotRow{id: id, remaining: ledger.MicroUSD(remaining)})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return out, fmt.Errorf("creditlots: iterate lots: %w", err)
	}
	rows.Close()

	left := amount
	for _, lot := range lots {
		if left == 0 {
			break
		}
		take := lot.remaining
		if take > left {
			take = left
		}
		res, err := db.ExecContext(ctx, `
UPDATE credit_lots SET remaining = remaining - ?
WHERE id = ? AND tenant_id = ? AND remaining >= ?`, int64(take), lot.id, tenantID, int64(take))
		if err != nil {
			return out, fmt.Errorf("creditlots: deduct lot %d: %w", lot.id, err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			// 并发修改，跳过该批次，继续下一批
			continue
		}
		out.Deducted += take
		out.Lots++
		left -= take
	}
	return out, nil
}

// BurnExpired 将租户所有已过期批次的 remaining 清零，返回影响行数。
func BurnExpired(ctx context.Context, db *sql.DB, tenantID int64) (int64, error) {
	if db == nil {
		return 0, nil
	}
	res, err := db.ExecContext(ctx, `
UPDATE credit_lots SET remaining = 0
WHERE tenant_id = ?
  AND expires_at IS NOT NULL
  AND expires_at < NOW(3)
  AND remaining > 0`, tenantID)
	if err != nil {
		return 0, fmt.Errorf("creditlots: burn expired: %w", err)
	}
	return res.RowsAffected()
}

// BurnExpiredAll 清理所有租户的过期批次（供 Reconciler 使用），返回影响行数。
func BurnExpiredAll(ctx context.Context, db *sql.DB) (int64, error) {
	if db == nil {
		return 0, nil
	}
	res, err := db.ExecContext(ctx, `
UPDATE credit_lots SET remaining = 0
WHERE expires_at IS NOT NULL
  AND expires_at < NOW(3)
  AND remaining > 0`)
	if err != nil {
		return 0, fmt.Errorf("creditlots: burn expired all: %w", err)
	}
	return res.RowsAffected()
}
