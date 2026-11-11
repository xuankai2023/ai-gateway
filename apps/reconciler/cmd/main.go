// Command main 账本对账器（reconciler）
//
// 职责：
//  1. 把过期 credit_lots 的 remaining 清零（A9）
//  2. 每小时扫 reserved > 0 且 reserved_at 超过 1h 的租户，调用 ledger.Refund
//     退回预占，并在 ledger_entries 写一条 kind='refund' 的流水
//
// 设计：纯 CLI 工具，不常驻（可用 k8s CronJob / systemd timer 调度）。
// 运行：./reconciler --config configs/config.yaml
// 退出码：0 = 正常完成；1 = 依赖初始化失败
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"ai-gateway/pkg/config"
	"ai-gateway/pkg/creditlots"
	"ai-gateway/pkg/db"
	"ai-gateway/pkg/ledger"
	"ai-gateway/pkg/redis"
)

// staleThreshold 预占超过 1 小时视为僵尸
const staleThreshold = 1 * time.Hour

// scanBatch 单次扫描租户数（LIMIT）
const scanBatch = 200

// reconcileTimeout 整次对账的硬时限
const reconcileTimeout = 5 * time.Minute

func main() {
	if err := run(); err != nil {
		log.Fatalf("reconciler failed: %v", err)
	}
}

func run() error {
	configPath := flag.String("config", config.DefaultPath, "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), reconcileTimeout)
	defer cancel()

	// 1. 初始化 MySQL
	pool, err := db.InitPool(ctx, cfg.MySQL.DSN, cfg.MySQL.PoolSize)
	if err != nil {
		return fmt.Errorf("init mysql: %w", err)
	}
	defer db.Close()

	// 2. 初始化 Redis
	redisDialTimeout, _ := time.ParseDuration(cfg.Redis.DialTimeout)
	if cfg.Redis.DialTimeout == "" {
		redisDialTimeout = 0
	}
	rcli, err := redis.InitPool(ctx, redis.Options{
		Addr:        cfg.Redis.Addr,
		Password:    cfg.Redis.Password,
		DB:          cfg.Redis.DB,
		PoolSize:    cfg.Redis.PoolSize,
		DialTimeout: redisDialTimeout,
	})
	if err != nil {
		return fmt.Errorf("init redis: %w", err)
	}
	defer redis.Close()

	// 3. 过期积分批次清零（不触碰 Redis 余额，A9）
	if n, err := creditlots.BurnExpiredAll(ctx, pool); err != nil {
		log.Printf("WARN: burn expired credit lots failed: %v", err)
	} else if n > 0 {
		log.Printf("burned expired credit lots: rows=%d", n)
	}

	// 4. 扫描 + 冲正
	log.Printf("reconciler start: scanning stale reserved (>%s)", staleThreshold)

	var (
		totalRefunded int
		totalAmount   ledger.MicroUSD
	)

	for {
		ids, reservedMap, err := scanStaleTenants(ctx, pool, staleThreshold, scanBatch)
		if err != nil {
			return fmt.Errorf("scan stale tenants: %w", err)
		}
		if len(ids) == 0 {
			break
		}

		for _, tid := range ids {
			amount := reservedMap[tid]
			if amount <= 0 {
				continue
			}

			if err := ledger.Refund(ctx, rcli, tid, amount); err != nil {
				log.Printf("WARN: refund tenant=%d failed: %v", tid, err)
				continue
			}

			if err := insertRefundEntry(ctx, pool, tid, amount); err != nil {
				log.Printf("WARN: insert refund entry tenant=%d failed: %v", tid, err)
				// 不阻塞继续处理下一个租户
			}

			totalRefunded++
			totalAmount += amount
			log.Printf("refunded tenant=%d amount=%s USD (%d μUSD)", tid, amount, int64(amount))
		}

		// 本批处理完，继续扫下一批（同一批刚 refund 完的 reserved 应已为 0，不会重复）
		if len(ids) < scanBatch {
			break
		}
	}

	log.Printf("reconciler done: refunded=%d amount=%s USD (%d μUSD)", totalRefunded, totalAmount, int64(totalAmount))
	return nil
}

// scanStaleTenants 返回 reserved > 0 且超过 threshold 未动的租户列表（limit batch 条）
//
// reserved_at 存在 Redis 里（不在 MySQL），所以这里需要在租户表里查出所有 id，
// 再逐个 HGET Redis。由于 reserved_at 只写在 HASH 字段，没有 Redis server-side 扫描，
// 因此扫描粒度由 scanBatch 控制。
func scanStaleTenants(ctx context.Context, pool *sql.DB, threshold time.Duration, batch int) ([]int64, map[int64]ledger.MicroUSD, error) {
	rcli := redis.GetClient()
	if rcli == nil {
		return nil, nil, fmt.Errorf("redis client not initialized")
	}

	rows, err := pool.QueryContext(ctx,
		"SELECT id FROM tenants ORDER BY id ASC LIMIT ?", batch,
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var (
		resultIDs []int64
		resMap    = make(map[int64]ledger.MicroUSD)
		cutoff    = time.Now().Add(-threshold)
	)

	for rows.Next() {
		var tid int64
		if err := rows.Scan(&tid); err != nil {
			return nil, nil, err
		}

		h, err := rcli.HGetAll(ctx, ledger.TenantKey(tid)).Result()
		if err != nil {
			log.Printf("WARN: HGETALL tenant=%d: %v", tid, err)
			continue
		}

		reserved, err := ledger.ParseMicroUSD(h["reserved"])
		if err != nil || reserved <= 0 {
			continue
		}

		// reserved_at 兼容 RFC3339Nano（v2）与 unix_nano（旧数据）
		atStr, ok := h["reserved_at"]
		if !ok {
			// 有 reserved 但没 reserved_at —— 可能是旧数据，保守起见也冲正
			resultIDs = append(resultIDs, tid)
			resMap[tid] = reserved
			continue
		}
		at, parseErr := time.Parse(time.RFC3339Nano, atStr)
		if parseErr != nil {
			if atNano, nanoErr := strconv.ParseInt(atStr, 10, 64); nanoErr == nil {
				at = time.Unix(0, atNano)
			} else {
				// 无法解析的时间戳：保守冲正，避免预占永久悬挂
				at = time.Time{}
			}
		}
		if at.Before(cutoff) {
			resultIDs = append(resultIDs, tid)
			resMap[tid] = reserved
		}
	}
	return resultIDs, resMap, rows.Err()
}

// insertRefundEntry 写一条 refund 流水到 ledger_entries
func insertRefundEntry(ctx context.Context, pool *sql.DB, tenantID int64, amount ledger.MicroUSD) error {
	const q = `
		INSERT INTO ledger_entries (request_id, tenant_id, delta, kind)
		VALUES (?, ?, ?, 'refund')
	`
	// refund 的 delta 是正数（把 reserved 退回到 balance 方向）
	_, err := pool.ExecContext(ctx, q,
		fmt.Sprintf("reconciler-%d", time.Now().UnixNano()),
		tenantID,
		int64(amount),
	)
	return err
}
