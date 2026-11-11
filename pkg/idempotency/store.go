// Package idempotency 幂等键 Redis 存储（阶段 11）
//
// Key 结构（均带租户前缀，天然租户隔离）：
//
//	idem:{tenant}:{key}        占位键（SETNX，处理中标记）
//	idem:resp:{tenant}:{key}   响应快照（首个请求成功后写入）
//
// 语义：
//   - Acquire 成功 = 首个请求，处理中；失败 = 已有同 key 请求
//   - 并发占位期间无快照 → 上层返回 409 idempotency_conflict
//   - 首请求成功 → Save 快照；重复 key → 回放快照 + Idempotent-Replay
//   - 首请求失败 → Release 占位，允许客户端重试
//
// 占位键与快照同为 24h TTL，与生产加固计划口径一致。
package idempotency

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// 存储 TTL（计划约定 24h）
const (
	PlaceholderTTL = 24 * time.Hour
	SnapshotTTL    = 24 * time.Hour
)

// Snapshot 一次成功响应的完整快照
type Snapshot struct {
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	Body        []byte `json:"body"`
}

// Store 幂等键存储
type Store struct {
	client *goredis.Client
}

// New 创建存储；client 为 nil 时所有操作返回错误（调用方 fail-open/503 自行决策）
func New(client *goredis.Client) *Store { return &Store{client: client} }

// placeholderKey 占位键
func placeholderKey(tenantID int64, key string) string {
	return fmt.Sprintf("idem:%d:%s", tenantID, key)
}

// snapshotKey 快照键
func snapshotKey(tenantID int64, key string) string {
	return fmt.Sprintf("idem:resp:%d:%s", tenantID, key)
}

// Acquire 抢占幂等键；acquired=true 表示本请求是首个处理者
func (s *Store) Acquire(ctx context.Context, tenantID int64, key string) (bool, error) {
	if s.client == nil {
		return false, fmt.Errorf("idempotency Acquire: client is nil")
	}
	return s.client.SetNX(ctx, placeholderKey(tenantID, key), "1", PlaceholderTTL).Result()
}

// Snapshot 读取响应快照；不存在返回 (nil, false, nil)
func (s *Store) Snapshot(ctx context.Context, tenantID int64, key string) (*Snapshot, bool, error) {
	if s.client == nil {
		return nil, false, fmt.Errorf("idempotency Snapshot: client is nil")
	}
	raw, err := s.client.Get(ctx, snapshotKey(tenantID, key)).Bytes()
	if err == goredis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("idempotency Snapshot get failed: %w", err)
	}
	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return nil, false, fmt.Errorf("idempotency Snapshot decode failed: %w", err)
	}
	return &snap, true, nil
}

// Save 写入响应快照（首个请求成功后）
func (s *Store) Save(ctx context.Context, tenantID int64, key string, snap *Snapshot) error {
	if s.client == nil {
		return fmt.Errorf("idempotency Save: client is nil")
	}
	raw, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("idempotency Save encode failed: %w", err)
	}
	return s.client.Set(ctx, snapshotKey(tenantID, key), raw, SnapshotTTL).Err()
}

// Release 释放占位键（首个请求失败时允许重试）
func (s *Store) Release(ctx context.Context, tenantID int64, key string) error {
	if s.client == nil {
		return fmt.Errorf("idempotency Release: client is nil")
	}
	return s.client.Del(ctx, placeholderKey(tenantID, key)).Err()
}
