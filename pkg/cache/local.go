// Package cache 进程内 TTL 缓存（热数据本地缓存）
//
// 设计要点：
//   - sync.Map 存储 + 后台 goroutine 周期清扫过期条目
//   - 泛型值类型，每个命名空间一个实例（auth / abilities / model）
//   - 容量软上限：写满后写入时驱逐一个任意条目，防内存膨胀
//   - 命中/未命中原子计数（Stats），阶段 7 由 observability 周期采集并暴露为
//     gateway_cache_hits_total / gateway_cache_misses_total{namespace} 指标
//
// 注意：
//   - 缓存条目的值类型若为切片/ map，调用方不得原地修改返回值（多请求共享同一引用）
//   - InvalidatePrefix("") 清空整个命名空间（渠道健康变更后全量失效用）
package cache

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// defaultSweepInterval 默认过期清扫周期
const defaultSweepInterval = 10 * time.Second

// Options 缓存实例配置
type Options struct {
	// Name 命名空间名（stats 标签 / 日志用），如 "auth" / "abilities" / "model"
	Name string
	// MaxEntries 容量软上限（<=0 表示不限）；写满后 Set 时驱逐任意条目
	MaxEntries int
	// DefaultTTL Set 未显式指定 TTL 时使用的过期时间（<=0 表示永不过期）
	DefaultTTL time.Duration
	// SweepInterval 过期清扫周期（<=0 用默认 10s）
	SweepInterval time.Duration
}

// Stats 缓存运行统计（阶段 7 由 observability 周期采集）
type Stats struct {
	Name   string
	Hits   int64
	Misses int64
	Size   int64
}

type entry[T any] struct {
	value     T
	expiresAt time.Time // 零值 = 永不过期
}

func (e entry[T]) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// Cache 泛型 TTL 缓存实例
type Cache[T any] struct {
	name    string
	max     int
	defTTL  time.Duration
	items   sync.Map // string -> entry[T]
	size    atomic.Int64
	hits    atomic.Int64
	misses  atomic.Int64
	stopCh  chan struct{}
	stopped atomic.Bool
}

// New 创建缓存实例并启动过期清扫 goroutine；不再使用时调用 Close 停掉清扫
func New[T any](opts Options) *Cache[T] {
	sweep := opts.SweepInterval
	if sweep <= 0 {
		sweep = defaultSweepInterval
	}
	c := &Cache[T]{
		name:   opts.Name,
		max:    opts.MaxEntries,
		defTTL: opts.DefaultTTL,
		stopCh: make(chan struct{}),
	}
	go c.sweeper(sweep)
	return c
}

// Get 查询缓存；未命中或已过期返回 ok=false（过期条目惰性删除）
func (c *Cache[T]) Get(key string) (T, bool) {
	var zero T
	v, ok := c.items.Load(key)
	if !ok {
		c.misses.Add(1)
		return zero, false
	}
	e := v.(entry[T])
	if e.expired(time.Now()) {
		c.deleteKey(key)
		c.misses.Add(1)
		return zero, false
	}
	c.hits.Add(1)
	return e.value, true
}

// Set 以 DefaultTTL 写入
func (c *Cache[T]) Set(key string, value T) {
	c.SetTTL(key, value, c.defTTL)
}

// SetTTL 以指定 TTL 写入；ttl<=0 表示永不过期
func (c *Cache[T]) SetTTL(key string, value T, ttl time.Duration) {
	e := entry[T]{value: value}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	if _, loaded := c.items.LoadOrStore(key, e); loaded {
		c.items.Store(key, e) // 覆盖已有 key，size 不变
		return
	}
	c.size.Add(1)
	c.evictIfFull()
}

// evictIfFull 超出容量软上限时驱逐任意条目（sync.Map 遍历顺序随机）
func (c *Cache[T]) evictIfFull() {
	if c.max <= 0 {
		return
	}
	for c.size.Load() > int64(c.max) {
		evicted := false
		c.items.Range(func(k, _ interface{}) bool {
			evicted = c.deleteKey(k.(string))
			return false // 驱逐一个即停
		})
		if !evicted {
			return // map 已空但计数未收敛（并发瞬态），放弃本次驱逐
		}
	}
}

// Invalidate 删除指定 key
func (c *Cache[T]) Invalidate(key string) {
	c.deleteKey(key)
}

// InvalidatePrefix 删除所有以 prefix 开头的 key，返回删除条数；prefix 为 "" 时清空整个命名空间
func (c *Cache[T]) InvalidatePrefix(prefix string) int {
	var removed int
	c.items.Range(func(k, _ interface{}) bool {
		if strings.HasPrefix(k.(string), prefix) && c.deleteKey(k.(string)) {
			removed++
		}
		return true
	})
	return removed
}

// deleteKey 条件删除：只有 key 真实存在时才删除并扣减计数，保证并发下 size 不为负
func (c *Cache[T]) deleteKey(key string) bool {
	if _, loaded := c.items.LoadAndDelete(key); loaded {
		c.size.Add(-1)
		return true
	}
	return false
}

// Stats 返回命中/未命中/当前条数快照
func (c *Cache[T]) Stats() Stats {
	return Stats{
		Name:   c.name,
		Hits:   c.hits.Load(),
		Misses: c.misses.Load(),
		Size:   c.size.Load(),
	}
}

// Close 停止清扫 goroutine，可重复调用
func (c *Cache[T]) Close() {
	if c.stopped.CompareAndSwap(false, true) {
		close(c.stopCh)
	}
}

// sweeper 周期清理过期条目
func (c *Cache[T]) sweeper(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case now := <-ticker.C:
			c.items.Range(func(k, v interface{}) bool {
				if v.(entry[T]).expired(now) {
					c.deleteKey(k.(string))
				}
				return true
			})
		}
	}
}
