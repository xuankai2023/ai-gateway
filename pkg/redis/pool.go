// Package redis Redis 连接池
//
// - 初始化时读 config.Redis 的 addr / password / db / pool_size / dial_timeout
// - 使用 github.com/redis/go-redis/v9
// - 提供 GetClient() 单例供限流 / 缓存 / 广播等组件共用
package redis

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// pingTimeout 启动时 Ping 的最长等待时间
const pingTimeout = 5 * time.Second

// defaultDialTimeout 未配置时的连接超时
const defaultDialTimeout = 3 * time.Second

var (
	mu     sync.RWMutex
	client *goredis.Client
)

// Options Redis 连接配置
type Options struct {
	Addr        string        // host:port，必填
	Password    string        // 可空
	DB          int           // 逻辑库编号，默认 0
	PoolSize    int           // 连接池大小，<=0 用 go-redis 默认（10*CPU）
	DialTimeout time.Duration // 连接超时，<=0 用 3s
}

// InitPool 创建连接池并 Ping 验证连通性
func InitPool(ctx context.Context, opts Options) (*goredis.Client, error) {
	if opts.Addr == "" {
		return nil, fmt.Errorf("redis.addr 不能为空")
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = defaultDialTimeout
	}

	cli := goredis.NewClient(&goredis.Options{
		Addr:        opts.Addr,
		Password:    opts.Password,
		DB:          opts.DB,
		PoolSize:    opts.PoolSize,
		DialTimeout: opts.DialTimeout,
	})

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := cli.Ping(pingCtx).Err(); err != nil {
		cli.Close()
		return nil, fmt.Errorf("redis 连接失败 (%s): %w", redactedAddr(opts.Addr), err)
	}

	mu.Lock()
	client = cli
	mu.Unlock()
	return cli, nil
}

// GetClient 返回全局客户端；未初始化时为 nil
func GetClient() *goredis.Client {
	mu.RLock()
	defer mu.RUnlock()
	return client
}

// Close 关闭全局连接池，可重复调用
func Close() {
	mu.Lock()
	defer mu.Unlock()
	if client != nil {
		client.Close()
		client = nil
	}
}

// redactedAddr 只保留 host:port，避免日志泄露可能的内嵌凭证
func redactedAddr(addr string) string {
	if i := strings.LastIndex(addr, "@"); i >= 0 {
		return addr[i+1:]
	}
	return addr
}
