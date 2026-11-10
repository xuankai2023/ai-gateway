// Package db 数据库连接池（MySQL）
//
// - 初始化时读 config.MySQL 的 dsn / pool_size
// - 使用标准库 database/sql + go-sql-driver/mysql
// - 提供 GetPool() 单例供业务代码 / 健康检查共用
package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// pingTimeout 启动时 Ping 的最长等待时间
const pingTimeout = 5 * time.Second

var (
	mu   sync.RWMutex
	pool *sql.DB
)

// InitPool 创建连接池并 Ping 验证连通性
// dsn 格式：user:password@tcp(host:port)/dbname?charset=utf8mb4&parseTime=True&loc=Local
func InitPool(ctx context.Context, dsn string, poolSize int) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("创建数据库连接池失败: %w", err)
	}

	if poolSize > 0 {
		db.SetMaxOpenConns(poolSize)
		db.SetMaxIdleConns(poolSize / 2)
		db.SetConnMaxLifetime(time.Hour)
	}

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("数据库连接失败 (%s): %w", redactedDSN(dsn), err)
	}

	mu.Lock()
	pool = db
	mu.Unlock()
	return db, nil
}

// GetPool 返回全局连接池；未初始化时为 nil
func GetPool() *sql.DB {
	mu.RLock()
	defer mu.RUnlock()
	return pool
}

// Close 关闭全局连接池，可重复调用
func Close() {
	mu.Lock()
	defer mu.Unlock()
	if pool != nil {
		pool.Close()
		pool = nil
	}
}

// redactedDSN 只保留 host:port/db，避免日志泄露密码
func redactedDSN(dsn string) string {
	// user:password@tcp(host:port)/dbname → host:port/dbname
	at := -1
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == '@' {
			at = i
			break
		}
	}
	if at < 0 {
		return dsn
	}
	rest := dsn[at+1:] // tcp(host:port)/dbname?...
	// 去掉 tcp( 前缀
	if len(rest) > 4 && rest[:4] == "tcp(" {
		rest = rest[4:]
	}
	// 去掉 ) 和后续 query
	if idx := indexByte(rest, ')'); idx >= 0 {
		rest = rest[:idx] + rest[idx+1:]
	}
	// 去掉 query
	if idx := indexByte(rest, '?'); idx >= 0 {
		rest = rest[:idx]
	}
	return rest
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
