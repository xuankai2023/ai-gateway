package db

import (
	"context"
	"strings"
	"testing"
)

func TestInitPoolInvalidDSN(t *testing.T) {
	// MySQL driver 对空 DSN 会报 network error，用明显非法的触发
	_, err := InitPool(context.Background(), "invalid", 0)
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
}

func TestInitPoolUnreachable(t *testing.T) {
	// 指向一个不存在的端口，期望 5s 内报错
	dsn := "user:secret@tcp(127.0.0.1:1)/ai_gateway?charset=utf8mb4&parseTime=True"

	_, err := InitPool(context.Background(), dsn, 2)
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "数据库连接失败") {
		t.Errorf("错误信息应包含连接失败: %v", err)
	}
	if !strings.Contains(msg, "127.0.0.1:1") {
		t.Errorf("错误信息应包含脱敏后的目标地址: %v", err)
	}
	if strings.Contains(msg, "secret") {
		t.Errorf("错误信息不应泄露密码: %v", err)
	}
}

func TestRedactedDSN(t *testing.T) {
	tests := []struct {
		dsn  string
		want string
	}{
		{"user:pass@tcp(localhost:3306)/ai_gateway", "localhost:3306/ai_gateway"},
		{"root:pw@tcp(10.0.0.5:3307)/testdb?charset=utf8", "10.0.0.5:3307/testdb"},
		{"no-tcp-format", "no-tcp-format"}, // fallback
	}
	for _, tt := range tests {
		got := redactedDSN(tt.dsn)
		if got != tt.want {
			t.Errorf("redactedDSN(%q) = %q, want %q", tt.dsn, got, tt.want)
		}
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	Close()
	Close()
	if GetPool() != nil {
		t.Error("Close 后 GetPool 应为 nil")
	}
}
