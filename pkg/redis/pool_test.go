package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestInitPoolPingAndClose(t *testing.T) {
	s := miniredis.RunT(t)

	cli, err := InitPool(context.Background(), Options{Addr: s.Addr(), DB: 1})
	if err != nil {
		t.Fatalf("InitPool 失败: %v", err)
	}
	if cli == nil {
		t.Fatal("返回 client 为 nil")
	}
	if GetClient() != cli {
		t.Error("GetClient 应返回刚初始化的实例")
	}

	Close()
	if GetClient() != nil {
		t.Error("Close 后 GetClient 应为 nil")
	}
	// 重复 Close 不应 panic
	Close()
}

func TestInitPoolUnreachable(t *testing.T) {
	s := miniredis.RunT(t)
	addr := s.Addr()
	s.Close() // 立即关停，Ping 必失败

	_, err := InitPool(context.Background(), Options{Addr: addr, DialTimeout: 1})
	if err == nil {
		t.Fatal("不可达地址应报错")
	}
	if !strings.Contains(err.Error(), "redis 连接失败") {
		t.Errorf("错误信息不符合预期: %v", err)
	}
	if GetClient() != nil {
		t.Error("失败后全局 client 应保持 nil")
	}
}

func TestInitPoolEmptyAddr(t *testing.T) {
	if _, err := InitPool(context.Background(), Options{}); err == nil {
		t.Fatal("空 addr 应报错")
	}
}

func TestRedactedAddr(t *testing.T) {
	if got := redactedAddr("user:secret@127.0.0.1:6379"); got != "127.0.0.1:6379" {
		t.Errorf("redactedAddr = %q, want 127.0.0.1:6379", got)
	}
	if got := redactedAddr("127.0.0.1:6379"); got != "127.0.0.1:6379" {
		t.Errorf("redactedAddr = %q, want 原样保留", got)
	}
}
