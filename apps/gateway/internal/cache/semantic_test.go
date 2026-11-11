package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// 指纹规范化：同输入稳定，模型/消息任一变化即不同（保证精确层不误命中）
func TestFingerprintStableAndSensitive(t *testing.T) {
	msgs := []Message{{Role: "user", Content: "hello"}}
	a := Fingerprint("m1", msgs)
	b := Fingerprint("m1", msgs)
	if a != b {
		t.Fatalf("同输入指纹不一致: %s vs %s", a, b)
	}
	if a == Fingerprint("m2", msgs) {
		t.Error("模型不同指纹应不同")
	}
	if a == Fingerprint("m1", []Message{{Role: "user", Content: "hello!"}}) {
		t.Error("消息不同指纹应不同")
	}
	if len(a) != 64 {
		t.Errorf("sha256 hex 长度 = %d, want 64", len(a))
	}
}

// cacheKeyIsolation 相同 prompt 在 tenant / project / epoch 任一不同时不得命中
func TestExactKeyIsolation(t *testing.T) {
	fp := Fingerprint("m1", []Message{{Role: "user", Content: "磁盘告警"}})
	base := exactKey(1, 2, 3, fp)
	for name, key := range map[string]string{
		"tenant":  exactKey(9, 2, 3, fp),
		"project": exactKey(1, 9, 3, fp),
		"epoch":   exactKey(1, 2, 9, fp),
	} {
		if key == base {
			t.Errorf("%s 变化后 key 仍相同: %s", name, key)
		}
	}
	if got, want := collection(1, 2), "semcache_1_2"; got != want {
		t.Errorf("collection = %q, want %q", got, want)
	}
}

// Store/Lookup：project 隔离 + epoch 变化后旧缓存失效
func TestCacheProjectAndEpochIsolation(t *testing.T) {
	s := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { cli.Close() })
	inst := New(Options{Redis: cli, TTL: time.Hour})
	ctx := context.Background()
	msgs := []Message{{Role: "user", Content: "你好"}}
	entry := &Entry{Body: []byte(`{"ok":true}`), ContentType: "application/json", ExpiresAt: time.Now().Add(time.Hour).Unix()}

	if err := inst.Store(ctx, 1, 2, 1, "m", msgs, entry); err != nil {
		t.Fatalf("Store: %v", err)
	}

	hit, layer, err := inst.Lookup(ctx, 1, 2, 1, "m", msgs)
	if err != nil || hit == nil || layer != LayerExact {
		t.Fatalf("同 tenant/project/epoch 应命中: hit=%v layer=%q err=%v", hit, layer, err)
	}

	if hit, _, _ := inst.Lookup(ctx, 1, 3, 1, "m", msgs); hit != nil {
		t.Error("不同 project 不应命中（缓存串项目）")
	}
	if hit, _, _ := inst.Lookup(ctx, 1, 2, 2, "m", msgs); hit != nil {
		t.Error("epoch 变化（知识库更新）后旧缓存应失效")
	}
	if hit, _, _ := inst.Lookup(ctx, 2, 2, 1, "m", msgs); hit != nil {
		t.Error("不同 tenant 不应命中")
	}
}
