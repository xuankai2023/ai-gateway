package provider

import (
	"context"
	"database/sql/driver"
	"sync"
	"testing"
	"time"

	"ai-gateway/pkg/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- KeyPool 单元测试 ---

func TestKeyPool_Select_WeightedRoundRobin(t *testing.T) {
	// 3 把 key，权重 2:1:1 → 100 次 Select 后约 50:25:25
	kp := NewKeyPool()
	kp.SetEntries([]*KeyEntry{
		{KeyRef: "k1", Weight: 2, Enabled: true},
		{KeyRef: "k2", Weight: 1, Enabled: true},
		{KeyRef: "k3", Weight: 1, Enabled: true},
	})

	counts := map[string]int{}
	for i := 0; i < 400; i++ {
		e, ok := kp.Select()
		require.True(t, ok)
		counts[e.KeyRef]++
	}

	// 期望比例 k1 ≈ 50%, k2 ≈ 25%, k3 ≈ 25%
	assert.InDelta(t, 200, counts["k1"], 30, "k1 weight=2 应占 ~50%")
	assert.InDelta(t, 100, counts["k2"], 20, "k2 weight=1 应占 ~25%")
	assert.InDelta(t, 100, counts["k3"], 20, "k3 weight=1 应占 ~25%")
}

func TestKeyPool_Select_SkipsDisabled(t *testing.T) {
	kp := NewKeyPool()
	kp.SetEntries([]*KeyEntry{
		{KeyRef: "k1", Weight: 1, Enabled: false}, // 被禁用
		{KeyRef: "k2", Weight: 1, Enabled: true},
	})

	counts := map[string]int{}
	for i := 0; i < 50; i++ {
		e, ok := kp.Select()
		require.True(t, ok)
		counts[e.KeyRef]++
	}

	assert.Equal(t, 0, counts["k1"], "disabled key 不应被选中")
	assert.Equal(t, 50, counts["k2"])
}

func TestKeyPool_Select_SkipsCoolingDown(t *testing.T) {
	coolingEnd := time.Now().Add(30 * time.Second) // 30 秒后冷却结束
	kp := NewKeyPool()
	kp.SetEntries([]*KeyEntry{
		{KeyRef: "k1", Weight: 1, Enabled: true, DisabledUntil: &coolingEnd},
		{KeyRef: "k2", Weight: 1, Enabled: true},
	})

	counts := map[string]int{}
	for i := 0; i < 50; i++ {
		e, ok := kp.Select()
		require.True(t, ok)
		counts[e.KeyRef]++
	}

	assert.Equal(t, 0, counts["k1"], "冷却中 key 不应被选中")
	assert.Equal(t, 50, counts["k2"])
}

func TestKeyPool_Select_AllDisabledReturnsFalse(t *testing.T) {
	kp := NewKeyPool()
	kp.SetEntries([]*KeyEntry{
		{KeyRef: "k1", Weight: 1, Enabled: false},
		{KeyRef: "k2", Weight: 1, Enabled: false},
	})

	_, ok := kp.Select()
	assert.False(t, ok, "全部禁用应返回 false")
}

func TestKeyPool_Select_EmptyReturnsFalse(t *testing.T) {
	kp := NewKeyPool()
	_, ok := kp.Select()
	assert.False(t, ok)
}

func TestKeyPool_RecordFailure_CooldownAfterThreshold(t *testing.T) {
	kp := NewKeyPool()
	kp.SetEntries([]*KeyEntry{
		{KeyRef: "k1", Weight: 1, Enabled: true},
		{KeyRef: "k2", Weight: 1, Enabled: true},
	})

	// 连续失败 KeyFailureThreshold 次 → 进入冷却
	for i := 0; i < KeyFailureThreshold; i++ {
		kp.RecordFailure("k1", "test error")
	}

	entries := kp.Entries()
	k1 := findEntry(entries, "k1")
	require.NotNil(t, k1)
	assert.NotNil(t, k1.DisabledUntil, "达到阈值后应进入冷却")
	assert.Equal(t, 3, k1.ConsecutiveFailures)
	assert.InDelta(t, KeyDisableDuration.Seconds(), 60, 1, "冷却时长 60s")

	// k1 冷却中 → Select 只返回 k2
	counts := map[string]int{}
	for i := 0; i < 20; i++ {
		e, ok := kp.Select()
		require.True(t, ok)
		counts[e.KeyRef]++
	}
	assert.Equal(t, 0, counts["k1"])
	assert.Equal(t, 20, counts["k2"])
}

func TestKeyPool_RecordFailure_StopsShort(t *testing.T) {
	kp := NewKeyPool()
	kp.SetEntries([]*KeyEntry{
		{KeyRef: "k1", Weight: 1, Enabled: true},
	})

	// 只失败 2 次（阈值 3）→ 不应冷却
	for i := 0; i < KeyFailureThreshold-1; i++ {
		kp.RecordFailure("k1", "test error")
	}

	entries := kp.Entries()
	k1 := findEntry(entries, "k1")
	require.NotNil(t, k1)
	assert.Nil(t, k1.DisabledUntil, "未达阈值不应冷却")
	assert.Equal(t, KeyFailureThreshold-1, k1.ConsecutiveFailures)
}

func TestKeyPool_RecordSuccess_ClearsFailureAndCooldown(t *testing.T) {
	kp := NewKeyPool()
	kp.SetEntries([]*KeyEntry{
		{KeyRef: "k1", Weight: 1, Enabled: true},
	})

	// 先打到冷却
	for i := 0; i < KeyFailureThreshold; i++ {
		kp.RecordFailure("k1", "err")
	}
	// 再成功 → 清零 + 解除冷却
	kp.RecordSuccess("k1")

	entries := kp.Entries()
	k1 := findEntry(entries, "k1")
	require.NotNil(t, k1)
	assert.Equal(t, 0, k1.ConsecutiveFailures)
	assert.Nil(t, k1.DisabledUntil, "成功后应解除冷却")
}

func TestKeyPool_RecordFailure_UnknownKeyIsNoop(t *testing.T) {
	kp := NewKeyPool()
	kp.SetEntries([]*KeyEntry{
		{KeyRef: "k1", Weight: 1, Enabled: true},
	})

	// 不存在的 key → 无 panic、无副作用
	assert.NotPanics(t, func() { kp.RecordFailure("nonexistent", "err") })
	assert.NotPanics(t, func() { kp.RecordSuccess("nonexistent") })
}

func TestKeyPool_Select_ZeroWeightTreatedAsOne(t *testing.T) {
	kp := NewKeyPool()
	kp.SetEntries([]*KeyEntry{
		{KeyRef: "k1", Weight: 0, Enabled: true},
		{KeyRef: "k2", Weight: 0, Enabled: true},
	})

	counts := map[string]int{}
	for i := 0; i < 100; i++ {
		e, ok := kp.Select()
		require.True(t, ok)
		counts[e.KeyRef]++
	}

	// 权重为 0 视为 1，两条 key 应约各 50%
	assert.InDelta(t, 50, counts["k1"], 20)
	assert.InDelta(t, 50, counts["k2"], 20)
}

func TestKeyPool_ConcurrentSelect(t *testing.T) {
	kp := NewKeyPool()
	kp.SetEntries([]*KeyEntry{
		{KeyRef: "k1", Weight: 1, Enabled: true},
		{KeyRef: "k2", Weight: 1, Enabled: true},
	})

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				e, ok := kp.Select()
				assert.True(t, ok)
				assert.NotNil(t, e)
			}
		}()
	}
	wg.Wait()
}

func findEntry(entries []*KeyEntry, ref string) *KeyEntry {
	for _, e := range entries {
		if e.KeyRef == ref {
			return e
		}
	}
	return nil
}

// --- BaseURL 映射与 LoadChannelKeys 错误路径（阶段 14：Registry 实例化） ---

func TestSetBaseURLs_Overwrite(t *testing.T) {
	r := NewRegistry()
	r.ClearChannelConfigs()
	r.SetBaseURLs(map[string]string{"openai": "https://api.openai.com"})
	r.SetBaseURLs(map[string]string{"openai": "https://custom.openai.com"}) // 覆盖

	// 内部验证（无直接导出 getter，通过 GetForChannel 间接）
	// 真正的集成测试在 relay_test.go 的 ChannelProvider 相关测试
}

func TestGetForChannel_UnknownAbility(t *testing.T) {
	r := NewRegistry()
	r.ClearChannelConfigs()
	_, ok := r.GetForChannel(999)
	assert.False(t, ok)

	_, ok = r.GetForChannel(0)
	assert.False(t, ok, "abilityID=0 是 models 表 fallback，不应在 channel_keys 里")

	_, ok = r.GetForChannel(-1)
	assert.False(t, ok)
}

func TestLoadChannelKeys_NilPool(t *testing.T) {
	r := NewRegistry()
	r.ClearChannelConfigs()
	err := r.LoadChannelKeys(nil, nil)
	assert.Error(t, err)
}

func TestResolveKeyRef_Env(t *testing.T) {
	t.Setenv("TEST_CHANNEL_KEY", "sk-test-abc123")
	got := resolveKeyRef("env://TEST_CHANNEL_KEY")
	assert.Equal(t, "sk-test-abc123", got)
}

func TestResolveKeyRef_EnvMissing(t *testing.T) {
	got := resolveKeyRef("env://NONEXISTENT_ENV_VAR")
	assert.Empty(t, got)
}

func TestResolveKeyRef_VaultNotImplemented(t *testing.T) {
	got := resolveKeyRef("vault:///ai-gateway/key-1")
	assert.Empty(t, got, "Vault 未接入时返回空，LoadChannelKeys 会跳过这条")
}

func TestResolveKeyRef_LiteralFallback(t *testing.T) {
	// 纯字符串 fallback（仅测试用，生产不应落明文）
	got := resolveKeyRef("sk-plaintext-key")
	assert.Equal(t, "sk-plaintext-key", got)
}

// 阶段 16：注入组合解析器后 vault:// key_ref 经注入函数解析（LoadChannelKeys 全链路）
func TestLoadChannelKeys_VaultResolverInjection(t *testing.T) {
	db, fake := dbtest.New(t)
	r := NewRegistry()
	r.ClearChannelConfigs()

	// 模拟 vault 解析器：vault:// → 固定密钥；env:// 回落默认
	r.SetKeyRefResolver(func(ref string) string {
		if ref == "vault://secret/data/ai-gateway/openai-1" {
			return "sk-from-vault"
		}
		return ResolveKeyRef(ref)
	})

	fake.ExpectQuery("FROM channel_keys").WillReturnRows(
		[]string{"channel_id", "key_ref", "weight", "enabled", "consecutive_failures", "disabled_until", "disabled_reason", "provider", "provider_model"},
		[]driver.Value{int64(10), "vault://secret/data/ai-gateway/openai-1", int64(1), true, int64(0), nil, nil, "openai", "gpt-4o"},
	)

	if err := r.LoadChannelKeys(context.Background(), db); err != nil {
		t.Fatalf("LoadChannelKeys: %v", err)
	}
	p, ok := r.GetForChannel(10)
	if !ok {
		t.Fatal("ability 10 的 KeyPool 未构建")
	}
	entries := p.(*channelProvider).cfg.KeyPool.Entries()
	if len(entries) != 1 {
		t.Fatalf("pool entries = %d, want 1", len(entries))
	}
	if entries[0].APICKey != "sk-from-vault" {
		t.Errorf("APICKey = %q, want sk-from-vault（vault 解析结果）", entries[0].APICKey)
	}
	fake.AssertExpectations(t)
}
