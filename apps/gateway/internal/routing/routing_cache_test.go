package routing

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"ai-gateway/pkg/async"
	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/dbtest"
)

// newTestRouter 构造实例级 Router（缓存 / 熔断器 / 执行器全部独立，测试间天然隔离）
func newTestRouter(t *testing.T) (*Router, *dbtest.FakeDB) {
	t.Helper()
	db, fake := dbtest.New(t)
	breakers := circuitbreaker.NewManager(circuitbreaker.Options{})
	r := NewRouter(db, breakers, nil, &async.Executor{})
	t.Cleanup(r.Close)
	return r, fake
}

var abilityColumns = []string{
	"id", "provider", "provider_model", "input_price_1k", "output_price_1k",
	"priority", "weight", "fallback",
}

// seedAbilitiesCache 预设一次 abilities 查询并让缓存填充（2 个渠道、2 个优先级层）
func seedAbilitiesCache(t *testing.T, r *Router, fake *dbtest.FakeDB) {
	t.Helper()
	fake.ExpectQuery("FROM abilities").WillReturnRows(abilityColumns,
		[]driver.Value{int64(1), "openai", "gpt-4o-mini", 0.01, 0.03, int64(100), int64(3), ""},
		[]driver.Value{int64(2), "anthropic", "claude-haiku", 0.02, 0.04, int64(50), int64(1), ""},
	)
	_, found, err := r.ResolveAbilities(context.Background(), 1, "gpt-4o")
	if err != nil || !found {
		t.Fatalf("首次查询应命中 DB 并 found=true，found=%v err=%v", found, err)
	}
}

func TestResolveAbilitiesCachesResult(t *testing.T) {
	r, fake := newTestRouter(t)
	seedAbilitiesCache(t, r, fake)

	// 连续 1000 次解析：全部走缓存，零新增 DB 查询（fake 对未预期查询直接报错）
	for i := 0; i < 1000; i++ {
		tiers, found, err := r.ResolveAbilities(context.Background(), 1, "gpt-4o")
		if err != nil || !found {
			t.Fatalf("第 %d 次解析应缓存命中，found=%v err=%v", i+2, found, err)
		}
		if len(tiers) != 2 || tiers[0].Priority != 100 || tiers[1].Priority != 50 {
			t.Fatalf("第 %d 次解析分层结果错误: %+v", i+2, tiers)
		}
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 1 {
		t.Fatalf("1001 次解析共 %d 次 DB 查询; want 1", calls)
	}
	if s := r.cache.Stats(); s.Hits != 1000 || s.Misses != 1 {
		t.Errorf("Stats = %+v; want Hits=1000 Misses=1", s)
	}
}

func TestResolveAbilitiesNegativeCached(t *testing.T) {
	r, fake := newTestRouter(t)

	fake.ExpectQuery("FROM abilities").WillReturnRows(abilityColumns) // 空结果

	for i := 0; i < 100; i++ {
		_, found, err := r.ResolveAbilities(context.Background(), 9, "no-such-model")
		if err != nil {
			t.Fatalf("第 %d 次解析 err = %v", i+1, err)
		}
		if found {
			t.Fatalf("第 %d 次解析 found 应为 false", i+1)
		}
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 1 {
		t.Fatalf("否定结果应被缓存：共 %d 次 DB 查询; want 1", calls)
	}
}

func TestResolveAbilitiesBreakerFilterFreshOnCacheHit(t *testing.T) {
	r, fake := newTestRouter(t)

	// 熔断器是 Router 实例级（NewManager），provider 名固定即可，无需进程级唯一
	const providerA, providerB = "cbtest-a", "cbtest-b"
	fake.ExpectQuery("FROM abilities").WillReturnRows(abilityColumns,
		[]driver.Value{int64(11), providerA, "model-a", 0.01, 0.01, int64(100), int64(1), ""},
		[]driver.Value{int64(12), providerB, "model-b", 0.01, 0.01, int64(90), int64(1), ""},
	)

	tiers, found, err := r.ResolveAbilities(context.Background(), 1, "m")
	if err != nil || !found || len(tiers) != 2 {
		t.Fatalf("首次解析异常: tiers=%d found=%v err=%v", len(tiers), found, err)
	}

	// 熔断 A（连续 5 次失败触发 OPEN）
	for i := 0; i < 5; i++ {
		_, _ = r.breakers.Call(providerA, func() (interface{}, error) {
			return nil, errors.New("upstream down")
		})
	}
	if !r.breakers.IsProviderOpen(providerA) {
		t.Fatal("前置条件：provider A 应处于 OPEN")
	}

	// 缓存命中（无新 DB 查询）但熔断过滤实时生效
	tiers, found, err = r.ResolveAbilities(context.Background(), 1, "m")
	if err != nil || !found {
		t.Fatalf("熔断过滤后 found 应为 true（还剩 B），err=%v", err)
	}
	if len(tiers) != 1 || tiers[0].Channels[0].Provider != providerB {
		t.Fatalf("A 应被熔断过滤，剩余: %+v", tiers)
	}

	// 两个 provider 全熔断 → found=false（走 models 兜底）
	for i := 0; i < 5; i++ {
		_, _ = r.breakers.Call(providerB, func() (interface{}, error) {
			return nil, errors.New("upstream down")
		})
	}
	_, found, err = r.ResolveAbilities(context.Background(), 1, "m")
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if found {
		t.Fatal("全部熔断时 found 应为 false")
	}

	if calls := len(fake.Calls()); calls != 1 {
		t.Fatalf("熔断过滤不应触发新查询，共 %d 次 DB 调用; want 1", calls)
	}
	fake.AssertExpectations(t)
}

func TestExecAbilityUpdateDisableInvalidatesCache(t *testing.T) {
	r, fake := newTestRouter(t)
	seedAbilitiesCache(t, r, fake)

	// 禁用写库命中 1 行 → 缓存被失效 → 下一次解析重新查库（返回空 → 不再路由到该渠道）
	fake.ExpectExec("UPDATE abilities SET enabled = 0").WillReturnResult(0, 1)
	r.execAbilityUpdate(true, disableChannelQuery, "auth failed", int64(1))

	if got := r.cache.Stats().Size; got != 0 {
		t.Fatalf("禁用后缓存应被清空，Size = %d", got)
	}

	fake.ExpectQuery("FROM abilities").WillReturnRows(abilityColumns) // 空：渠道已全部禁用
	_, found, err := r.ResolveAbilities(context.Background(), 1, "gpt-4o")
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if found {
		t.Fatal("禁用生效后应 found=false（下一次请求即不路由到该渠道）")
	}

	fake.AssertExpectations(t)
}

func TestExecAbilityUpdateFailureInvalidatesCache(t *testing.T) {
	r, fake := newTestRouter(t)
	seedAbilitiesCache(t, r, fake)

	// 失败计数 +1 必然改变行 → RowsAffected=1 → 失效缓存
	fake.ExpectExec("consecutive_failures = consecutive_failures + 1").WillReturnResult(0, 1)
	r.execAbilityUpdate(true, "UPDATE abilities SET consecutive_failures = consecutive_failures + 1 WHERE id = ?", int64(1))

	if got := r.cache.Stats().Size; got != 0 {
		t.Fatalf("失败记录后缓存应被清空，Size = %d", got)
	}
	fake.AssertExpectations(t)
}

func TestExecAbilityUpdateSuccessZeroRowsKeepsCache(t *testing.T) {
	r, fake := newTestRouter(t)
	seedAbilitiesCache(t, r, fake)

	// 稳态成功清零：守卫子句使 RowsAffected=0 → 不失效缓存（防成功流量把缓存清空）
	fake.ExpectExec("consecutive_failures = 0").WillReturnResult(0, 0)
	r.execAbilityUpdate(true, recordSuccessQuery, int64(1))

	if got := r.cache.Stats().Size; got != 1 {
		t.Fatalf("RowsAffected=0 时缓存应保持，Size = %d", got)
	}

	// 缓存仍在：下一次解析零 DB 查询
	if _, found, err := r.ResolveAbilities(context.Background(), 1, "gpt-4o"); err != nil || !found {
		t.Fatalf("缓存应仍有效，found=%v err=%v", found, err)
	}
	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 2 { // 1 次 SELECT + 1 次 UPDATE
		t.Fatalf("共 %d 次 DB 调用; want 2", calls)
	}
}

func TestExecAbilityUpdateErrorKeepsCache(t *testing.T) {
	r, fake := newTestRouter(t)
	seedAbilitiesCache(t, r, fake)

	fake.ExpectExec("UPDATE abilities SET enabled = 0").WillReturnError(errors.New("db down"))
	r.execAbilityUpdate(true, disableChannelQuery, "x", int64(1))

	if got := r.cache.Stats().Size; got != 1 {
		t.Fatalf("写库失败时缓存应保持，Size = %d", got)
	}
	fake.AssertExpectations(t)
}

func TestAsyncDisableChannelWiresInvalidation(t *testing.T) {
	r, fake := newTestRouter(t)
	seedAbilitiesCache(t, r, fake)

	fake.ExpectExec("UPDATE abilities SET enabled = 0").WillReturnResult(0, 1)
	r.AsyncDisableChannel(1, "auth failed")

	// 异步 goroutine：轮询等待缓存被清空
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.cache.Stats().Size == 0 {
			fake.AssertExpectations(t)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("AsyncDisableChannel 未在超时前失效缓存")
}

func TestAsyncNoopOnInvalidID(t *testing.T) {
	r, fake := newTestRouter(t)

	r.AsyncDisableChannel(0, "x")
	r.AsyncRecordFailure(-1, "x")
	r.AsyncRecordSuccess(0)

	time.Sleep(50 * time.Millisecond) // 给潜在 goroutine 留执行窗口
	if calls := len(fake.Calls()); calls != 0 {
		t.Fatalf("非法 ID 不应产生 DB 调用，共 %d 次", calls)
	}
}

func TestRecordSuccessQueryHasGuardClause(t *testing.T) {
	// 守卫子句是"成功流量不清空缓存"的关键，防止后续重构误删
	if !strings.Contains(recordSuccessQuery, "consecutive_failures > 0") {
		t.Error("recordSuccessQuery 缺少守卫子句：稳态成功请求会导致缓存被反复失效")
	}
}

// TestTwoRoutersIsolated 两个 Router 缓存互不干扰（阶段 14 验收）
func TestTwoRoutersIsolated(t *testing.T) {
	db1, fake1 := dbtest.New(t)
	db2, fake2 := dbtest.New(t)
	r1 := NewRouter(db1, circuitbreaker.NewManager(circuitbreaker.Options{}), nil, &async.Executor{})
	r2 := NewRouter(db2, circuitbreaker.NewManager(circuitbreaker.Options{}), nil, &async.Executor{})
	t.Cleanup(r1.Close)
	t.Cleanup(r2.Close)

	// r1 有路由（2 渠道）；r2 无记录
	fake1.ExpectQuery("FROM abilities").WillReturnRows(abilityColumns,
		[]driver.Value{int64(1), "openai", "gpt-4o-mini", 0.01, 0.03, int64(100), int64(3), ""},
	)
	fake2.ExpectQuery("FROM abilities").WillReturnRows(abilityColumns) // 空

	_, found1, err := r1.ResolveAbilities(context.Background(), 1, "gpt-4o")
	if err != nil || !found1 {
		t.Fatalf("r1 应 found=true: found=%v err=%v", found1, err)
	}
	_, found2, err := r2.ResolveAbilities(context.Background(), 1, "gpt-4o")
	if err != nil {
		t.Fatalf("r2 err = %v", err)
	}
	if found2 {
		t.Fatal("r2 不应串 r1 的路由缓存")
	}
	fake1.AssertExpectations(t)
	fake2.AssertExpectations(t)
}
