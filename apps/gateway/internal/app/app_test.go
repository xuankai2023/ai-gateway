package app

// 阶段 14 验收测试：同一进程内起两个完整 App，缓存 / 熔断器 / 注册表 / 用量写入器
// 全部实例级隔离，互不干扰（包级全局态已拆除的直接证据）

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"ai-gateway/pkg/config"
	"ai-gateway/pkg/dbtest"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// newIsolatedApp 构造带独立 fake DB + 独立 miniredis 的完整 App
func newIsolatedApp(t *testing.T) (*App, *dbtest.FakeDB) {
	t.Helper()
	db, fake := dbtest.New(t)
	s := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { cli.Close() })

	a := New(Options{Config: config.Default(), DB: db, Redis: cli})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = a.Close(ctx)
	})
	return a, fake
}

var abilityColumns = []string{
	"id", "provider", "provider_model", "input_price_1k", "output_price_1k",
	"priority", "weight", "fallback",
}

func TestTwoAppsCoexistIsolated(t *testing.T) {
	app1, fake1 := newIsolatedApp(t)
	app2, fake2 := newIsolatedApp(t)

	// 组件必须是各自独立的实例（不是同一个指针）
	if app1.Breakers == app2.Breakers || app1.Router == app2.Router ||
		app1.Relay == app2.Relay || app1.Providers == app2.Providers ||
		app1.Usage == app2.Usage || app1.Async == app2.Async {
		t.Fatal("两个 App 共享了组件实例（仍有全局态残留）")
	}

	// --- 路由缓存隔离：同名模型各自缓存各自 DB 的结果 ---
	fake1.ExpectQuery("FROM abilities").WillReturnRows(abilityColumns,
		[]driver.Value{int64(1), "provider-a", "model-a", 0.01, 0.01, int64(100), int64(1), ""})
	fake2.ExpectQuery("FROM abilities").WillReturnRows(abilityColumns,
		[]driver.Value{int64(2), "provider-b", "model-b", 0.02, 0.02, int64(100), int64(1), ""})

	tiers1, found1, err := app1.Router.ResolveAbilities(context.Background(), 1, "shared-model")
	if err != nil || !found1 {
		t.Fatalf("app1 首次解析异常: found=%v err=%v", found1, err)
	}
	tiers2, found2, err := app2.Router.ResolveAbilities(context.Background(), 1, "shared-model")
	if err != nil || !found2 {
		t.Fatalf("app2 首次解析异常: found=%v err=%v", found2, err)
	}
	if got := tiers1[0].Channels[0].Provider; got != "provider-a" {
		t.Fatalf("app1 解析到 %q, want provider-a", got)
	}
	if got := tiers2[0].Channels[0].Provider; got != "provider-b" {
		t.Fatalf("app2 解析到 %q, want provider-b（串了 app1 的缓存）", got)
	}

	// --- 熔断器隔离：app1 熔断同名 provider，app2 不受影响 ---
	for i := 0; i < 5; i++ {
		_, _ = app1.Breakers.Call("provider-x", func() (interface{}, error) {
			return nil, errors.New("upstream down")
		})
	}
	if !app1.Breakers.IsProviderOpen("provider-x") {
		t.Fatal("前置条件：app1 的 provider-x 应处于 OPEN")
	}
	if app2.Breakers.IsProviderOpen("provider-x") {
		t.Fatal("app1 熔断传导到了 app2（熔断器全局态残留）")
	}

	// --- 缓存失效隔离：app1 失效 abilities 只清自己 ---
	app1.InvalidateCache("abilities")

	// app1 下次解析重新查库
	fake1.ExpectQuery("FROM abilities").WillReturnRows(abilityColumns,
		[]driver.Value{int64(1), "provider-a", "model-a", 0.01, 0.01, int64(100), int64(1), ""})
	if _, found, err := app1.Router.ResolveAbilities(context.Background(), 1, "shared-model"); err != nil || !found {
		t.Fatalf("app1 失效后重查异常: found=%v err=%v", found, err)
	}

	// app2 下次解析仍命中自己的缓存（fake2 无新预期查询；若被误清会查库报错）
	tiers2again, found2again, err := app2.Router.ResolveAbilities(context.Background(), 1, "shared-model")
	if err != nil || !found2again {
		t.Fatalf("app2 缓存应不受 app1 失效影响: found=%v err=%v", found2again, err)
	}
	if got := tiers2again[0].Channels[0].Provider; got != "provider-b" {
		t.Fatalf("app2 缓存内容被污染: %q", got)
	}

	// DB 调用次数收口：app1 两次（首次 + 失效后重查），app2 仅一次
	if calls := len(fake1.Calls()); calls != 2 {
		t.Errorf("app1 共 %d 次 DB 查询; want 2", calls)
	}
	if calls := len(fake2.Calls()); calls != 1 {
		t.Errorf("app2 共 %d 次 DB 查询; want 1（缓存未被 app1 失效）", calls)
	}
	fake1.AssertExpectations(t)
	fake2.AssertExpectations(t)
}
