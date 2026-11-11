package relay

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/dbtest"
	"ai-gateway/pkg/provider"
)

// newDBTestRelay 构造带 fake DB 的 Relay（兜底路由解析回源 fake DB）
func newDBTestRelay(t *testing.T) (*Relay, *dbtest.FakeDB) {
	t.Helper()
	db, fake := dbtest.New(t)
	r := New(nil, circuitbreaker.NewManager(circuitbreaker.Options{}), provider.NewRegistry(), db, DefaultAttemptTimeout)
	t.Cleanup(r.Close)
	return r, fake
}

var modelColumns = []string{
	"provider", "provider_model", "input_price_1k", "output_price_1k",
	"pricing_model", "pricing_config", "price_version",
}

func TestResolveModelCachesResult(t *testing.T) {
	r, fake := newDBTestRelay(t)

	fake.ExpectQuery("FROM models").WillReturnRows(modelColumns,
		[]driver.Value{"openai", "gpt-4o-mini", int64(10000), int64(30000), "linear", nil, int64(1)},
	)

	// 连续 1000 次解析：1 次 DB 查询，其余全部缓存命中
	for i := 0; i < 1000; i++ {
		route, err := r.resolveModel(context.Background(), 1, "gpt-4o")
		if err != nil {
			t.Fatalf("第 %d 次解析 err = %v", i+1, err)
		}
		if route.providerType != "openai" || route.providerModel != "gpt-4o-mini" {
			t.Fatalf("第 %d 次解析结果错误: %+v", i+1, route)
		}
		if route.inputPrice1k != 10000 || route.outputPrice1k != 30000 {
			t.Fatalf("第 %d 次解析价格错误: %+v", i+1, route)
		}
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 1 {
		t.Fatalf("1000 次解析共 %d 次 DB 查询; want 1", calls)
	}
	if s := r.modelCache.Stats(); s.Hits != 999 || s.Misses != 1 {
		t.Errorf("Stats = %+v; want Hits=999 Misses=1", s)
	}
}

func TestResolveModelNotFoundNotCached(t *testing.T) {
	r, fake := newDBTestRelay(t)

	// 否定结果不缓存：两次解析各查一次 DB
	for i := 0; i < 2; i++ {
		fake.ExpectQuery("FROM models").WillReturnRows(modelColumns)
	}

	for i := 0; i < 2; i++ {
		_, err := r.resolveModel(context.Background(), 1, "no-such-model")
		if !errors.Is(err, ErrModelNotFound) {
			t.Fatalf("第 %d 次应返回 ErrModelNotFound，err = %v", i+1, err)
		}
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 2 {
		t.Fatalf("ErrModelNotFound 被缓存了？共 %d 次 DB 查询; want 2", calls)
	}
}

func TestResolveModelDBErrorNotCached(t *testing.T) {
	r, fake := newDBTestRelay(t)

	for i := 0; i < 2; i++ {
		fake.ExpectQuery("FROM models").WillReturnError(errors.New("connection refused"))
	}

	for i := 0; i < 2; i++ {
		_, err := r.resolveModel(context.Background(), 1, "gpt-4o")
		if err == nil || errors.Is(err, ErrModelNotFound) {
			t.Fatalf("第 %d 次应返回 DB 错误，err = %v", i+1, err)
		}
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 2 {
		t.Fatalf("DB 错误结果被缓存了？共 %d 次 DB 查询; want 2", calls)
	}
}

func TestResolveModelCacheIsolatesTenant(t *testing.T) {
	r, fake := newDBTestRelay(t)

	// 不同 tenant 的同名模型各自缓存，互不串数据
	fake.ExpectQuery("FROM models").WillReturnRows(modelColumns,
		[]driver.Value{"openai", "gpt-4o-mini", int64(10000), int64(30000), "linear", nil, int64(1)},
	)
	fake.ExpectQuery("FROM models").WillReturnRows(modelColumns,
		[]driver.Value{"volcengine", "doubao-lite", int64(1000), int64(2000), "linear", nil, int64(1)},
	)

	r1, err := r.resolveModel(context.Background(), 1, "my-model")
	if err != nil {
		t.Fatalf("tenant1 err = %v", err)
	}
	r2, err := r.resolveModel(context.Background(), 2, "my-model")
	if err != nil {
		t.Fatalf("tenant2 err = %v", err)
	}
	if r1.providerType == r2.providerType {
		t.Fatalf("不同租户命中了同一条缓存: %+v vs %+v", r1, r2)
	}

	// 再查一次确认各自命中自己的缓存（无新查询）
	r1again, _ := r.resolveModel(context.Background(), 1, "my-model")
	r2again, _ := r.resolveModel(context.Background(), 2, "my-model")
	if r1again.providerType != "openai" || r2again.providerType != "volcengine" {
		t.Fatalf("缓存隔离失效: %+v vs %+v", r1again, r2again)
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 2 {
		t.Fatalf("共 %d 次 DB 查询; want 2", calls)
	}
}
