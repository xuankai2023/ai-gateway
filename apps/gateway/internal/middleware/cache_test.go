package middleware

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	semcache "ai-gateway/apps/gateway/internal/cache"
	"ai-gateway/apps/gateway/internal/usagelog"
	"ai-gateway/pkg/async"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/dbtest"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
)

// newSemanticTestCache 用 miniredis 构建精确哈希层实例（无向量层）
func newSemanticTestCache(t *testing.T) (*semcache.Cache, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { cli.Close() })
	return semcache.New(semcache.Options{Redis: cli, TTL: time.Hour}), s
}

// semanticTestRig 一次测试的全部组件：fake DB + App + 路由引擎
type semanticTestRig struct {
	fake   *dbtest.FakeDB
	app    *app.App
	engine *gin.Engine
}

// newSemanticTestRig 组装链路：Trace → tenant(按请求头) → SemanticCache → handler
func newSemanticTestRig(t *testing.T, inst *semcache.Cache, h gin.HandlerFunc) *semanticTestRig {
	t.Helper()
	db, fake := dbtest.New(t)
	a := &app.App{
		Config:        config.Default(),
		DB:            db,
		Async:         &async.Executor{},
		Usage:         usagelog.New(nil, usagelog.Options{}),
		SemanticCache: inst,
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Trace())
	r.Use(func(c *gin.Context) {
		id, _ := strconv.ParseInt(c.GetHeader("X-Test-Tenant"), 10, 64)
		if id <= 0 {
			id = 1
		}
		c.Set("tenant_id", id)
		c.Set("api_key_id", int64(7))
		if p, _ := strconv.ParseInt(c.GetHeader("X-Test-Project"), 10, 64); p > 0 {
			c.Set("project_id", p)
		}
		c.Next()
	})
	r.Use(SemanticCache(a))
	if h != nil {
		r.POST("/v1/chat/completions", h)
	}
	return &semanticTestRig{fake: fake, app: a, engine: r}
}

// doSemanticRequest 执行一次请求
func doSemanticRequest(engine *gin.Engine, tenant int64, body string) *httptest.ResponseRecorder {
	return doSemanticRequestProject(engine, tenant, 0, body)
}

// doSemanticRequestProject 执行一次带项目归属的请求
func doSemanticRequestProject(engine *gin.Engine, tenant, project int64, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Test-Tenant", strconv.FormatInt(tenant, 10))
	if project > 0 {
		req.Header.Set("X-Test-Project", strconv.FormatInt(project, 10))
	}
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	return w
}

// waitAsyncStore 排空 App 异步执行器（缓存写回等后台任务）
func waitAsyncStore(t *testing.T, a *app.App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Async.Wait(ctx); err != nil {
		t.Fatalf("异步写回超时: %v", err)
	}
}

const semanticBody = `{"model":"demo","messages":[{"role":"user","content":"讲个笑话"}]}`

// 验收口径 1：相同 prompt 第二次调用零上游请求（精确哈希命中）
func TestSemanticCacheExactHit(t *testing.T) {
	inst, _ := newSemanticTestCache(t)
	rig := newSemanticTestRig(t, inst, nil)
	rig.fake.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"cache_enabled"}, []driver.Value{int64(1)},
	)

	var calls atomic.Int64
	rig.engine.POST("/v1/chat/completions", func(c *gin.Context) {
		calls.Add(1)
		c.JSON(http.StatusOK, gin.H{"content": "hello"})
	})

	first := doSemanticRequest(rig.engine, 1, semanticBody)
	if first.Code != http.StatusOK {
		t.Fatalf("首次 status = %d", first.Code)
	}
	if first.Header().Get("X-Cache") != "" {
		t.Error("首次未命中不应带 X-Cache")
	}
	waitAsyncStore(t, rig.app)

	second := doSemanticRequest(rig.engine, 1, semanticBody)
	if second.Code != http.StatusOK {
		t.Fatalf("第二次 status = %d", second.Code)
	}
	if second.Header().Get("X-Cache") != "HIT" {
		t.Errorf("X-Cache = %q, want HIT", second.Header().Get("X-Cache"))
	}
	if second.Header().Get("X-Cache-Layer") != semcache.LayerExact {
		t.Errorf("X-Cache-Layer = %q, want exact", second.Header().Get("X-Cache-Layer"))
	}
	if second.Body.String() != first.Body.String() {
		t.Errorf("命中响应体应与首次一致: %q vs %q", second.Body.String(), first.Body.String())
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("handler 调用 %d 次, want 1（第二次应命中缓存）", got)
	}
	rig.fake.AssertExpectations(t)
}

// 验收口径 2：不同租户相同 prompt 不串数据
func TestSemanticCacheTenantIsolation(t *testing.T) {
	inst, _ := newSemanticTestCache(t)
	rig := newSemanticTestRig(t, inst, nil)
	for i := 0; i < 2; i++ {
		rig.fake.ExpectQuery("FROM tenants").WillReturnRows(
			[]string{"cache_enabled"}, []driver.Value{int64(1)},
		)
	}

	var calls atomic.Int64
	rig.engine.POST("/v1/chat/completions", func(c *gin.Context) {
		calls.Add(1)
		c.JSON(http.StatusOK, gin.H{"tenant": c.GetInt64("tenant_id")})
	})

	// 租户 1 首次写入 + 命中
	if w := doSemanticRequest(rig.engine, 1, semanticBody); w.Code != http.StatusOK {
		t.Fatalf("tenant1 首次 status = %d", w.Code)
	}
	waitAsyncStore(t, rig.app)
	hit1 := doSemanticRequest(rig.engine, 1, semanticBody)
	if hit1.Header().Get("X-Cache") != "HIT" {
		t.Fatal("tenant1 第二次应命中")
	}

	// 租户 2 相同 prompt 必须回源（不能读到租户 1 的缓存）
	miss2 := doSemanticRequest(rig.engine, 2, semanticBody)
	if miss2.Header().Get("X-Cache") != "" {
		t.Errorf("tenant2 不应命中 tenant1 缓存, X-Cache=%q", miss2.Header().Get("X-Cache"))
	}

	var body map[string]any
	_ = json.Unmarshal(miss2.Body.Bytes(), &body)
	if body["tenant"] != float64(2) {
		t.Errorf("tenant2 响应串了租户数据: %v", body["tenant"])
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("handler 调用 %d 次, want 2（两租户各一次）", got)
	}
	rig.fake.AssertExpectations(t)
}

// 验收口径 3：同租户不同项目相同 prompt 不串数据（缓存按 project 隔离）
func TestSemanticCacheProjectIsolation(t *testing.T) {
	inst, _ := newSemanticTestCache(t)
	rig := newSemanticTestRig(t, inst, nil)
	// 租户开关只查一次（本地缓存 60s）；项目版本各查一次
	rig.fake.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"cache_enabled"}, []driver.Value{int64(1)},
	)
	for i := 0; i < 2; i++ {
		rig.fake.ExpectQuery("FROM projects").WillReturnRows(
			[]string{"index_epoch"}, []driver.Value{int64(1)},
		)
	}

	var calls atomic.Int64
	rig.engine.POST("/v1/chat/completions", func(c *gin.Context) {
		calls.Add(1)
		c.JSON(http.StatusOK, gin.H{"project": c.GetInt64("project_id")})
	})

	if w := doSemanticRequestProject(rig.engine, 1, 10, semanticBody); w.Code != http.StatusOK {
		t.Fatalf("project10 首次 status = %d", w.Code)
	}
	waitAsyncStore(t, rig.app)

	// 同项目同 prompt：命中
	hit := doSemanticRequestProject(rig.engine, 1, 10, semanticBody)
	if hit.Header().Get("X-Cache") != "HIT" {
		t.Fatal("project10 第二次应命中")
	}

	// 另一项目同 prompt：必须回源，且响应体是项目 11 的数据
	miss := doSemanticRequestProject(rig.engine, 1, 11, semanticBody)
	if miss.Header().Get("X-Cache") != "" {
		t.Errorf("project11 不应命中 project10 缓存, X-Cache=%q", miss.Header().Get("X-Cache"))
	}
	var body map[string]any
	_ = json.Unmarshal(miss.Body.Bytes(), &body)
	if body["project"] != float64(11) {
		t.Errorf("project11 响应串了项目数据: %v", body["project"])
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("handler 调用 %d 次, want 2（两项目各一次）", got)
	}
	rig.fake.AssertExpectations(t)
}

// 流式请求不缓存（每次真实执行）
func TestSemanticCacheStreamBypass(t *testing.T) {
	inst, _ := newSemanticTestCache(t)
	rig := newSemanticTestRig(t, inst, nil)
	rig.fake.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"cache_enabled"}, []driver.Value{int64(1)},
	)

	var calls atomic.Int64
	rig.engine.POST("/v1/chat/completions", func(c *gin.Context) {
		calls.Add(1)
		c.JSON(http.StatusOK, gin.H{"content": "stream"})
	})

	body := `{"model":"demo","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	for i := 0; i < 2; i++ {
		w := doSemanticRequest(rig.engine, 1, body)
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d", w.Code)
		}
		if w.Header().Get("X-Cache") != "" {
			t.Error("流式请求不应命中缓存")
		}
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("流式请求 handler 调用 %d 次, want 2", got)
	}
}

// 租户开关关闭 → 完全旁路，不查不写
func TestSemanticCacheDisabledTenant(t *testing.T) {
	inst, _ := newSemanticTestCache(t)
	rig := newSemanticTestRig(t, inst, nil)
	rig.fake.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"cache_enabled"}, []driver.Value{int64(0)},
	)

	var calls atomic.Int64
	rig.engine.POST("/v1/chat/completions", func(c *gin.Context) {
		calls.Add(1)
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	for i := 0; i < 2; i++ {
		doSemanticRequest(rig.engine, 1, semanticBody)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("开关关闭时 handler 调用 %d 次, want 2", got)
	}
	rig.fake.AssertExpectations(t)
}

// fakeEmbedder 固定向量
type fakeEmbedder struct{}

func (fakeEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0, 0}, nil
}

func (fakeEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = []float32{1, 0, 0}
	}
	return out, nil
}

// fakeIndex 按 collection 隔离的单点向量索引：任意查询命中该 collection 最近一次 upsert 的指纹
type fakeIndex struct {
	mu         sync.Mutex
	fps        map[string]string
	searchCols []string
	upsertCols []string
}

func (f *fakeIndex) Search(_ context.Context, col string, _ []float32, _ float64) (string, float64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.searchCols = append(f.searchCols, col)
	fp := f.fps[col]
	if fp == "" {
		return "", 0, false, nil
	}
	return fp, 0.99, true, nil
}

func (f *fakeIndex) Upsert(_ context.Context, col, _ string, fingerprint string, _ []float32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fps == nil {
		f.fps = make(map[string]string)
	}
	f.fps[col] = fingerprint
	f.upsertCols = append(f.upsertCols, col)
	return nil
}

// 向量层：不同 prompt 但向量相似 ≥ 0.95 时命中（回源精确层取响应体）
func TestSemanticCacheVectorHit(t *testing.T) {
	s := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	defer cli.Close()
	inst := semcache.New(semcache.Options{
		Redis:          cli,
		TTL:            time.Hour,
		Embedder:       fakeEmbedder{},
		VectorIndex:    &fakeIndex{},
		ScoreThreshold: 0.95,
	})
	rig := newSemanticTestRig(t, inst, nil)
	rig.fake.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"cache_enabled"}, []driver.Value{int64(1)},
	)

	var calls atomic.Int64
	rig.engine.POST("/v1/chat/completions", func(c *gin.Context) {
		calls.Add(1)
		c.JSON(http.StatusOK, gin.H{"content": "cached"})
	})

	if w := doSemanticRequest(rig.engine, 1, `{"model":"demo","messages":[{"role":"user","content":"你好"}]}`); w.Code != http.StatusOK {
		t.Fatalf("首次 status = %d", w.Code)
	}
	waitAsyncStore(t, rig.app)

	// 不同 prompt：精确层 miss，向量层命中（fake 索引返回首个指纹）
	w := doSemanticRequest(rig.engine, 1, `{"model":"demo","messages":[{"role":"user","content":"您好呀"}]}`)
	if w.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("向量相似应命中, X-Cache=%q body=%s", w.Header().Get("X-Cache"), w.Body.String())
	}
	if w.Header().Get("X-Cache-Layer") != semcache.LayerSemantic {
		t.Errorf("X-Cache-Layer = %q, want semantic", w.Header().Get("X-Cache-Layer"))
	}
	if !strings.Contains(w.Body.String(), "cached") {
		t.Errorf("命中响应体异常: %s", w.Body.String())
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("handler 调用 %d 次, want 1", got)
	}
}

// 未配置语义缓存实例（a.SemanticCache == nil）→ 完全直通
func TestSemanticCacheNilInstance(t *testing.T) {
	db, fake := dbtest.New(t)
	a := &app.App{
		Config: config.Default(),
		DB:     db,
		Async:  &async.Executor{},
		Usage:  usagelog.New(nil, usagelog.Options{}),
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("tenant_id", int64(1))
		c.Next()
	})
	r.Use(SemanticCache(a))
	var calls atomic.Int64
	r.POST("/v1/chat/completions", func(c *gin.Context) {
		calls.Add(1)
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	w := doSemanticRequest(r, 1, semanticBody)
	if w.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("nil 实例应直通：status=%d calls=%d", w.Code, calls.Load())
	}
	if calls := len(fake.Calls()); calls != 0 {
		t.Fatalf("nil 实例不应触达 DB（开关查询跳过），共 %d 次", calls)
	}
}
