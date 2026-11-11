package middleware

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"database/sql"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/dbtest"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
)

// rlApp 用指定默认配额 + miniredis 构造测试 App（阶段 14：配额默认值取自 a.Config.Gateway）
func rlApp(t *testing.T, rps, tpm, conc int) *app.App {
	t.Helper()
	s := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { cli.Close() })
	cfg := config.Default()
	cfg.Gateway.DefaultRPSLimit = rps
	cfg.Gateway.DefaultTPMLimit = tpm
	cfg.Gateway.DefaultConcurrencyLimit = conc
	return &app.App{Config: cfg, Redis: cli}
}

// rlAppWithDB 用指定默认配额 + fake DB + miniredis 构造测试 App（配额查询回源场景）
func rlAppWithDB(t *testing.T, rps, tpm, conc int, db *sql.DB) *app.App {
	t.Helper()
	a := rlApp(t, rps, tpm, conc)
	a.DB = db
	return a
}

// rlAppWithRedis 用指定（不可用的）Redis 地址构造测试 App（fail-open 场景）
func rlAppWithRedis(t *testing.T, rps, tpm, conc int, redisAddr string) *app.App {
	t.Helper()
	cli := goredis.NewClient(&goredis.Options{Addr: redisAddr, DialTimeout: 50 * time.Millisecond})
	t.Cleanup(func() { cli.Close() })
	cfg := config.Default()
	cfg.Gateway.DefaultRPSLimit = rps
	cfg.Gateway.DefaultTPMLimit = tpm
	cfg.Gateway.DefaultConcurrencyLimit = conc
	return &app.App{Config: cfg, Redis: cli}
}

// buildRateLimitEngine 组装测试链路：Trace → 注入 tenant_id → RateLimit → handler
func buildRateLimitEngine(tenantID int64, a *app.App, handler gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Trace())
	r.Use(func(c *gin.Context) {
		c.Set("tenant_id", tenantID)
		c.Next()
	})
	r.Use(RateLimit(a))
	r.POST("/v1/chat/completions", handler)
	return r
}

// doRequest 执行一次 POST 请求
func doRequest(engine *gin.Engine, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	return w
}

// okHandler 200 落点
func okHandler(c *gin.Context) { c.Status(http.StatusOK) }

// assert429Contract 校验 429 响应的完整契约：Retry-After / rate_limit_error / request_id
func assert429Contract(t *testing.T, w *httptest.ResponseRecorder, wantCode string) {
	t.Helper()
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429, body=%s", w.Code, w.Body.String())
	}
	if ra := w.Header().Get("Retry-After"); ra == "" {
		t.Error("缺少 Retry-After 头")
	} else if n, err := strconv.Atoi(ra); err != nil || n < 1 {
		t.Errorf("Retry-After = %q, want >=1 的秒数", ra)
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应不是 JSON: %v, raw=%q", err, w.Body.String())
	}
	errObj, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("error 字段缺失: %v", body)
	}
	if errObj["type"] != "rate_limit_error" {
		t.Errorf("error.type = %v, want rate_limit_error", errObj["type"])
	}
	if errObj["code"] != wantCode {
		t.Errorf("error.code = %v, want %s", errObj["code"], wantCode)
	}
	id, _ := body["request_id"].(string)
	if id == "" {
		t.Error("request_id 缺失")
	}
	if header := w.Header().Get("X-Request-Id"); header != id {
		t.Errorf("X-Request-Id = %q, 与 request_id = %q 不一致", header, id)
	}
}

// 验收口径 1：并发 200 请求打 RPS=10 的租户，约 190 个 429
func TestRateLimitRPSBurstAcceptance(t *testing.T) {

	engine := buildRateLimitEngine(1, rlApp(t, 10, 1<<30, 1<<30), okHandler)

	const total = 200
	var wg sync.WaitGroup
	codes := make([]int, total)
	first429 := sync.Once{}
	var contractW *httptest.ResponseRecorder
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			w := doRequest(engine, `{"messages":[{"role":"user","content":"hi"}]}`)
			codes[idx] = w.Code
			if w.Code == http.StatusTooManyRequests {
				first429.Do(func() { contractW = w })
			}
		}(i)
	}
	wg.Wait()

	allowed, rejected := 0, 0
	for _, code := range codes {
		switch code {
		case http.StatusOK:
			allowed++
		case http.StatusTooManyRequests:
			rejected++
		default:
			t.Fatalf("出现非预期状态码 %d", code)
		}
	}
	// 桶容量 10：放行约 10 个（运行期补充可能多放几个），其余全部 429
	if allowed < 10 || allowed > 25 {
		t.Errorf("放行 %d 个, want [10, 25]", allowed)
	}
	if rejected < total-25 {
		t.Errorf("429 数 %d, want >= %d（约 190）", rejected, total-25)
	}
	if contractW != nil {
		assert429Contract(t, contractW, "rps_limit_exceeded")
	}
}

// 验收口径 2：并发数超限时多余请求立即 429 而非排队挂死
func TestRateLimitConcurrencyRejectsImmediately(t *testing.T) {

	block := make(chan struct{})
	engine := buildRateLimitEngine(2, rlApp(t, 1<<30, 1<<30, 5),
		func(c *gin.Context) {
			<-block // 占用并发槽不放
			c.Status(http.StatusOK)
		})

	const total = 20
	results := make(chan *httptest.ResponseRecorder, total)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- doRequest(engine, `{"messages":[{"role":"user","content":"hi"}]}`)
		}()
	}

	// 5 个请求挂在 handler，其余 15 个必须立即拿到 429（不等 handler 结束）
	rejected := 0
	deadline := time.After(5 * time.Second)
	for rejected < total-5 {
		select {
		case w := <-results:
			if w.Code != http.StatusTooManyRequests {
				t.Fatalf("溢出请求应 429, got %d", w.Code)
			}
			if w.Header().Get("Retry-After") == "" {
				t.Fatal("并发超限 429 缺少 Retry-After")
			}
			rejected++
		case <-deadline:
			t.Fatalf("只收到 %d 个 429，其余请求疑似排队挂死（应全部立即拒绝）", rejected)
		}
	}

	close(block) // 放行占位的 5 个
	wg.Wait()
	close(results)
	okCount := 0
	for w := range results {
		if w.Code == http.StatusOK {
			okCount++
		}
	}
	if okCount != 5 {
		t.Errorf("解除阻塞后应有 5 个 200, got %d", okCount)
	}
}

// 配额从 DB 读取并本地缓存：100 次请求只查 1 次 DB，且 DB 配的 rps=2 生效
func TestRateLimitQuotaFromDBCached(t *testing.T) {
	db, fake := dbtest.New(t)

	fake.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"rps_limit", "tpm_limit", "concurrency_limit"},
		[]driver.Value{int64(2), nil, nil}, // tpm/conc NULL → 回落默认
	)

	engine := buildRateLimitEngine(3, rlAppWithDB(t, 100, 1<<30, 1<<30, db), okHandler)

	codes := make([]int, 0, 100)
	for i := 0; i < 100; i++ {
		codes = append(codes, doRequest(engine, `{"messages":[{"role":"user","content":"hi"}]}`).Code)
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 1 {
		t.Fatalf("100 次请求共 %d 次 DB 调用; want 1（配额应走本地缓存）", calls)
	}
	// DB 配置 rps=2 生效（而非默认 100）：只有前 2 个放行
	if codes[0] != http.StatusOK || codes[1] != http.StatusOK {
		t.Errorf("前 2 请求应放行（DB rps=2）: %v", codes[:2])
	}
	if codes[2] != http.StatusTooManyRequests {
		t.Errorf("第 3 请求应 429: %d", codes[2])
	}
}

// DB 查不到配额行（租户刚建未配列）→ fail-open 用全局默认，不缓存
func TestRateLimitQuotaRowMissingDefaults(t *testing.T) {
	db, fake := dbtest.New(t)

	// 两次请求各查一次（失败不缓存），都返回空行
	for i := 0; i < 2; i++ {
		fake.ExpectQuery("FROM tenants").WillReturnRows(
			[]string{"rps_limit", "tpm_limit", "concurrency_limit"},
		)
	}

	engine := buildRateLimitEngine(4, rlAppWithDB(t, 100, 100, 5, db), okHandler)
	for i := 0; i < 2; i++ {
		if w := doRequest(engine, `{"messages":[{"role":"user","content":"hi"}]}`); w.Code != http.StatusOK {
			t.Fatalf("第 %d 次应放行（默认配额）, got %d", i+1, w.Code)
		}
	}
	fake.AssertExpectations(t)
}

// TPM 滑动窗口：请求体估算 token 超限额 → 429 kind=tpm
func TestRateLimitTPMRejects(t *testing.T) {

	engine := buildRateLimitEngine(5, rlApp(t, 1<<30, 100, 1<<30), okHandler)

	// content 400 字符 → 估算 4 + 400/4 = 104 > 100
	bigContent := strings.Repeat("a", 400)
	body := fmt.Sprintf(`{"messages":[{"role":"user","content":%q}]}`, bigContent)
	w := doRequest(engine, body)
	assert429Contract(t, w, "tpm_limit_exceeded")
}

// Redis 不可用 → fail-open 放行（告警日志），不挡正常流量
func TestRateLimitRedisDownFailOpen(t *testing.T) {
	s := miniredis.RunT(t)
	addr := s.Addr()
	s.Close() // 关停，所有命令必失败

	engine := buildRateLimitEngine(6,
		rlAppWithRedis(t, 1, 1, 1, addr),
		okHandler)
	if w := doRequest(engine, `{"messages":[{"role":"user","content":"hi"}]}`); w.Code != http.StatusOK {
		t.Fatalf("Redis 宕机应 fail-open 放行, got %d", w.Code)
	}
}

// 中间件读完 body 后必须回填，handler 能读到完整请求体
func TestRateLimitBodyRestored(t *testing.T) {

	want := `{"messages":[{"role":"user","content":"hello world"}]}`
	engine := buildRateLimitEngine(7, rlApp(t, 100, 1<<30, 1<<30),
		func(c *gin.Context) {
			body, err := io.ReadAll(c.Request.Body)
			if err != nil || string(body) != want {
				c.Status(http.StatusInternalServerError)
				return
			}
			c.Status(http.StatusOK)
		})

	if w := doRequest(engine, want); w.Code != http.StatusOK {
		t.Fatalf("body 未被完整回填, status = %d", w.Code)
	}
}

// 缺少 tenant_id context（中间件链被错误裁剪）→ 500
func TestRateLimitMissingTenantContext(t *testing.T) {

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Trace())
	r.Use(RateLimit(rlApp(t, 1, 1, 1)))
	r.POST("/v1/chat/completions", okHandler)

	if w := doRequest(r, `{"messages":[]}`); w.Code != http.StatusInternalServerError {
		t.Fatalf("缺 tenant_id 应 500, got %d", w.Code)
	}
}
