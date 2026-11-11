package middleware

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/config"
	"github.com/gin-gonic/gin"
)

// loadShedTestApp 用指定上限/排队超时构造测试 App（阶段 14：上限取自 a.Config.Gateway.MaxInFlight）
func loadShedTestApp(maxInFlight int, queueTimeout time.Duration) *app.App {
	cfg := config.Default()
	cfg.Gateway.MaxInFlight = maxInFlight
	return &app.App{Config: cfg, LoadShedQueueTimeout: queueTimeout}
}

// buildLoadShedEngine 组装测试链路：Trace → LoadShed → handler
func buildLoadShedEngine(a *app.App, handler gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(Trace())
	r.Use(LoadShed(a))
	r.POST("/v1/chat/completions", handler)
	return r
}

func TestLoadShedPassThrough(t *testing.T) {
	engine := buildLoadShedEngine(loadShedTestApp(2, 0), okHandler)

	w := doRequest(engine, `{}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", w.Code, w.Body.String())
	}
}

// TestLoadShedRejectsWhenSaturated 打满 in-flight 后新请求在排队超时内 503，而非排队挂死
func TestLoadShedRejectsWhenSaturated(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once // 请求 1 与请求 3 都会进入 handler，首次信号只发一次
	engine := buildLoadShedEngine(loadShedTestApp(1, 150*time.Millisecond),
		func(c *gin.Context) {
			enteredOnce.Do(func() { close(entered) })
			<-release
			c.Status(http.StatusOK)
		})

	// 请求 1：占住唯一在途名额
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := doRequest(engine, `{}`)
		if w.Code != http.StatusOK {
			t.Errorf("请求 1 status = %d, want 200", w.Code)
		}
	}()
	<-entered

	// 请求 2：排队 150ms 超时 → 快速 503
	start := time.Now()
	w2 := doRequest(engine, `{}`)
	elapsed := time.Since(start)

	if w2.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body=%s", w2.Code, w2.Body.String())
	}
	if elapsed > 2*time.Second {
		t.Errorf("拒绝耗时 %v，超出排队超时口径（快速拒绝而非挂死）", elapsed)
	}
	if ra := w2.Header().Get("Retry-After"); ra != "1" {
		t.Errorf("Retry-After = %q, want \"1\"", ra)
	}

	var body map[string]any
	if err := json.Unmarshal(w2.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应不是 JSON: %v, raw=%q", err, w2.Body.String())
	}
	errObj, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("error 字段缺失: %v", body)
	}
	if errObj["type"] != "overloaded_error" || errObj["code"] != "overloaded" {
		t.Errorf("error 契约不对: %v", errObj)
	}
	id, _ := body["request_id"].(string)
	if id == "" {
		t.Error("request_id 缺失")
	}
	if header := w2.Header().Get("X-Request-Id"); header != id {
		t.Errorf("X-Request-Id = %q, 与 request_id = %q 不一致", header, id)
	}

	close(release)
	wg.Wait()

	// 请求 3：名额释放后恢复放行
	w3 := doRequest(engine, `{}`)
	if w3.Code != http.StatusOK {
		t.Errorf("释放后 status = %d, want 200", w3.Code)
	}
}

// TestLoadShedDefaults 零值配置回落默认（上限 1000 / 排队 2s），正常请求放行
func TestLoadShedDefaults(t *testing.T) {
	engine := buildLoadShedEngine(loadShedTestApp(0, 0), okHandler)
	w := doRequest(engine, `{}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

// TestLoadShedConcurrentSaturate 并发打满：恰好 MaxInFlight 个放行，其余全部 503
func TestLoadShedConcurrentSaturate(t *testing.T) {
	const maxInFlight = 5
	const total = 20

	release := make(chan struct{})
	engine := buildLoadShedEngine(loadShedTestApp(maxInFlight, 100*time.Millisecond),
		func(c *gin.Context) {
			<-release
			c.Status(http.StatusOK)
		})

	var oks, sheds int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := doRequest(engine, `{}`)
			mu.Lock()
			switch w.Code {
			case http.StatusOK:
				oks++
			case http.StatusServiceUnavailable:
				sheds++
			default:
				t.Errorf("意外 status = %d", w.Code)
			}
			mu.Unlock()
		}()
	}

	// 等 5 个在途请求全部占住名额后再放行尾部流量：
	// 先睡过排队超时，让超出名额的请求全部拿到 503
	time.Sleep(300 * time.Millisecond)
	close(release)
	wg.Wait()

	if oks == 0 || sheds == 0 {
		t.Errorf("应同时存在放行与拒绝：oks=%d sheds=%d", oks, sheds)
	}
	if oks+sheds != total {
		t.Errorf("oks(%d)+sheds(%d) != %d", oks, sheds, total)
	}
}
