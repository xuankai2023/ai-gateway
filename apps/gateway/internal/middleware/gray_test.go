package middleware

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/dbtest"
	"github.com/gin-gonic/gin"
)

// newGrayTest 构建独立 App + Gray 中间件实例（缓存实例级，测试间天然隔离）
func newGrayTest(t *testing.T) (*dbtest.FakeDB, *app.App, gin.HandlerFunc) {
	t.Helper()
	db, fake := dbtest.New(t)
	a := newTestApp(t, db, nil)
	return fake, a, Gray(a)
}

// runGray 以指定 tenant/traceID 执行一次 Gray 中间件，返回灰度判定结果与回填后的 body
func runGray(gray gin.HandlerFunc, tenantID int64, traceID, model string) (targetModel string, isGray bool, bodyAfter []byte) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body := []byte(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}]}`, model))
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Set("tenant_id", tenantID)
	c.Set("trace_id", traceID)

	gray(c)
	if v, ok := c.Get("target_model"); ok {
		targetModel, _ = v.(string)
	}
	isGray = c.GetBool("is_gray")
	if c.Request.Body != nil {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(c.Request.Body)
		bodyAfter = buf.Bytes()
	}
	return targetModel, isGray, bodyAfter
}

// expectGrayRule 预设 gray_rules 查询返回一条启用规则
func expectGrayRule(fake *dbtest.FakeDB, targetModel string, weight int) {
	fake.ExpectQuery("FROM gray_rules").WillReturnRows(
		[]string{"target_model", "weight"},
		[]driver.Value{targetModel, int64(weight)},
	)
}

// TestGray_WeightDistribution weight=10 时 1000 请求命中灰度 80~120 次（验收口径 1）
func TestGray_WeightDistribution(t *testing.T) {
	fake, _, gray := newGrayTest(t)
	expectGrayRule(fake, "gpt-4o-gray", 10)

	hits := 0
	for i := 0; i < 1000; i++ {
		target, isGray, _ := runGray(gray, 1, fmt.Sprintf("trace-%d", i), "gpt-4o")
		if isGray {
			hits++
			if target != "gpt-4o-gray" {
				t.Fatalf("target_model = %q, want gpt-4o-gray", target)
			}
		} else if target != "" {
			t.Fatalf("非灰度请求不应设置 target_model，实际 %q", target)
		}
	}
	if hits < 80 || hits > 120 {
		t.Fatalf("灰度命中 %d 次，期望 80~120（weight=10，1000 请求）", hits)
	}
	// 规则只查一次 DB（其余全部缓存命中）
	if q := countCalls(fake, "query"); q != 1 {
		t.Fatalf("gray_rules 查询次数 = %d，期望 1（缓存生效）", q)
	}
	fake.AssertExpectations(t)
}

// TestGray_DisableTakesEffectImmediately 规则禁用后流量立即全回原模型（验收口径 2）
func TestGray_DisableTakesEffectImmediately(t *testing.T) {
	fake, a, gray := newGrayTest(t)

	// 规则启用（weight=100 全量灰度）
	expectGrayRule(fake, "gpt-4o-gray", 100)
	_, isGray, _ := runGray(gray, 1, "t-1", "gpt-4o")
	if !isGray {
		t.Fatal("weight=100 时请求应全部走灰度")
	}

	// 禁用规则（UPDATE enabled=0）+ 经 App 失效缓存 → 下一次请求即全部回原模型
	fake.ExpectQuery("FROM gray_rules").WillReturnRows(
		[]string{"target_model", "weight"}) // 空结果集 = 无启用规则
	a.InvalidateCache("gray")
	for i := 0; i < 10; i++ {
		_, isGray, _ := runGray(gray, 1, fmt.Sprintf("t-off-%d", i), "gpt-4o")
		if isGray {
			t.Fatal("规则禁用并失效缓存后仍有请求走灰度")
		}
	}
	fake.AssertExpectations(t)
}

// TestGray_NoRulePassthrough 无规则直通且否定结果缓存（不多余打 DB）
func TestGray_NoRulePassthrough(t *testing.T) {
	fake, _, gray := newGrayTest(t)
	fake.ExpectQuery("FROM gray_rules").WillReturnRows(
		[]string{"target_model", "weight"}) // 空结果集

	for i := 0; i < 50; i++ {
		_, isGray, _ := runGray(gray, 1, fmt.Sprintf("t-%d", i), "unknown-model")
		if isGray {
			t.Fatal("无规则请求不应走灰度")
		}
	}
	if q := countCalls(fake, "query"); q != 1 {
		t.Fatalf("否定结果应缓存：查询次数 = %d，期望 1", q)
	}
	fake.AssertExpectations(t)
}

// TestGray_WeightZero weight=0 全部回原模型
func TestGray_WeightZero(t *testing.T) {
	fake, _, gray := newGrayTest(t)
	expectGrayRule(fake, "gpt-4o-gray", 0)

	for i := 0; i < 100; i++ {
		_, isGray, _ := runGray(gray, 1, fmt.Sprintf("t-%d", i), "gpt-4o")
		if isGray {
			t.Fatal("weight=0 时不应有请求走灰度")
		}
	}
	fake.AssertExpectations(t)
}

// TestGray_TenantIsolation 租户间规则隔离：tenant 2 无规则不串 tenant 1 的灰度
func TestGray_TenantIsolation(t *testing.T) {
	fake, _, gray := newGrayTest(t)
	expectGrayRule(fake, "gpt-4o-gray", 100)            // tenant 1 有规则
	fake.ExpectQuery("FROM gray_rules").WillReturnRows( // tenant 2 无规则
		[]string{"target_model", "weight"})

	if _, isGray, _ := runGray(gray, 1, "t-1", "gpt-4o"); !isGray {
		t.Fatal("tenant 1 应走灰度")
	}
	if _, isGray, _ := runGray(gray, 2, "t-2", "gpt-4o"); isGray {
		t.Fatal("tenant 2 不应串 tenant 1 的灰度规则")
	}
	fake.AssertExpectations(t)
}

// TestGray_TwoAppsIsolated 两个 App 的灰度缓存互不干扰（阶段 14 验收）
func TestGray_TwoAppsIsolated(t *testing.T) {
	db1, fake1 := dbtest.New(t)
	db2, fake2 := dbtest.New(t)
	a1 := newTestApp(t, db1, nil)
	a2 := newTestApp(t, db2, nil)
	gray1 := Gray(a1)
	gray2 := Gray(a2)

	// App1 有规则（全量灰度）；App2 无规则
	expectGrayRule(fake1, "gpt-4o-gray", 100)
	fake2.ExpectQuery("FROM gray_rules").WillReturnRows(
		[]string{"target_model", "weight"})

	if _, isGray, _ := runGray(gray1, 1, "t-1", "gpt-4o"); !isGray {
		t.Fatal("App1 应走灰度")
	}
	if _, isGray, _ := runGray(gray2, 1, "t-1", "gpt-4o"); isGray {
		t.Fatal("App2 不应串 App1 的灰度缓存")
	}
	fake1.AssertExpectations(t)
	fake2.AssertExpectations(t)
}

// TestGray_BodyPreserved 探读 body 后完整回填，下游 handler 绑定不受影响
func TestGray_BodyPreserved(t *testing.T) {
	fake, _, gray := newGrayTest(t)
	expectGrayRule(fake, "gpt-4o-gray", 100)

	_, _, body := runGray(gray, 1, "t-body", "gpt-4o")
	want := `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`
	if string(body) != want {
		t.Fatalf("body 回填不一致：got %q want %q", body, want)
	}
	fake.AssertExpectations(t)
}

// TestGray_InvalidJSONPassthrough 非法 JSON 不裁决，交给 handler 报 400
func TestGray_InvalidJSONPassthrough(t *testing.T) {
	fake, _, gray := newGrayTest(t)

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader([]byte(`{invalid`)))
	c.Set("tenant_id", int64(1))
	c.Set("trace_id", "t-bad")

	gray(c)
	if c.GetBool("is_gray") {
		t.Fatal("非法 JSON 不应走灰度")
	}
	if calls := len(fake.Calls()); calls != 0 {
		t.Fatalf("非法 JSON 不应触达 DB，共 %d 次", calls)
	}
}

// TestGray_MissingTenantPassthrough tenant 缺失直通（由后续中间件裁决）
func TestGray_MissingTenantPassthrough(t *testing.T) {
	fake, _, gray := newGrayTest(t)

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		bytes.NewReader([]byte(`{"model":"gpt-4o"}`)))
	c.Set("trace_id", "t-no-tenant")

	gray(c)
	if c.GetBool("is_gray") {
		t.Fatal("tenant 缺失不应走灰度")
	}
	if calls := len(fake.Calls()); calls != 0 {
		t.Fatalf("tenant 缺失不应触达 DB，共 %d 次", calls)
	}
}

// countCalls 统计指定方法的调用次数
func countCalls(fake *dbtest.FakeDB, method string) int {
	n := 0
	for _, c := range fake.Calls() {
		if c.Method == method {
			n++
		}
	}
	return n
}
