package middleware

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ai-gateway/pkg/dbtest"
	"github.com/gin-gonic/gin"
)

// newAuthTest 构建独立 App + Auth 中间件实例（缓存实例级，测试间天然隔离）
// 返回 fake DB（预设查询用）与中间件实例
func newAuthTest(t *testing.T) (*dbtest.FakeDB, gin.HandlerFunc) {
	t.Helper()
	db, fake := dbtest.New(t)
	a := newTestApp(t, db, nil)
	return fake, Auth(a)
}

// runAuth 以指定 key 执行一次 Auth 中间件，返回响应记录器与 gin context
func runAuth(t *testing.T, auth gin.HandlerFunc, key string) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()
	return runMiddleware(t, auth, func(c *gin.Context) {
		if key != "" {
			c.Request.Header.Set("Authorization", "Bearer "+key)
		}
	})
}

// expectValidKey 预设一组合法 key 的 DB 返回（api_keys + tenants 各查一次）
func expectValidKey(fake *dbtest.FakeDB) {
	fake.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(2), int64(1), int64(7), nil, nil},
	)
	fake.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"status"},
		[]driver.Value{"active"},
	)
}

func TestAuthCachesPositiveLookup(t *testing.T) {
	fake, auth := newAuthTest(t)
	expectValidKey(fake) // 只预设一组：第二次起任何 DB 查询都会报错

	// 第 1 次：回源 DB
	w, c := runAuth(t, auth, "sk-test-key")
	if w.Code != http.StatusOK || c.IsAborted() {
		t.Fatalf("首次请求应放行，w.Code = %d", w.Code)
	}
	if got := c.GetInt64("tenant_id"); got != 1 {
		t.Errorf("tenant_id = %d; want 1", got)
	}
	if got := c.GetInt64("api_key_id"); got != 2 {
		t.Errorf("api_key_id = %d; want 2", got)
	}
	if got := c.GetInt64("project_id"); got != 7 {
		t.Errorf("project_id = %d; want 7", got)
	}
	if got := c.GetString("tenant_status"); got != "active" {
		t.Errorf("tenant_status = %q; want active", got)
	}

	// 后 999 次：全部走缓存，零 DB 查询（fake 对未预期查询直接报错 → 401，循环断言可捕获）
	for i := 0; i < 999; i++ {
		w, c = runAuth(t, auth, "sk-test-key")
		if w.Code != http.StatusOK || c.IsAborted() {
			t.Fatalf("第 %d 次请求应走缓存放行，w.Code = %d", i+2, w.Code)
		}
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 2 {
		t.Fatalf("1000 次请求共产生 %d 次 DB 调用; want 2（api_keys + tenants 各一次）", calls)
	}
}

func TestAuthInvalidKeyNotCached(t *testing.T) {
	fake, auth := newAuthTest(t)

	// 否定结果不缓存：两次请求各查一次 DB（共 2 次），都 401
	for i := 0; i < 2; i++ {
		fake.ExpectQuery("FROM api_keys").WillReturnRows(
			[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		)
	}

	for i := 0; i < 2; i++ {
		w, _ := runAuth(t, auth, "sk-bad-key")
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("第 %d 次应 401，w.Code = %d", i+1, w.Code)
		}
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 2 {
		t.Fatalf("否定结果被缓存了？共 %d 次 DB 调用; want 2", calls)
	}
}

func TestAuthExpiredKeyNotCached(t *testing.T) {
	fake, auth := newAuthTest(t)

	past := time.Now().Add(-time.Hour)
	for i := 0; i < 2; i++ {
		fake.ExpectQuery("FROM api_keys").WillReturnRows(
			[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
			[]driver.Value{int64(2), int64(1), nil, past, nil},
		)
	}

	for i := 0; i < 2; i++ {
		w, _ := runAuth(t, auth, "sk-expired-key")
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("第 %d 次应 401，w.Code = %d", i+1, w.Code)
		}
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 2 {
		t.Fatalf("过期 key 被缓存了？共 %d 次 DB 调用; want 2", calls)
	}
}

func TestAuthInactiveTenantNotCached(t *testing.T) {
	fake, auth := newAuthTest(t)

	for i := 0; i < 2; i++ {
		fake.ExpectQuery("FROM api_keys").WillReturnRows(
			[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
			[]driver.Value{int64(2), int64(1), nil, nil, nil},
		)
		fake.ExpectQuery("FROM tenants").WillReturnRows(
			[]string{"status"},
			[]driver.Value{"suspended"},
		)
	}

	for i := 0; i < 2; i++ {
		w, _ := runAuth(t, auth, "sk-suspended-key")
		if w.Code != http.StatusForbidden {
			t.Fatalf("第 %d 次应 403，w.Code = %d", i+1, w.Code)
		}
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 4 {
		t.Fatalf("租户停用结果被缓存了？共 %d 次 DB 调用; want 4", calls)
	}
}

func TestAuthCachedEntryExpiryRecheck(t *testing.T) {
	fake, auth := newAuthTest(t)

	// 第一次：key 120ms 后过期 → 通过并写入缓存
	soon := time.Now().Add(120 * time.Millisecond)
	fake.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(2), int64(1), nil, soon, nil},
	)
	fake.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"status"},
		[]driver.Value{"active"},
	)
	// 第二次：缓存条目已过期 → 回源 DB，DB 里仍是过期状态 → 401
	past := time.Now().Add(-time.Hour)
	fake.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(2), int64(1), nil, past, nil},
	)

	w, _ := runAuth(t, auth, "sk-soon-expire")
	if w.Code != http.StatusOK {
		t.Fatalf("未过期时应放行，w.Code = %d", w.Code)
	}

	time.Sleep(200 * time.Millisecond) // 等缓存条目越过 expires_at（TTL 未到但绝对过期时间已到）

	w, _ = runAuth(t, auth, "sk-soon-expire")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("过期后应回源 DB 并 401，w.Code = %d", w.Code)
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 3 {
		t.Fatalf("共 %d 次 DB 调用; want 3", calls)
	}
}

func TestAuthDBErrorNotCached(t *testing.T) {
	fake, auth := newAuthTest(t)

	for i := 0; i < 2; i++ {
		fake.ExpectQuery("FROM api_keys").WillReturnError(errors.New("connection refused"))
	}

	for i := 0; i < 2; i++ {
		w, _ := runAuth(t, auth, "sk-any-key")
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("第 %d 次应 401，w.Code = %d", i+1, w.Code)
		}
	}

	fake.AssertExpectations(t)
	if calls := len(fake.Calls()); calls != 2 {
		t.Fatalf("DB 错误结果被缓存了？共 %d 次 DB 调用; want 2", calls)
	}
}

func TestAuthMissingHeaderNoDBCall(t *testing.T) {
	fake, auth := newAuthTest(t)

	w, _ := runAuth(t, auth, "")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("无 header 应 401，w.Code = %d", w.Code)
	}
	if calls := len(fake.Calls()); calls != 0 {
		t.Fatalf("无 header 不应触达 DB，共 %d 次调用", calls)
	}
}

func TestAuthErrorUnifiedContract(t *testing.T) {
	_, auth := newAuthTest(t)

	// 未走 Trace 中间件的纯 Auth 调用：apperr 兜底生成 request_id
	w, _ := runAuth(t, auth, "")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("w.Code = %d, want 401", w.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应不是 JSON: %v, raw=%q", err, w.Body.String())
	}
	errObj, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("error 字段缺失: %v", body)
	}
	if errObj["type"] != "authentication_error" || errObj["code"] != "missing_api_key" {
		t.Errorf("error 契约不对: %v", errObj)
	}
	id, _ := body["request_id"].(string)
	if id == "" {
		t.Error("request_id 缺失")
	}
	if w.Header().Get("X-Request-Id") != id {
		t.Errorf("X-Request-Id = %q, 与 request_id = %q 不一致", w.Header().Get("X-Request-Id"), id)
	}
}

func TestAuthMalformedHeader(t *testing.T) {
	fake, auth := newAuthTest(t)

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	c.Request.Header.Set("Authorization", "NotBearer xyz")
	auth(c)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("非法 header 格式应 401，w.Code = %d", w.Code)
	}
	if calls := len(fake.Calls()); calls != 0 {
		t.Fatalf("非法 header 不应触达 DB，共 %d 次调用", calls)
	}
}

// TestAuth_TwoAppsIsolated 两个 App 的 Auth 中间件缓存互不干扰（阶段 14 验收）
func TestAuth_TwoAppsIsolated(t *testing.T) {
	// App1：key 映射到 tenant 1；App2：同 key 映射到 tenant 99
	db1, fake1 := dbtest.New(t)
	db2, fake2 := dbtest.New(t)
	a1 := newTestApp(t, db1, nil)
	a2 := newTestApp(t, db2, nil)
	auth1 := Auth(a1)
	auth2 := Auth(a2)

	fake1.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(2), int64(1), nil, nil, nil},
	)
	fake1.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"status"}, []driver.Value{"active"},
	)
	fake2.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(3), int64(99), nil, nil, nil},
	)
	fake2.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"status"}, []driver.Value{"active"},
	)

	_, c1 := runAuth(t, auth1, "sk-shared-key")
	_, c2 := runAuth(t, auth2, "sk-shared-key")
	if got := c1.GetInt64("tenant_id"); got != 1 {
		t.Fatalf("App1 tenant_id = %d, want 1", got)
	}
	if got := c2.GetInt64("tenant_id"); got != 99 {
		t.Fatalf("App2 tenant_id = %d, want 99（串缓存了）", got)
	}
	fake1.AssertExpectations(t)
	fake2.AssertExpectations(t)
}

// --- 阶段 16：API key 轮换宽限期（retire_at） ---

// 共存期内（retire_at 在未来）旧 key 仍可用
func TestAuthRetiredKeyWithinGraceStillValid(t *testing.T) {
	fake, auth := newAuthTest(t)
	future := time.Now().Add(time.Hour)
	fake.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(2), int64(1), nil, nil, future},
	)
	fake.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"status"}, []driver.Value{"active"},
	)

	w, c := runAuth(t, auth, "sk-old-key")
	if w.Code != http.StatusOK || c.IsAborted() {
		t.Fatalf("共存期内旧 key 应放行, w.Code = %d", w.Code)
	}
	fake.AssertExpectations(t)
}

// 过宽限期（retire_at 已过）旧 key 401（检查在 tenants 查询之前，无需 tenants 期望）
func TestAuthRetiredKeyPastGraceRejected(t *testing.T) {
	fake, auth := newAuthTest(t)
	past := time.Now().Add(-time.Hour)
	fake.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(2), int64(1), nil, nil, past},
	)

	w, c := runAuth(t, auth, "sk-old-key")
	if w.Code != http.StatusUnauthorized || !c.IsAborted() {
		t.Fatalf("过宽限期旧 key 应 401, w.Code = %d", w.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应不是 JSON: %v", err)
	}
	errObj := body["error"].(map[string]any)
	if errObj["code"] != "api_key_retired" {
		t.Errorf("error.code = %v, want api_key_retired", errObj["code"])
	}
	fake.AssertExpectations(t)
}

// 缓存命中路径也要做 retire 校验（缓存条目含 retire_at）
func TestAuthCachedRetiredKeyRejected(t *testing.T) {
	fake, auth := newAuthTest(t)
	// 首次查询时 key 还在宽限期内 → 缓存
	future := time.Now().Add(50 * time.Millisecond)
	fake.ExpectQuery("FROM api_keys").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "expires_at", "retire_at"},
		[]driver.Value{int64(2), int64(1), nil, nil, future},
	)
	fake.ExpectQuery("FROM tenants").WillReturnRows(
		[]string{"status"}, []driver.Value{"active"},
	)

	w, _ := runAuth(t, auth, "sk-soon-retired")
	if w.Code != http.StatusOK {
		t.Fatalf("宽限期内应放行, got %d", w.Code)
	}

	// 等 retire_at 过期 → 缓存命中但 valid() 应拒绝（无新 DB 查询）
	time.Sleep(120 * time.Millisecond)
	w, c := runAuth(t, auth, "sk-soon-retired")
	if w.Code != http.StatusUnauthorized || !c.IsAborted() {
		t.Fatalf("缓存命中但已过 retire_at，应 401, got %d", w.Code)
	}
	fake.AssertExpectations(t)
}
