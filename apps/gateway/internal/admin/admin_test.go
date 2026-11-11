// 阶段 15：Admin API 端到端测试（dbtest fake DB + miniredis 广播）
package admin

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/pkg/broadcast"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/dbtest"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
)

const testToken = "test-admin-token"

// newAdminTestApp 构造带 fake DB + miniredis + 广播总线的完整 App 与管理面引擎
func newAdminTestApp(t *testing.T) (*app.App, *dbtest.FakeDB, *gin.Engine) {
	t.Helper()
	db, fake := dbtest.New(t)
	s := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { cli.Close() })

	a := app.New(app.Options{
		Config: config.Default(),
		DB:     db,
		Redis:  cli,
		Bus:    broadcast.Init(cli),
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = a.Close(ctx)
	})

	router := NewRouter(a, testToken)
	if router == nil {
		t.Fatal("NewRouter 返回 nil（token 非空）")
	}
	return a, fake, router
}

// do 执行一次带 token 的管理面请求
func do(router *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Token", testToken)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// drainAsync 排空异步任务（审计写库 / 广播发布）后校验 fake 预期
func drainAsync(t *testing.T, a *app.App, fake *dbtest.FakeDB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.Async.Wait(ctx); err != nil {
		t.Fatalf("async drain: %v", err)
	}
	fake.AssertExpectations(t)
}

// --- 鉴权 ---

func TestAdminRequiresToken(t *testing.T) {
	_, _, router := newAdminTestApp(t)

	for _, req := range []struct{ method, path string }{
		{http.MethodGet, "/admin/v1/tenants"},
		{http.MethodPost, "/admin/v1/tenants"},
		{http.MethodGet, "/admin/v1/config"},
	} {
		r := httptest.NewRequest(req.method, req.path, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Errorf("%s %s 无 token = %d, want 401", req.method, req.path, w.Code)
		}
	}

	// 错误 token 同样 401
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants", nil)
	req.Header.Set("X-Admin-Token", "wrong")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("错误 token = %d, want 401", w.Code)
	}
}

// token 为空 → NewRouter 返回 nil（main 不启动管理面）
func TestAdminEmptyTokenDisabled(t *testing.T) {
	db, _ := dbtest.New(t)
	a := app.New(app.Options{Config: config.Default(), DB: db})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = a.Close(ctx)
	}()
	if NewRouter(a, "") != nil {
		t.Fatal("token 为空应返回 nil")
	}
}

// --- 租户 CRUD + 审计闭环 ---

func TestTenantCreateUpdateFlow(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	// 1. 创建租户
	fake.ExpectExec("INSERT INTO tenants").WillReturnResult(42, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(1, 1)

	w := do(router, http.MethodPost, "/admin/v1/tenants", `{"name":"acme","tier":"pro","budget_monthly":500}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create tenant = %d, body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Data struct {
			ID     int64  `json:"id"`
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Data.ID != 42 {
		t.Fatalf("create 响应异常: %v %+v", err, resp)
	}
	drainAsync(t, a, fake)

	// 2. 更新配额与状态 → 审计 + auth/quota 定向失效广播
	fake.ExpectExec("UPDATE tenants").WillReturnResult(0, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(2, 1)

	w = do(router, http.MethodPatch, "/admin/v1/tenants/42", `{"rps_limit":100,"status":"disabled"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("update tenant = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)

	// 3. 更新不存在的租户 → 404
	fake.ExpectExec("UPDATE tenants").WillReturnResult(0, 0)
	w = do(router, http.MethodPatch, "/admin/v1/tenants/999", `{"tier":"pro"}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("update missing tenant = %d, want 404", w.Code)
	}
	drainAsync(t, a, fake)

	// 4. 非法字段 → 400，不触库
	w = do(router, http.MethodPatch, "/admin/v1/tenants/42", `{"password":"x"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("非法字段 = %d, want 400", w.Code)
	}
	drainAsync(t, a, fake)
}

// budget_monthly 变更时必须同步写入 budget_micro（精确 μUSD，0 → NULL）
func TestTenantBudgetMicroSync(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	fake.ExpectExec("INSERT INTO tenants").WillReturnResult(1, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(1, 1)
	w := do(router, http.MethodPost, "/admin/v1/tenants", `{"name":"acme","budget_monthly":500}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create tenant = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)

	fake.ExpectExec("UPDATE tenants").WillReturnResult(0, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(2, 1)
	w = do(router, http.MethodPatch, "/admin/v1/tenants/1", `{"budget_monthly":12.5}`)
	if w.Code != http.StatusOK {
		t.Fatalf("update tenant = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)

	var insertVals, updateVals []any
	for _, c := range fake.Calls() {
		switch c.Method {
		case "exec":
			if strings.Contains(c.SQL, "INSERT INTO tenants") {
				for _, v := range c.Args {
					insertVals = append(insertVals, v.Value)
				}
			}
			if strings.Contains(c.SQL, "UPDATE tenants") {
				for _, v := range c.Args {
					updateVals = append(updateVals, v.Value)
				}
			}
		}
	}
	// INSERT (name, tier, budget_monthly, budget_micro, rps, tpm, concurrency)
	if len(insertVals) < 4 || insertVals[3] != int64(500_000_000) {
		t.Fatalf("insert budget_micro = %v, want 500000000 (args=%v)", insertVals, insertVals)
	}
	// UPDATE SET budget_micro = ?, budget_monthly = ? WHERE id = ?
	if len(updateVals) < 1 || updateVals[0] != int64(12_500_000) {
		t.Fatalf("update budget_micro = %v, want 12500000 (args=%v)", updateVals, updateVals)
	}
}

// 租户列表 / 详情查询
func TestTenantListGet(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	cols := []string{"id", "name", "tier", "budget_monthly", "status",
		"rps_limit", "tpm_limit", "concurrency_limit", "cache_enabled", "created_at"}
	fake.ExpectQuery("FROM tenants ORDER BY id").WillReturnRows(cols,
		[]driver.Value{int64(1), "acme", "pro", 500.0, "active", int64(100), nil, nil, true, nil})
	fake.ExpectQuery("FROM tenants WHERE id = ?").WillReturnRows(cols,
		[]driver.Value{int64(1), "acme", "pro", 500.0, "active", int64(100), nil, nil, true, nil})

	w := do(router, http.MethodGet, "/admin/v1/tenants", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"acme"`) {
		t.Fatalf("list = %d, body=%s", w.Code, w.Body.String())
	}
	w = do(router, http.MethodGet, "/admin/v1/tenants/1", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"rps_limit":100`) {
		t.Fatalf("get = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)
}

// 租户列表游标分页 / 状态过滤
func TestTenantListPagination(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	cols := []string{"id", "name", "tier", "budget_monthly", "status",
		"rps_limit", "tpm_limit", "concurrency_limit", "cache_enabled", "created_at"}
	fake.ExpectQuery("FROM tenants ORDER BY id").WillReturnRows(cols,
		[]driver.Value{int64(1), "acme", "pro", 500.0, "active", nil, nil, nil, true, nil},
		[]driver.Value{int64(2), "beta", "free", 0.0, "active", nil, nil, nil, false, nil})
	fake.ExpectQuery("FROM tenants WHERE id > ? AND status = ?").WillReturnRows(cols,
		[]driver.Value{int64(2), "beta", "free", 0.0, "active", nil, nil, nil, false, nil})

	w := do(router, http.MethodGet, "/admin/v1/tenants?limit=1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("first page = %d, body=%s", w.Code, w.Body.String())
	}
	var page struct {
		Data       []tenantRow `json:"data"`
		NextCursor *int64      `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatalf("first page 响应解析失败: %v", err)
	}
	if len(page.Data) != 1 || page.Data[0].ID != 1 || page.NextCursor == nil || *page.NextCursor != 1 {
		t.Fatalf("first page = %+v, want 1 row and next_cursor=1", page)
	}

	w = do(router, http.MethodGet, "/admin/v1/tenants?limit=10&cursor=1&status=active", "")
	if w.Code != http.StatusOK {
		t.Fatalf("second page = %d, body=%s", w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatalf("second page 响应解析失败: %v", err)
	}
	if len(page.Data) != 1 || page.Data[0].ID != 2 || page.NextCursor != nil {
		t.Fatalf("second page = %+v, want 1 row and next_cursor=null", page)
	}

	w = do(router, http.MethodGet, "/admin/v1/tenants?status=unknown", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid status = %d, want 400", w.Code)
	}
	drainAsync(t, a, fake)
}

// --- API Key 签发 / 吊销 / 轮换（阶段 16 闭环）---

func TestAPIKeyLifecycle(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	// 1. 签发：INSERT api_keys + 审计；响应含明文 key（只此一次）
	fake.ExpectExec("INSERT INTO api_keys").WillReturnResult(7, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(1, 1)

	w := do(router, http.MethodPost, "/admin/v1/tenants/42/keys", `{"name":"prod","expires_in_days":30}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create key = %d, body=%s", w.Code, w.Body.String())
	}
	var createResp struct {
		Data struct {
			ID     int64  `json:"id"`
			APIKey string `json:"api_key"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("create 响应解析失败: %v", err)
	}
	if !strings.HasPrefix(createResp.Data.APIKey, "sk-") || len(createResp.Data.APIKey) != 3+48 {
		t.Fatalf("明文 key 格式异常: %q", createResp.Data.APIKey)
	}
	drainAsync(t, a, fake)

	// 2. 轮换：SELECT 旧 key → INSERT 新 key → UPDATE retire_at + 审计
	fake.ExpectQuery("FROM api_keys WHERE id = ?").WillReturnRows(
		[]string{"tenant_id", "name"}, []driver.Value{int64(42), "prod"})
	fake.ExpectExec("INSERT INTO api_keys").WillReturnResult(8, 1)
	fake.ExpectExec("UPDATE api_keys SET retire_at").WillReturnResult(0, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(2, 1)

	w = do(router, http.MethodPost, "/admin/v1/keys/7/rotate", `{"grace_period_seconds":3600}`)
	if w.Code != http.StatusOK {
		t.Fatalf("rotate = %d, body=%s", w.Code, w.Body.String())
	}
	var rotResp struct {
		Data struct {
			OldID    int64  `json:"old_id"`
			NewID    int64  `json:"new_id"`
			APIKey   string `json:"api_key"`
			RetireAt string `json:"retire_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &rotResp); err != nil {
		t.Fatalf("rotate 响应解析失败: %v", err)
	}
	if rotResp.Data.OldID != 7 || rotResp.Data.NewID != 8 {
		t.Errorf("rotate id 异常: %+v", rotResp.Data)
	}
	if !strings.HasPrefix(rotResp.Data.APIKey, "sk-") {
		t.Errorf("rotate 未返回新明文 key: %q", rotResp.Data.APIKey)
	}
	if rotResp.Data.RetireAt == "" {
		t.Error("rotate 响应缺少 retire_at")
	}
	drainAsync(t, a, fake)

	// 3. 吊销：DELETE + 审计
	fake.ExpectExec("DELETE FROM api_keys WHERE id = ?").WillReturnResult(0, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(3, 1)

	w = do(router, http.MethodDelete, "/admin/v1/keys/8", "")
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)
}

// 列表不返回 hash_key（安全验收：DB 只有哈希，响应连哈希都不给）
func TestAPIKeyListNoHash(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	fake.ExpectQuery("FROM api_keys WHERE tenant_id = ?").WillReturnRows(
		[]string{"id", "tenant_id", "name", "expires_at", "retire_at", "created_at"},
		[]driver.Value{int64(7), int64(42), "prod", nil, nil, nil})

	w := do(router, http.MethodGet, "/admin/v1/tenants/42/keys", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list keys = %d", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "hash") {
		t.Errorf("key 列表泄露 hash 字段: %s", body)
	}
	drainAsync(t, a, fake)
}

// --- 配置热更新（Admin 入口 + gw:config 推送）---

func TestConfigHotUpdateViaAdmin(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	before := a.Hot.DefaultRPSLimit()

	// PUT /config：本节点立即生效 + 审计；payload 推送到 gw:config 频道
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(1, 1)
	w := do(router, http.MethodPut, "/admin/v1/config",
		`{"default_rps_limit": 88, "request_timeout": "45s"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT config = %d, body=%s", w.Code, w.Body.String())
	}
	if got := a.Hot.DefaultRPSLimit(); got != 88 {
		t.Fatalf("热更新未生效: DefaultRPSLimit = %d, want 88（原值 %d）", got, before)
	}
	if got := a.Hot.RequestTimeout(); got != 45*time.Second {
		t.Fatalf("RequestTimeout = %v, want 45s", got)
	}
	drainAsync(t, a, fake)

	// GET /config 回读快照
	w = do(router, http.MethodGet, "/admin/v1/config", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"default_rps_limit":88`) {
		t.Fatalf("GET config = %d, body=%s", w.Code, w.Body.String())
	}

	// 非法项拒绝
	w = do(router, http.MethodPut, "/admin/v1/config", `{"default_rps_limit": -1}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("非法值 = %d, want 400", w.Code)
	}
	drainAsync(t, a, fake)
}

// --- channel_keys：拒绝明文 key_ref + 触发 KeyPool 重载 ---

func TestChannelKeyRefValidation(t *testing.T) {
	_, _, router := newAdminTestApp(t)

	// 明文 key_ref 拒绝（阶段 16：不落库）
	w := do(router, http.MethodPost, "/admin/v1/channel_keys",
		`{"channel_id":1,"key_ref":"sk-plaintext-secret"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("明文 key_ref = %d, want 400, body=%s", w.Code, w.Body.String())
	}
}

func TestChannelKeyCreateReloadsPool(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	// 创建：INSERT + KeyPool 重载（SELECT channel_keys）+ abilities 失效广播 + 审计
	fake.ExpectExec("INSERT INTO channel_keys").WillReturnResult(9, 1)
	fake.ExpectQuery("FROM channel_keys").WillReturnRows(
		[]string{"id", "channel_id", "key_ref", "weight", "enabled", "consecutive_failures", "disabled_until", "disabled_reason"},
		[]driver.Value{int64(9), int64(1), "env://TEST_ADMIN_CK", int64(1), true, int64(0), nil, nil})
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(1, 1)

	w := do(router, http.MethodPost, "/admin/v1/channel_keys",
		`{"channel_id":1,"key_ref":"env://TEST_ADMIN_CK","weight":3}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create channel_key = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)

	// KeyPool 已重载：ability 1 的池子应含新 key（env 未设置 → 解析为空，池内无可用 key 但 pool 存在）
	// 这里只验证流程闭环（SELECT 重载被执行过）；key 解析细节由 keypool 测试覆盖
}

// --- 审计查询 ---

func TestAuditLogsList(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	fake.ExpectQuery("FROM admin_audit_logs").WillReturnRows(
		[]string{"id", "actor", "action", "payload", "created_at"},
		[]driver.Value{int64(1), "admin", "tenant.create", `{"id":42}`, "2026-11-11 10:00:00"})

	w := do(router, http.MethodGet, "/admin/v1/audit_logs?limit=10", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "tenant.create") {
		t.Fatalf("audit list = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)
}

// --- 能力表 CRUD（带 abilities 缓存失效） ---

func TestAbilityCRUD(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	fake.ExpectExec("INSERT INTO abilities").WillReturnResult(5, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(1, 1)
	w := do(router, http.MethodPost, "/admin/v1/abilities",
		`{"tenant_id":1,"model_name":"gpt-4o","provider":"openai","provider_model":"gpt-4o","weight":10}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create ability = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)

	fake.ExpectExec("UPDATE abilities").WillReturnResult(0, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(2, 1)
	w = do(router, http.MethodPatch, "/admin/v1/abilities/5", `{"enabled":false,"weight":0}`)
	if w.Code != http.StatusOK {
		t.Fatalf("update ability = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)

	fake.ExpectExec("DELETE FROM abilities WHERE id = ?").WillReturnResult(0, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(3, 1)
	w = do(router, http.MethodDelete, "/admin/v1/abilities/5", "")
	if w.Code != http.StatusOK {
		t.Fatalf("delete ability = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)
}

// --- 灰度规则 CRUD（带 gray 缓存失效） ---

func TestGrayRuleCRUD(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	fake.ExpectExec("INSERT INTO gray_rules").WillReturnResult(3, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(1, 1)
	w := do(router, http.MethodPost, "/admin/v1/gray_rules",
		`{"tenant_id":1,"model_name":"gpt-4o","target_model":"gpt-4o-mini","weight":10}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create gray_rule = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)

	// weight 越界拒绝
	w = do(router, http.MethodPost, "/admin/v1/gray_rules",
		`{"tenant_id":1,"model_name":"m","target_model":"n","weight":101}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("weight 101 = %d, want 400", w.Code)
	}

	fake.ExpectExec("DELETE FROM gray_rules WHERE id = ?").WillReturnResult(0, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(2, 1)
	w = do(router, http.MethodDelete, "/admin/v1/gray_rules/3", "")
	if w.Code != http.StatusOK {
		t.Fatalf("delete gray_rule = %d", w.Code)
	}
	drainAsync(t, a, fake)
}

// --- models CRUD ---

func TestModelCRUD(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	fake.ExpectExec("INSERT INTO models").WillReturnResult(11, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(1, 1)
	w := do(router, http.MethodPost, "/admin/v1/models",
		`{"tenant_id":1,"model_name":"gpt-4o","provider":"openai","provider_model":"gpt-4o-2024-08-06","input_price_1k":0.0025,"output_price_1k":0.01}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create model = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)

	fake.ExpectExec("UPDATE models").WillReturnResult(0, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(2, 1)
	w = do(router, http.MethodPatch, "/admin/v1/models/11", `{"is_active":false}`)
	if w.Code != http.StatusOK {
		t.Fatalf("update model = %d, body=%s", w.Code, w.Body.String())
	}
	drainAsync(t, a, fake)

	fake.ExpectExec("DELETE FROM models WHERE id = ?").WillReturnResult(0, 0)
	w = do(router, http.MethodDelete, "/admin/v1/models/999", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("delete missing model = %d, want 404", w.Code)
	}
	drainAsync(t, a, fake)
}

// --- 跨节点广播验证：写操作 → gw:events 频道收到 cache_invalidate{prefix=auth} ---

func TestWritePublishesInvalidation(t *testing.T) {
	a, fake, router := newAdminTestApp(t)

	// 订阅者（模拟另一个网关节点）
	sub := a.Redis.Subscribe(context.Background(), broadcast.Channel)
	defer sub.Close()
	ch := sub.Channel()

	fake.ExpectExec("DELETE FROM api_keys WHERE id = ?").WillReturnResult(0, 1)
	fake.ExpectExec("INSERT INTO admin_audit_logs").WillReturnResult(1, 1)

	if w := do(router, http.MethodDelete, "/admin/v1/keys/7", ""); w.Code != http.StatusOK {
		t.Fatalf("revoke = %d", w.Code)
	}

	// 100ms 级传播断言：1s 内必须收到（生产广播延迟 < 100ms，此处放宽）
	select {
	case msg := <-ch:
		if !strings.Contains(msg.Payload, "cache_invalidate") || !strings.Contains(msg.Payload, "auth") {
			t.Errorf("广播事件异常: %s", msg.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("1s 内未收到 cache_invalidate 广播")
	}
	drainAsync(t, a, fake)
}
