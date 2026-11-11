// 诊断包查询端点测试
package admin

import (
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// diagColumns request_diagnostics 查询列（与 scanDiagnostics 顺序一致）
var diagColumns = []string{
	"id", "trace_id", "tenant_id", "model", "stream", "request_digest", "attempts",
	"status_code", "error_type", "error_message", "total_cost", "duration_ms", "created_at",
}

// TestDiagnostics_ByTraceID 按 trace_id 精确查询：attempts 链路完整且 error_message 已脱敏
func TestDiagnostics_ByTraceID(t *testing.T) {
	_, fake, router := newAdminTestApp(t)

	attemptsJSON := `[
	  {"attempt":1,"channel_id":101,"provider":"openai","base_url":"https://api.openai.com",
	   "status":"failed","error_kind":"rate_limit","error_message":"429 too many","duration_ms":120,
	   "breaker_state_before":"closed"},
	  {"attempt":2,"channel_id":102,"provider":"anthropic","base_url":"https://api.anthropic.com",
	   "status":"success","duration_ms":890,"breaker_state_before":"closed"}
	]`
	errMsg := "upstream exhausted: API_KEY=sk-leakedkey999"
	errType := "rate_limit"
	createdAt := "2026-11-11 10:00:00.000"

	fake.ExpectQuery("WHERE trace_id = ?").WillReturnRows(diagColumns,
		[]driver.Value{int64(1), "trace-abc", int64(42), "gpt-4o", false,
			"len=100 sha256=xyz", []byte(attemptsJSON), int64(200), errType, errMsg,
			int64(3500), int64(1010), createdAt},
	)

	w := do(router, http.MethodGet, "/admin/v1/diagnostics?trace_id=trace-abc", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("data 长度 = %d, want 1", len(resp.Data))
	}

	entry := resp.Data[0]
	if entry["trace_id"] != "trace-abc" {
		t.Errorf("trace_id = %v", entry["trace_id"])
	}

	// attempts 链路完整：2 跳，含渠道 / provider / 耗时 / 熔断状态
	attempts, ok := entry["attempts"].([]any)
	if !ok || len(attempts) != 2 {
		t.Fatalf("attempts = %v", entry["attempts"])
	}
	a1 := attempts[0].(map[string]any)
	if a1["channel_id"].(float64) != 101 || a1["error_kind"] != "rate_limit" || a1["duration_ms"].(float64) != 120 {
		t.Errorf("attempt1 字段不完整: %v", a1)
	}
	a2 := attempts[1].(map[string]any)
	if a2["status"] != "success" || a2["breaker_state_before"] != "closed" {
		t.Errorf("attempt2 字段不完整: %v", a2)
	}

	// 明文 key 不出现在响应中
	if strings.Contains(w.Body.String(), "sk-leakedkey999") {
		t.Error("响应含明文 API key")
	}
	if !strings.Contains(w.Body.String(), "[REDACTED]") {
		t.Error("响应应含脱敏占位符")
	}
}

// TestDiagnostics_ByTenant 按租户 + 时间范围查询
func TestDiagnostics_ByTenant(t *testing.T) {
	_, fake, router := newAdminTestApp(t)

	createdAt := "2026-11-11 10:00:00.000"
	fake.ExpectQuery("WHERE tenant_id = ?").WillReturnRows(diagColumns,
		[]driver.Value{int64(2), "trace-1", int64(7), "gpt-4o-mini", true,
			"len=50 sha256=aaa", []byte(`[]`), int64(200), nil, nil,
			int64(100), int64(300), createdAt},
	)

	w := do(router, http.MethodGet, "/admin/v1/diagnostics?tenant_id=7&limit=50", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"trace_id":"trace-1"`) {
		t.Errorf("响应缺 trace-1: %s", w.Body.String())
	}
}

// TestDiagnostics_MissingParams 参数缺失 / 非法 → 400
func TestDiagnostics_MissingParams(t *testing.T) {
	_, _, router := newAdminTestApp(t)

	// 无 trace_id 也无 tenant_id
	w := do(router, http.MethodGet, "/admin/v1/diagnostics", "")
	if w.Code != http.StatusBadRequest {
		t.Errorf("无参数 status = %d, want 400", w.Code)
	}

	// tenant_id 非数字
	w = do(router, http.MethodGet, "/admin/v1/diagnostics?tenant_id=abc", "")
	if w.Code != http.StatusBadRequest {
		t.Errorf("非法 tenant_id status = %d, want 400", w.Code)
	}

	// from 格式错误
	w = do(router, http.MethodGet, "/admin/v1/diagnostics?tenant_id=1&from=not-a-date", "")
	if w.Code != http.StatusBadRequest {
		t.Errorf("非法 from status = %d, want 400", w.Code)
	}
}

// TestDiagnostics_TimeFormats from/to 支持 RFC3339 与 YYYY-MM-DD
func TestDiagnostics_TimeFormats(t *testing.T) {
	_, fake, router := newAdminTestApp(t)

	// 两次查询：RFC3339 与日期格式
	fake.ExpectQuery("WHERE tenant_id = ?").WillReturnRows(diagColumns)
	fake.ExpectQuery("WHERE tenant_id = ?").WillReturnRows(diagColumns)

	w := do(router, http.MethodGet,
		"/admin/v1/diagnostics?tenant_id=1&from=2026-11-01T00:00:00Z&to=2026-11-11T23:59:59Z", "")
	if w.Code != http.StatusOK {
		t.Errorf("RFC3339 status = %d, body = %s", w.Code, w.Body.String())
	}
	w = do(router, http.MethodGet, "/admin/v1/diagnostics?tenant_id=1&from=2026-11-01&to=2026-11-11", "")
	if w.Code != http.StatusOK {
		t.Errorf("YYYY-MM-DD status = %d, body = %s", w.Code, w.Body.String())
	}
}
