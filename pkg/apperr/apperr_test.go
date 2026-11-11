package apperr

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// newTestContext 构造带可选 trace_id 的 gin 测试上下文
func newTestContext(traceID string) (*httptest.ResponseRecorder, *gin.Context) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if traceID != "" {
		c.Set(traceIDContextKey, traceID)
	}
	return w, c
}

// parseEnvelope 解析统一错误响应体
func parseEnvelope(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应不是 JSON: %v, raw=%q", err, w.Body.String())
	}
	return body
}

func TestWriteUnifiedShape(t *testing.T) {
	w, c := newTestContext("trace-abc-123")

	Write(c, http.StatusBadRequest, "invalid_request_error", "invalid_request_body", "bad body")

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	body := parseEnvelope(t, w)

	errObj, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("error 字段缺失或类型不对: %v", body["error"])
	}
	if errObj["type"] != "invalid_request_error" {
		t.Errorf("error.type = %v", errObj["type"])
	}
	if errObj["code"] != "invalid_request_body" {
		t.Errorf("error.code = %v", errObj["code"])
	}
	if errObj["message"] != "bad body" {
		t.Errorf("error.message = %v", errObj["message"])
	}
	if body["request_id"] != "trace-abc-123" {
		t.Errorf("request_id = %v, want trace-abc-123", body["request_id"])
	}
}

func TestWriteRequestIDMatchesTraceIDAndHeader(t *testing.T) {
	// 模拟 Trace 中间件已注入 trace_id 并写过响应头的场景
	w, c := newTestContext("trace-xyz")
	c.Header(HeaderRequestID, "trace-xyz")

	Abort(c, http.StatusUnauthorized, "authentication_error", "missing_api_key", "missing api key")

	body := parseEnvelope(t, w)
	if body["request_id"] != "trace-xyz" {
		t.Errorf("request_id = %v, want trace-xyz（与日志 trace_id 一致）", body["request_id"])
	}
	if got := w.Header().Get(HeaderRequestID); got != "trace-xyz" {
		t.Errorf("X-Request-Id header = %q, want trace-xyz", got)
	}
	if !c.IsAborted() {
		t.Error("Abort 应中断后续处理")
	}
}

func TestWriteFallbackGeneratesRequestID(t *testing.T) {
	// Trace 中间件缺失（如 Recover 兜住 Trace 自身的 panic）时仍必须带 request_id
	w, c := newTestContext("")

	Write(c, http.StatusInternalServerError, "internal_error", "internal_panic", "internal error")

	body := parseEnvelope(t, w)
	id, _ := body["request_id"].(string)
	if len(id) != 32 {
		t.Errorf("request_id = %q, want 32 位十六进制", id)
	}
	// 兜底 ID 必须回写 context，保证同请求后续日志用同一个 ID
	if c.GetString(traceIDContextKey) != id {
		t.Errorf("trace_id 未回写 context: %q", c.GetString(traceIDContextKey))
	}
	if w.Header().Get(HeaderRequestID) != id {
		t.Errorf("X-Request-Id header = %q, want %q", w.Header().Get(HeaderRequestID), id)
	}

	// 同一 trace_id 的后续错误响应，ID 保持不变（模拟同请求后续中间件再写错误）
	w2, c2 := newTestContext(c.GetString(traceIDContextKey))
	Write(c2, http.StatusBadGateway, "upstream_error", "upstream_exhausted", "exhausted")
	body2 := parseEnvelope(t, w2)
	if body2["request_id"] != id {
		t.Errorf("request_id 漂移: %v → %v", id, body2["request_id"])
	}
}

func TestBodyBuilderMatchesWriteContract(t *testing.T) {
	// SSE 错误帧等非 JSON 响应场景复用同一契约
	_, c := newTestContext("trace-frame")
	body := Body(c, "upstream_error", "stream_upstream_error", "upstream provider error")

	errObj, ok := body["error"].(map[string]string)
	if !ok {
		t.Fatalf("error 字段类型不对: %T", body["error"])
	}
	if errObj["type"] != "upstream_error" || errObj["code"] != "stream_upstream_error" || errObj["message"] != "upstream provider error" {
		t.Errorf("error 字段内容不对: %v", errObj)
	}
	if body["request_id"] != "trace-frame" {
		t.Errorf("request_id = %v, want trace-frame", body["request_id"])
	}

	// Body 产物必须可序列化为合法 JSON（SSE data 帧直接发送）
	b, err := json.Marshal(body)
	if err != nil || !strings.Contains(string(b), `"request_id":"trace-frame"`) {
		t.Errorf("Body 序列化失败: %v %s", err, b)
	}
}
