package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/provider"
	"github.com/gin-gonic/gin"
)

// doChatRequestWith 用指定 body 发起 chat 请求
func doChatRequestWith(t *testing.T, r *gin.Engine, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// assertInvalidRequest 断言 400 + invalid_request_error 契约
func assertInvalidRequest(t *testing.T, w *httptest.ResponseRecorder, wantCode string) {
	t.Helper()
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("响应不是 JSON: %v, raw=%q", err, w.Body.String())
	}
	errObj, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("error 字段缺失: %v", resp)
	}
	if errObj["type"] != "invalid_request_error" || errObj["code"] != wantCode {
		t.Errorf("error 契约 = %v, want code=%s", errObj, wantCode)
	}
}

func TestValidateTooManyMessages(t *testing.T) {
	r, _ := newChatTestRouter(t, nil)

	msgs := make([]map[string]string, 0, maxMessages+1)
	for i := 0; i < maxMessages+1; i++ {
		msgs = append(msgs, map[string]string{"role": "user", "content": "hi"})
	}
	body, _ := json.Marshal(map[string]any{"model": "test-model", "messages": msgs})

	w := doChatRequestWith(t, r, body)
	assertInvalidRequest(t, w, "too_many_messages")
}

func TestValidateContentTooLarge(t *testing.T) {
	r, _ := newChatTestRouter(t, nil)

	body, _ := json.Marshal(map[string]any{
		"model": "test-model",
		"messages": []map[string]string{
			{"role": "user", "content": strings.Repeat("a", maxContentSize+1)},
		},
	})

	w := doChatRequestWith(t, r, body)
	assertInvalidRequest(t, w, "content_too_large")
}

func TestValidateInvalidModelName(t *testing.T) {
	r, _ := newChatTestRouter(t, nil)

	for _, model := range []string{
		"bad model",              // 空格
		"model/inj;ection",       // 分号
		"模型",                     // 非 ASCII
		strings.Repeat("m", 129), // 超 128 字符
		"",                       // 空（binding required 也拦，校验层同样兜住）
	} {
		body, _ := json.Marshal(map[string]any{
			"model":    model,
			"messages": []map[string]string{{"role": "user", "content": "hi"}},
		})
		w := doChatRequestWith(t, r, body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("model %q: status = %d, want 400", model, w.Code)
			continue
		}
		var resp map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		errObj, _ := resp["error"].(map[string]any)
		if errObj["type"] != "invalid_request_error" {
			t.Errorf("model %q: error = %v, want invalid_request_error", model, errObj)
		}
	}
}

// TestValidateBoundaryAccepted 边界值放行：100 条 messages / 512KB content / 128 字符模型名
func TestValidateBoundaryAccepted(t *testing.T) {
	fake := &fakeProvider{name: "fake-boundary", response: &provider.ChatResponse{
		Content: "ok", InputTokens: 1, OutputTokens: 1,
	}}
	r, registry := newChatTestRouter(t, []routing.Ability{{ID: 0, Provider: "fake-boundary", ProviderModel: "m"}})
	registerFake(registry, "fake-boundary", fake)

	msgs := make([]map[string]string, 0, maxMessages)
	for i := 0; i < maxMessages; i++ {
		msgs = append(msgs, map[string]string{"role": "user", "content": "hi"})
	}
	body, _ := json.Marshal(map[string]any{"model": strings.Repeat("m", 128), "messages": msgs})

	w := doChatRequestWith(t, r, body)
	if w.Code != http.StatusOK {
		t.Fatalf("100 条 messages + 128 字符模型名应放行, status = %d, body=%s", w.Code, w.Body.String())
	}

	body2, _ := json.Marshal(map[string]any{
		"model": "test-model",
		"messages": []map[string]string{
			{"role": "user", "content": strings.Repeat("a", maxContentSize)},
		},
	})
	w2 := doChatRequestWith(t, r, body2)
	if w2.Code != http.StatusOK {
		t.Fatalf("恰好 512KB content 应放行, status = %d, body=%s", w2.Code, w2.Body.String())
	}
}

// TestValidateModelNameAllowedChars 白名单字符集内的模型名放行
func TestValidateModelNameAllowedChars(t *testing.T) {
	fake := &fakeProvider{name: "fake-chars", response: &provider.ChatResponse{
		Content: "ok", InputTokens: 1, OutputTokens: 1,
	}}
	r, registry := newChatTestRouter(t, []routing.Ability{{ID: 0, Provider: "fake-chars", ProviderModel: "m"}})
	registerFake(registry, "fake-chars", fake)

	for _, model := range []string{"gpt-4o", "claude-3.5_sonnet", "openai/gpt-4o", "arn:aws:bedrock:us::model/x", "a.b:c/d-e_f"} {
		body, _ := json.Marshal(map[string]any{
			"model":    model,
			"messages": []map[string]string{{"role": "user", "content": "hi"}},
		})
		w := doChatRequestWith(t, r, body)
		if w.Code != http.StatusOK {
			t.Errorf("model %q 应放行, status = %d, body=%s", model, w.Code, w.Body.String())
		}
	}
}
