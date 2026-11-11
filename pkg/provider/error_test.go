package provider

import (
	"context"
	"errors"
	"net"
	"testing"
)

// TestClassifyByStatus 状态码 + 上游类型 → Kind 归类表驱动
func TestClassifyByStatus(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		errType    string
		wantKind   ErrorKind
	}{
		{"401 unauthorized", 401, "invalid_authentication_error", KindAuth},
		{"403 forbidden", 403, "permission_error", KindAuth},
		{"404 model not found", 404, "model_not_found", KindAuth},
		{"404 any type", 404, "not_found_error", KindAuth},
		{"408 request timeout", 408, "timeout_error", KindTimeout},
		{"425 too early", 425, "too_early", KindServer},
		{"429 rate limit", 429, "rate_limit_error", KindRateLimit},
		{"500 server error", 500, "server_error", KindServer},
		{"502 bad gateway", 502, "bad_gateway", KindServer},
		{"503 overloaded", 503, "service_unavailable", KindOverloaded},
		{"529 anthropic overloaded", 529, "overloaded_error", KindOverloaded},
		{"400 bad request", 400, "invalid_request_error", KindUnknown},
		{"422 unprocessable", 422, "invalid_request_error", KindUnknown},
		// 状态码不典型时按上游类型兜底归类
		{"type model_not_found fallback", 400, "model_not_found", KindAuth},
		{"type overload fallback", 500, "overloaded_error", KindOverloaded},
		{"zero status unknown", 0, "", KindUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewUpstreamError("test", tt.statusCode, tt.errType, "msg")
			if err.Kind != tt.wantKind {
				t.Errorf("Kind = %q, want %q", err.Kind, tt.wantKind)
			}
		})
	}
}

// TestKindDerivesPermanentAndRetryable Permanent / Retryable 完全由 Kind 派生
func TestKindDerivesPermanentAndRetryable(t *testing.T) {
	tests := []struct {
		kind      ErrorKind
		permanent bool
		retryable bool
	}{
		{KindAuth, true, true}, // 鉴权失败：禁用本渠道，换渠道重试
		{KindRateLimit, false, true},
		{KindServer, false, true},
		{KindTimeout, false, true},
		{KindOverloaded, false, true},
		{KindUnknown, false, false}, // 请求级错误：换渠道也不会成功
	}

	for _, tt := range tests {
		t.Run(string(tt.kind), func(t *testing.T) {
			e := &UpstreamError{Kind: tt.kind}
			if got := e.Permanent(); got != tt.permanent {
				t.Errorf("Permanent() = %v, want %v", got, tt.permanent)
			}
			if got := e.Retryable(); got != tt.retryable {
				t.Errorf("Retryable() = %v, want %v", got, tt.retryable)
			}
			// Kind 自身方法与 UpstreamError 方法必须一致（同一派生源）
			if e.Kind.Permanent() != tt.permanent || e.Kind.Retryable() != tt.retryable {
				t.Errorf("Kind methods inconsistent for %q", tt.kind)
			}
		})
	}
}

// timeoutNetError 模拟超时的 net.Error
type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

func TestNewNetworkErrorKind(t *testing.T) {
	tests := []struct {
		name     string
		cause    error
		wantKind ErrorKind
	}{
		{"context deadline", context.DeadlineExceeded, KindTimeout},
		{"net timeout", timeoutNetError{}, KindTimeout},
		{"connection refused", errors.New("connection refused"), KindServer},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewNetworkError("test", tt.cause)
			if err.Kind != tt.wantKind {
				t.Errorf("Kind = %q, want %q", err.Kind, tt.wantKind)
			}
			if !err.Retryable() {
				t.Error("network error should be retryable")
			}
			if err.Permanent() {
				t.Error("network error should not be permanent")
			}
			if !errors.Is(err, tt.cause) {
				t.Error("Unwrap should preserve cause for errors.Is")
			}
		})
	}
}

func TestNewNetworkErrorPreservesNetErrorChain(t *testing.T) {
	err := NewNetworkError("test", timeoutNetError{})
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Error("errors.As should still reach net.Error through Unwrap")
	}
}

func TestNewInvalidResponseError(t *testing.T) {
	err := NewInvalidResponseError("test", 200, "empty choices")
	if err.Kind != KindServer {
		t.Errorf("Kind = %q, want server", err.Kind)
	}
	if !err.Retryable() || err.Permanent() {
		t.Errorf("invalid response: retryable=%v permanent=%v, want retryable non-permanent",
			err.Retryable(), err.Permanent())
	}
}

func TestUpstreamErrorMessage(t *testing.T) {
	withCause := NewNetworkError("openai", errors.New("dial tcp: connection refused"))
	if got := withCause.Error(); got != "openai: upstream call failed: dial tcp: connection refused" {
		t.Errorf("Error() = %q", got)
	}

	plain := NewUpstreamError("openai", 429, "rate_limit_error", "slow down")
	want := "openai: upstream error [429] rate_limit_error: slow down"
	if got := plain.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// TestClassifyCoversOldStatusTable 旧 retryableStatus 表的行为必须被 Kind 派生完全覆盖（防回归）
func TestClassifyCoversOldStatusTable(t *testing.T) {
	// 旧表：401/403/404/408/425/429 + 5xx 可重试，其余不可重试
	retryable := map[int]bool{401: true, 403: true, 404: true, 408: true, 425: true, 429: true}
	for code := 400; code <= 599; code++ {
		retryable[code] = retryable[code] || (code >= 500 && code <= 599)
	}
	for code := 400; code <= 599; code++ {
		err := NewUpstreamError("t", code, "some_type", "m")
		if got := err.Retryable(); got != retryable[code] {
			t.Errorf("status %d: Retryable() = %v, want %v", code, got, retryable[code])
		}
		wantPermanent := code == 401 || code == 403 || code == 404
		if got := err.Permanent(); got != wantPermanent {
			t.Errorf("status %d: Permanent() = %v, want %v", code, got, wantPermanent)
		}
	}
}

func TestNewUpstreamErrorFieldsPreserved(t *testing.T) {
	err := NewUpstreamError("volcengine", 401, "auth_error", "bad key")
	if err.Provider != "volcengine" || err.StatusCode != 401 || err.Type != "auth_error" || err.Message != "bad key" {
		t.Errorf("fields not preserved: %+v", err)
	}
}
