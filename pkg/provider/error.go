package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
)

// ErrorKind 上游错误归类：重试 / 禁用 / 告警决策的唯一依据
//
// 分类只发生在错误构造时（本文件），下游一律通过 Kind 派生行为，
// 不再看状态码或匹配错误字符串。
type ErrorKind string

const (
	// KindAuth 渠道级永久失败：凭证无效（401/403）或模型不存在（404/model_not_found）。
	// 本渠道应禁用，但请求可以换下一个渠道重试。
	KindAuth ErrorKind = "auth"
	// KindRateLimit 上游限流（429）：瞬态，换渠道重试
	KindRateLimit ErrorKind = "rate_limit"
	// KindServer 上游服务端错误（5xx）与非法响应：瞬态，换渠道重试
	KindServer ErrorKind = "server"
	// KindTimeout 上游超时（408 / 网络超时）：瞬态，换渠道重试
	KindTimeout ErrorKind = "timeout"
	// KindOverloaded 上游过载（503/529 或 overloaded 类型）：瞬态，换渠道重试
	KindOverloaded ErrorKind = "overloaded"
	// KindUnknown 请求级错误（400/422 等）与未归类错误：换渠道也不会成功
	KindUnknown ErrorKind = "unknown"
)

// Permanent 该类失败是否应永久禁用渠道（鉴权失败 / 模型不存在，重试无意义）
func (k ErrorKind) Permanent() bool { return k == KindAuth }

// Retryable 该类失败是否值得换下一个渠道重试（请求级错误换渠道也不会成功）
func (k ErrorKind) Retryable() bool { return k != KindUnknown }

// UpstreamError 结构化上游错误
//
// 所有 Provider 的上游错误出口统一返回它，路由层用 errors.As 识别，
// 不再依赖错误字符串或各 Provider 私有的错误类型。
type UpstreamError struct {
	Provider   string    // 供应商标识
	StatusCode int       // 上游 HTTP 状态码；网络错误为 0
	Type       string    // 上游原始错误类型（如 invalid_authentication_error），仅作观测
	Kind       ErrorKind // 归类结果，重试/禁用决策依据
	Message    string    // 上游错误信息
	Cause      error     // 网络/IO 等底层错误（用于 errors.Is/errors.As）
}

func (e *UpstreamError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: upstream call failed: %v", e.Provider, e.Cause)
	}
	return fmt.Sprintf("%s: upstream error [%d] %s: %s", e.Provider, e.StatusCode, e.Type, e.Message)
}

// Unwrap 保留底层错误链（context.Canceled / net.Error 等仍可识别）
func (e *UpstreamError) Unwrap() error { return e.Cause }

// Permanent 永久性失败：重试无意义，应禁用渠道（由 Kind 派生）
func (e *UpstreamError) Permanent() bool { return e.Kind.Permanent() }

// Retryable 是否可换渠道重试（由 Kind 派生）
//
// 401/403/404 虽然对该渠道是永久失败（应禁用），但请求本身可以换下一个渠道；
// 400/422 等请求级错误（KindUnknown）换渠道也不会成功。
func (e *UpstreamError) Retryable() bool { return e.Kind.Retryable() }

// NewUpstreamError 按状态码 + 上游错误类型归类 Kind
func NewUpstreamError(providerName string, statusCode int, errType, message string) *UpstreamError {
	return &UpstreamError{
		Provider:   providerName,
		StatusCode: statusCode,
		Type:       errType,
		Kind:       classify(statusCode, errType),
		Message:    message,
	}
}

// NewNetworkError 网络/IO 错误统一视为可重试；超时可识别时归入 KindTimeout
func NewNetworkError(providerName string, cause error) *UpstreamError {
	kind := KindServer
	if isTimeout(cause) {
		kind = KindTimeout
	}
	return &UpstreamError{
		Provider: providerName,
		Type:     "network_error",
		Kind:     kind,
		Message:  cause.Error(),
		Cause:    cause,
	}
}

// NewInvalidResponseError 上游 2xx 但响应结构非法，换渠道可重试
func NewInvalidResponseError(providerName string, statusCode int, message string) *UpstreamError {
	return &UpstreamError{
		Provider:   providerName,
		StatusCode: statusCode,
		Type:       "invalid_response",
		Kind:       KindServer,
		Message:    message,
	}
}

// classify 状态码 + 上游错误类型 → Kind
//
// 特定状态码优先；上游类型信号次之（比通用 5xx 更具体，如 500 + overloaded_error）；
// 最后才是通用 5xx 兜底。
func classify(statusCode int, errType string) ErrorKind {
	switch statusCode {
	case 401, 403, 404:
		// 404 对渠道而言是永久性配置错误（模型不存在），与鉴权失败同属
		// "本渠道禁用、换渠道可重试" 的桶
		return KindAuth
	case 408:
		return KindTimeout
	case 425:
		// Too Early：重放风险拒绝，瞬态，换渠道可重试
		return KindServer
	case 429:
		return KindRateLimit
	}
	// 上游类型信号（保留旧行为：任何状态码下 type=model_not_found 都视为渠道级永久失败）
	switch {
	case errType == "model_not_found":
		return KindAuth
	case strings.Contains(errType, "overload"):
		return KindOverloaded
	}
	if statusCode == 503 || statusCode == 529 { // 529 为 Anthropic 过载状态码
		return KindOverloaded
	}
	if statusCode >= 500 && statusCode <= 599 {
		return KindServer
	}
	return KindUnknown
}

// isTimeout 判断底层错误是否为超时（含 http.Client 包装的 *url.Error）
func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
