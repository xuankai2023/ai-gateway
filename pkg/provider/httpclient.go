// HTTP Client 工厂：上游调用的连接池调优与分层超时
//
// 设计要点：
//   - 连接池参数（MaxIdleConns=512 / MaxIdleConnsPerHost=128 / IdleConnTimeout=90s /
//     TLSHandshakeTimeout=5s / HTTP/2）由 newPooledTransport 统一产出，非流式与流式
//     两个 Transport 实例共用同一份调优（分实例是因为 ResponseHeaderTimeout 与读空闲
//     超时是 Transport 级配置，只能流式独占，且流式长连接不应与非流式抢占 idle 连接）。
//   - 非流式 client：总超时 = attempt_timeout（与 relay 单次 attempt 上限一致）。
//   - 流式 client：无总超时（长回答不被误杀），ResponseHeaderTimeout=15s 防建连后
//     干等响应头，读空闲超时 60s 防半死连接挂住 goroutine。
package provider

import (
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// 流式 client 的固定超时参数（计划阶段 4 定死的值）
const (
	// StreamResponseHeaderTimeout 流式请求发出后等待响应头的上限
	StreamResponseHeaderTimeout = 15 * time.Second
	// StreamReadIdleTimeout 流式响应体两次读到数据之间的最长空闲间隔
	StreamReadIdleTimeout = 60 * time.Second
)

// ErrStreamReadIdle 流式响应体在 StreamReadIdleTimeout 内没有任何数据到达
var ErrStreamReadIdle = errors.New("provider: stream read idle timeout")

// Clients 一对注入 Provider 的 HTTP client
type Clients struct {
	// NonStream 非流式调用专用：带总超时（attempt_timeout）
	NonStream *http.Client
	// Stream 流式调用专用：无总超时，仅有响应头超时 + 读空闲超时
	Stream *http.Client
}

// NewClients 构造一对共享连接池调优的 client
// attemptTimeout 来自 gateway.attempt_timeout 配置
func NewClients(attemptTimeout time.Duration) *Clients {
	streamTransport := newPooledTransport()
	streamTransport.ResponseHeaderTimeout = StreamResponseHeaderTimeout
	return &Clients{
		NonStream: &http.Client{
			Transport: newPooledTransport(),
			Timeout:   attemptTimeout,
		},
		Stream: &http.Client{
			Transport: &idleTimeoutRoundTripper{
				base:        streamTransport,
				idleTimeout: StreamReadIdleTimeout,
			},
		},
	}
}

// newPooledTransport 产出统一调优的 Transport（连接复用是高并发下控 TIME_WAIT 的关键）
func newPooledTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// --- 读空闲超时 ---

// idleTimeoutRoundTripper 给响应体套读空闲超时看门狗
type idleTimeoutRoundTripper struct {
	base        http.RoundTripper
	idleTimeout time.Duration
}

// RoundTrip 包装响应体；请求发出/响应头到达阶段的超时由 base Transport 负责
func (t *idleTimeoutRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = newIdleTimeoutBody(resp.Body, t.idleTimeout)
	return resp, nil
}

// idleTimeoutBody 读空闲超时看门狗：每次 Read 到数据后重置计时，
// 超过 idleTimeout 没有数据到达则关闭底层 body 让阻塞的 Read 报错返回。
// 每条流式响应一个看门狗 goroutine，Close 时回收。
type idleTimeoutBody struct {
	body    io.ReadCloser
	timeout time.Duration
	reset   chan struct{}
	done    chan struct{}
	fired   atomic.Bool
	once    sync.Once
}

func newIdleTimeoutBody(body io.ReadCloser, timeout time.Duration) *idleTimeoutBody {
	b := &idleTimeoutBody{
		body:    body,
		timeout: timeout,
		reset:   make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	go b.watchdog()
	return b
}

// watchdog 初始即武装：响应头到达后第一帧数据也受读空闲超时约束
func (b *idleTimeoutBody) watchdog() {
	timer := time.NewTimer(b.timeout)
	defer timer.Stop()
	for {
		select {
		case <-b.reset:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(b.timeout)
		case <-timer.C:
			b.fired.Store(true)
			_ = b.body.Close() // 让阻塞中的 Read 立即报错返回
			return
		case <-b.done:
			return
		}
	}
}

// Read 每次拿到数据后重置看门狗；看门狗触发后统一报 ErrStreamReadIdle
func (b *idleTimeoutBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	if b.fired.Load() {
		return n, ErrStreamReadIdle
	}
	if n > 0 {
		select {
		case b.reset <- struct{}{}:
		default:
		}
	}
	return n, err
}

// Close 停看门狗并关闭底层 body（幂等）
func (b *idleTimeoutBody) Close() error {
	b.once.Do(func() { close(b.done) })
	return b.body.Close()
}
