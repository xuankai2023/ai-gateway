package provider

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewClients_TimeoutSplit(t *testing.T) {
	clients := NewClients(42 * time.Second)

	if clients.NonStream.Timeout != 42*time.Second {
		t.Errorf("NonStream.Timeout = %v, want 42s（attempt_timeout 总超时）", clients.NonStream.Timeout)
	}
	if clients.Stream.Timeout != 0 {
		t.Errorf("Stream.Timeout = %v, want 0（流式无总超时）", clients.Stream.Timeout)
	}
}

func TestNewClients_TransportTuning(t *testing.T) {
	clients := NewClients(30 * time.Second)

	tr, ok := clients.NonStream.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("NonStream.Transport type = %T, want *http.Transport", clients.NonStream.Transport)
	}
	if tr.MaxIdleConns != 512 {
		t.Errorf("MaxIdleConns = %d, want 512", tr.MaxIdleConns)
	}
	if tr.MaxIdleConnsPerHost != 128 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 128", tr.MaxIdleConnsPerHost)
	}
	if tr.IdleConnTimeout != 90*time.Second {
		t.Errorf("IdleConnTimeout = %v, want 90s", tr.IdleConnTimeout)
	}
	if tr.TLSHandshakeTimeout != 5*time.Second {
		t.Errorf("TLSHandshakeTimeout = %v, want 5s", tr.TLSHandshakeTimeout)
	}
	if !tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 = false, want true（HTTP/2 开启）")
	}
}

func TestNewClients_StreamTransportLayers(t *testing.T) {
	clients := NewClients(30 * time.Second)

	wrapper, ok := clients.Stream.Transport.(*idleTimeoutRoundTripper)
	if !ok {
		t.Fatalf("Stream.Transport type = %T, want *idleTimeoutRoundTripper", clients.Stream.Transport)
	}
	if wrapper.idleTimeout != StreamReadIdleTimeout {
		t.Errorf("idleTimeout = %v, want %v", wrapper.idleTimeout, StreamReadIdleTimeout)
	}
	base, ok := wrapper.base.(*http.Transport)
	if !ok {
		t.Fatalf("wrapper.base type = %T, want *http.Transport", wrapper.base)
	}
	if base.ResponseHeaderTimeout != StreamResponseHeaderTimeout {
		t.Errorf("ResponseHeaderTimeout = %v, want %v", base.ResponseHeaderTimeout, StreamResponseHeaderTimeout)
	}
	// 连接池调优与非流式一致
	if base.MaxIdleConns != 512 || base.MaxIdleConnsPerHost != 128 {
		t.Errorf("流式 Transport 连接池 = %d/%d, want 512/128", base.MaxIdleConns, base.MaxIdleConnsPerHost)
	}
}

func TestIdleTimeoutBody_Fires(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()

	body := newIdleTimeoutBody(reader, 50*time.Millisecond)
	start := time.Now()
	_, err := body.Read(make([]byte, 64))
	elapsed := time.Since(start)

	if !errors.Is(err, ErrStreamReadIdle) {
		t.Fatalf("err = %v, want ErrStreamReadIdle", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("读空闲超时触发耗时 %v, 应在 50ms 左右", elapsed)
	}
	if err := body.Close(); err != nil {
		t.Errorf("Close err = %v", err)
	}
	// Close 幂等
	_ = body.Close()
}

func TestIdleTimeoutBody_ResetOnData(t *testing.T) {
	reader, writer := io.Pipe()
	body := newIdleTimeoutBody(reader, 100*time.Millisecond)
	defer body.Close()

	// 每 30ms 滴一帧（小于 100ms 空闲上限），共 5 帧
	go func() {
		for i := 0; i < 5; i++ {
			_, _ = writer.Write([]byte("chunk"))
			time.Sleep(30 * time.Millisecond)
		}
		_ = writer.Close()
	}()

	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll err = %v, want nil（持续有数据不应触发空闲超时）", err)
	}
	if string(got) != strings.Repeat("chunk", 5) {
		t.Errorf("got %q, want 5 个 chunk", string(got))
	}
}

func TestNonStreamClient_TotalTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer ts.Close()

	clients := NewClients(50 * time.Millisecond)
	start := time.Now()
	resp, err := clients.NonStream.Get(ts.URL)
	if err == nil {
		resp.Body.Close()
		t.Fatal("err = nil, want 总超时错误")
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Errorf("总超时触发耗时 %v, 应在 50ms 左右", elapsed)
	}
}

func TestStreamClient_ResponseHeaderTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond) // 迟迟不发响应头
	}))
	defer ts.Close()

	transport := newPooledTransport()
	transport.ResponseHeaderTimeout = 50 * time.Millisecond
	client := &http.Client{Transport: &idleTimeoutRoundTripper{base: transport, idleTimeout: time.Minute}}

	start := time.Now()
	resp, err := client.Get(ts.URL)
	if err == nil {
		resp.Body.Close()
		t.Fatal("err = nil, want 响应头超时错误")
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Errorf("响应头超时触发耗时 %v, 应在 50ms 左右", elapsed)
	}
}

func TestStreamClient_NoTotalTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		// 300ms 持续滴流（远大于非流式 client 的 50ms 总超时）
		for i := 0; i < 10; i++ {
			_, _ = w.Write([]byte("data: x\n\n"))
			flusher.Flush()
			time.Sleep(30 * time.Millisecond)
		}
	}))
	defer ts.Close()

	// 构造与非流式相同的 50ms attempt 总超时，但流式 client 不受其约束
	clients := NewClients(50 * time.Millisecond)

	resp, err := clients.Stream.Get(ts.URL)
	if err != nil {
		t.Fatalf("Stream.Get err = %v, want nil", err)
	}
	defer resp.Body.Close()

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll err = %v, want nil（流式无总超时，长响应应完整读完）", err)
	}
	if len(got) != len("data: x\n\n")*10 {
		t.Errorf("body 长度 = %d, want %d", len(got), len("data: x\n\n")*10)
	}
}

func TestStreamClient_ReadIdleTimeout(t *testing.T) {
	stall := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte("data: first\n\n"))
		w.(http.Flusher).Flush()
		<-stall // 此后不再发数据，模拟半死连接
	}))
	defer func() { close(stall); ts.Close() }()

	transport := newPooledTransport()
	transport.ResponseHeaderTimeout = 5 * time.Second
	client := &http.Client{Transport: &idleTimeoutRoundTripper{base: transport, idleTimeout: 80 * time.Millisecond}}

	resp, err := client.Get(ts.URL)
	if err != nil {
		t.Fatalf("Get err = %v, want nil（响应头正常到达）", err)
	}
	defer resp.Body.Close()

	start := time.Now()
	_, err = io.ReadAll(resp.Body)
	if !errors.Is(err, ErrStreamReadIdle) {
		t.Fatalf("ReadAll err = %v, want ErrStreamReadIdle", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("读空闲超时触发耗时 %v, 应在 80ms 左右", elapsed)
	}
}

func TestConnectionReuse(t *testing.T) {
	var mu sync.Mutex
	ports := map[string]int{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ports[r.RemoteAddr]++
		mu.Unlock()
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	clients := NewClients(5 * time.Second)
	for i := 0; i < 8; i++ {
		resp, err := clients.NonStream.Get(ts.URL)
		if err != nil {
			t.Fatalf("第 %d 次请求失败: %v", i, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ports) != 1 {
		t.Errorf("8 次请求使用了 %d 条连接, want 1（连接复用生效，TIME_WAIT 与 QPS 解耦）", len(ports))
	}
	for port, count := range ports {
		if count != 8 {
			t.Errorf("连接 %s 服务了 %d 次请求, want 8", port, count)
		}
	}
}
