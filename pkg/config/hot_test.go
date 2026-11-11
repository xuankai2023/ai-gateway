package config

import (
	"strings"
	"testing"
	"time"
)

func TestHotValuesFromConfig(t *testing.T) {
	c := Default()
	c.Gateway.MaxAttempts = 3
	c.Gateway.DefaultRPSLimit = 20
	c.Gateway.DefaultTPMLimit = 100000
	c.Gateway.DefaultConcurrencyLimit = 10

	h := NewHotValues(c)
	if got := h.MaxAttempts(); got != 3 {
		t.Errorf("MaxAttempts = %d, want 3", got)
	}
	if got := h.DefaultRPSLimit(); got != 20 {
		t.Errorf("DefaultRPSLimit = %d, want 20", got)
	}
	if got := h.RequestTimeout(); got <= 0 {
		t.Errorf("RequestTimeout = %v, want > 0（默认值应生效）", got)
	}
	if got := h.StreamMaxDuration(); got <= 0 {
		t.Errorf("StreamMaxDuration = %v, want > 0（默认值应生效）", got)
	}
}

// ApplyMap：Pub/Sub 推送部分更新，校验非法值整体拒绝
func TestHotValuesApplyMap(t *testing.T) {
	h := NewHotValues(Default())

	applied, err := h.ApplyMap(map[string]any{
		"max_attempts":        float64(5),
		"default_rps_limit":   float64(50),
		"request_timeout":     "45s",
		"stream_max_duration": float64(600), // 数字按秒解析
	})
	if err != nil {
		t.Fatalf("ApplyMap 失败: %v", err)
	}
	if len(applied) != 4 {
		t.Errorf("applied = %v, want 4 项", applied)
	}
	if got := h.MaxAttempts(); got != 5 {
		t.Errorf("MaxAttempts = %d, want 5", got)
	}
	if got := h.DefaultRPSLimit(); got != 50 {
		t.Errorf("DefaultRPSLimit = %d, want 50", got)
	}
	if got := h.RequestTimeout(); got != 45*time.Second {
		t.Errorf("RequestTimeout = %v, want 45s", got)
	}
	if got := h.StreamMaxDuration(); got != 600*time.Second {
		t.Errorf("StreamMaxDuration = %v, want 600s", got)
	}
}

// 非法值整体拒绝：之前的合法项也不应生效
func TestHotValuesApplyMapRejectsAllOnError(t *testing.T) {
	h := NewHotValues(Default())
	before := h.DefaultRPSLimit()

	_, err := h.ApplyMap(map[string]any{
		"default_rps_limit": float64(999),
		"max_attempts":      float64(-1), // 非法
	})
	if err == nil {
		t.Fatal("含非法项应报错")
	}
	if got := h.DefaultRPSLimit(); got != before {
		t.Errorf("非法批量更新导致部分生效: DefaultRPSLimit = %d, want %d", got, before)
	}

	// 非白名单 key 拒绝
	if _, err := h.ApplyMap(map[string]any{"listen_addr": ":9090"}); err == nil {
		t.Fatal("非热更新项应拒绝")
	} else if !strings.Contains(err.Error(), "listen_addr") {
		t.Errorf("错误信息应包含被拒项: %v", err)
	}
}

// Snapshot 供 Admin GET /config 使用
func TestHotValuesSnapshot(t *testing.T) {
	h := NewHotValues(Default())
	snap := h.Snapshot()
	for _, k := range []string{"max_attempts", "default_rps_limit", "default_tpm_limit",
		"default_concurrency_limit", "request_timeout", "stream_max_duration"} {
		if _, ok := snap[k]; !ok {
			t.Errorf("Snapshot 缺少 key: %s", k)
		}
	}
}

// ApplyConfig（SIGHUP 通道）：从完整配置重新应用
func TestHotValuesApplyConfig(t *testing.T) {
	h := NewHotValues(Default())
	c := Default()
	c.Gateway.DefaultRPSLimit = 77
	c.Gateway.RequestTimeout = "90s"
	h.ApplyConfig(c)
	if got := h.DefaultRPSLimit(); got != 77 {
		t.Errorf("DefaultRPSLimit = %d, want 77", got)
	}
	if got := h.RequestTimeout(); got != 90*time.Second {
		t.Errorf("RequestTimeout = %v, want 90s", got)
	}
}
