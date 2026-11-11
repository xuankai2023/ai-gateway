// check_baseline 单元测试：构造 k6 summary JSON 验证退化判定
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// mustBaseline 构造基线对象（测试用）
func mustBaseline() *BaselineFile {
	var b BaselineFile
	b.Thresholds.P99LatencyMs = 2000
	b.Thresholds.QPS = 100
	b.Thresholds.ErrorRate = 0.01
	b.Thresholds.P99TTFTMs = 3000
	b.Tolerance.LatencyRegression = 1.10
	b.Tolerance.QPSDrop = 0.85
	b.Tolerance.TTFTRegression = 1.10
	return &b
}

// makeSummary 构造 K6Summary
func makeSummary(p99, qps, errRate, ttft float64) *K6Summary {
	s := &K6Summary{Metrics: map[string]struct {
		Values map[string]float64 `json:"values"`
	}{}}
	put := func(metric, key string, v float64) {
		m := s.Metrics[metric]
		if m.Values == nil {
			m.Values = map[string]float64{}
		}
		m.Values[key] = v
		s.Metrics[metric] = m
	}
	put("http_req_duration", "p(99)", p99)
	put("http_reqs", "rate", qps)
	put("error_rate", "rate", errRate)
	put("ttft_ms", "p(99)", ttft)
	return s
}

// TestCheck_AllPass 全部指标在容忍度内 → PASS
func TestCheck_AllPass(t *testing.T) {
	s := makeSummary(2000, 100, 0.005, 3000)
	if v := check(mustBaseline(), s); len(v) != 0 {
		t.Errorf("应无退化, got %v", v)
	}
	// 边界：恰好等于基线
	s = makeSummary(2200, 85, 0.01, 3300)
	if v := check(mustBaseline(), s); len(v) != 0 {
		t.Errorf("边界值应 PASS, got %v", v)
	}
}

// TestCheck_LatencyRegression p99 退化 >10% → FAIL
func TestCheck_LatencyRegression(t *testing.T) {
	s := makeSummary(2300, 100, 0.005, 3000) // 2300 > 2000×1.1=2200
	v := check(mustBaseline(), s)
	if len(v) != 1 || v[0].metric != "p99_latency_ms" {
		t.Fatalf("期望 p99 退化, got %v", v)
	}
}

// TestCheck_QPSDrop QPS 下降 >15% → FAIL
func TestCheck_QPSDrop(t *testing.T) {
	s := makeSummary(2000, 84, 0.005, 3000) // 84 < 100×0.85=85
	v := check(mustBaseline(), s)
	if len(v) != 1 || v[0].metric != "qps" {
		t.Fatalf("期望 QPS 退化, got %v", v)
	}
}

// TestCheck_ErrorRate 错误率超基线 → FAIL
func TestCheck_ErrorRate(t *testing.T) {
	s := makeSummary(2000, 100, 0.02, 3000)
	v := check(mustBaseline(), s)
	if len(v) != 1 || v[0].metric != "error_rate" {
		t.Fatalf("期望 error_rate 退化, got %v", v)
	}
}

// TestCheck_TTFTRegression TTFT 退化 >10% → FAIL
func TestCheck_TTFTRegression(t *testing.T) {
	s := makeSummary(2000, 100, 0.005, 3400) // 3400 > 3000×1.1=3300
	v := check(mustBaseline(), s)
	if len(v) != 1 || v[0].metric != "p99_ttft_ms" {
		t.Fatalf("期望 TTFT 退化, got %v", v)
	}
}

// TestCheck_Multiple 多项退化同时报
func TestCheck_Multiple(t *testing.T) {
	s := makeSummary(5000, 50, 0.05, 5000)
	v := check(mustBaseline(), s)
	if len(v) != 4 {
		t.Fatalf("期望 4 项退化, got %d: %v", len(v), v)
	}
}

// TestCheck_MissingMetric 指标缺失（如 ttft 未采集）不误报
func TestCheck_MissingMetric(t *testing.T) {
	s := makeSummary(2000, 100, 0.005, 3000)
	delete(s.Metrics, "ttft_ms")
	if v := check(mustBaseline(), s); len(v) != 0 {
		t.Errorf("缺失指标不应 FAIL, got %v", v)
	}
}

// TestLoadBaseline 基线文件解析
func TestLoadBaseline(t *testing.T) {
	b, err := loadBaseline("baseline.json")
	if err != nil {
		t.Fatalf("loadBaseline: %v", err)
	}
	if b.Thresholds.P99LatencyMs <= 0 || b.Thresholds.QPS <= 0 {
		t.Errorf("baseline.json 阈值未解析: %+v", b.Thresholds)
	}
}

// TestLoadSummary summary JSON 解析（模拟 k6 --summary-export 输出）
func TestLoadSummary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "summary.json")
	raw := map[string]any{
		"metrics": map[string]any{
			"http_req_duration": map[string]any{"values": map[string]float64{"p(99)": 1500}},
			"http_reqs":         map[string]any{"values": map[string]float64{"rate": 95.5}},
		},
	}
	data, _ := json.Marshal(raw)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := loadSummary(path)
	if err != nil {
		t.Fatalf("loadSummary: %v", err)
	}
	if v, ok := metricValue(s, "http_req_duration", "p(99)"); !ok || v != 1500 {
		t.Errorf("p(99) = %v (ok=%v), want 1500", v, ok)
	}
}

// TestCheck_ZeroToleranceDefault 容忍度未配置时用默认值
func TestCheck_ZeroToleranceDefault(t *testing.T) {
	var b BaselineFile
	b.Thresholds.P99LatencyMs = 2000
	b.Tolerance.LatencyRegression = 0 // 未配置 → 默认 1.10
	s := makeSummary(2300, 100, 0.005, 3000)
	v := check(&b, s)
	if len(v) != 1 || v[0].metric != "p99_latency_ms" {
		t.Fatalf("默认容忍度应生效, got %v", v)
	}
}

// TestCheck_ZeroThresholdSkipped 基线阈值未配置时该条不检查
func TestCheck_ZeroThresholdSkipped(t *testing.T) {
	var b BaselineFile
	b.Thresholds.P99LatencyMs = 0 // 未配置
	s := makeSummary(999999, 100, 0.005, 3000)
	if v := check(&b, s); len(v) != 0 {
		t.Errorf("阈值未配置应跳过, got %v", v)
	}
}
