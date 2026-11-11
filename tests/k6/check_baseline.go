// k6 性能基线门禁（CI perf job 调用）
//
// 读 k6 --summary-export 产出的 summary JSON，与 baseline.json 的阈值对比：
//   - p99 延迟 > baseline.p99 × 1.10（退化 >10%）→ FAIL
//   - QPS < baseline.qps × 0.85（下降 >15%）→ FAIL
//   - error_rate > baseline.error_rate → FAIL
//   - TTFT p99 > baseline.ttft × 1.10 → FAIL
//
// 任一命中即退出码 1；全部通过退出码 0。
//
// 用法：
//
//	k6 run tests/k6/gateway_benchmark.js --summary-export=/tmp/k6_summary.json
//	go run ./tests/k6/check_baseline.go \
//	    -summary /tmp/k6_summary.json -baseline tests/k6/baseline.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

// BaselineFile baseline.json 的顶层结构
type BaselineFile struct {
	Thresholds struct {
		P99LatencyMs float64 `json:"p99_latency_ms"`
		QPS          float64 `json:"qps"`
		ErrorRate    float64 `json:"error_rate"`
		P99TTFTMs    float64 `json:"p99_ttft_ms"`
	} `json:"thresholds"`
	Tolerance struct {
		LatencyRegression float64 `json:"latency_regression"`
		QPSDrop           float64 `json:"qps_drop"`
		TTFTRegression    float64 `json:"ttft_regression"`
	} `json:"tolerance"`
}

// K6Summary k6 --summary-export 输出（只取需要的字段）
type K6Summary struct {
	Metrics map[string]struct {
		Values map[string]float64 `json:"values"`
	} `json:"metrics"`
}

// violation 一条退化记录
type violation struct {
	metric   string
	baseline float64
	actual   float64
	rule     string
}

func main() {
	summaryPath := flag.String("summary", "/tmp/k6_summary.json", "k6 --summary-export 输出路径")
	baselinePath := flag.String("baseline", "tests/k6/baseline.json", "基线文件路径")
	flag.Parse()

	baseline, err := loadBaseline(*baselinePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load baseline: %v\n", err)
		os.Exit(2)
	}
	summary, err := loadSummary(*summaryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load summary: %v\n(需先 k6 run --summary-export=%s)\n", err, *summaryPath)
		os.Exit(2)
	}

	violations := check(baseline, summary)
	report(baseline, summary, violations)
	if len(violations) > 0 {
		os.Exit(1)
	}
}

// check 逐条对比，返回所有退化项（空 = 全部通过）
func check(bl *BaselineFile, s *K6Summary) []violation {
	var out []violation

	// k6 http_req_duration 单位 ms；p99 退化 >10% 即 fail
	if v, ok := metricValue(s, "http_req_duration", "p(99)"); ok && bl.Thresholds.P99LatencyMs > 0 {
		limit := bl.Thresholds.P99LatencyMs * tol(bl.Tolerance.LatencyRegression, 1.10)
		if v > limit {
			out = append(out, violation{"p99_latency_ms", bl.Thresholds.P99LatencyMs, v,
				fmt.Sprintf("p99 > baseline × %.2f (%.0fms)", tol(bl.Tolerance.LatencyRegression, 1.10), limit)})
		}
	}

	// QPS 下降 >15% 即 fail
	if v, ok := metricValue(s, "http_reqs", "rate"); ok && bl.Thresholds.QPS > 0 {
		limit := bl.Thresholds.QPS * tol(bl.Tolerance.QPSDrop, 0.85)
		if v < limit {
			out = append(out, violation{"qps", bl.Thresholds.QPS, v,
				fmt.Sprintf("qps < baseline × %.2f (%.1f)", tol(bl.Tolerance.QPSDrop, 0.85), limit)})
		}
	}

	// error_rate 超过基线即 fail（无容忍度）
	if v, ok := metricValue(s, "error_rate", "rate"); ok && bl.Thresholds.ErrorRate > 0 {
		if v > bl.Thresholds.ErrorRate {
			out = append(out, violation{"error_rate", bl.Thresholds.ErrorRate, v,
				"error_rate > baseline"})
		}
	}

	// TTFT p99 退化 >10% 即 fail（仅当脚本采集了 ttft_ms）
	if v, ok := metricValue(s, "ttft_ms", "p(99)"); ok && bl.Thresholds.P99TTFTMs > 0 {
		limit := bl.Thresholds.P99TTFTMs * tol(bl.Tolerance.TTFTRegression, 1.10)
		if v > limit {
			out = append(out, violation{"p99_ttft_ms", bl.Thresholds.P99TTFTMs, v,
				fmt.Sprintf("ttft p99 > baseline × %.2f (%.0fms)", tol(bl.Tolerance.TTFTRegression, 1.10), limit)})
		}
	}

	return out
}

// metricValue 从 k6 summary 取指标值；ok=false 表示指标缺失
func metricValue(s *K6Summary, metric, key string) (float64, bool) {
	m, ok := s.Metrics[metric]
	if !ok {
		return 0, false
	}
	v, ok := m.Values[key]
	return v, ok
}

// tol 容忍度兜底（0 = 未配置时用默认值）
func tol(v, def float64) float64 {
	if v <= 0 {
		return def
	}
	return v
}

func loadBaseline(path string) (*BaselineFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var b BaselineFile
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &b, nil
}

func loadSummary(path string) (*K6Summary, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s K6Summary
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &s, nil
}

// report 打印对比明细
func report(b *BaselineFile, s *K6Summary, violations []violation) {
	fmt.Println("=== k6 baseline comparison ===")
	printMetric(s, "http_req_duration", "p(99)", "p99 latency", "ms")
	printMetric(s, "http_reqs", "rate", "qps", "/s")
	printMetric(s, "error_rate", "rate", "error rate", "")
	printMetric(s, "ttft_ms", "p(99)", "ttft p99", "ms")

	if len(violations) == 0 {
		fmt.Println("\nPASS: all metrics within baseline tolerance")
		return
	}
	fmt.Printf("\nFAIL: %d regression(s):\n", len(violations))
	for _, v := range violations {
		fmt.Printf("  - %s: baseline=%.4g actual=%.4g (%s)\n", v.metric, v.baseline, v.actual, v.rule)
	}
}

func printMetric(s *K6Summary, metric, key, label, unit string) {
	if v, ok := metricValue(s, metric, key); ok {
		fmt.Printf("  %-12s %.2f%s\n", label+":", v, unit)
	} else {
		fmt.Printf("  %-12s (missing)\n", label+":")
	}
}
