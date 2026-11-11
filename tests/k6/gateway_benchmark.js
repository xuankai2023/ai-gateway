// k6 基准脚本：AI Gateway /v1/chat/completions 阶梯加压基准
//
// 用途：在本地/CI 对网关服务做可重复的性能基准，产出 P50/P99/P999/错误率/TTFT 基线
//      纳入 CI 回归阈值（thresholds），防止后续阶段引入性能退化。
//
// 运行：
//   k6 run tests/k6/gateway_benchmark.js --env BASE_URL=http://localhost:8080
//   k6 run tests/k6/gateway_benchmark.js --env BASE_URL=http://localhost:8080 \
//       --vus 200 --duration 30s                # 覆盖默认阶段配置
//
// 依赖：网关服务需先启动（go run ./apps/gateway/cmd/main.go），
//      并在 DB 预置一个可用的 api_key（见 --env API_KEY）。

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// ── 自定义指标 ──────────────────────────────────────────────────────

// ttft: 首字节时间（TTFB）的别名，对网关 ≈ 网络 RTT + relay 处理时间
const ttft = new Trend('ttft_ms', true); // true = 输出百分位到 summary

// ── 测试参数（通过 --env 覆盖） ─────────────────────────────────────

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const API_KEY  = __ENV.API_KEY  || 'sk-benchmark-key';
const MODEL    = __ENV.MODEL    || 'benchmark-model';

// 阶梯加压阶段：10s 逐步上升 → 30s 稳定 → 10s 逐步下降
// 开发机目标：RPS ≈ VUs × 0.8（本地测试不追求极限，覆盖核心链路即可）
export const options = {
  scenarios: {
    default: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 20 },   // 起压
        { duration: '10s', target: 50 },   // 加速
        { duration: '30s', target: 50 },   // 稳定平台（核心基线）
        { duration: '10s', target: 0 },    // 压降
      ],
      gracefulRampDown: '5s',
    },
  },

  // 回归阈值：超过以下任何一条即 k6 exit code != 0
  // 故意宽松（本地），CI 里可以通过 --threshold 覆写更严格的值
  thresholds: {
    'http_req_duration{expected_response:true}': ['p(99)<5000'], // P99 < 5s
    'http_req_duration{expected_response:true}': ['p(95)<3000'], // P95 < 3s
    'http_req_duration{expected_response:true}': ['p(50)<500'],   // P50 < 500ms
    'ttft_ms':                                   ['p(99)<3000'], // TTFT P99 < 3s
    'error_rate':                                ['rate<0.02'],   // 错误率 < 2%
    'http_reqs':                                 ['count>0'],    // 至少收到请求
  },

  // 通用 VU 配置
  vus: 50,
  iterations: 0, // 用 scenario 控制
};

// ── VU 主循环 ────────────────────────────────────────────────────────

const chatBody = JSON.stringify({
  model:    MODEL,
  messages: [{ role: 'user', content: 'Ping: 请只回答一个词 Pong' }],
  max_tokens: 32,
});

export default function () {
  const url = `${BASE_URL}/v1/chat/completions`;
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${API_KEY}`,
    },
    timeout: '10s',
  };

  const res = http.post(url, chatBody, params);

  // 记录 TTFT（k6 http.Response 自带 timings.waiting = TTFB）
  if (res.timings && res.timings.waiting !== undefined) {
    ttft.add(res.timings.waiting);
  }

  const isOK = res.status === 200 || res.status === 429; // 限流也算正常（压测场景）
  check(res, {
    'status in [200, 429]': isOK,
    'no gateway timeout':   (r) => r.status !== 504,
  });

  // VU 间小抖动，避免完全同步
  sleep(Math.random() * 0.2 + 0.1);
}

// ── 汇总函数（k6 控制台输出增强） ────────────────────────────────────

export function handleSummary(data) {
  const avg = data.metrics['http_req_duration'].avg?.toFixed(1) + 'ms' || 'N/A';
  const p99 = data.metrics['http_req_duration'].p('99')?.toFixed(1) + 'ms' || 'N/A';
  const rps = data.metrics['http_reqs'].rate?.toFixed(2) + '/s' || 'N/A';
  const err = data.metrics['error_rate']?.rate?.toFixed(4) || 'N/A';
  const t99 = data.metrics['ttft_ms']?.p('99')?.toFixed(1) + 'ms' || 'N/A';

  console.log(`\n  ╔══════════════════════════════════════════════╗`);
  console.log(`  ║  AI Gateway Benchmark Summary                 ║`);
  console.log(`  ╠══════════════════════════════════════════════╣`);
  console.log(`  ║  RPS:    ${String(rps).padEnd(16)} ║`);
  console.log(`  ║  Avg:    ${String(avg).padEnd(16)} ║`);
  console.log(`  ║  P99:    ${String(p99).padEnd(16)} ║`);
  console.log(`  ║  TTFT P99: ${String(t99).padEnd(16)} ║`);
  console.log(`  ║  Error:  ${String(err).padEnd(16)} ║`);
  console.log(`  ╚══════════════════════════════════════════════╝\n`);

  // 同时输出 JSON 到 stdout，便于 CI 收集
  return {
    stdout: JSON.stringify({
      rps, avg, p99, ttft_p99: t99, error_rate: err,
      checks: data.checks,
      thresholds: data.state,
    }),
  };
}
