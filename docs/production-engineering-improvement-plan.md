# 工程化配套设施改进计划

> 本文档聚焦"工程化运维配套"，不涉及具体业务逻辑。
> 对照参考：GitHub krillinai/OpenCreator 项目的生产级设施实践。
>
> 前置状态：production-hardening-plan.md 阶段 0~12 已完成，运行时基建（限流/熔断/账本/缓存/幂等）已就绪；
> 当前缺口集中在 CI/CD、安全脱敏、诊断排障、性能门禁等工程化配套。
>
> 纪律：每项设施均给出可直接实施的完整方案（代码骨架/配置结构/API 契约），不留"后续再说"的模糊地带。

---

## 1. 现状盘点

### 1.1 ai-gateway 已具备的生产设施

| 设施 | 实现位置 | 状态 |
|---|---|---|
| 分布式限流（RPS/TPM/并发） | `pkg/ratelimit` + `middleware/ratelimit.go` | ✅ Redis Lua 原子 |
| 熔断器 + 跨节点广播 | `pkg/circuitbreaker` + `pkg/broadcast` | ✅ gobreaker |
| 账本预扣/结算/退款 | `pkg/ledger` | ✅ Redis Lua 原子 |
| 幂等键 | `pkg/idempotency` + `middleware/idempotency.go` | ✅ SETNX + 响应快照 |
| 语义缓存 | `internal/cache/semantic.go` | ✅ 精确哈希 + 向量 |
| 优雅关停 + 用量 flush | `cmd/main.go` 关停序列 | ✅ 5 阶段 |
| 探针拆分 | `/livez` `/readyz` | ✅ |
| k8s HPA/PDB/NetworkPolicy | `infra/k8s/` | ✅ |
| OTel + Prometheus 指标 | `pkg/observability` | ✅ TTFT/池/缓存 |
| API Key 轮换 + 宽限期 | `internal/admin/keys.go` | ✅ |

### 1.2 当前工程化配套缺口

| 设施 | 现状 | 风险 |
|---|---|---|
| **CI/CD Pipeline** | `.github/workflows/ci.yml` 仅注释占位，无实际 job | 无自动化质量门禁，所有测试依赖手动运行 |
| **通用日志脱敏** | 仅有 `RedactKey` / `BodyDigest`，只覆盖 API key | 审计日志/错误响应中可能泄露 token/password/cookie |
| **诊断包机制** | 无，排障依赖 grep 日志 | 线上问题定位慢，无请求级快照 |
| **CI 性能基线门禁** | 有 k6 脚本（`tests/k6/`）但未接入 CI | 重构可能引入延迟退化而无感知 |
| **依赖漏洞扫描** | 无 | 引入有漏洞的依赖无告警 |
| **Git Secret 扫描** | 无 | API key 可能被提交到仓库 |
| **DB 迁移可逆性校验** | 无 CI 校验 | up/down 不可逆导致回滚失败 |
| **配置 schema 校验** | 无 | config.yaml 字段拼写错误运行时才发现 |
| **代码覆盖率门禁** | 无 | 核心包无测试覆盖率底线 |
| **遥测本地持久化** | usagelog 用内存 channel，满了丢弃 | 压测/DB 慢时丢用量日志 |

---

## 2. 可借鉴的 OpenCreator 生产级设施

以下 10 项按落地优先级排序，每项给出完整可实施方案。

---

### 2.1 CI/CD Pipeline（P0 — 最高优先级）

**现状**：`.github/workflows/ci.yml` 完全是注释占位。

**完整方案**：替换为以下可直接运行的 YAML（PostgreSQL + Redis 用 GitHub service 容器）。

```yaml
name: CI
on:
  push:
    branches: [master]
  pull_request:
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  lint:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
          cache: true
      - run: go vet ./...
      - uses: golangci/golangci-lint-action@v6
        with:
          version: latest
          args: --timeout=5m

  unit-test:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: gateway
          POSTGRES_PASSWORD: gateway
          POSTGRES_DB: gateway_test
        ports: ["5432:5432"]
        options: >-
          --health-cmd "pg_isready -U gateway"
          --health-interval 5s
          --health-timeout 5s
          --health-retries 10
      redis:
        image: redis:7
        ports: ["6379:6379"]
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 5s
          --health-timeout 5s
          --health-retries 10
    env:
      GATEWAY_TEST_DB_DSN: "postgres://gateway:gateway@localhost:5432/gateway_test?sslmode=disable"
      GATEWAY_TEST_REDIS_ADDR: "localhost:6379"
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
          cache: true
      - run: go mod download
      - name: Run migrations up
        run: go run ./apps/migrate/cmd -dsn "$GATEWAY_TEST_DB_DSN" -path sql up
      - name: Run migrations down (reversibility check)
        run: go run ./apps/migrate/cmd -dsn "$GATEWAY_TEST_DB_DSN" -path sql down 1
      - name: Re-run migrations up
        run: go run ./apps/migrate/cmd -dsn "$GATEWAY_TEST_DB_DSN" -path sql up
      - run: go test ./... -race -count=1 -coverprofile=coverage.out
      - name: Coverage gate
        run: |
          go tool cover -func=coverage.out | tail -1 | awk '{print $3}' | sed 's/%//' > coverage.txt
          COV=$(cat coverage.txt)
          echo "Total coverage: ${COV}%"
          awk "BEGIN {exit !($COV >= 60.0)}" || { echo "Coverage below 60%"; exit 1; }
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: coverage
          path: coverage.out

  build:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
          cache: true
      - run: go build ./...
      - run: go build -o /tmp/migrate ./apps/migrate/cmd
      - run: go build -o /tmp/reconciler ./apps/reconciler/cmd

  audit:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
          cache: true
      - run: go install golang.org/x/vuln/cmd/govulncheck@latest
      - run: govulncheck ./...

  secret-scan:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: gitleaks/gitleaks-action@v2
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

  perf:
    if: github.ref == 'refs/heads/master'
    runs-on: ubuntu-latest
    timeout-minutes: 15
    services:
      postgres: { ... 同 unit-test ... }
      redis: { ... 同 unit-test ... }
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version-file: go.mod, cache: true }
      - name: Start gateway
        run: |
          go run ./apps/gateway/cmd &
          sleep 5
      - uses: grafana/k6-action@v0.3.1
        with:
          filename: tests/k6/gateway_benchmark.js
      - name: Compare with baseline
        run: go run ./tests/k6/check_baseline.go
```

**子项说明**：

| 子项 | 目的 |
|---|---|
| `lint` | `go vet` + `golangci-lint`，静态检查 |
| `unit-test` | 带 DB/Redis 的完整测试，含迁移可逆性校验 + 覆盖率门禁 |
| `build` | 编译所有 app，含 migrate/reconciler 二进制 |
| `audit` | `govulncheck` 依赖漏洞扫描 |
| `secret-scan` | gitleaks 防止 key 提交到 git |
| `perf` | k6 基准 + 基线对比（仅 main 分支） |

**验收**：PR 合入前 lint/unit-test/build/audit/secret-scan 全绿；覆盖率 ≥60%；govulncheck 高危漏洞阻断。

---

### 2.2 通用日志/诊断脱敏引擎（P1）

**现状**：`pkg/observability/redact.go` 只有 `RedactKey` / `BodyDigest`。

**完整方案**：新增 `pkg/observability/redact_text.go`，覆盖以下 6 类正则模式（对齐 OpenCreator `redaction.ts`）。

```go
package observability

import "regexp"

const redacted = "[REDACTED]"

// 1. 赋值形式：KEY=xxx / TOKEN=xxx / SECRET=xxx / PASSWORD=xxx / COOKIE=xxx / AUTH=xxx
var assignmentSecretPattern = regexp.MustCompile(
    `(?i)([A-Z0-9_]*(?:KEY|TOKEN|SECRET|PASSWORD|COOKIE|AUTH)[A-Z0-9_]*)=(?!` + redacted + `)([^\s"',}\]]+)`)

// 2. 标签形式：password: xxx / token: xxx / api-key: xxx / secret: xxx
var labeledSecretPattern = regexp.MustCompile(
    `(?i)(\b(?:password|passwd|token|api[\s_-]?key|secret)\b\s*[:=]\s*)(?!` + redacted + `)([^\s"',}\]]+)`)

// 3. HTTP Authorization 头：Authorization: Bearer xxx
var authorizationHeaderPattern = regexp.MustCompile(
    `(?i)(Authorization:\s*[A-Za-z][A-Za-z0-9._~-]*\s+)([^\s"',}\]]+)`)

// 4. Cookie 头：Cookie: xxx / Set-Cookie: xxx
var cookieHeaderPattern = regexp.MustCompile(
    `(?i)((?:Set-)?Cookie:\s*)(?!` + redacted + `)([^\r\n]+)`)

// 5. OpenAI 风格独立 key：sk-xxxxxxxxxxxxxxxx
var standaloneAPIKeyPattern = regexp.MustCompile(
    `\bsk-[A-Za-z0-9_-]{8,}\b`)

// 6. Agent capability token：occap_xxx
var agentCapabilityPattern = regexp.MustCompile(
    `\boccap_[A-Za-z0-9_-]+\b`)

// sensitiveFields 按字段名判定（归一化后匹配）
var sensitiveFields = map[string]bool{
    "accesstoken": true, "authorization": true, "cookie": true,
    "expiresat": true, "password": true, "passwd": true, "setcookie": true,
}

// RedactText 对字符串逐模式脱敏
func RedactText(input string) string {
    if input == "" {
        return input
    }
    input = assignmentSecretPattern.ReplaceAllString(input, `${1}=`+redacted)
    input = labeledSecretPattern.ReplaceAllString(input, `${1}`+redacted)
    input = authorizationHeaderPattern.ReplaceAllString(input, `${1}`+redacted)
    input = cookieHeaderPattern.ReplaceAllString(input, `${1}`+redacted)
    input = standaloneAPIKeyPattern.ReplaceAllString(input, redacted)
    input = agentCapabilityPattern.ReplaceAllString(input, redacted)
    return input
}

// RedactValue 递归脱敏 map/slice/struct（通过反射）
func RedactValue(input any) any { ... }
```

**`RedactValue` 实现要点**：
- `string` → `RedactText`
- `[]any` → 逐项递归
- `map[string]any` → key 命中 `sensitiveFields` 直接 `[REDACTED]`，否则 value 递归
- `struct` → 遍历字段，字段名（含 json tag）命中 `sensitiveFields` 直接替换
- 其他类型原样返回

**使用场景**：
- Admin 审计日志（`afterWrite`）请求体自动过 `RedactValue`
- 错误日志中的上游响应体过 `RedactText`
- 诊断包导出前过 `RedactValue`

**验收**（单元测试覆盖）：
- `RedactText("Authorization: Bearer sk-abc123def456")` → `Authorization: Bearer [REDACTED]`
- `RedactText("DB_PASSWORD=mysecret123")` → `DB_PASSWORD=[REDACTED]`
- `RedactText("password: hunter2")` → `password: [REDACTED]`
- `RedactValue(map[string]any{"password":"x","data":"ok"})` → `{"password":"[REDACTED]","data":"ok"}`
- 已脱敏内容（含 `[REDACTED]`）不重复替换

---

### 2.3 CI 性能基线门禁（P1，依赖 P0 CI）

**现状**：k6 脚本存在但未接入 CI。

**完整方案**：

**基线文件** `tests/k6/baseline.json`：

```json
{
  "scenarios": {
    "chat_completion_normal": {
      "vus": 50,
      "duration": "30s",
      "thresholds": {
        "p99_latency_ms": 2000,
        "qps": 100,
        "error_rate": 0.01
      }
    },
    "chat_completion_stream": {
      "vus": 20,
      "duration": "30s",
      "thresholds": {
        "p99_ttft_ms": 5000,
        "qps": 30,
        "error_rate": 0.01
      }
    }
  }
}
```

**对比脚本** `tests/k6/check_baseline.go`：

```go
// 逻辑：
// 1. 读取 k6 输出的 summary JSON
// 2. 对每个场景，对比实际值与 baseline.thresholds
// 3. 退化判定（任一命中即 fail）：
//    - p99 延迟 > baseline.p99 * 1.1（退化 >10%）
//    - QPS < baseline.qps * 0.85（下降 >15%）
//    - error_rate > baseline.error_rate
// 4. 输出 PASS/FAIL 明细，退出码 0/1
```

**CI 集成**：见 2.1 的 `perf` job（仅 main 分支运行，避免每个 PR 都跑耗时压测）。

**验收**：人为注入 20% 延迟（如 handler 加 `time.Sleep(200ms)`），CI perf job fail 并输出退化明细。

---

### 2.4 诊断包机制（P2，依赖 P1 脱敏）

**现状**：无请求级诊断快照。

**完整方案**：

**数据库表**（迁移 `014_diagnostics.up.sql`）：

```sql
CREATE TABLE request_diagnostics (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    trace_id      VARCHAR(64) NOT NULL,
    tenant_id     BIGINT NOT NULL,
    model         VARCHAR(128) NOT NULL,
    stream        BOOLEAN NOT NULL DEFAULT FALSE,
    request_digest VARCHAR(128) NOT NULL,  -- BodyDigest 输出，不落明文
    attempts      JSON NOT NULL,            -- 路由决策链路
    status_code   INT NOT NULL,
    error_type    VARCHAR(64),
    error_message TEXT,
    total_cost    DECIMAL(12,6),
    duration_ms   INT NOT NULL,
    created_at    DATETIME(3) NOT NULL DEFAULT NOW(3),
    INDEX idx_trace_id (trace_id),
    INDEX idx_tenant_created (tenant_id, created_at)
);
```

**`attempts` JSON 结构**：

```json
[
  {
    "attempt": 1,
    "channel_id": 101,
    "provider": "openai",
    "base_url": "https://api.openai.com",
    "status": "failed",
    "error_kind": "rate_limit",
    "error_message": "429 Too Many Requests",
    "duration_ms": 120,
    "breaker_state_before": "closed"
  },
  {
    "attempt": 2,
    "channel_id": 102,
    "provider": "anthropic",
    "base_url": "https://api.anthropic.com",
    "status": "success",
    "error_kind": null,
    "error_message": null,
    "duration_ms": 890,
    "breaker_state_before": "closed"
  }
]
```

**采样策略**：
- 默认采样率 1%（`diagnostics.sample_rate` 配置项，默认 0.01）
- 错误请求（5xx / 上游耗尽）100% 采样
- 采样在 relay 层完成，不影响主链路（异步写入 `request_diagnostics`）

**Admin API**：

```
GET /admin/v1/diagnostics?trace_id={trace_id}
GET /admin/v1/diagnostics?tenant_id={id}&from={ts}&to={ts}&limit=100
```

响应在返回前对 `attempts[].error_message` 字段过 `RedactText`。

**验收**：
- 给定 trace_id，API 返回该请求的完整 attempt 链路（含每个渠道的错误 Kind 和耗时）
- 响应中不含明文 key/password
- 采样率配置生效（10000 请求约 100 条诊断记录）

---

### 2.5 依赖漏洞扫描（P0，并入 CI）

**现状**：无。

**完整方案**：见 2.1 的 `audit` job，用 `govulncheck`。

**本地使用**：

```bash
go install golang.org/x/vuln/cmd/govulncheck@latest
govulncheck ./...
```

**策略**：
- PR 必跑，发现高危漏洞（CVSS ≥ 7.0）阻断合入
- 中低危漏洞记录 issue，排期修复

**验收**：`go.mod` 引入一个已知有高危漏洞的旧版本依赖，CI audit job fail。

---

### 2.6 Git Secret 扫描（P0，并入 CI）

**现状**：无。

**完整方案**：见 2.1 的 `secret-scan` job，用 `gitleaks`。

**配置** `.gitleaks.toml`（项目根目录）：

```toml
[allowlist]
  description = "Allow test fixtures and example configs"
  paths = [
    '''(.*?)(jpg|gif|doc|pdf|bin|svg|socket)$''',
    '''config\.example\.yaml''',
    '''_test\.go$''',
  ]

[extend]
  useDefault = true
```

**验收**：提交一个含 `sk-test1234567890` 的文件，CI secret-scan job fail。

---

### 2.7 DB 迁移可逆性校验（P0，并入 CI）

**现状**：迁移脚本靠人工保证 up/down 可逆。

**完整方案**：见 2.1 `unit-test` job 中的三步：

```bash
# 1. up 全量
go run ./apps/migrate/cmd -dsn "$DSN" -path sql up
# 2. down 1 步（验证最近一次迁移可回滚）
go run ./apps/migrate/cmd -dsn "$DSN" -path sql down 1
# 3. 重新 up（验证 down 后可重新 up）
go run ./apps/migrate/cmd -dsn "$DSN" -path sql up
```

**扩展**：对所有迁移逐一验证 down 可逆（可选，耗时较长时仅跑最近 3 个）。

**验收**：写一个 down 脚本有语法错误的迁移，CI unit-test job 在 down 步骤 fail。

---

### 2.8 配置 Schema 校验（P1）

**现状**：`config.yaml` 字段拼写错误运行时才发现。

**完整方案**：新增 `pkg/config/validate.go` 的 `Validate()` 方法，在 `Load()` 时调用：

```go
func (c *Config) Validate() error {
    var errs []string
    // 必填项
    if c.Server.HTTPPort == 0 { errs = append(errs, "server.http_port is required") }
    if c.Database.DSN == "" { errs = append(errs, "database.dsn is required") }
    if c.Redis.Addr == "" { errs = append(errs, "redis.addr is required") }
    // 范围校验
    if c.Gateway.MaxInFlight <= 0 { errs = append(errs, "gateway.max_in_flight must be > 0") }
    if c.Gateway.RequestTimeout <= 0 { errs = append(errs, "gateway.request_timeout must be > 0") }
    // 互斥/依赖校验
    if c.SemanticCache.Enabled && c.Qdrant.Addr == "" {
        errs = append(errs, "qdrant.addr required when semantic_cache.enabled")
    }
    if len(errs) > 0 {
        return fmt.Errorf("config validation failed:\n  - %s", strings.Join(errs, "\n  - "))
    }
    return nil
}
```

**`main.go` 启动时调用**：

```go
cfg, err := config.Load(path)
if err != nil { log.Fatal(err) }
if err := cfg.Validate(); err != nil { log.Fatal(err) }
```

**验收**：`config.yaml` 中 `max_in_flight: -1`，启动立即报错退出，错误信息明确。

---

### 2.9 代码覆盖率门禁（P0，并入 CI）

**现状**：无覆盖率底线。

**完整方案**：见 2.1 `unit-test` job 的 coverage gate：

```bash
go test ./... -coverprofile=coverage.out
COV=$(go tool cover -func=coverage.out | tail -1 | awk '{print $3}' | sed 's/%//')
awk "BEGIN {exit !($COV >= 60.0)}" || { echo "Coverage below 60%"; exit 1; }
```

**策略**：
- 总覆盖率 ≥ 60%（当前项目已有大量单测，此门槛可达）
- 核心包（`pkg/ledger`、`pkg/ratelimit`、`pkg/circuitbreaker`、`pkg/provider`）单独要求 ≥ 80%
- 覆盖率报告作为 artifact 上传

**验收**：删除某个核心包的测试使覆盖率降至 59%，CI unit-test job fail。

---

### 2.10 遥测本地 Outbox 持久化（P3）

**现状**：usagelog 内存 channel 满了丢弃。

**完整方案**（当对用量日志零丢失有强需求时实施）：

**设计**：channel 满时降级写本地磁盘 outbox，后台异步 flush 到 DB。

```go
// pkg/usagelog/outbox.go
type DiskOutbox struct {
    dir    string
    mu     sync.Mutex
    file   *os.File
    enc    *json.Encoder
}

// Append 追加写入本地 WAL（追加模式，fsync 可选）
func (o *DiskOutbox) Append(entry Entry) error

// Flush 读取 WAL，批量写入 DB，成功后截断 WAL
func (o *DiskOutbox) Flush(db *sql.DB) (flushed int, err error)
```

**集成**：`writer.go` 的 `Add()` 在 channel 满时调用 `outbox.Append()` 而非丢弃；后台 goroutine 周期 `outbox.Flush()`。

**幂等**：每条 entry 带唯一 ID（`request_id + attempt`），DB INSERT 用 `ON DUPLICATE KEY UPDATE` 防重复。

**验收**：模拟 DB 不可用 30s，期间 1000 条用量日志全部写入本地 WAL；DB 恢复后 Flush 成功，DB 中恰好 1000 条无重复。

---

### 2.11 凭证文件原子写入（P3）

**现状**：provider key 走 env/vault，无本地凭证文件。

**完整方案**（仅当引入本地凭证文件时实施）：

```go
// pkg/config/atomic_file.go
func WritePrivateJSONFile(path string, value any) error {
    dir := filepath.Dir(path)
    if err := os.MkdirAll(dir, 0o700); err != nil { return err }
    // 临时文件：O_CREATE|O_EXCL|O_WRONLY，权限 0o600
    tmp := filepath.Join(dir, fmt.Sprintf(".%s.%d.%s.tmp",
        filepath.Base(path), os.Getpid(), randHex(6)))
    f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
    if err != nil { return err }
    data, _ := json.MarshalIndent(value, "", "  ")
    if _, err := f.Write(append(data, '\n')); err != nil {
        f.Close(); os.Remove(tmp); return err
    }
    f.Close()
    // 原子替换
    if err := os.Rename(tmp, path); err != nil {
        os.Remove(tmp); return err
    }
    return os.Chmod(path, 0o600)  // 最终权限
}
```

**验收**：写入过程中 `kill -9` 进程，目标文件要么是旧内容要么是新内容，无半写；`stat` 显示权限 `0o600`。

---

## 3. 落地优先级总览

| 优先级 | 设施 | 工作量 | 收益 | 依赖 |
|---|---|---|---|---|
| 🔴 P0 | CI/CD Pipeline（含 lint/test/build/audit/secret-scan/coverage） | 中 | 没有 CI 等于没有质量门禁 | 无 |
| 🔴 P0 | 依赖漏洞扫描（govulncheck） | 小 | 安全合规 | CI |
| 🔴 P0 | Git Secret 扫描（gitleaks） | 小 | 防 key 泄露 | CI |
| 🔴 P0 | DB 迁移可逆性校验 | 小 | 防回滚失败 | CI |
| 🔴 P0 | 代码覆盖率门禁 | 小 | 质量底线 | CI |
| 🟡 P1 | 通用脱敏引擎（6 类正则 + 递归） | 中 | 安全合规刚需 | 无 |
| 🟡 P1 | CI 性能基线门禁（k6 + baseline） | 中 | 防止延迟退化 | CI |
| 🟡 P1 | 配置 Schema 校验 | 小 | 防配置错误 | 无 |
| 🟡 P2 | 诊断包机制（表结构 + API + 采样） | 中 | 排障效率质变 | 脱敏 |
| 🟢 P3 | 遥测 Outbox 持久化 | 大 | 用量日志零丢失 | 无 |
| 🟢 P3 | 凭证文件原子写入 | 小 | 仅引入本地凭证时需要 | 无 |

---

## 4. 与 production-hardening-plan.md 的关系

- `production-hardening-plan.md` 聚焦**运行时高并发基建**（限流/熔断/账本/缓存/幂等），阶段 0~12 已完成。
- 本文档聚焦**工程化运维配套**（CI/脱敏/诊断/性能门禁），是 hardening 计划的补充。
- 两者无依赖冲突，可并行推进；建议先完成本文档 P0（CI/CD 全家桶），再继续 hardening 阶段 13+。
