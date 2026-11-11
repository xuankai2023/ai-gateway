# 积分系统测试文档

对应设计文档：[credits-system.md](./credits-system.md)

## 1. 测试结论

积分系统（MicroUSD 整数账本 + 四种定价模型 + 批次过期）已完成实现并通过全部自动化测试：

| 命令 | 结果 |
| --- | --- |
| `go build ./...` | 通过 |
| `go vet ./...` | 通过 |
| `go test ./...` | 全部通过 |
| `LEDGER_STRESS=1 go test ./pkg/ledger -run TestA1 -count=1 -timeout 10m` | 通过，1,000,000 次预占+结算，耗时 118.6s，余额误差 = 0 |

所有验收项 A1–A12 均有对应测试，且不存在「只实现一半」的分支。

## 2. 运行方式

```bash
# 全量回归（无需外部 MySQL / Redis，测试使用 miniredis + dbtest）
go build ./...
go vet ./...
go test ./...

# 只跑积分系统相关包
go test ./pkg/ledger ./pkg/pricing ./pkg/creditlots \
        ./apps/gateway/internal/middleware \
        ./apps/gateway/internal/handler \
        ./apps/gateway/internal/usagelog \
        ./apps/gateway/internal/relay \
        ./apps/gateway/internal/admin

# A1 百万次精度压测（约 2 分钟）
LEDGER_STRESS=1 go test ./pkg/ledger -run TestA1 -count=1 -timeout 10m
```

## 3. 验收矩阵（A1–A12）

| 编号 | 验收目标 | 测试文件 / 用例 | 结果 |
| --- | --- | --- | --- |
| A1 | 100 万次预占/结算后余额零漂移 | `pkg/ledger/ledger_test.go` → `TestA1IntegerPrecisionNoDrift`（`LEDGER_STRESS=1` 跑 1,000,000 次） | 通过 |
| A2 | 余额不足返回 402，且不修改 reserved | `pkg/ledger/ledger_test.go` → `TestReservesV2/A2...`；`middleware/quota_test.go` → `TestLedgerQuota402WhenInsufficient` | 通过 |
| A3 | `balance <= trust_quota` 跳过预占 | `TestReservesV2/balance<=trust_quota...`、`.../balance==trust_quota...`、`TestLedgerQuotaTrustQuotaSkips` | 通过 |
| A4 | linear：1000 输入 + 500 输出 = 4500 μUSD | `pkg/pricing/engine_test.go` → `TestA4Linear` | 通过 |
| A5 | graduated：5K 走 tier0，20K 走 tier1，边界与最高档 | `TestA5Graduated`（5K / 20K / 10000 边界 / 10001 / 无限档） | 通过 |
| A6 | package：2500 tokens → 3 个 block | `TestA6PackageCeil` | 通过 |
| A7 | per_unit：4 次 × $0.02 = 80000 μUSD | `TestA7PerUnit` | 通过 |
| A8 | 价格版本快照不被改价影响，版本号落库；预占与结算复用同一快照 | `TestA8PriceVersionSnapshot`；`TestSettleCostPrefersQuotaSnapshot`；`usagelog/writer_test.go` | 通过 |
| A9 | 过期批次 remaining 清零，不影响 Redis 余额 | `pkg/creditlots/creditlots_test.go` → `TestA9BurnExpired`、`TestBurnExpiredAll` | 通过 |
| A10 | 先到期先扣：A 剩 3 / B 剩 10，扣 5 → A=0、B=8 | `TestA10DeductOrderByExpiryThenFIFO` | 通过 |
| A11 | `go build ./...`、`go vet ./...`、`go test ./...` 全绿 | 全仓库 | 通过 |
| A12 | 余额不足 → 402 且上游供应商零请求 | `handler/chat_completion_integration_test.go` → `TestIntegration_InsufficientBalanceBlocksUpstream` | 通过 |

## 4. 单元测试明细

### 4.1 `pkg/ledger`（金额精度与账本状态机）

- `types_test.go`
  - `FromDollars`：0.000001 / 0.0015 / 0.01 / 10 / 负数，逐项精确断言。
  - `MicroUSD.String`：6 位小数格式化（含负数）。
  - `ParseMicroUSD`：整数解析、空串、非法小数报错。
  - `Dollars` 往返精度。
- `ledger_test.go`
  - `TestReservesV2`：成功预占、信任额度跳过、等于信任额度边界、A2 拦截、available 计算、nil client。
  - `TestSettles`：预占多退少补、reserved=0 直接扣、负数拒绝、nil client。
  - `TestRefund`：部分退款、全额退款并删除 `reserved_at`、超额退款归零、负数拒绝、nil client。
  - `TestGetBalance`：三字段读取、新租户默认 trust_quota、nil client。
  - `TestReservesThenSettleFullFlow`：完整预占 → 结算 → reserved 归零。
  - `TestMultiReservesAggregate`：3 次预占累计 1.5 USD。
  - `TestA1IntegerPrecisionNoDrift`：默认 10,000 次；`LEDGER_STRESS=1` 时 1,000,000 次。

### 4.2 `pkg/pricing`（四种定价模型）

- 价格单位统一 μUSD，整数截断。
- `PriceLinear` / `PriceGraduated` / `PricePackage` / `PricePerUnit` 全部通过 `Engine.Estimate` / `Engine.Settle` 走同一入口。
- 覆盖空配置、非法梯价、`block_size_tokens<=0`、未知定价模型、负数 usage 等错误路径。
- 空 `PricingModel` 自动归一化为 `linear`。

### 4.3 `pkg/creditlots`（批次扣减 / 过期）

- SQL 顺序：`expires_at IS NULL ASC, expires_at ASC, id ASC`（先到期先扣、永久额度最后）。
- 单条 UPDATE 带 `remaining >= take` 条件，并发修改时 `RowsAffected=0` 自动跳过。
- 批次不足时只扣现有额度，不报错（余额允许后付费为负）。
- 过期批次 `remaining=0`，不写 Redis 余额。
- 覆盖 nil DB、0 金额无 SQL、部分扣减、并发跳过。

### 4.4 `apps/gateway/internal/middleware`

- `quota_test.go`
  - 按定价快照预占估算金额。
  - A2 402：`billing_error / insufficient_balance`，handler 不执行，reserved 不变。
  - 信任额度跳过、定价失败 fail-open、models 缺失回退 abilities、无定价链路保底 0.01 USD、Redis nil fail-open、请求体读取后恢复。
- `usage_capture_test.go`
  - 成功结算：`delta = final - reserved`，余额精确到 μUSD。
  - 失败退款：`reserved` 归零，流水 delta 为负。
  - 信任额度跳过（reserved=0）仍写 settle 流水并扣批次。
  - 无预占且 finalCost=0 时不产生任何 DB 写入。

### 4.5 `apps/gateway/internal/handler`

- `pricing_settle_test.go`
  - 定价快照（graduated）结算，版本号透传。
  - **A8 请求级快照**：Ledger 预占时写入 `ledger_pricing_row`，结算时即使 DB 已改价也按原版本结算。
  - models 缺失时回退 abilities 线性单价。
- `chat_completion_integration_test.go`
  - 正常成功全链路：余额 `-25 μUSD`、reserved=0。
  - A12 余额不足：HTTP 402，上游调用次数 = 0，reserved 不变。
  - 失败请求不扣余额。

### 4.6 `apps/gateway/internal/usagelog`

- 批量 INSERT 列数由 13 → 14，新增 `price_version`。
- 3 行批量的参数数 = 42，占位符组数 = 3。
- `Cost` / `ReservedCost` / `FinalCost` 全部按 int64 μUSD 写入。

### 4.7 `apps/gateway/internal/relay`

- `models` 查询新增 `pricing_model` / `pricing_config` / `price_version`。
- 价格列按 `BIGINT μUSD` 扫描，缓存命中语义不变（1000 次解析仅 1 次 DB 查询）。

### 4.8 `apps/gateway/internal/admin`

- models 列表输出同时提供 USD 展示值与 μUSD 精确值。
- 创建 / 更新模型时美元入参自动转 μUSD。
- 支持 `pricing_model`、`pricing_config`、`price_version` 维护。
- 租户 `budget_monthly` 变更时同步写 `budget_micro`（`500` → `500000000`，`0` → `NULL`），由 `TestTenantBudgetMicroSync` 校验。

## 5. 数据库迁移验证（真实环境冒烟）

> 自动化测试不依赖真实 MySQL；发布前需在预发执行一次 012 迁移并核对。

```bash
mysql -u root -p ai_gateway < sql/012_int_amounts.up.sql
```

检查列类型：

```sql
SHOW COLUMNS FROM models    LIKE 'input_price_1k';      -- bigint
SHOW COLUMNS FROM models    LIKE 'pricing_model';       -- varchar(16)
SHOW COLUMNS FROM tenants   LIKE 'balance';             -- bigint
SHOW COLUMNS FROM tenants   LIKE 'trust_quota';         -- bigint
SHOW COLUMNS FROM tenants   LIKE 'budget_micro';        -- bigint
SHOW COLUMNS FROM ledger_entries LIKE 'delta';          -- bigint
SHOW COLUMNS FROM usage_logs LIKE 'cost';               -- bigint
SHOW COLUMNS FROM usage_logs LIKE 'price_version';      -- int
SHOW TABLES LIKE 'credit_lots';
```

金额换算抽查（迁移前后应满足 `新值 = ROUND(旧值 × 1,000,000)`）：

```sql
SELECT id, input_price_1k, output_price_1k, pricing_model, price_version FROM models LIMIT 10;
SELECT id, balance, reserved, trust_quota, budget_micro FROM tenants LIMIT 10;
SELECT request_id, delta, kind FROM ledger_entries ORDER BY id DESC LIMIT 10;
SELECT request_id, cost, reserved_cost, final_cost, price_version FROM usage_logs ORDER BY id DESC LIMIT 10;
```

回滚验证（仅在预发）：

```bash
mysql -u root -p ai_gateway < sql/012_int_amounts.down.sql
```

## 6. Redis 手工核验

```bash
# 账本 HASH：balance / reserved / trust_quota 均应为 μUSD 整数
redis-cli HGETALL ledger:tenant:1

# reserved_at 应为 unix_nano（v2 写入）
redis-cli HGET ledger:tenant:1 reserved_at
```

预期：

- 预占成功：`reserved` 增加估算值，`reserved_at` 有值。
- 结算成功：`reserved=0`，`reserved_at` 被删除，`balance` 扣真实成本。
- 失败退款：`reserved` 回到预占前金额。
- 信任额度内（`balance <= trust_quota`）：`reserved` 始终为 0。

## 7. 发布检查清单

1. 停止旧版本写入 → 执行 `012_int_amounts.up.sql` → 部署新版本网关。
2. 确认 Redis 账本 key 仍为 `ledger:tenant:{id}`，历史数据无需迁移。
3. 抽查新请求 `usage_logs`：`cost` 为 BIGINT μUSD，`price_version` 有值。
4. 抽查 `ledger_entries`：`delta` 为 BIGINT μUSD，`kind` 为 `settle` / `refund`。
5. 构造余额不足租户：确认返回 402 `insufficient_balance`，且上游无调用。
6. 构造过期批次：运行 Reconciler，确认 `credit_lots.remaining = 0` 且 Redis 余额不变。
7. 验证 Reconciler 能识别 RFC3339Nano（v2 旧数据）与 unix_nano（当前数据）两种 `reserved_at`。

## 8. 已知边界

- 自动化测试使用 `miniredis` + `dbtest` 模拟 Redis / MySQL，不覆盖真实数据库驱动行为；发布前必须执行第 5、6 节冒烟。
- `per_unit` 估算从请求体读取 `n` 或 `count` 字段；其他自定义字段需在 `quota.estimateRequestCost` 中显式扩展。
- `credit_lots` 采用方案 B：Redis 结算成功后异步扣批次，不参与预占；过期清理由 Reconciler 兜底。Redis 与 MySQL 之间不做分布式事务，这是设计取舍。
- 价格版本快照只在 `models` 表维护；纯 abilities 渠道回退为 linear、版本号固定 1。
