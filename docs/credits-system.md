# 积分系统完整复刻

> 复刻目标：借鉴 Resvary CreditLedger 的整数精度余额模型 + OpenCreator 的多价格计费 + nself 的多维度配额，完整复刻 ai-gateway 的积分/价格/预扣分三件套。
>
> 边界：**不动支付、订阅、发票**（自行充值发放）。只做积分账户、价格引擎、预扣拦截。
>
> 前置状态：阶段 0~12 已完成，账本三件套（阶段 10）已落地但缺余额拦截、float64 精度风险、价格模型单一。

---

## 1. 核心决策（先定死，后面所有实现都对齐）

| # | 决策 | 结论 | 理由 |
|---|---|---|---|
| D1 | 金额精度 | **全部用 int64 微美元**（1 USD = 1,000,000 μUSD） | 彻底消灭 `0.1 + 0.2 != 0.3` 浮点累积误差；Redis Lua / MySQL BIGINT / Go int64 全链路统一 |
| D2 | 账本存储 | Redis Hash `ledger:tenant:{id}` 保留 Lua 原子；MySQL `tenants` 与 Redis 双写快照（对账用，Redis 为准） | 性能优先（Redis 微秒级）+ 可审计（MySQL 持久化） |
| D3 | 余额不足拦截 | **Reserves 内硬拦截**（402 Payment Required），不再 fail-open | 让账本真的能挡风险，不是摆设 |
| D4 | 价格模型 | 4 种：linear（线性）/ graduated（阶梯）/ package（按块）/ per_unit（固定件） | 兼容 Chat（linear）+ 图像固定价（per_unit）+ 阶梯优惠（graduated）+ 视频按块（package） |
| D5 | 价格版本 | `price_version` 单调递增，usage_logs 记录实际使用的版本号 | 调价时在途请求不受影响 |
| D6 | 积分批次 | `credit_lots`（赠送/充值/活动），Reserves 时按确定性优先级扣 lot | 实现"活动赠送积分月底清零先花" |
| D7 | 预计 vs 最终 | Reserves 用 **models 表的 input_price_1k × 估算 token 数** 精算（不再固定 0.01 USD）；Settles 用上游真实 usage 整单重算 | 避免误差过大导致频繁退款 |

---

## 2. 整数精度方案（全链路替换）

### 2.1 换算表

```
1 USD = 1,000,000 μUSD（微美元）
0.001 USD = 1,000 μUSD
0.00001 USD = 10 μUSD
最小单位 = 1 μUSD ≈ $0.000001
```

### 2.2 Go 侧

所有 ledger / pricing / billing 相关金额字段从 `float64` → `int64`。

```go
// pkg/ledger/types.go
type MicroUSD int64

const (
    MicroUSDPerDollar MicroUSD = 1_000_000
)

// Float 转 int64（四舍五入）
func FromDollars(f float64) MicroUSD {
    return MicroUSD(math.Round(f * float64(MicroUSDPerDollar)))
}

// int64 转美元字符串（展示用，保留 6 位小数）
func (m MicroUSD) String() string {
    return fmt.Sprintf("%d.%06d", m/MicroUSDPerDollar, abs(m)%MicroUSDPerDollar)
}

// 计算：input 1000 tokens × $0.0015/1k + output 500 tokens × $0.006/1k
// 全部整数运算，零浮点
func CalcLinear(inputTokens, outputTokens int64, inputPrice1k, outputPrice1k MicroUSD) MicroUSD {
    inputCost := inputTokens * inputPrice1k / 1000
    outputCost := outputTokens * outputPrice1k / 1000
    return inputCost + outputCost
}
```

### 2.3 Redis Lua 侧

所有 HINCRBYFLOAT → HINCRBY（整数版），存的都是字符串化的 int64。

```lua
-- 改造后的 Reserves Lua（整数版）
local balance = tonumber(redis.call('HGET', key, 'balance') or 0)
local reserved_after = redis.call('HINCRBY', key, 'reserved', amount)
```

### 2.4 MySQL 侧

```sql
-- 旧 DECIMAL(12,4) 存美元 —— 全部改成 BIGINT 存微美元
-- price 字段从 DECIMAL(10,6) 改成 BIGINT
-- 迁移脚本见第 7 节
```

---

## 3. 余额模型（复刻 Resvary Balance Model）

### 3.1 三快照

```
posted   = 已发放积分（充值 + 赠送 + 月包）- 已结算扣减
reserved = 当前正在预占的积分（Settles 后清 0，Refund 后也清 0）
available = posted - reserved          -- 真正可花的
```

与当前 tenants 表字段映射：

```
tenants.balance  → 重命名为 posted （已发放-已结算）
tenants.reserved  → 保留，语义不变
tenants.trust_quota → 保留，单位也换成 μUSD
```

### 3.2 Redis Key 结构（不变，内容换成整数）

```
HASH ledger:tenant:{id}
  balance      → posted（int64 字符串）
  reserved     → reserved（int64 字符串，Settles 后置 0）
  trust_quota  → int64 字符串（默认 10_000_000 μUSD = $10）
  reserved_at  → unix_nano 字符串（reconciler 扫）
  price_version → 当前生效的全局价格版本（int 字符串，Reserves 时读）

新增（可选，用于积分批次）：
  lot:{lot_id} → HASH { tenant_id, amount, remaining, source, expires_at }
```

---

## 4. 价格引擎

### 4.1 四种价格模型

#### Model A：linear（默认，Chat/Embedding）

```
cost = input_tokens × input_price_1k / 1000 + output_tokens × output_price_1k / 1000
```

models 表的 input_price_1k / output_price_1k（换成 μUSD BIGINT）直接用。

#### Model B：graduated（阶梯，用量越多越便宜）

```
pricing_config JSON:
{
  "tiers": [
    {"max_tokens": 10000, "input_price_1k": 1500,   "output_price_1k": 6000},
    {"max_tokens": 50000, "input_price_1k": 1200,   "output_price_1k": 5000},
    {"max_tokens": 0,     "input_price_1k": 800,    "output_price_1k": 3000}   -- 0 = 无限
  ]
}
```

阶梯判定：**按单请求 token 总量**选档（不是累计用量）。Graduated 是单请求量大就便宜，鼓励大请求。

#### Model C：package（按块计费，视频/长文生成）

```
pricing_config JSON:
{
  "block_size_tokens": 1000,
  "price_per_block": 20000  -- 每 1000 tokens = $0.02
}
```

`ceil(total_tokens / block_size_tokens) × price_per_block`。不足一块按一块算。

#### Model D：per_unit（固定件，图像生成）

```
pricing_config JSON:
{
  "price_per_unit": 20000,   -- 每张图 $0.02
  "unit_field": "count"      -- 从请求体哪个字段取数量
}
```

不按 token 算。比如 DALL-E 3 一张图固定 $0.02 = 20000 μUSD。

### 4.2 models 表扩展

```sql
ALTER TABLE models
  ADD COLUMN pricing_model    VARCHAR(16)  NOT NULL DEFAULT 'linear'   -- linear / graduated / package / per_unit
  ADD COLUMN pricing_config   JSON         NULL                         -- 上述各模型的参数
  ADD COLUMN price_version    INT          NOT NULL DEFAULT 1          -- 价格版本号（单调递增）
  ADD COLUMN price_updated_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
  CHANGE COLUMN input_price_1k  BIGINT NOT NULL DEFAULT 0   COMMENT 'μUSD / 1k input tokens'
  CHANGE COLUMN output_price_1k BIGINT NOT NULL DEFAULT 0   COMMENT 'μUSD / 1k output tokens';
```

### 4.3 pkg/pricing/engine.go

```go
// 价格引擎入口
type Engine struct{}

// PriceModel 价格模型（4 选 1）
type PriceModel string
const (
    PriceLinear    PriceModel = "linear"
    PriceGraduated PriceModel = "graduated"
    PricePackage   PriceModel = "package"
    PricePerUnit   PriceModel = "per_unit"
)

// Estimate 按模型的 pricing_model 估本次调用成本（用于 Reserves）
// usage 是预估的 token 数 / unit 数（handler 在 relay 之前就能拿到输入 token 估算值）
func (e *Engine) Estimate(model *ModelRow, usage *EstimateInput) (cost ledger.MicroUSD, err error) {
    switch model.PricingModel {
    case PriceLinear:
        return estimateLinear(model.InputPrice1k, model.OutputPrice1k, usage.InputTokens, usage.OutputTokens), nil
    case PriceGraduated:
        return estimateGraduated(model.PricingConfig, usage.InputTokens, usage.OutputTokens)
    case PricePackage:
        return estimatePackage(model.PricingConfig, usage.InputTokens+usage.OutputTokens)
    case PricePerUnit:
        return estimatePerUnit(model.PricingConfig, usage.UnitCount)
    default:
        return 0, fmt.Errorf("unknown pricing_model: %s", model.PricingModel)
    }
}

// Settle 按实际 usage 算最终成本（用于 Settles）
// 与 Estimate 同一套计算逻辑，只是 input/output tokens 是上游真实返回的
func (e *Engine) Settle(model *ModelRow, actual *ActualUsage) (cost ledger.MicroUSD, err error) {
    return e.Estimate(model, &EstimateInput{...})  // 同逻辑，参数换成真实值
}
```

### 4.4 价格版本管理

```go
// 调价流程：
// 1. Admin API / SQL UPDATE models SET input_price_1k=xxx, price_version=price_version+1
// 2. Redis / 本地缓存失效 abilities + model 命名空间
// 3. 在途请求：handler 内部已从缓存拿到旧 version 的 ModelRow，继续用旧价
// 4. 新请求：拿到新 version 的 ModelRow，用新价
// 5. usage_logs.price_version 记录实际使用的版本号，对账时可以按版本重算

// usage_logs 扩展（第 7 节迁移）
ALTER TABLE usage_logs ADD COLUMN price_version INT NOT NULL DEFAULT 1 AFTER model;
```

---

## 5. 预扣拦截完整流程（核心）

### 5.1 链路位置

```
LoadShed → Auth → Timeout → RateLimit → Idempotency → SemanticCache → **Ledger(Reserves+拦截)** → handler(LedgerCapture)
```

### 5.2 Reserves Lua 改造（加余额硬检查）

```lua
-- scriptReserves v2（整数 + 余额拦截 + credit_lots 扣减）
-- KEYS[1] = ledger:tenant:{tenant}
-- ARGV[1] = estimated_cost（μUSD，整数字符串）
-- ARGV[2] = trust_quota（默认值）
-- ARGV[3] = reserved_at（unix_nano）
--
-- 返回：{reserved_after, trust_quota_skipped, balance, available, blocked}
--   blocked = 1 表示余额不足（available - estimated_cost < 0），不执行预占
--   blocked = 0 表示预占成功或跳过

local key        = KEYS[1]
local estimated  = tonumber(ARGV[1])     -- 预计本次要花多少 μUSD
local trust_def  = tonumber(ARGV[2])
local reserved_at = ARGV[3]

-- 读账本（不存在则初始化为 0）
local balance = tonumber(redis.call('HGET', key, 'balance') or 0)
local reserved = tonumber(redis.call('HGET', key, 'reserved') or 0)
local trust = tonumber(redis.call('HGET', key, 'trust_quota') or trust_def)
local available = balance - reserved

-- trust_quota 路径：余额非常少的租户免预占（让他们能跑，不挡死）
if balance <= trust then
    return {reserved, 1, balance, available, 0}  -- trust_quota_skipped=1, blocked=0
end

-- === 新：余额不足硬拦截 ===
if available < estimated then
    -- 余额不够，不执行预占，返回 blocked=1
    return {reserved, 0, balance, available, 1}
end

-- === 正常预占 ===
local reserved_after = redis.call('HINCRBY', key, 'reserved', estimated)
redis.call('HSET', key, 'reserved_at', reserved_at)
return {reserved_after, 0, balance, available, 0}
```

### 5.3 middleware/quota.go 改造（拦截 402）

```go
func Ledger(a *app.App) gin.HandlerFunc {
    return func(c *gin.Context) {
        // ... tenant_id 校验（不变）...

        // 0. 估算本次成本（新：从 models 缓存取 pricing_model + token 估算）
        modelRow, err := resolveModelFromContext(c)  // 复用 routing 的缓存
        if err != nil {
            // 拿不到模型 → 放行到 relay，relay 会报错 404 model_not_found
            c.Next()
            return
        }
        estimatedTokens := estimateInputTokens(c.Request)  // 复用 ratelimit/estimate.go
        estimatedCost, err := pricing.Default.Estimate(modelRow, &pricing.EstimateInput{
            InputTokens:  estimatedTokens,
            OutputTokens: estimatedTokens / 2,  // 粗估输出是输入一半
        })
        if err != nil {
            // 价格模型解析失败 → fail-open，让请求过去（防止配置 bug 挡死所有流量）
            observability.L().Error("ledger: pricing estimate failed, fail-open", zap.Error(err))
            c.Next()
            return
        }

        // 1. Redis 未初始化 → fail-open
        if a.Redis == nil {
            c.Set("ledger_reserved", int64(0))
            c.Set("ledger_trust_quota_skipped", true)
            c.Set("ledger_estimated_cost", int64(estimatedCost))
            c.Next()
            return
        }

        // 2. 调用 Reserves（新：传 estimatedCost，读 blocked）
        ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
        defer cancel()
        reservedAfter, skipped, available, blocked, err := ledger.ReservesV2(ctx, a.Redis, tenantID, int64(estimatedCost))
        if err != nil {
            observability.L().Warn("ledger: reserves failed, fail-open", zap.Error(err))
            c.Set("ledger_reserved", int64(0))
            c.Set("ledger_trust_quota_skipped", true)
        } else if blocked {
            // === 新：余额不足，402 拦截 ===
            apperr.Abort(c, http.StatusPaymentRequired, "billing_error", "insufficient_balance",
                fmt.Sprintf("insufficient credits: available=%d μUSD, estimated=%d μUSD", available, estimatedCost))
            return
        } else if skipped {
            c.Set("ledger_reserved", int64(0))
            c.Set("ledger_trust_quota_skipped", true)
        } else {
            c.Set("ledger_reserved", int64(estimatedCost))
            c.Set("ledger_trust_quota_skipped", false)
        }
        c.Set("ledger_estimated_cost", int64(estimatedCost))  // Settles 时知道原来预占了多少

        c.Next()
    }
}
```

### 5.4 middleware/usage_capture.go 改造（整数结算）

```go
// Settles 用 int64 替换 float64
// ledger_final_cost 由 handler 从上游真实 usage 算出（int64 μUSD）
// Settles 内部 Lua 也是整数 HINCRBY

// ledger_entries 的 delta 也用 int64
```

---

## 6. 积分批次（credit_lots）

### 6.1 设计目标

```
租户余额 = posted（总额）
posted 由若干 lot 组成：
  lot_1: 充值 $100   → 有效期 永久
  lot_2: 活动赠送 $5  → 有效期 本月底
  lot_3: 活动赠送 $10 → 有效期 下月底

Reserves 时扣 lot 的顺序（确定性）：
  1. 最近过期的 lot 先扣（promo 月底清零的先花掉）
  2. 同过期时间 FIFO
  3. 永久 lot 最后扣（因为不会过期）

lot 过期自动 burn（不还给用户）
```

### 6.2 credit_lots 表

```sql
CREATE TABLE credit_lots (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT       NOT NULL,
    amount          BIGINT       NOT NULL COMMENT '发放总额 μUSD',
    remaining       BIGINT       NOT NULL COMMENT '剩余 μUSD',
    source          VARCHAR(16)  NOT NULL COMMENT 'topup | promo | subscription | admin_grant',
    source_ref      VARCHAR(128) NULL     COMMENT '外部引用：充值订单号 / 活动 ID',
    expires_at      DATETIME(3)  NULL     COMMENT '过期时间 NULL=永久',
    created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_lot_tenant_expiry (tenant_id, expires_at ASC),
    CONSTRAINT fk_lot_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 6.3 Reserves 时如何扣 lot

两种实现路径，选 A：

**路径 A：Reserves Lua 里扣 Redis lot（高性能）**

```lua
-- scriptReservesV3 扩展：扣 lot
-- 在 balance/reserved 原子操作之外，额外扣 credit_lot:{tenant_id}:{lot_id} 的 remaining
-- 但 Lua 里没法直接查 MySQL 的过期时间...
-- 所以 lot 数据需要同步到 Redis ZSET：
--   ZSET credit_lots:{tenant_id}
--     member = lot_id, score = expires_at_timestamp（永久 = INF）

-- Reserves 时：
--   ZRANGE credit_lots:{tenant_id} 0 -1 BYSCORE  -- 按过期时间排序
--   依次扣 lot 的 remaining，直到凑够 estimated_cost
--   过期的 lot（score < now）跳过
```

**路径 B：Reserves 不扣 lot，Settles 时从 lot 扣（简单但不实时）**

Reserves 只做 balance/reserved 的原子操作（当前方案不变）。Settles 完成后，再用一个独立事务按"最近过期优先"的 SQL 从 credit_lots 表扣减 remaining。

> 阶段 10 **先上路径 B**（简单、不影响 Redis Lua 复杂度），路径 A 留到阶段 15 性能优化时再做。

### 6.4 lot 过期清理（reconciler 扩展）

```sql
-- reconciler 新增：扫 expires_at < NOW() AND remaining > 0 的 lot
-- 把 remaining 清零 = burn（过期积分不还给用户）
UPDATE credit_lots SET remaining = 0
WHERE tenant_id = ? AND expires_at < NOW(3) AND remaining > 0
```

---

## 7. 数据库迁移脚本

### 7.1 迁移文件：`sql/012_int_amounts.up.sql`

```sql
-- ============================================================================
-- 阶段 12 复刻：整数精度 + 价格模型 + lot 批次
--
-- 所有金额字段从 DECIMAL(美元) → BIGINT(μUSD)
-- 新增 pricing_model / pricing_config / price_version
-- 新增 credit_lots 表
-- usage_logs 补 price_version
-- ============================================================================

-- --- 1. models 表金额类型改造 + 新字段 ---
-- 先加新列再填值，最后 DROP 旧列 + RENAME，避免改列时隐式转换出问题

-- 第一步：加 BIGINT 新列
ALTER TABLE models
    ADD COLUMN pricing_model    VARCHAR(16)  NOT NULL DEFAULT 'linear',
    ADD COLUMN pricing_config   JSON         NULL,
    ADD COLUMN price_version    INT          NOT NULL DEFAULT 1,
    ADD COLUMN price_updated_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    ADD COLUMN input_price_1k_micro  BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN output_price_1k_micro BIGINT NOT NULL DEFAULT 0;

-- 第二步：把旧 DECIMAL 值 × 1,000,000 填到新 BIGINT 列
UPDATE models SET
    input_price_1k_micro  = CAST(input_price_1k  * 1000000 AS SIGNED),
    output_price_1k_micro = CAST(output_price_1k * 1000000 AS SIGNED);

-- 第三步：DROP 旧列 + RENAME 新列
ALTER TABLE models
    DROP COLUMN input_price_1k,
    DROP COLUMN output_price_1k,
    CHANGE COLUMN input_price_1k_micro  input_price_1k  BIGINT NOT NULL DEFAULT 0  COMMENT 'μUSD per 1k input tokens',
    CHANGE COLUMN output_price_1k_micro output_price_1k BIGINT NOT NULL DEFAULT 0  COMMENT 'μUSD per 1k output tokens';

-- --- 2. tenants 表金额类型改造 ---
ALTER TABLE tenants
    ADD COLUMN balance_micro     BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN reserved_micro    BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN trust_quota_micro BIGINT NOT NULL DEFAULT 10000000;  -- $10 默认

UPDATE tenants SET
    balance_micro     = CAST(balance     * 1000000 AS SIGNED),
    reserved_micro    = CAST(reserved    * 1000000 AS SIGNED),
    trust_quota_micro = CAST(trust_quota * 1000000 AS SIGNED);

ALTER TABLE tenants
    DROP COLUMN balance,
    DROP COLUMN reserved,
    DROP COLUMN trust_quota,
    CHANGE COLUMN balance_micro     balance     BIGINT NOT NULL DEFAULT 0  COMMENT 'posted (μUSD)',
    CHANGE COLUMN reserved_micro    reserved    BIGINT NOT NULL DEFAULT 0  COMMENT 'reserved (μUSD)',
    CHANGE COLUMN trust_quota_micro trust_quota BIGINT NOT NULL DEFAULT 10000000 COMMENT '免预占额度 (μUSD, default $10)';

-- --- 3. ledger_entries 表金额类型改造 ---
ALTER TABLE ledger_entries
    ADD COLUMN delta_micro BIGINT NOT NULL DEFAULT 0;

UPDATE ledger_entries SET delta_micro = CAST(delta * 1000000 AS SIGNED);

ALTER TABLE ledger_entries
    DROP COLUMN delta,
    CHANGE COLUMN delta_micro delta BIGINT NOT NULL DEFAULT 0 COMMENT 'Δ balance (μUSD)';

-- --- 4. usage_logs 表金额类型改造 + 补 price_version ---
ALTER TABLE usage_logs
    ADD COLUMN cost_micro             BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN reserved_cost_micro    BIGINT NULL,
    ADD COLUMN final_cost_micro       BIGINT NULL,
    ADD COLUMN price_version          INT    NOT NULL DEFAULT 1 AFTER model;

UPDATE usage_logs SET
    cost_micro          = CAST(cost * 1000000 AS SIGNED),
    reserved_cost_micro = CASE WHEN reserved_cost IS NOT NULL THEN CAST(reserved_cost * 1000000 AS SIGNED) ELSE NULL END,
    final_cost_micro    = CASE WHEN final_cost    IS NOT NULL THEN CAST(final_cost    * 1000000 AS SIGNED) ELSE NULL END;

ALTER TABLE usage_logs
    DROP COLUMN cost,
    DROP COLUMN reserved_cost,
    DROP COLUMN final_cost,
    CHANGE COLUMN cost_micro          cost          BIGINT NOT NULL DEFAULT 0 COMMENT 'total charged (μUSD)',
    CHANGE COLUMN reserved_cost_micro reserved_cost BIGINT NULL COMMENT 'Reserves 时预占 (μUSD)',
    CHANGE COLUMN final_cost_micro    final_cost    BIGINT NULL COMMENT 'Settles 最终结算 (μUSD)';

-- --- 5. 新增 credit_lots 表 ---
CREATE TABLE credit_lots (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id       BIGINT       NOT NULL,
    amount          BIGINT       NOT NULL COMMENT '发放总额 μUSD',
    remaining       BIGINT       NOT NULL COMMENT '剩余 μUSD',
    source          VARCHAR(16)  NOT NULL COMMENT 'topup | promo | subscription | admin_grant',
    source_ref      VARCHAR(128) NULL     COMMENT '外部引用',
    expires_at      DATETIME(3)  NULL     COMMENT '过期时间 NULL=永久',
    created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_lot_tenant_expiry (tenant_id, expires_at ASC),
    CONSTRAINT fk_lot_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --- 6. budget_monthly 保持 DECIMAL(12,2) 不变（按月预算是美元，不需要 μUSD 精度）---
--    但新增：budget_micro BIGINT 精确存储
ALTER TABLE tenants ADD COLUMN budget_micro BIGINT NULL COMMENT '月度预算 μUSD（NULL 无限）';
UPDATE tenants SET budget_micro = CAST(budget_monthly * 1000000 AS SIGNED) WHERE budget_monthly > 0;
```

### 7.2 迁移文件：`sql/012_int_amounts.down.sql`

```sql
-- 回滚：全部反过来，BIGINT → DECIMAL

-- credit_lots 丢表
DROP TABLE IF EXISTS credit_lots;

-- usage_logs
ALTER TABLE usage_logs
    ADD COLUMN cost          DECIMAL(10,6) NOT NULL DEFAULT 0,
    ADD COLUMN reserved_cost DECIMAL(10,6) NULL,
    ADD COLUMN final_cost    DECIMAL(10,6) NULL;
UPDATE usage_logs SET
    cost          = CAST(cost_micro / 1000000.0 AS DECIMAL(10,6)),
    reserved_cost = CASE WHEN reserved_cost_micro IS NOT NULL
                        THEN CAST(reserved_cost_micro / 1000000.0 AS DECIMAL(10,6)) ELSE NULL END,
    final_cost    = CASE WHEN final_cost_micro IS NOT NULL
                        THEN CAST(final_cost_micro / 1000000.0 AS DECIMAL(10,6)) ELSE NULL END;
ALTER TABLE usage_logs
    DROP COLUMN cost_micro, DROP COLUMN reserved_cost_micro, DROP COLUMN final_cost_micro,
    DROP COLUMN price_version;

-- ledger_entries
ALTER TABLE ledger_entries ADD COLUMN delta DECIMAL(12,4) NOT NULL DEFAULT 0;
UPDATE ledger_entries SET delta = CAST(delta_micro / 1000000.0 AS DECIMAL(12,4));
ALTER TABLE ledger_entries DROP COLUMN delta_micro;

-- tenants
ALTER TABLE tenants
    ADD COLUMN balance     DECIMAL(12,4) NOT NULL DEFAULT 0,
    ADD COLUMN reserved    DECIMAL(12,4) NOT NULL DEFAULT 0,
    ADD COLUMN trust_quota DECIMAL(12,4) NOT NULL DEFAULT 10;
UPDATE tenants SET
    balance     = CAST(balance_micro / 1000000.0 AS DECIMAL(12,4)),
    reserved    = CAST(reserved_micro / 1000000.0 AS DECIMAL(12,4)),
    trust_quota = CAST(trust_quota_micro / 1000000.0 AS DECIMAL(12,4));
ALTER TABLE tenants
    DROP COLUMN balance_micro, DROP COLUMN reserved_micro, DROP COLUMN trust_quota_micro,
    DROP COLUMN budget_micro;

-- models
ALTER TABLE models
    ADD COLUMN input_price_1k  DECIMAL(10,6) NOT NULL DEFAULT 0,
    ADD COLUMN output_price_1k DECIMAL(10,6) NOT NULL DEFAULT 0;
UPDATE models SET
    input_price_1k  = CAST(input_price_1k_micro  / 1000000.0 AS DECIMAL(10,6)),
    output_price_1k = CAST(output_price_1k_micro / 1000000.0 AS DECIMAL(10,6));
ALTER TABLE models
    DROP COLUMN input_price_1k_micro, DROP COLUMN output_price_1k_micro,
    DROP COLUMN pricing_model, DROP COLUMN pricing_config,
    DROP COLUMN price_version, DROP COLUMN price_updated_at;
```

---

## 8. Go 代码改动清单（不写实现，只列需要改的文件和接口签名）

### 8.1 新增包

| 文件 | 职责 |
|---|---|
| `pkg/ledger/types.go` | `MicroUSD` 类型 + 转换函数 |
| `pkg/ledger/lua_v2.go` | `scriptReservesV2`（余额拦截）+ `scriptSettlesV2`（整数版）+ `scriptRefundV2` |
| `pkg/ledger/reserves.go` | `ReservesV2(ctx, client, tenantID, estimatedCost MicroUSD)` 返回 `(reservedAfter MicroUSD, skipped bool, available MicroUSD, blocked bool, err error)` |
| `pkg/pricing/engine.go` | 价格引擎 Estimate / Settle |
| `pkg/pricing/graduated.go` | 阶梯定价实现 |
| `pkg/pricing/package.go` | 按块定价实现 |
| `pkg/pricing/perunit.go` | 固定件定价实现 |
| `pkg/pricing/linear.go` | 线性定价（从当前逻辑抽离） |
| `pkg/pricing/engine_test.go` | 四种模型单测 + 价格版本测试 |

### 8.2 改造现有文件

| 文件 | 改动 |
|---|---|
| `pkg/ledger/ledger.go` | 三个 Lua 脚本 float → int；Reserves/Settles/Refund 返回值签名统一 MicroUSD |
| `pkg/ledger/ledger_test.go` | float64 → MicroUSD 表驱动；新增 blocked=true 拦截断言 |
| `apps/gateway/internal/middleware/quota.go` | 调用 `ReservesV2`；blocked=true 时 402 Abort；estimatedCost 从 pricing.Engine 算 |
| `apps/gateway/internal/middleware/usage_capture.go` | Settles 用 MicroUSD；ledger_entries delta 用 MicroUSD |
| `apps/gateway/internal/handler/chat_completion.go` | logAndRecord 的 cost/reserved/final 全部 MicroUSD；写入 usage_logs.price_version |
| `apps/gateway/internal/usagelog/writer.go` | Entry 结构体 cost/reserved_cost/final_cost 从 float64 → int64（μUSD）；批量 INSERT 参数类型同步 |
| `apps/reconciler/cmd/main.go` | Redis 对账用 int64；DB ledger_entries 用 BIGINT；新增 credit_lots 过期清理 |

### 8.3 不改的文件（明确边界）

| 包/文件 | 不改的原因 |
|---|---|
| `pkg/provider/*` | Provider 只管调上游、解析 usage，不管钱 |
| `pkg/routing/*` | 路由只查 abilities/models，不管价格（价格在 handler 里算） |
| `apps/gateway/internal/relay/*` | relay 只管调 Provider，不管账本 |
| `pkg/cache/*` | 缓存只管 key/value，不管金额精度 |
| `pkg/ratelimit/*` | 限流按 RPS/TPM/并发，不涉及金额 |

---

## 9. 验收清单

| # | 验收 | 口径 |
|---|---|---|
| A1 | 整数精度 | 连续 1,000,000 次 Reserves 1 μUSD → Settles → balance 回到初始值，误差 0（无浮点累积） |
| A2 | 余额拦截 | available=5000 μUSD，estimated=6000 μUSD → 402 Payment Required，Redis reserved 未增加 |
| A3 | trust_quota 免预占 | balance=5 μUSD（< trust_quota 默认 $10）→ Reserves skipped=true，不查余额 |
| A4 | linear 定价 | input 1000 tokens × $0.0015/1k + output 500 × $0.006/1k = $0.0045 = 4500 μUSD，断言精确 |
| A5 | graduated 阶梯 | 单请求 5K tokens 走 tier0 价，单请求 20K 走 tier1 价，阶梯边界正确 |
| A6 | package 按块 | block=1000 tokens，总 2500 tokens → ceil(2500/1000)=3 块 × 单价 = 精确 3× |
| A7 | per_unit 固定 | 图像生成 count=4，单价 $0.02 → 80000 μUSD = $0.08 |
| A8 | 价格版本 | UPDATE models SET input_price_1k=xxx, price_version=price_version+1 → 新请求用新价、在途请求用旧价 |
| A9 | lot 过期清理 | reconciler 扫 expires_at < NOW() 的 lot，remaining 清零，不还给租户 balance |
| A10 | lot 扣减顺序 | 两个 lot：lot_A 本月底过期 remaining=3，lot_B 永久 remaining=10，Reserves 5 → lot_A  remaining=0, lot_B remaining=8 |
| A11 | 全链路编译 | `go build ./...` 零错误，`go vet ./...` 零告警 |
| A12 | 集成测试 | 余额充足 → 200 + settle；余额不足 → 402 + 不调用上游（mock provider 零请求） |

---

## 10. 落地顺序

```
Step 1: 写 pkg/ledger/types.go（MicroUSD 类型 + 转换）
Step 2: 改 ledger.go 三个 Lua 脚本（float → int64）+ ReservesV2（加 blocked 返回值）
Step 3: 写 pkg/pricing/engine.go + linear/graduated/package/per_unit 四模型
Step 4: 写 pricing 全部单测（engine_test.go + 四模型各一组表驱动）
Step 5: 改 quota.go（调用 ReservesV2 + 402 拦截 + pricing.Estimate 精算）
Step 6: 改 usage_capture.go（整数 Settles/Refund + ledger_entries BIGINT delta）
Step 7: 改 handler logAndRecord + usagelog writer（MicroUSD 贯穿）
Step 8: 迁移脚本 sql/012_int_amounts.up.sql + .down.sql
Step 9: reconciler 扩展（lot 过期清理 + 整数对账）
Step 10: 全部测试 + go vet + go build
```

每步完成后编译通过再进下一步，禁止跨步。

---

## 附录 A：与外部参考的对照

| 设计点 | 来源 | 本复刻实现 |
|---|---|---|
| integer 金额精度 | Resvary CreditLedger (BigInt) | MicroUSD int64 (10^-6 USD) |
| posted/reserved/available 余额模型 | Resvary Balance Model | balance/reserved 两快照（available = balance - reserved），语义等价 |
| Reserves/Settles/Refund 三件套 | Resvary Usage Lifecycle | 完全一致，Redis Lua 原子（替代 Resvary 的 SERIALIZABLE 事务） |
| credit_lots 过期优先扣 | Resvary grant policies + lot allocation | 同：最近过期优先，永久 lot 最后扣 |
| 4 种价格模型 | Resvary pricing_config | linear / graduated / package / per_unit |
| 价格版本管理 | Resvary immutable price version | price_version 单调递增 + usage_logs 记录 |
| trust_quota 免预占 | 本项目阶段 10 已有 | 保留，语义不变 |
| 余额硬拦截（402） | nself hard limit enforcement | Reserves Lua 里 available < estimated → blocked=1 |
| 整数版 Reconciler | Resvary runMetered reclaim | 扫 Redis reserved_at + DB lot 过期时间 |

**核心差异**：Resvary 用 Postgres SERIALIZABLE 事务保证余额强一致；本复刻用 Redis Lua 追求低延迟 + Redis/MySQL 双写对账兜底。两者设计理念相同（银行级账本），只是事务载体不同。
