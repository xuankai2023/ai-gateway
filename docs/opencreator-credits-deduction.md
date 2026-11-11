# OpenCreator 积分扣减完整方案

> 本文只描述 OpenCreator 现有实现，作为 ai-gateway 积分扣减的参考方案。
> 不修改 ai-gateway 任何现有代码；第 13 节仅给出映射关系与接入点。
>
> 代码来源（私有仓库，已核对源码）：
> - `xuankai2023/cxk-opencreator-publisher`：账户、积分、订阅、Stripe、统一积分服务
> - `xuankai2023/cxk-opencreator-consumer`：消费端积分预扣与 Redis 扣减
> - `xuankai2023/cxk-opencreator-admin-next`：管理端发放/扣减积分
> - `xuankai2023/cxk-opencreator-frontend`：余额展示与计费页

---

## 0. 结论先行

这套方案可以照搬，核心不是"用 Redis 还是用 MongoDB"，而是四个机制同时存在：

| 机制 | 解决的问题 | OpenCreator 的做法 |
|---|---|---|
| DB 记账 | 余额不能只存在缓存里 | MongoDB 是记账真源，Redis 是扣减层 |
| 缓存回填 | 缓存丢了、过期了还能恢复 | 未命中时从 MongoDB `SET NX` 回填 |
| TTL 续期 | 扣到一半缓存过期 | 剩余寿命低于 1/4 自动续期 |
| pending 补偿 | 预扣成功但结算失败 | 预扣时原子写 pending 记录，失败不回滚，留待补偿 |
| 幂等 | 支付/重试导致重复加减 | Redis `SET NX` + 7 天 TTL |

两个必须满足的前提：

1. **pending 记录必须有消费方**，否则它只是"记录失败"，不是"补偿失败"。
2. **改库之后必须触发缓存失效或回填**，否则运营改 MongoDB 不会被扣减端看到。

---

## 1. 设计目标与边界

### 1.1 目标

- 高频扣减不能每次打数据库
- 余额不能出现负数（并发下也不能）
- 缓存丢失后可以恢复，不能"Redis 一重启余额归零"
- 一次请求的扣减要么完整生效，要么有据可查、可补
- 支付回调重试不能重复加积分

### 1.2 边界

- 本文只覆盖"积分扣减"：预扣、结算、补偿、回填、对账
- 不覆盖支付渠道本身（Stripe Checkout / Portal 由 publisher 对接）
- 不覆盖前端展示细节

---

## 2. 数据模型

### 2.1 三类积分

OpenCreator 把用户积分拆成三类，来源、有效期、扣减优先级都不同：

| 类型 | 来源 | 是否过期 | 扣减优先级 |
|---|---|---|---|
| `subscription_credits` | 订阅按周期发放 | 跟随订阅周期（`end_timestamp`） | 1（最先扣） |
| `topup_credits` | 一次性充值（Stripe） | 不过期 | 2 |
| `gift_credits` | 系统赠送 | 有过期时间（`expire_timestamp`） | 3（最后扣） |

先扣订阅积分、再扣充值积分、最后扣赠送积分，是为了让"会过期的积分先用掉"。

### 2.2 MongoDB 集合与字段

| 集合 | 关键字段 | 说明 |
|---|---|---|
| `user` | `user_id`、`user_email`、`credits` | 充值积分余额（`topup_credits`） |
| `subscription_user_credits` | `user_id`、`credits_balance`、`status`、`end_timestamp` | 订阅积分，只取 `status=active` 且未过期的一条 |
| `gift_credits` | `user_id`、`amount`、`expire_timestamp`、`gift_credit_id` | 赠送积分，只取未过期的一条 |
| `credits_inout_record` | 流水 | 积分出入账记录 |
| `credits_topup_record` | 充值单 | 充值记录，配合 Stripe |

对应 DAO：

- `dao/credits_inout_record.py`
- `dao/credits_topup_record.py`
- `dao/subscription_user_credits.py`
- `dao/gift_credits.py`

### 2.3 总余额口径

`services/unified_credits/unified_credits_service.py` 定义：

```
total_credits = topup_credits
              + active_subscription_credits（未过期）
              + active_gift_credits（未过期）
```

三个查询并行执行：

```python
topup_task        = self.get_topup_credits(user_id)
subscription_task = self.get_subscription_credits(user_id)
gift_task         = self.get_gift_credits(user_id)
topup, subscription, gift = await asyncio.gather(...)
```

关键点：

- `topup` 来自 `UserService.get_user_by_id().credits`，转成 `Decimal`
- `subscription` 来自 `a_get_one_user_active_credits`，无活跃订阅返回 `None`
- `gift` 来自 `get_available_gift_credits_from_db_only`，**只返回未过期的**
- 展示时用 `str(round(result))`，内部计算全程 `Decimal`，避免浮点误差

---

## 3. Redis 键设计

定义在 `services/unified_credits/__base/constants/redis_keys.py` 和
`services/billing/*/__base/constants/redis_keys.py`：

| Key | Value | TTL | 用途 |
|---|---|---|---|
| `opencreator:credit_balance_check:{user_id}` | 字符串数字 | `CREDIT_BALANCE_CHECK_REDIS_TTL` | 充值积分扣减余额 |
| `opencreator:subscription_user_credits:{user_id}` | 字符串数字 | `min(订阅剩余时间, 7d)` | 订阅积分扣减余额 |
| `opencreator:gift_credits:{user_id}` | JSON：`{amount, expire_timestamp, gift_credit_id}` | `min(赠送剩余时间, 7d)` | 赠送积分扣减余额 |
| `opencreator:pending_deduction:{deduction_id}` | JSON：`{user_id, cost, sub, user, gift, ts}` | 24h | 预扣凭证，用于补偿 |
| `opencreator:credits:idempotency:{idempotency_key}` | `"1"` | 7 天 | 加积分幂等锁 |

TTL 常量的含义：

- 订阅/赠送的 TTL 取"实际剩余时间"和 7 天的较小值，避免缓存比业务数据活得更久
- pending 的 24h 是"避免 pending 在服务重启前就过期"
- 幂等键 7 天是"覆盖 Stripe 等支付平台的重试窗口"

---

## 4. 读路径：缓存回填与 TTL 续期

### 4.1 两个装饰器

扣减方法上叠加了两个装饰器（源码在 `consumer/common/decorators/redis/`）：

```python
@ensure_redis_key_existed(
    get_key_func=lambda _, user_id: f"{CREDIT_BALANCE_CHECK_REDIS_KEY_PREFIX}:{user_id}",
    sync_redis_func=lambda cls, user_id: cls.init_user_balance(user_id),
)
@ensure_redis_ttl(
    get_key_func=lambda _, user_id: f"{CREDIT_BALANCE_CHECK_REDIS_KEY_PREFIX}:{user_id}",
    default_expire=CREDIT_BALANCE_CHECK_REDIS_TTL,
    refresh_threshold=CREDIT_BALANCE_CHECK_REDIS_TTL // 4,
)
async def check_for_pre_deduction(cls, user_id: str, pre_deduct_cost: int): ...
```

职责分工：

| 装饰器 | 触发条件 | 动作 |
|---|---|---|
| `ensure_redis_key_existed` | key 不存在 | 调 `sync_redis_func` 从 MongoDB 读出余额并写入 Redis |
| `ensure_redis_ttl` | 剩余 TTL < `refresh_threshold` | 把 TTL 重置为 `default_expire` |

`refresh_threshold = TTL // 4` 的意思是：缓存只要还剩不到 1/4 寿命，就续期，防止"扣减进行中 key 突然过期"。

### 4.2 三个初始化函数

`init_user_balance`：

```python
user = await UserDao.get_user_by_id(user_id)
credits_balance = user.credits or "0"
await redis_client.set(
    f"{CREDIT_BALANCE_CHECK_REDIS_KEY_PREFIX}:{user_id}",
    credits_balance,
    ex=CREDIT_BALANCE_CHECK_REDIS_TTL,
    nx=True,          # 关键：只有不存在时才写，避免覆盖并发扣减结果
)
```

`init_subscription_user_credits`：

- 先 `exists` 判断，已存在直接返回
- 从 `SubscriptionUserCreditsDao.get_active_subscription_credits_by_user_id` 取 `credits_balance`
- TTL = `min(end_timestamp - now, 7d)`
- `SET NX`

`init_gift_credits`：

- 从 `GiftCreditsDao.get_valid_gift_credits_by_user_id` 取赠送记录
- value 是 JSON，字段名必须和 Lua 脚本里读的字段一致
- TTL = `min(expire_timestamp - now, 7d)`
- `SET NX`

### 4.3 并发安全

三条规则保证回填不会把并发扣减的结果覆盖掉：

1. 所有回填都用 `SET NX`，key 已存在就放弃写
2. 初始化前先 `exists` / `SET NX` 返回值判断
3. 写入路径是"先写 Mongo，再删 Redis"，不是"先写 Redis 再回写 Mongo"

---

## 5. 写路径：Cache Aside

`unified_credits_service.py` 文件头写明：

```
写入策略（Cache Aside Pattern）
1. 先写 MongoDB（数据源）
2. 再删 Redis（缓存失效）
优点：
- 写入失败时 Redis 是旧值（偏低），不是偏高，风险可控
- 下次读取时 @ensure_redis_key_existed 装饰器会从 MongoDB 回填最新值
```

顺序不能反：

- **先删缓存再写库**：删完到写库之间读请求会回填旧值
- **先写库再删缓存**：最坏情况是缓存短暂偏旧，但下一次读会回填正确值，且旧值是"偏低"，不会让用户凭空多出积分

这条规则同样适用于运营改库：**改完 MongoDB 后要删对应的 Redis key**，否则扣减端看不到新余额。

---

## 6. 扣减路径（核心）

### 6.1 调用链

```
业务请求（生成任务）
  └─ check_for_pre_deduction(user_id, pre_deduct_cost)
       ├─ init_gift_credits(user_id)                 # 缓存回填
       ├─ init_subscription_user_credits(user_id)    # 缓存回填
       ├─ 生成 deduction_id = uuid4()
       ├─ Lua：校验 + 扣减 + 原子写 pending
       └─ 返回 (是否成功, 订阅扣减额, 充值扣减额, 赠送扣减额, deduction_id)
  └─ 执行业务（调模型、生成图片/视频）
  └─ do_deduct(user_id, 实际消耗)                    # 结算
```

### 6.2 预扣 Lua 脚本逐段解析

脚本定义在 `consumer/services/billing/credit_balance_check_service_v2.py`，
`KEYS` 4 个，`ARGV` 5 个：

```
KEYS[1] = opencreator:subscription_user_credits:{user_id}
KEYS[2] = opencreator:credit_balance_check:{user_id}
KEYS[3] = opencreator:gift_credits:{user_id}
KEYS[4] = opencreator:pending_deduction:{deduction_id}

ARGV[1] = cost
ARGV[2] = deduction_id
ARGV[3] = user_id
ARGV[4] = ts
ARGV[5] = pending_ttl
```

**第一段：读取三类余额**

```lua
local subscription_user_credits = tonumber(redis.call("GET", KEYS[1]) or "0")
local user_credits              = tonumber(redis.call("GET", KEYS[2]) or "0")

local gift_credits_raw_json = redis.call("GET", KEYS[3])
local gift_credits = 0
local gift_credits_data = {}

if gift_credits_raw_json then
    local ok, decoded = pcall(cjson.decode, gift_credits_raw_json)
    if ok and decoded and type(decoded) == "table" then
        gift_credits_data = decoded
        if gift_credits_data["amount"] then
            gift_credits = tonumber(gift_credits_data["amount"]) or 0
        end
    else
        gift_credits_data = { amount = "0" }
    end
end
```

赠送积分是 JSON，解析用 `pcall` 包住，解析失败按 0 处理，不让脚本报错。

**第二段：总额不足直接返回 -1**

```lua
local current_balance = subscription_user_credits + user_credits + gift_credits
local sub_deduct, user_deduct, gift_deduct = 0, 0, 0

if current_balance < cost then
    return { -1, 0, 0, 0 }
end
```

**第三段：按顺序扣减**

```lua
if subscription_user_credits >= cost then
    redis.call("DECRBY", KEYS[1], cost)
    sub_deduct = cost
else
    local remain = cost - subscription_user_credits
    if subscription_user_credits > 0 then
        redis.call("DECRBY", KEYS[1], subscription_user_credits)
        sub_deduct = subscription_user_credits
    end

    if user_credits >= remain then
        redis.call("DECRBY", KEYS[2], remain)
        user_deduct = remain
    else
        local remain2 = remain - user_credits
        if user_credits > 0 then
            redis.call("DECRBY", KEYS[2], user_credits)
            user_deduct = user_credits
        end

        gift_credits = gift_credits - remain2
        gift_deduct = remain2
        gift_credits_data["amount"] = tostring(gift_credits)

        local ttl = redis.call("TTL", KEYS[3])
        redis.call("SET", KEYS[3], cjson.encode(gift_credits_data))
        if ttl > 0 then
            redis.call("EXPIRE", KEYS[3], ttl)
        end
    end
end
```

赠送积分写回时必须保留原 TTL，否则一次扣减就把过期时间冲掉了。

**第四段：原子写 pending**

```lua
local pending_data = cjson.encode({
    user_id = user_id,
    cost    = cost,
    sub     = sub_deduct,
    user    = user_deduct,
    gift    = gift_deduct,
    ts      = ts
})
redis.call("SET", KEYS[4], pending_data, "EX", pending_ttl)

return { current_balance - cost, sub_deduct, user_deduct, gift_deduct }
```

**扣减和写 pending 在同一个 Lua 脚本里完成**，两者要么一起成功，要么一起失败。这是后面能补偿的前提。

### 6.3 返回值与业务判断

Python 层把 Lua 返回值转换成：

```python
# 成功
(True, sub_deducted, user_deducted, gift_deducted, deduction_id)

# 余额不足
(False, 0, 0, 0, "")
```

业务侧只看第一个值决定放行还是拒绝；后四个值用于后续结算和对账。

### 6.4 结算：`do_deduct`

`do_deduct(user_id, deduct_cost)` 同样叠加了回填与 TTL 续期装饰器，返回三元组：

```python
(subscription_deducted, user_deducted, gift_deducted)
```

职责是把"预扣"变成"最终扣减"：

- 预扣按估算值扣，结算按实际值扣
- 实际值小于预扣值时，差额退回对应积分类型
- 实际值大于预扣值时，补扣差额

> 说明：`do_deduct` 的函数体未逐行核对，以上为其签名、装饰器与返回值所确定的契约。

---

## 7. pending deduction 补偿

### 7.1 数据结构

```
key   = opencreator:pending_deduction:{deduction_id}
value = {"user_id":..., "cost":..., "sub":..., "user":..., "gift":..., "ts":...}
TTL   = 24h
```

### 7.2 作用

预扣成功、业务执行失败（模型超时、任务取消、进程崩溃）时：

- **不回滚 Redis**：回滚会和并发扣减打架，且回滚失败就没有任何记录
- **依赖 pending 记录**：谁扣的、扣了多少、什么时候扣的，全在记录里
- 由补偿任务扫描 pending，按 `deduction_id` 幂等处理：确认消费 或 退回

### 7.3 与"直接退款"的区别

| 做法 | 问题 |
|---|---|
| 失败立即 `Refund` | 退款失败就彻底丢账；并发下可能重复退款；没有凭据可查 |
| 失败写 pending | 有凭据、可重放、可审计；最坏情况是 pending 过期（24h），需要监控 |

### 7.4 需要补的一环

当前代码里 pending 的**写入**是确认的，但未在 `consumer/services/billing` 与 `consumer/scripts` 中确认到消费方。
落地时必须补一个扫描任务，否则 pending 只会堆积、不会补偿。

扫描任务的要求：

- 按 `opencreator:pending_deduction:*` 扫描，或维护一个待处理索引（`ZSET` 按 `ts` 排序更高效）
- 只处理 `ts` 超过业务最长执行时间的记录
- 处理动作必须幂等：以 `deduction_id` 作为幂等键，重复执行不重复补扣/退回
- 处理完成后删除 pending 记录

---

## 8. 幂等

### 8.1 加积分幂等

```python
async def _try_acquire_idempotency_lock(self, idempotency_key: str) -> bool:
    redis_key = f"{CREDITS_IDEMPOTENCY_KEY_PREFIX}:{idempotency_key}"
    result = await redis_client.set(redis_key, "1", ex=CREDITS_IDEMPOTENCY_TTL, nx=True)
    return result is not None
```

- `SET NX EX 7d`：第一次返回 `True`，重复调用返回 `None`
- 操作失败时 `_release_idempotency_lock` 删 key，允许重试
- 7 天覆盖 Stripe 重试窗口

### 8.2 幂等键来源

- 充值：Stripe webhook 的 event id
- 订阅发放：订阅周期标识 + 用户 id
- 赠送：`gift_credit_id`

### 8.3 扣减幂等

扣减侧以 `deduction_id` 为幂等键，保证同一笔预扣只结算一次、只补偿一次。

---

## 9. 生命周期：充值 / 订阅 / 赠送

### 9.1 充值

```
Stripe Checkout 完成
  → webhook 到 publisher
  → 校验 event id 幂等（SET NX 7d）
  → add_topup_credits(user_id, amount, source, idempotency_key)
      → 写 MongoDB user.credits
      → 删 opencreator:credit_balance_check:{user_id}
  → 写 credits_topup_record / credits_inout_record
```

兑换比例常量：

```python
DEFAULT_TOPUP_CREDITS_PER_DOLLAR = 400
PRO_TOPUP_CREDITS_PER_DOLLAR = 500
```

### 9.2 订阅

- 套餐与 Stripe plan 的映射在 `data/templates/*/stripe-subscription-plan-map.json`
- 订阅模板在 `data/templates/*/opencreator-subscription-template.json`
- 周期发放逻辑在 `services/subscription_credits_reward.py`
- 历史数据修复脚本：`scripts/fix_yearly_subscription_credits.py`

订阅积分写入 `subscription_user_credits.credits_balance`，同时删
`opencreator:subscription_user_credits:{user_id}`。

### 9.3 赠送

- 发放：`services/gift_credits/gift_credits_service.py`
- 过期：`services/gift_credits/gift_credits_expiration_handler.py`
- 过期处理必须同时删 `opencreator:gift_credits:{user_id}`，否则缓存里还有未过期的旧值

---

## 10. 对账

脚本：`consumer/scripts/check_credits_consistency.py`

### 10.1 比对维度

| 维度 | MongoDB 来源 | Redis 来源 |
|---|---|---|
| 充值积分 | `user.credits` | `opencreator:credit_balance_check:{user_id}` |
| 订阅积分 | `subscription_user_credits.credits_balance`（`status=active` 且 `end_timestamp > now`） | `opencreator:subscription_user_credits:{user_id}` |
| 赠送积分 | `gift_credits.amount`（未过期） | `opencreator:gift_credits:{user_id}` 的 JSON `amount` |

### 10.2 输出

统计项：

```python
stats = {
    "total_users": ...,
    "inconsistent_users": ...,
    "user_credit_issues": ...,
    "subscription_credit_issues": ...,
    "gift_credit_issues": ...,
}
```

两个开关控制口径：

```python
INCLUDE_REDIS_MISSING = False      # 是否把"Redis 没这个 key"算作不一致
INCLUDE_ZERO_MONGO_VALUE = False   # 是否把"Mongo 为 0"算作不一致
```

### 10.3 使用方式

- 读取 `.env.local` 里的 `MONGO_ATLAS_URI` / `MONGO_DB` / `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB`
- 逐用户拉取快照后比对
- 只报告，不自动修复；修复由人工确认后执行

辅助脚本：`scripts/check_user_credits_by_email.py`，按邮箱定位单个用户，排查客诉时使用。

---

## 11. 管理与运维

### 11.1 管理端接口

`cxk-opencreator-admin-next`：

| 路径 | 作用 |
|---|---|
| `src/app/api/v1/giveCredits` | 发放积分（GET 查询 / POST 发放） |
| `src/app/api/v1/deductCredits` | 扣减积分 |
| `src/app/api/v1/templates/ai/estimate-credits` | 预估消耗 |
| `src/backend/constants/credit-rates.ts` | 积分费率常量 |
| `src/app/(admin)/(others-pages)/giveCredits/page.tsx` | 发放页面 |
| `src/components/tables/CreditsTopupTable.tsx` | 充值记录表 |

### 11.2 运维脚本

`consumer/credits_tool.sh` 提供命令行积分操作，用于线上排障和临时调整。

### 11.3 前端

- `src/api/modules/credits.ts`：积分接口封装
- `src/api/types/credits.ts`：类型定义
- `src/features/billing/`：余额卡片、套餐定价、分析事件

---

## 12. 失败模式与取舍

| 失败场景 | 结果 | 兜底 |
|---|---|---|
| Redis 整体丢失 | 扣减层余额没了 | 下一次读触发回填，从 MongoDB 恢复（已发生扣减的差额由 pending/流水恢复） |
| 回填与并发扣减同时发生 | 可能覆盖新余额 | `SET NX` + 先判断 `exists`，不会覆盖已存在的 key |
| 缓存 TTL 在扣减中途到期 | 扣减值丢失 | `ensure_redis_ttl` 在剩余 1/4 时续期 |
| 预扣成功后进程崩溃 | 积分被扣但业务未执行 | pending 记录 + 补偿任务 |
| 补偿任务重复执行 | 重复补扣/退回 | `deduction_id` 幂等 |
| 支付回调重试 | 重复加积分 | `SET NX` 幂等键 7 天 |
| 运营直接改 MongoDB | 扣减端看不到 | 改完必须删对应 Redis key |
| 兜底兜不住 | 账不平 | 一致性脚本离线比对 + 人工修复 |

需要正视的取舍：

1. **MongoDB 与 Redis 之间是最终一致**，中间存在短暂偏差
2. **pending 如果没人消费，就只是日志**，不会自动恢复
3. **一致性脚本不自动修复**，需要人工介入
4. 代码注释里明确写着"当前以 MongoDB 为准，TODO 后续切换到以 Redis 为准"，说明这套本身也是过渡方案，不是终态

---

## 13. 映射到 ai-gateway（只列映射，不改代码）

| OpenCreator | ai-gateway 对应位置 | 说明 |
|---|---|---|
| MongoDB `user.credits` | `credit_lots` / `tenants` 余额字段 | 记账真源 |
| `opencreator:credit_balance_check:{user_id}` | [ledger:tenant:{id}](file:///Users/xuankai/Desktop/ai-gateway/pkg/ledger/lua_v2.go) | 扣减层 |
| `init_user_balance` + `ensure_redis_key_existed` | 需新增 `Rebuild(tenantID)` | 未命中时从 MySQL 回填 |
| `ensure_redis_ttl` | Lua 脚本里续期 | 防止扣减中途过期 |
| `check_for_pre_deduction` | [quota.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/middleware/quota.go) 的 `ReservesV2` | 预扣 |
| `do_deduct` | [usage_capture.go](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/middleware/usage_capture.go) 的 `Settles` | 结算 |
| `pending_deduction` | 需新增 `ledger:pending:{request_id}` 或复用 [DiskOutbox](file:///Users/xuankai/Desktop/ai-gateway/apps/gateway/internal/usagelog/writer.go) | 补偿凭据 |
| `_try_acquire_idempotency_lock` | [pkg/idempotency](file:///Users/xuankai/Desktop/ai-gateway/pkg/idempotency) | 幂等 |
| `check_credits_consistency.py` | 需新增 `cmd/credits-check` | 离线对账 |
| admin `giveCredits` / `deductCredits` | 管理面只读 + 运营 SQL | 不开放写接口 |

接入顺序（每一步都可以独立发布）：

1. 加 `Rebuild(tenantID)` 与读路径回填
2. 加 TTL 续期
3. `Settles` 失败改走 pending，去掉直接 `Refund`
4. 加 pending 消费任务
5. 加离线一致性检查命令

---

## 14. 落地检查清单

- [ ] Redis 每个余额 key 都有 TTL，且扣减前会续期
- [ ] 所有回填写入都用 `SET NX`
- [ ] 写路径顺序是"先写库、再删缓存"
- [ ] 运营改库后有明确的缓存失效步骤
- [ ] 预扣与 pending 写入在同一个 Lua 脚本里
- [ ] pending 有消费方，且消费动作幂等
- [ ] 加积分有幂等键，TTL 覆盖支付平台重试窗口
- [ ] 赠送积分扣减后保留原 TTL
- [ ] 订阅/赠送过期时同步删缓存
- [ ] 有一致性比对脚本，且有固定的执行节奏
- [ ] 监控：pending 堆积数、回填次数、缓存命中率、余额不足拒绝率

---

## 15. 未确认项

以下内容在本次代码核对中没有逐行确认，落地前需要再查：

1. `do_deduct` 函数体：是否再次写 MongoDB、是否有事务
2. pending 的实际消费方：代码里未确认到扫描任务
3. 订阅周期发放的触发方式（定时任务还是支付回调驱动）
4. 管理端 `giveCredits` / `deductCredits` 的后端接口契约与鉴权方式
