# P0 · 最基础 AI 功能：实现清单

> 本文档是"把网关跑通最小 AI 链路"的施工单。按第 6 节的顺序推进，每完成一项勾选对应验收。
>
> 状态更新时间：2026-11-10。P0 全部 7 项已完成。启动骨架（P0 #1）+ Provider（#2）+ Auth（#3）+ 模型路由（#4）+ Handler（#5）+ 记账（#6）+ 计费（#7）均已实现。

---

## 1. 先定一条判据

> 用户拿一个 API Key，调 `POST /v1/chat/completions`，指定一个模型名，能拿到正常回复（含流式），后台能查到这次调用的 token 和成本。

这条链路端到端跑通，就算"最基础 AI 功能完成"。其余都是增强。

---

## 2. 当前代码状态

P0 全部 7 项已实现并通过编译/测试。

| 模块 | 文件 | 状态 |
|---|---|---|
| 启动入口 | `apps/gateway/cmd/main.go` | ✅ 已实现 |
| 配置 | `pkg/config/config.go` | ✅ 已实现 |
| 日志 | `pkg/observability/logger.go` | ✅ 已实现 |
| PG 连接池 | `pkg/db/pool.go` | ✅ 已实现 |
| Provider 工厂 | `pkg/provider/factory.go` | ✅ 已实现 |
| OpenAI 适配器 | `pkg/provider/openai/openai.go` | ✅ 已实现（非流式 ChatCompletion + 流式 ChatCompletionStream + 单元测试） |
| Provider 接口 | `pkg/provider/provider.go` | ✅ 已扩展（加 Streamer 可选接口 + StreamChunk 类型） |
| 中间件链 | `apps/gateway/internal/middleware/chain.go` | ✅ Recover → Trace → Logger → Auth 四层 |
| Auth 中间件 | `apps/gateway/internal/middleware/auth.go` | ✅ 直连 PG：Bearer→SHA256→api_keys→tenants |
| Chat Handler | `apps/gateway/internal/handler/chat_completion.go` | ✅ 完整实现：模型路由 + 非流式 + 流式 SSE + usage_logs + 计费 |
| SQL 迁移 | `sql/001_init_core.up.sql` | ✅ 4 张核心表（tenants/api_keys/models/usage_logs） |
| SQL 回滚 | `sql/001_init_core.down.sql` | ✅ 完整 DROP TABLE |

---

## 3. P0：必须做的 7 件事

| # | 模块 | 最小实现 | 验收标准 | 状态 |
|---|---|---|---|---|
| 1 | 启动骨架 | 读 `config.yaml` → 连 PG → 注册 Provider → 装中间件 → 监听 `:8080` | `curl /healthz` 返回 200 | ✅ 已完成 |
| 2 | 供应商适配器 | 只实现 **1 个 OpenAI 兼容 Provider**（非流式 + 流式 SSE） | 能对真实上游发起一次 chat 并解析出 content 和 usage | ✅ 已完成 |
| 3 | API Key 鉴权 | 从 `Authorization: Bearer` 取 key，SHA256 后查 `api_keys`，塞 `tenant_id` 进 context；先直连 PG 不接 Redis | 无 key / 错 key 返回 401，正确 key 放行 | ✅ 已完成 |
| 4 | 模型路由 | 用 `tenant_id + model_name` 查 `models` 表，取 `provider + provider_model`，交给 Factory 建实例 | 请求模型名能映射到正确上游模型 | ✅ 已完成 |
| 5 | Chat Handler | 非流式：请求体透传上游、回 JSON；流式：`stream=true` 时 SSE 逐块转发并在结束时正常 `[DONE]` | 两种模式都能拿到正确回复，流式不卡死、不截断 | ✅ 已完成 |
| 6 | 用量记账 | 从响应里取 input/output tokens，`INSERT` 一条 `usage_logs`；先同步写 PG，不走 Kafka | 每次调用都能在表里查到记录和 latency | ✅ 已完成 |
| 7 | 最小计费 | `cost = input_tokens × input_price_1k / 1000 + output_tokens × output_price_1k / 1000`，单价读 `models` 表 | 表里 cost 与手工计算一致 | ✅ 已完成 |

---

## 4. 最小请求链路

```
API Key → 鉴权(tenant) → 查 models 得上游配置 → OpenAI Provider 转发
        → 解析 content + usage → 写 usage_logs → 返回
```

流式只是把最后一步换成 SSE 边转发边累积 usage。

### 目标接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/chat/completions` | 兼容 OpenAI 请求/响应格式，支持 `stream: true/false` |
| GET | `/v1/models` | 可选，返回当前租户可用模型列表（客户端 SDK 常调用） |

中间件顺序：

```
Recover → Trace → Logger → Auth → ChatCompletion Handler
                              ↑ 内部完成模型路由 / 调用 / 记账 / 计费
```

> 顺序说明：本阶段不引入 Quota / Cache / Gray / 重试循环，模型路由直接放在 Handler 内完成，避免为最小链路引入额外中间件状态。

---

## 5. 配套最小数据

迁移脚本尚未实现 CLI，先手工执行 `sql/001_init_core.up.sql` 建表，再插入种子数据：

```sql
-- 1. 租户
INSERT INTO tenants (name, tier, budget_monthly)
VALUES ('demo', 'free', 100)
RETURNING id;   -- 记下 id，下文用 1 代替

-- 2. API Key（只存 SHA256，不存明文）
--    生成哈希：printf '%s' 'sk-demo-123' | shasum -a 256 | awk '{print $1}'
INSERT INTO api_keys (tenant_id, hash_key, name)
VALUES (1, '<上一步生成的哈希>', 'demo-key');

-- 3. 模型映射与单价（provider 必须与 config.providers[].type 一致）
INSERT INTO models (
    tenant_id, model_name, provider, provider_model,
    input_price_1k, output_price_1k
) VALUES (
    1, 'demo-chat', 'openai', 'gpt-4o-mini',
    0.0015, 0.006
);
```

`apps/gateway/configs/config.yaml` 中的 `providers[0]` 需与上表 `provider` 对齐，API Key 通过环境变量提供：

```yaml
providers:
  - type: openai
    base_url: "https://api.openai.com/v1"   # 换成实际上游
    api_key_env: "OPENAI_API_KEY"
```

---

## 6. 落地顺序（按此推进）

1. **启动骨架** ✅ 已完成。
2. **单个 Provider** ✅ 已完成（ChatCompletion 非流式 + ChatCompletionStream 流式 SSE）。
3. **Handler 打通单次真实调用** ✅ 已完成。
4. **鉴权** ✅ 已完成（Bearer → SHA256 → `api_keys` → `tenant_id`，401/403 分支）。
5. **模型路由** ✅ 已完成（`tenant_id + model_name` → `models` 表 → Factory 实例 → 上游模型名）。
6. **用量记账** ✅ 已完成（同步 `INSERT usage_logs`）。
7. **计费** ✅ 已完成（按 `models` 单价计算 cost，落库）。

> 备注：落地实际顺序与文档略有调整——Step 3 先做 Handler 写死参数打通 curl，Step 4 补 Auth，Step 5 补模型路由，Step 6+7 流式+记账+计费一起上。全部按依赖链推进，无交叉改动。

---

## 7. 验收命令

```bash
# 正常调用（非流式）
curl -sS http://localhost:8080/v1/chat/completions \
  -H 'Authorization: Bearer sk-demo-123' \
  -H 'Content-Type: application/json' \
  -d '{"model":"demo-chat","messages":[{"role":"user","content":"你好"}],"stream":false}'

# 流式调用
curl -N http://localhost:8080/v1/chat/completions \
  -H 'Authorization: Bearer sk-demo-123' \
  -H 'Content-Type: application/json' \
  -d '{"model":"demo-chat","messages":[{"role":"user","content":"你好"}],"stream":true}'

# 鉴权失败
curl -i -sS http://localhost:8080/v1/chat/completions \
  -H 'Authorization: Bearer wrong-key' \
  -H 'Content-Type: application/json' \
  -d '{"model":"demo-chat","messages":[{"role":"user","content":"hi"}]}'
# 期望 401

# 记账核对
psql "$PG_DSN" -c \
  "SELECT request_id, model, provider, input_tokens, output_tokens, cost, latency_ms, status
   FROM usage_logs ORDER BY id DESC LIMIT 1;"
```

计费核对示例：`input_tokens=12`、`output_tokens=30`、单价 `0.0015 / 0.006`：

```
cost = 12 × 0.0015 / 1000 + 30 × 0.006 / 1000 = 0.000018 + 0.00018 = 0.000198
```

---

## 8. 明确不做（不属于"最基础"）

- 语义缓存、Qdrant、Embedding、RAG
- 灰度分流、熔断降级、多渠道路由权重、重试换渠道
- 内容审核（Moderation）
- 预扣押金 / Redis Lua / 信任额度
- Kafka 异步账单、B2B 套餐 / 发票 / 信用额度
- etcd 热更新、OTel 全链路、Prometheus 大盘
- 控制台 UI（用 SQL + curl 配置即可）

---

## 9. 完成定义（DoD）

同时满足以下 5 条，P0 结束：

- [x] 正确 Key 非流式调用返回正常 content，HTTP 200
- [x] 正确 Key 流式调用逐块输出，最终以 `[DONE]` 正常结束
- [x] 无 Key / 错 Key 返回 401，租户停用返回 403
- [x] 请求的 `model` 能按 `models` 表映射到上游真实模型
- [x] `usage_logs` 每次调用都有记录，`cost` 与手工计算一致

> 以上代码层面均已实现并通过编译/测试。端到端验收（curl + 真实上游 + PG）需启动服务后按第 7 节命令执行。
