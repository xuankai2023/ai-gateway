# AI Gateway 对外接口文档

版本：v1（P0）
Base URL：`http://localhost:8080/v1`

协议与 OpenAI Chat Completions 兼容，前端可直接用 `fetch` 或任意 OpenAI SDK 调用。

---

## 1. 鉴权

所有 `/v1/*` 接口都需要在请求头携带网关 Key：

```
Authorization: Bearer <API_KEY>
```

- 缺失或格式错误 → `401`
- Key 不存在或已过期 → `401`
- 租户被停用 → `403`

> Key 属于服务端凭证，**不要写进前端打包产物**。生产环境请让前端调用自己的后端，再由后端转发；本地开发可用 dev proxy（见第 7 节）。

---

## 2. 可用模型

| model | 上游 | 说明 |
|---|---|---|
| `deepseek-flash` | DeepSeek | 快速模型 |
| `deepseek-v4-pro` | DeepSeek | 高能力模型 |

`model` 必须传上表中的值，传入未配置的模型返回 `404`。

---

## 3. 对话补全

```
POST /v1/chat/completions
Content-Type: application/json
Authorization: Bearer <API_KEY>
```

### 3.1 请求参数

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `model` | string | 是 | 模型名，见上表 |
| `messages` | array | 是 | 至少 1 条消息 |
| `messages[].role` | string | 是 | `system` / `user` / `assistant` |
| `messages[].content` | string | 是 | 纯文本；暂不支持数组、图片、tool_calls |
| `temperature` | number | 否 | 采样温度，> 0 时生效 |
| `max_tokens` | number | 否 | 最大输出 token 数 |
| `stream` | boolean | 否 | `true` 返回 SSE 流，默认 `false` |

请求体上限 10MB。

> `max_tokens` 建议不低于 800 或不传：DeepSeek 会先消耗推理 token，设置过小可能导致 `content` 为空（`finish_reason` 仍为 `stop`）。

### 3.2 非流式响应

```json
{
  "id": "chatcmpl-20261110180409",
  "object": "chat.completion",
  "created": 1794305049,
  "model": "deepseek-flash",
  "choices": [
    {
      "index": 0,
      "message": { "role": "assistant", "content": "接入成功" },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 37,
    "completion_tokens": 40,
    "total_tokens": 77
  }
}
```

### 3.3 流式响应（`stream: true`）

响应头：

```
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
X-Accel-Buffering: no
```

响应体为 SSE，每帧格式 `data: <json>\n\n`，最后一帧固定为 `data: [DONE]\n\n`：

```
data: {"id":"chatcmpl-20261110180412","object":"chat.completion.chunk","created":1794305052,"model":"deepseek-v4-pro","choices":[{"index":0,"delta":{"content":"你"},"finish_reason":null}]}

data: {"id":"chatcmpl-20261110180412","object":"chat.completion.chunk","created":1794305052,"model":"deepseek-v4-pro","choices":[{"index":0,"delta":{"content":"好"},"finish_reason":null}]}

data: {"id":"chatcmpl-20261110180412","object":"chat.completion.chunk","created":1794305052,"model":"deepseek-v4-pro","choices":[{"index":0,"delta":{"content":""},"finish_reason":"stop"}]}

data: [DONE]
```

注意：

- 部分模型的 `delta.content` 可能先出现空字符串帧，前端需跳过空内容。
- `finish_reason: "stop"` 表示生成结束，随后一定会有 `[DONE]`。
- 流式模式下如果上游连接阶段失败，网关会自动换渠道重试；一旦已开始下发数据则不再重试，中途异常会下发一帧 `{"error":{...}}` 后跟 `[DONE]`。

---

## 4. 错误响应

统一格式：

```json
{ "error": { "message": "...", "type": "..." } }
```

| HTTP | type | 场景 |
|---|---|---|
| 400 | `invalid_request_error` | 请求体不是合法 JSON、缺 model/messages |
| 401 | `authentication_error` | 未带 Key、Key 无效或过期 |
| 403 | `forbidden` | 租户被停用 |
| 404 | `model_error` | 模型未配置 |
| 413 | `request_error` | 请求体超过 10MB |
| 500 | `internal_error` | 服务内部错误（DB、路由等） |
| 502 | `upstream_error` | 所有上游渠道调用失败 |

---

## 5. 前端调用示例

### 5.1 非流式

```ts
const BASE_URL = "http://localhost:8080/v1";
const API_KEY = "sk-xxx"; // 生产环境请放到自己的后端

async function chat(content: string) {
  const res = await fetch(`${BASE_URL}/chat/completions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${API_KEY}`,
    },
    body: JSON.stringify({
      model: "deepseek-flash",
      messages: [{ role: "user", content }],
    }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => null);
    throw new Error(err?.error?.message ?? `HTTP ${res.status}`);
  }

  const data = await res.json();
  return data.choices[0].message.content as string;
}
```

### 5.2 流式（逐字输出）

`EventSource` 不支持 POST 和自定义 Header，必须用 `fetch` + `ReadableStream`：

```ts
async function chatStream(
  content: string,
  onDelta: (text: string) => void,
  signal?: AbortSignal,
) {
  const res = await fetch(`${BASE_URL}/chat/completions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${API_KEY}`,
    },
    body: JSON.stringify({
      model: "deepseek-flash",
      messages: [{ role: "user", content }],
      stream: true,
    }),
    signal,
  });

  if (!res.ok || !res.body) {
    throw new Error(`HTTP ${res.status}`);
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    // SSE 事件以空行分隔
    const events = buffer.split("\n\n");
    buffer = events.pop() ?? "";

    for (const event of events) {
      for (const line of event.split("\n")) {
        if (!line.startsWith("data: ")) continue;
        const payload = line.slice(6);
        if (payload === "[DONE]") return;
        try {
          const frame = JSON.parse(payload);
          const text = frame?.choices?.[0]?.delta?.content;
          if (text) onDelta(text);
        } catch {
          // 忽略无法解析的帧
        }
      }
    }
  }
}
```

中止请求：调用时传入 `AbortController.signal`，`abort()` 后网关会同步停止读取上游，不继续计费。

---

## 6. 其他接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 健康检查（无需鉴权），DB 正常返回 200 |
| GET | `http://localhost:8081/metrics` | Prometheus 指标（独立端口） |

---

## 7. CORS 跨域

网关已开启 CORS，浏览器可以直接调用，无需额外代理。

允许来源由配置项控制（[config.yaml](../../apps/gateway/configs/config.yaml)）：

```yaml
gateway:
  # ["*"] 允许任意来源，仅建议本地开发使用；生产请改成具体前端域名
  cors_allowed_origins: ["http://localhost:5173", "https://your-app.com"]
```

行为说明：

- 预检 `OPTIONS /v1/chat/completions` 返回 `204`，**不需要鉴权**；
- 允许的请求头：`Authorization`、`Content-Type`；
- 允许的方法：`GET`、`POST`、`OPTIONS`；
- 预检结果缓存 24 小时（`Access-Control-Max-Age: 86400`）；
- 来源不在白名单时不会返回 CORS 头，浏览器会拦截。

本地开发如果前端和网关不同源，也可以继续用 Vite 代理（可选）：

```ts
// vite.config.ts
export default defineConfig({
  server: {
    proxy: {
      "/v1": {
        target: "http://localhost:8080",
        changeOrigin: true,
      },
    },
  },
});
```

此时前端把 `BASE_URL` 改成 `/v1` 即可。

---

## 8. 计费与限流说明

- 每次调用会写一条 `usage_logs`，返回体里的 `usage` 为上游真实 token 数。
- 当前两个模型的 `input_price_1k` / `output_price_1k` 均配置为 0，因此 `cost` 为 0；需要计费时更新 `abilities` 表对应价格。
- P0 阶段路由未挂限流中间件，接入方请自行控制并发。

---

## 9. 管理面 API（Admin，阶段 15）

管理面独立监听 `:8082`（默认，生产经 NetworkPolicy 仅内网可达），鉴权用独立 `X-Admin-Token` 头（与业务 api_keys 体系隔离）。租户 / API key / 模型 / 渠道 / 灰度规则的变更一律走 Admin API，**不再手工插 SQL**；所有写操作落 `admin_audit_logs` 审计并广播缓存失效（全节点 100ms 级生效）。

```bash
# 创建租户
curl -X POST :8082/admin/v1/tenants -H "X-Admin-Token: $ADMIN_TOKEN" \
  -d '{"name":"acme","tier":"pro","budget_monthly":500,"rps_limit":100}'

# 签发 API key（明文只在本次响应返回一次，DB 只存 sha256）
curl -X POST :8082/admin/v1/tenants/1/keys -H "X-Admin-Token: $ADMIN_TOKEN" \
  -d '{"name":"prod","expires_in_days":90}'

# 吊销 key（全节点立即 401）
curl -X DELETE :8082/admin/v1/keys/7 -H "X-Admin-Token: $ADMIN_TOKEN"

# 轮换 key（阶段 16：新 key 立即生效，旧 key 进入宽限期，过宽限期 401）
curl -X POST :8082/admin/v1/keys/7/rotate -H "X-Admin-Token: $ADMIN_TOKEN" \
  -d '{"grace_period_seconds":86400}'

# 配置热更新（不重进程生效；白名单：max_attempts/default_rps_limit/
# default_tpm_limit/default_concurrency_limit/request_timeout/stream_max_duration）
curl -X PUT :8082/admin/v1/config -H "X-Admin-Token: $ADMIN_TOKEN" \
  -d '{"default_rps_limit":50}'

# 审计查询
curl :8082/admin/v1/audit_logs -H "X-Admin-Token: $ADMIN_TOKEN"
```

其他资源（`models` / `abilities` / `channel_keys` / `gray_rules`）同样提供 `GET 列表 / POST 创建 / PATCH 更新 / DELETE 删除`。`channel_keys.key_ref` 只接受 `env://VAR` 或 `vault://path` 引用，拒绝明文。
