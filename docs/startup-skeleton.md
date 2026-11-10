# 阶段 1 · 启动骨架：计划与验收

> 目标一句话：`go run .` 能起一个进程，连上 PostgreSQL，注册好 Provider，装上中间件，监听 `:8080`，`curl /healthz` 返回 200。

本阶段不处理 AI 请求，只保证"进程能干净地起来、能干净地退出、依赖缺失时能明确报错"。

---

## 1. 完成定义（DoD）

同时满足以下三条，本阶段才算完成：

1. 本机执行 `cd apps/gateway && go run .` 后，进程持续运行，启动日志按阶段打印完毕。
2. `curl -i http://localhost:8080/healthz` 返回 `200`，响应体包含 `"db":"ok"`。
3. 按下 `Ctrl+C` 后进程在 5 秒内退出，日志有 `shutdown complete`，端口 8080 被释放。

---

## 2. 范围

### 范围内

- 根目录 Go 模块初始化（`go.mod`），让 `apps/gateway`、`apps/migrate`、`pkg/` 共用
- `config.yaml` 加载（只做启动必需字段，缺失可选配置不阻塞）
- 最小结构化日志（JSON 到 stdout）
- PostgreSQL 连接池初始化 + 启动时 Ping
- Provider 工厂注册链路打通（只注册，不实现真实调用）
- 中间件链最小三件套：Recover → Trace（仅 trace_id）→ Logger
- `GET /healthz` 健康检查
- 优雅退出（SIGINT/SIGTERM）
- 失败即退出（配置缺失 / PG 不可达 / 端口占用 / 未知 Provider 类型）

### 范围外（明确不做）

- 任何 AI 请求处理（`/v1/chat/completions`、流式、计费）
- Auth / Quota / Cache / Gray / ProviderSelect / UsageCapture 中间件
- etcd 热更新、Redis、Kafka、Qdrant、OTel Collector、Prometheus、Grafana
- 数据库迁移与种子数据（骨架阶段只需要一个可连的 PG 库）
- Console 前端

---

## 3. 现状盘点

| 文件 | 当前状态 | 本阶段要补什么 |
|---|---|---|
| `go.mod` | 不存在 | 新建根模块 |
| `apps/gateway/cmd/main.go` | 只有注释 | 完整启动流程 + 优雅退出 |
| `pkg/config/config.go` | 只有包注释 | 结构体 + yaml 加载 + 校验 |
| `apps/gateway/configs/config.example.yaml` | 有示例 | 补 `providers` 段，复制为 `config.yaml` |
| `pkg/db/pool.go` | 只有包注释 | pgxpool 初始化 / Ping / Close |
| `pkg/observability/logger.go` | 只有注释 | zap JSON logger + 字段注入 |
| `pkg/provider/provider.go` | 接口已定义 | 不动 |
| `pkg/provider/factory.go` | 有注册/获取，`AllRegistered` 是 TODO | 补 `AllRegistered` + 未知类型报错 |
| `pkg/provider/openai/openai.go` | 只有 TODO | 最小骨架实现（构造函数 + 未实现错误） |
| `apps/gateway/internal/middleware/recover.go` | 只有注释 | 实现 |
| `apps/gateway/internal/middleware/trace.go` | 只有注释 | 仅生成/透传 `trace_id`（不接 OTel） |
| `apps/gateway/internal/middleware/logger.go` | 只有注释 | 实现 |
| `infra/docker-compose.yml` | 只有注释，无任何 service | 补最小 `postgres` 服务 |
| `infra/scripts/init-local.sh` | 占位脚本 | 本阶段只补 PG 启动部分（可选） |

---

## 4. 前置条件（先确认，否则验收跑不起来）

| 项 | 现状 | 处理 |
|---|---|---|
| Go | 已安装 `go1.26.0 darwin/arm64` | 无需处理 |
| Docker / Docker Compose | **未安装**（`docker` 命令不存在） | 安装 Docker Desktop 或 OrbStack |
| psql 客户端 | **未安装** | 随 Docker 镜像使用，或 `brew install libpq` |
| PostgreSQL 实例 | 无 | 二选一：① 补 compose 的 `postgres` 服务；② 使用已有 PG，改 DSN |

> 阻塞点：没有 PG 就无法完成 A4 验收。需要先决定用「本地容器 PG」还是「远程 PG」。

推荐的本地 PG 配置（写进 `infra/docker-compose.yml`）：

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: root
      POSTGRES_PASSWORD: root
      POSTGRES_DB: ai_gateway
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U root -d ai_gateway"]
      interval: 3s
      timeout: 3s
      retries: 10
volumes:
  pgdata:
```

启动：`cd infra && docker compose up -d postgres`

---

## 5. 实施计划（按依赖顺序，7 步）

### Step 1 · 工程初始化

- 根目录 `go mod init ai-gateway`
- 引入依赖：`gin-gonic/gin`、`jackc/pgx/v5`（pgxpool）、`go.uber.org/zap`、`gopkg.in/yaml.v3`
- 确认 `go build ./...` 通过（此时还只有空包）

**完成标志**：`go build ./...` 无报错；`go.mod` / `go.sum` 存在。

### Step 2 · 配置加载

- `pkg/config/config.go`：定义 `Config`，本阶段只强校验：
  - `postgres.dsn`（非空）
  - `gateway.listen_addr`（非空，默认 `:8080`）
- 可选字段（etcd / redis / kafka / qdrant）解析但不校验，缺失不影响启动
- 新增 `providers` 列表：`type` / `base_url` / `api_key_env`
- 支持 `--config` 参数，默认 `configs/config.yaml`；文件不存在时返回明确错误

**完成标志**：错误路径能打印出"配置文件不存在，请从 config.example.yaml 复制"。

### Step 3 · 最小日志

- `pkg/observability/logger.go`：初始化 zap（JSON、stdout、`level=info`）
- 提供 `With(fields...)`，后续中间件/请求日志复用
- 启动日志统一格式：`stage=<name> msg=<text> elapsed_ms=<n>`

**完成标志**：启动日志为可直接被 Loki 采集的 JSON，一条一行。

### Step 4 · PostgreSQL 连接池

- `pkg/db/pool.go`：`InitPool(ctx, dsn, poolSize)` → `*pgxpool.Pool`
- 启动时 `Ping`，超时 5s；失败返回错误由 main 退出
- 提供 `GetPool()` 单例与 `Close()`

**完成标志**：PG 正常时日志出现 `db connected`；DSN 错误时 5s 内退出。

### Step 5 · Provider 注册

- `pkg/provider/openai/openai.go`：实现 `Name()` 与构造函数；`ChatCompletion` 等返回 `ErrNotImplemented`（本阶段不接上游）
- `factory.go`：补 `AllRegistered()`；`Get` 遇未知类型返回 error 而不是 nil
- main 里按 `config.providers` 逐个注册，遇到未知 `type` 直接启动失败并打印该类型

**完成标志**：启动日志 `providers registered: [openai]`。

### Step 6 · 中间件最小三件套

| 顺序 | 中间件 | 本阶段职责 |
|---|---|---|
| 1 | Recover | 捕获 panic，返回 `{"code":500,"message":"internal error"}`，记录 stacktrace |
| 2 | Trace | 读取 `X-Request-Id`，没有则生成 UUID；`ctx.Set("trace_id")` 并回写响应头 |
| 3 | Logger | 请求开始/结束各一条日志，含 `trace_id` / `method` / `path` / `status` / `latency_ms` |

> 说明：仓库注释里 Trace 要接 OTel span，本阶段**只做 trace_id**，OTel 接入放到可观测阶段，避免为一个 200 的健康检查引入 Collector 依赖。

**完成标志**：一次 `/healthz` 请求产生 start / end 两条日志，`trace_id` 一致。

### Step 7 · main 组装 + 路由 + 优雅退出

- 启动顺序：`config → logger → db → providers → middleware → router → listen`
- 路由：仅 `GET /healthz`（内部对 DB 做 Ping，超时 1s）
- `healthz` 响应：

```json
{"status":"ok","db":"ok","version":"dev"}
```

- `http.Server` 设置 `ReadHeaderTimeout`；监听失败（端口占用）直接退出
- 监听 `SIGINT` / `SIGTERM`，`Shutdown` 超时 5s，最后关闭连接池

**完成标志**：见第 7 节验收表。

---

## 6. 启动时序

```
main
 ├─ config.Load(configs/config.yaml)      失败 → exit 1
 ├─ observability.NewLogger()
 ├─ db.InitPool(ctx) + Ping               失败 → exit 1（≤5s）
 ├─ provider.Register(openai)             未知 type → exit 1
 ├─ middleware.Chain(Recover→Trace→Logger)
 ├─ gin router: GET /healthz
 └─ http.ListenAndServe(:8080)            端口占用 → exit 1
        │
        └─ SIGINT/SIGTERM → Shutdown(5s) → pool.Close() → exit 0
```

---

## 7. 验收标准

| ID | 场景 | 操作 | 期望结果 |
|---|---|---|---|
| A1 | 编译 | `go build ./...` | 退出码 0，无报错 |
| A2 | 静态检查 | `go vet ./...` | 退出码 0 |
| A3 | 正常启动 | `cd apps/gateway && go run .` | 日志依次出现 config / db / providers / listen 四个阶段，最后一行含 `:8080` |
| A4 | 健康检查 | `curl -i http://localhost:8080/healthz` | `HTTP/1.1 200`，body 含 `"status":"ok"` 与 `"db":"ok"` |
| A5 | 启动耗时 | 正常环境启动 | 从执行到 listen 完成 ≤ 3s（不含 PG 建连异常重试） |
| A6 | 配置缺失 | 重命名 `config.yaml` 后启动 | 退出码 1，日志含"配置文件不存在"及期望路径 |
| A7 | PG 不可达 | 停掉 PG 后启动 | 5s 内退出码 1，日志含 `db connect failed` 与脱敏后的 host:port |
| A8 | 端口占用 | 先占用 8080 再启动 | 退出码 1，日志含 `listen :8080 failed` |
| A9 | 优雅退出 | 运行中 `Ctrl+C` | 5s 内退出，日志含 `shutdown complete`；`lsof -iTCP:8080 -sTCP:LISTEN` 无输出 |
| A10 | 可选依赖缺失 | 不配 etcd/redis/kafka/qdrant | 启动不受影响，`/healthz` 仍 200 |
| A11 | 未知 Provider | `providers[0].type: foo` | 退出码 1，日志明确指出 `unknown provider type: foo` |
| A12（可选） | panic 兜底 | 单测触发 handler panic | 返回 500 JSON，进程不退出，日志有 stacktrace |

---

## 8. 验收操作清单（可直接复制执行）

```bash
# 0. 前置：启动 PG（首次需先补 infra/docker-compose.yml 的 postgres 服务）
cd infra && docker compose up -d postgres && cd ..

# 1. 编译与静态检查（A1 / A2）
go build ./... && go vet ./...

# 2. 准备配置
cp apps/gateway/configs/config.example.yaml apps/gateway/configs/config.yaml

# 3. 启动（A3 / A5）
cd apps/gateway && go run .

# 4. 另开终端验证健康检查（A4）
curl -i http://localhost:8080/healthz

# 5. 验证退出（A9）
#    回到启动终端按 Ctrl+C，然后执行：
lsof -nP -iTCP:8080 -sTCP:LISTEN
```

失败场景（A6 / A7 / A8 / A11）分别按"重命名配置、停 PG、占端口、改 provider type"注入后启动一次即可。

---

## 9. 风险与注意事项

| 风险 | 影响 | 对策 |
|---|---|---|
| 本机未装 Docker | 无法起 PG，A4/A7 无法验收 | 先安装 Docker Desktop 或 OrbStack；或改用远程 PG 并改 DSN |
| 仓库无 `go.mod`，模块路径未定 | import 路径返工 | 先用 `ai-gateway` 作为 module 名，推送远端前统一替换 |
| `config.example.yaml` 无 `providers` 段 | Step 5 无法注册 | 本阶段同步补最小 `providers` 配置 |
| 启动即强依赖 PG | PG 抖动导致网关不可用 | 骨架阶段可以接受（fail-fast）；后续阶段再引入降级启动 |
| 日志里打印完整 DSN | 泄露数据库密码 | 打印前脱敏为 `host:port/db` |
| Trace 只做 trace_id | 与仓库注释不一致 | 在代码注释中标注"OTel 接入延后"，避免误解 |

---

## 10. 交付物

- 根 `go.mod` / `go.sum`
- `apps/gateway/cmd/main.go` 完整启动实现
- `pkg/config/config.go`、`pkg/db/pool.go`、`pkg/observability/logger.go` 实现
- `pkg/provider/openai/openai.go` 最小实现、`pkg/provider/factory.go` 补齐
- 三个中间件实现（Recover / Trace / Logger）
- `apps/gateway/configs/config.example.yaml` 增补 `providers`
- `infra/docker-compose.yml` 最小 `postgres` 服务
- 本文件第 7 节验收表全部通过

---

## 11. 下一阶段衔接

本阶段通过后，进入「单次真实调用」：实现 OpenAI 兼容 Provider 的 `ChatCompletion`，先用写死的 API Key 打通一次上游请求，再补鉴权与模型路由。启动骨架不做任何改动。
