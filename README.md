# AI Gateway · 生产级多供应商 AI 网关

Monorepo 结构：
- apps/gateway    Go 网关（核心数据面）
- apps/worker    Python Worker（异步任务）
- apps/console   Next.js 管理控制台
- apps/migrate   Go 数据库迁移 CLI
- pkg/           Go 共享库（Provider / Config / OTel / DB / Quota）
- sql/           SQL 迁移脚本
- infra/         Docker Compose + K8s + Prometheus + Grafana

详细架构图见 docs/architecture/index.html

## 本地开发

```bash
# 一键起全中间件 + 跑迁移
bash infra/scripts/init-local.sh

# 起 Gateway
cd apps/gateway && go run .

# 起 Worker
cd apps/worker && python -m celery -A app.main worker
```

## 🔴 New API (QuantumNous/new-api) 借鉴的 5 个核心设计

| 借鉴点 | 来源 | 落在哪个文件 | 为什么借鉴 |
|---|---|---|---|
| **abilities 倒排索引** | 正排 models 表是"主体→属性"，高频查询是反向的"谁能服务这个模型" | `sql/abilities 表` + `apps/gateway/internal/middleware/provider_select.go` | 写少读多的数据预建索引，路由查询 O(1) |
| **retry 复用 = 换渠道 + 降级** | 重试次数 `retry` 同时复用为优先级层级下标 | `apps/gateway/internal/middleware/provider_select.go` | 零额外状态对象，"换渠道"和"降级优先级"咬合在一起 |
| **预扣押金 → 整单重算 → 差额退款** | 完整账本生命周期，Redis Lua 原子扣减 | `apps/gateway/internal/middleware/quota.go` + `usage_capture.go` + `pkg/quota/limiter.go` | 防止用户刷爆跑路，余额>信任额度跳过预扣优化高并发 |
| **BodyStorage** | 一份请求体全链路复用（中间件→Handler→重试） | `apps/gateway/internal/model/body_storage.go` | Gin Request.Body 读一次就没了，防止重试发空 body |
| **SSE 流式引擎** | 三协程 + 断开即止损 + 写超时 + panic 全覆盖 | `apps/gateway/internal/stream/stream_scanner.go` | 流式是 AI 网关最容易出事的地方，New API 的防御设计值得抄 |

架构本身比 New API 好（Monorepo 拆分、etcd Watch 热更、独立 Worker、可观测全栈），只借鉴以上 5 个机制。

## 目录归属

| 目录 | 归属 | 说明 |
|---|---|---|
| pkg/provider/ | Go | Provider 统一接口 + 各供应商实现 |
| pkg/config/ | Go | 配置加载（yaml + etcd Watch） |
| pkg/observability/ | Go | OTel + Prometheus + Logger 封装 |
| pkg/db/ | Go | MySQL 连接池 |
| pkg/quota/ | Go | 限流算法 + etcd 协调 |
| apps/gateway/ | Go | 核心数据面网关 |
| apps/worker/ | Python | 异步任务 Worker |
| apps/console/ | TS/Next | 管理控制台 |
| apps/migrate/ | Go | 数据库迁移 CLI |
| sql/ | SQL | 迁移脚本 |
| infra/ | YAML/Docker | 基础设施配置 |
| docs/ | HTML | 架构文档 |
