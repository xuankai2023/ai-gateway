# RAG 多项目知识库隔离 · 设计方案

> 状态：**已按本方案落地**（`sql/013_projects_assistants_documents`、`apps/gateway/internal/rag`、`pkg/vector`、`apps/gateway/cmd/ingest`）。
> 适用场景：运维平台、官网等多个项目共用一套网关，使用者是非技术用户，AI 需要检索各自的知识库。
> 落地差异：`assistants` 不存 `provider_model`（上游映射唯一事实源仍是 `abilities`）；语义缓存已支持项目隔离 + 知识库版本（`projects.index_epoch`）。

---

## 0. 一页结论

| 决策点 | 结论 |
|---|---|
| 隔离强度 | 每项目一个向量 collection（silo 模型），项目数 ≤ 50 时使用 |
| 项目归属怎么定 | API Key 绑定 `project_id`，由鉴权中间件写入 context，**不看请求体** |
| 用户怎么选助手 | 只传 `model`（如 `ops-assistant`），由服务端映射到项目 + 知识库 + 提示词 |
| 请求体能否带知识库字段 | 可以，但只能**收窄**（如 `namespace`），且必须校验归属；不能扩大 |
| 默认行为 | 拿不到 `project_id` 直接拒绝，不允许"不过滤" |
| 缓存 | 精确缓存 key 含 `tenant_id + project_id + epoch`；语义缓存 collection 按项目隔离 |
| 向量库 | Qdrant（按 project 分 collection `kb_<slug>`，检索再带 project_id 过滤） |

一句话：**同一份代码、同一个网关，但 Key、助手、collection、缓存、账单五条线全部按项目分开。**

---

## 1. 目标与非目标

### 1.1 目标

1. 官网访客只能检索官网内容，运维人员只能检索运维内容，**不靠提示词约束，靠代码强制**。
2. 非技术用户不需要理解"知识库/collection/向量"这些概念，只面对"运维助手""官网客服"。
3. 文档更新、删除后，检索结果同步生效。
4. 每次问答能追溯到用了哪个助手、检索到哪些片段、花了多少 token。
5. 项目数增长时，隔离方式可以平滑演进，不用推翻重来。

### 1.2 非目标（本阶段明确不做）

- 文档级 ACL（项目内按角色可见性）——P3 再做，但数据结构预留。
- 跨项目共享知识库（如"公共产品手册"被多个项目引用）。
- 多模态（图片、表格 OCR）、图谱、Agent 工具调用。
- 自动爬取官网：P1 先支持手工/脚本导入。
- 管理后台 UI：P1 用 SQL + CLI。

---

## 2. 业界做法对照

多租户 RAG 的三种标准形态（AWS 在 Bedrock 多租户方案里就是这么分的）：

| 形态 | 做法 | 代表实现 | 适用 |
|---|---|---|---|
| **silo** | 每租户独立索引/collection | Weaviate 内置 multi-tenancy；每个 Bedrock Knowledge Base 独立索引 | 租户少、隔离要求高、要能整库删除 |
| **pool** | 共享索引 + 租户字段过滤 | Qdrant 官方多租户文档（单 collection + payload 索引）、Pinecone namespace | 租户上千、运维 collection 成本高 |
| **bridge** | 大租户 silo，小租户 pool | 混合部署 | 租户规模差异极大 |

本项目取 **silo**：项目只有几个，隔离彻底、删库干净、排查简单。项目数超过 50 或单项目文档量差异过大时，再评估 bridge。

入口层面对标的是"Key 作用域到应用/项目"这一层：OpenAI 平台的 API Key 作用域是 Project；Dify、FastGPT、RAGFlow 的 API Key 绑定应用/知识库，检索接口用 Key 反查授权范围。这是行业默认做法，不是特殊设计。

---

## 3. 现状盘点与差距

已有：

- `tenants` / `api_keys` / `models` / `abilities` / `usage_logs`（见 `sql/001`、`sql/002`）。
- 鉴权中间件 `apps/gateway/internal/middleware/auth.go` 已注入 `tenant_id` / `project_id` / `api_key_id` / `tenant_status`。
- 对话链路 `apps/gateway/internal/handler/chat_completion.go` 已完成模型路由、重试、流式、助手解析与资料注入。
- `apps/gateway/internal/cache/semantic.go` 已实现两级缓存，并按 tenant + project + epoch 隔离。
- `pkg/vector` 提供 OpenAI 兼容 embedding 与 Qdrant REST 客户端（无第三方依赖）。

本次新增（已落地）：

| 能力 | 位置 |
|---|---|
| 项目 / 助手 / 文档元数据与 usage_logs 维度 | `sql/013_projects_assistants_documents.*.sql` |
| 助手解析、检索拼装、文档入库 | `apps/gateway/internal/rag` |
| 入库 CLI | `apps/gateway/cmd/ingest` |
| 配置项 | `configs/config.example.yaml` 的 `rag:` 段 |

---

## 4. 隔离模型：三道闸

### 4.1 接入层（唯一可信来源）

```
Authorization: Bearer sk-xxx  →  api_keys.hash_key  →  {tenant_id, project_id}
```

- `api_keys.project_id` 非空 = 项目 Key；为空且 `scope='tenant'` = 管理 Key（仅内部用）。
- 鉴权通过后写入 context：`tenant_id`、`project_id`、`api_key_id`。
- 请求体里任何形似项目的字段，**都不参与鉴权判断**。

### 4.2 检索层（强制默认拒绝）

- 检索函数的签名里 `project_id` 是必填参数，缺失返回错误，而不是"跳过过滤"。
- collection 名由服务端用 `project.slug` 拼接，**不接受调用方可传入的 collection 名**。
- 每次检索都带 payload 条件 `project_id = <ctx>`，即使 collection 已经隔离（双保险，防止未来切 pool 时漏改）。

### 4.3 存储层

- Qdrant collection 命名：`kb_<project_slug>`，slug 全局唯一（建表时约束）。
- 向量 payload 保留 `project_id`、`doc_id`、`chunk_no`、`title`、`source`、`acl`、`updated_at`、`text`。
- 删除项目 = 删除对应 collection + `documents` 记录，无残留。

### 4.4 越权行为约定

| 场景 | 返回 |
|---|---|
| 项目 Key 在请求体传了别的项目 ID / namespace | `403 forbidden`，并记录审计日志 |
| 项目 Key 请求管理接口 | `403` |
| 管理 Key 请求项目助手 | `400`，必须显式指定项目（P1 不支持，直接拒绝） |
| 检索结果为空 | 正常回答"没有找到相关资料"，不跨项目兜底 |

---

## 5. 数据模型（增量设计）

以下 DDL 已落地为 `sql/013_projects_assistants_documents.up.sql` / `.down.sql`，走现有 migrate 工具。

### 5.1 项目

```sql
CREATE TABLE projects (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id   BIGINT       NOT NULL,
    name        VARCHAR(128) NOT NULL,           -- 运维平台 / 官网
    slug        VARCHAR(64)  NOT NULL,           -- 用于 collection 名与 URL
    status      VARCHAR(16)  NOT NULL DEFAULT 'active',
    index_epoch BIGINT       NOT NULL DEFAULT 0, -- 知识库版本：文档变更 +1，缓存 key 携带
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_projects_slug (slug),
    UNIQUE KEY uk_projects_tenant_name (tenant_id, name),
    CONSTRAINT fk_projects_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 5.2 API Key 绑定项目

```sql
ALTER TABLE api_keys
    ADD COLUMN project_id BIGINT NULL AFTER tenant_id,
    ADD COLUMN scope VARCHAR(16) NOT NULL DEFAULT 'project' AFTER name,  -- project | tenant
    ADD INDEX idx_api_keys_project (project_id),
    ADD CONSTRAINT fk_api_keys_project FOREIGN KEY (project_id) REFERENCES projects(id);
```

规则：

- `scope='project'`：`project_id` 必填，只能用本项目助手。
- `scope='tenant'`：`project_id` 为空，只能调管理接口，不能直接问答。
- 现有存量 Key 迁移时置 `scope='project'` 并把 `project_id` 回填到默认项目，避免线上断流。

### 5.3 助手（产品配置与路由分离）

```sql
CREATE TABLE assistants (
    id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id          BIGINT       NOT NULL,
    project_id         BIGINT       NOT NULL,
    model_name         VARCHAR(64)  NOT NULL,      -- 对外入口：ops-assistant / site-assistant
    system_prompt      TEXT,
    top_k              INT          NOT NULL DEFAULT 5,
    score_threshold    DECIMAL(4,3) NOT NULL DEFAULT 0.300,
    max_context_tokens INT          NOT NULL DEFAULT 2000,
    enabled            TINYINT(1)   NOT NULL DEFAULT 1,
    created_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_assistants_entry (tenant_id, model_name),
    INDEX idx_assistants_project (project_id),
    CONSTRAINT fk_assistants_project FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

为什么单独建表而不是往 `models` 加列：

- `models` / `abilities` 的职责是"模型名 → 上游渠道"，属于基础设施配置。
- 助手是产品配置（知识库、提示词、召回参数），会随业务频繁改。
- 混在一起后，改一句提示词要动路由表，排障时也分不清是路由问题还是产品配置问题。

路由仍然复用现有机制：为 `ops-assistant` 在 `abilities` 里插一条指向 `deepseek-chat` 的记录即可，`relay` 不需要改。

### 5.4 文档元数据

```sql
CREATE TABLE documents (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    project_id  BIGINT       NOT NULL,
    doc_key     VARCHAR(256) NOT NULL,             -- 源系统唯一标识：URL / 文件路径 / 工单号
    title       VARCHAR(512) NOT NULL DEFAULT '',
    source_type VARCHAR(32)  NOT NULL DEFAULT 'md',-- md / pdf / html / manual
    checksum    CHAR(64)     NOT NULL,             -- 内容 SHA256，用于跳过未变更文档
    chunk_count INT          NOT NULL DEFAULT 0,
    embed_model VARCHAR(64)  NOT NULL,             -- 记录 embedding 模型版本
    status      VARCHAR(16)  NOT NULL DEFAULT 'pending', -- pending / indexed / failed / deleted
    indexed_at  DATETIME(3),
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_documents_project_key (project_id, doc_key),
    INDEX idx_documents_status (project_id, status),
    CONSTRAINT fk_documents_project FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 5.5 用量与检索日志

```sql
ALTER TABLE usage_logs
    ADD COLUMN project_id BIGINT NULL AFTER api_key_id,
    ADD COLUMN assistant_id BIGINT NULL AFTER project_id,
    ADD COLUMN retrieved_count INT NOT NULL DEFAULT 0,
    ADD COLUMN retrieval_ms INT NOT NULL DEFAULT 0;
```

P2 再加 `retrieval_logs`（记录 query、命中的 chunk、分数），用于评测与排障；P1 先只落 `usage_logs` 的汇总字段。

---

## 6. 请求链路

```
Key
 │
 ├─ Auth 中间件：sha256 → api_keys → {tenant_id, project_id, scope}
 │
 ├─ Handler：解析 model → assistants 配置（project_id 必须匹配 ctx）
 │
 ├─ Query Embedding（embedding 模型按项目固定）
 │
 ├─ 检索：collection = kb_<slug>，filter project_id + acl
 │
 ├─ 组装上下文：编号片段 + 「只依据资料回答」提示词 + token 预算
 │
 ├─ relay：沿用现有 abilities 路由 / 重试 / 熔断 / 流式
 │
 └─ 输出：正文 + citations + usage_logs(project_id, retrieved_count, retrieval_ms)
```

关键顺序：**先鉴权拿项目，再解析助手，最后才允许检索**。检索永远发生在鉴权之后，Handler 不接受外部传入的 scope。

---

## 7. 接口设计

### 7.1 对话接口保持 OpenAI 兼容

请求不需要新增必填字段：

```json
POST /v1/chat/completions
Authorization: Bearer sk-ops-xxx

{
  "model": "ops-assistant",
  "messages": [{ "role": "user", "content": "昨天那条磁盘告警怎么处理？" }],
  "stream": false
}
```

可选字段（只收窄，不扩大）：

| 字段 | 说明 |
|---|---|
| `rag.namespace` | 项目内的子范围，如 `faq` / `manual`，后端校验属于当前项目 |
| `rag.top_k` | 覆盖助手的默认召回数，设上限（如 10） |
| `rag.enabled` | 显式关闭检索，用于闲聊，默认 `true` |

传了不属于本项目的 `namespace` → `403`，不是静默忽略（静默忽略会让调用方以为生效了）。

### 7.2 响应增加引用

非流式：在 `choices[0].message` 上增加 `citations`，OpenAI SDK 会忽略未知字段。

```json
{
  "choices": [{
    "index": 0,
    "message": {
      "role": "assistant",
      "content": "先确认磁盘使用率…[1]",
      "citations": [
        { "index": 1, "doc_id": 12, "title": "磁盘告警处理手册", "source": "/ops/alert/disk.md" }
      ]
    },
    "finish_reason": "stop"
  }]
}
```

流式：把 `citations` 放在最后一个 `finish_reason: "stop"` 的 chunk 上，不额外插入自定义事件，避免客户端解析失败。

> 只回 `title` / `source`，不回向量分数和 chunk 原文，避免把内部数据结构暴露给前端。

### 7.3 管理与入库接口

| 方法 | 路径 | 说明 | P 阶段 |
|---|---|---|---|
| POST | `/admin/documents/ingest` | 单文档导入（tenant scope Key） | P1（也可先做 CLI） |
| DELETE | `/admin/documents/:id` | 删除文档并清空向量 | P2 |
| POST | `/admin/documents/:id/reindex` | 重建索引 | P2 |

P1 若不做管理接口，用 CLI 直接操作 DB + Qdrant 也可以，但接口形态要先定下来，避免后面返工。

---

## 8. 检索层规范

### 8.1 接口签名（示意）

```go
type Scope struct {
    TenantID    int64
    ProjectID   int64   // 必填，来自 context
    AssistantID int64
    ACL         []string // P3 用，P1 留空
}

type Chunk struct {
    DocID   int64
    Title   string
    Source  string
    Text    string
    Score   float32
}

type Retriever interface {
    Retrieve(ctx context.Context, scope Scope, query string, opts Options) ([]Chunk, error)
}
```

约束：

- `Scope` 只能由中间件 + 助手配置构造，**不能从 JSON body 直接绑定**。
- `ProjectID == 0` 时返回 `ErrMissingScope`，不允许继续。
- `Options` 里的 `Collection` 字段不对外开放，由 `Scope` 推导。

### 8.2 Qdrant 组织

| 项 | 约定 |
|---|---|
| collection | `kb_<project_slug>`，如 `kb_ops`、`kb_site` |
| point id | `uuidv5(doc_id + ":" + chunk_no)`，保证重复入库幂等 |
| payload 索引 | `project_id`(keyword)、`doc_id`(keyword)、`acl`(keyword)、`updated_at`(datetime) |
| 向量维度 | 与 embedding 模型绑定，写入 collection 配置，禁止混用 |
| 距离 | cosine |

### 8.3 召回与拼装

- 默认 `top_k=5`，`score_threshold=0.3`；同文档相邻 chunk 去重，避免上下文重复。
- 拼装顺序：system prompt → 编号资料 → 历史对话 → 用户问题。
- 提示词必须包含三条：只用资料回答、资料不足要说不知道、引用编号。
- 检索结果总 token 不超过 `max_context_tokens`，超出按分数截断。
- 检索超时（建议 800ms）单独设上限，超时降级为"无资料回答"，不能拖垮整个请求。

### 8.4 两个项目的差异

| | 运维助手 | 官网助手 |
|---|---|---|
| 使用者 | 内部员工 | 匿名访客 |
| 内容 | 手册、告警处置、SOP | 产品介绍、FAQ、价格 |
| 风险 | 误操作、内部信息外泄 | 提示词注入、恶意刷量 |
| 额外措施 | 项目内 ACL（P3） | 更严格的限流、禁止输出 system prompt、答案附"以官网为准" |

---

## 9. 入库管线

### 9.1 流程

```
读取源 → 解析（md/html/pdf）→ 清洗 → 按标题层级切分
      → 计算 checksum（未变则跳过）
      → embedding（按项目固定模型）
      → 删除旧 point（按 doc_id）
      → upsert 新 point
      → 更新 documents.status / chunk_count / indexed_at
```

### 9.2 切分策略

- 优先按 Markdown 标题层级切，二级标题为最小单元，超长再按段落切。
- 单 chunk 300–500 字，overlap 50 字；表格整块保留，不跨块拆。
- chunk 头部拼上文档标题和章节路径（"磁盘告警手册 > 3.2 处理步骤"），显著提升召回。
- 运维文档偏步骤，官网偏问答，两套参数分开配置，不要共用一个全局值。

### 9.3 一致性与删除

- `documents.checksum` 未变直接跳过，避免全量重跑。
- 更新 = 先删 `doc_id` 旧 point 再写新 point，失败则标记 `status='failed'`，可重试。
- 删除文档 = 删 `documents` 行 + Qdrant `delete by filter doc_id`；两步都要有，否则会出现"删了记录但还检索得到"。
- 项目下线 = 删 collection，最彻底。

### 9.4 embedding 升级

- 模型名写进 `documents.embed_model` 和 collection 配置。
- 升级时新建 `kb_<slug>_v2`，双写、验证后再切读，最后删旧库。不要原地混写。

### 9.5 放在哪

- P1：Go CLI（`apps/gateway/cmd/ingest`，flags：`-config -project -path -title -type -doc-key -tenant`），复用现有 config 与 DB。
- P2：需要异步/定时同步时，再用 `apps/worker`（目前是 Python 占位）接队列。

---

## 10. 缓存

### 10.1 精确缓存

key 组成（已实现，`internal/cache/semantic.go`）：

```
semcache:exact:{tenant_id}:{project_id}:{index_epoch}:sha256(model | 有序 messages)
```

漏掉 `project_id` 会直接串项目；漏掉 `epoch` 会在文档更新后继续吐旧答案。助手提示词与检索资料都已拼进 messages，因此不需要额外的 assistant_id 段。

### 10.2 语义缓存

已落地项目隔离：

- collection 按租户 + 项目拆分：`semcache_{tenant}_{project}`，不共享 collection；
- 向量命中后回源精确层，精确层 key 带 `index_epoch`，知识库更新 +1 后旧答案自然失效；
- 检索失败 / Redis 不可用一律 fail-open（退化为无缓存），不返回 5xx。

---

## 11. 计费与可观测性

- `usage_logs` 增加 `project_id` / `assistant_id` / `retrieved_count` / `retrieval_ms`。
- embedding 调用单独记一条（`provider='embedding'`），否则成本算不清。
- 指标至少三个：检索耗时 P95、空召回率、引用点击率（有前端时）。
- 审计日志：越权尝试（403）必须记录 Key、项目、请求的目标项目。

---

## 12. 分阶段落地

### P1 · 单项目闭环（先跑通一个项目）

范围：建表、Key 绑定、单个助手、单个 collection、手工入库 CLI、检索 + 引用、usage_logs 扩展。

验收：

- [ ] 用官网 Key 调 `site-assistant`，能答官网内容并返回 citations
- [ ] 用运维 Key 调 `ops-assistant`，能答运维内容
- [ ] 用官网 Key 传 `model=ops-assistant` → `403`
- [ ] 删除一篇文档后，同样问题不再引用该文档
- [ ] `usage_logs` 能按 `project_id` 聚合出调用量

### P2 · 多项目与隔离加固

范围：第二个项目接入、越权用例、删除/重建索引接口、精确缓存（带项目 key）、检索日志。

验收：

- [ ] 两个项目同时在线，互不串结果（用同一句话分别问两个助手，引用来源不重叠）
- [ ] 请求体传 `rag.namespace` 越界 → `403` 且落审计日志
- [ ] 缓存开启后，A 项目的问题不会命中 B 项目缓存
- [ ] 文档更新后 1 分钟内检索结果更新

### P3 · 质量与规模

范围：文档 ACL、评测集、重排模型、pool/bridge 评估（语义缓存与按项目 key 已在 P1/P2 落地）。

验收：

- [ ] 50 条真实问题的评测集，召回率与答案准确率有基线
- [ ] 角色不同看到的文档不同
- [ ] 项目数或文档量达到阈值时有明确的迁移方案

---

## 13. 风险与取舍

| 风险 | 说明 | 应对 |
|---|---|---|
| silo 的 collection 数量上限 | 项目上百后运维成本高 | 预留 `project_id` payload，切 pool 时只改检索层 |
| 项目内越权 | P1 只有项目级隔离 | P3 加 ACL，payload 已预留字段 |
| 检索质量不可控 | 切分和提示词决定成败，换向量库救不了 | P2 起建评测集，先量化再优化 |
| embedding 升级 | 必须重建索引 | collection 版本化 + 双写切换 |
| 删除不彻底 | 只删 DB 不删向量 | 删除流程做成幂等任务，可重跑 |
| 助手与路由耦合 | 新增助手要同时配 `assistants` 和 `abilities` | 入库脚本里一并写入，避免手工漏配 |
| 官网被刷 | 匿名访客 + LLM 成本 | 按 Key/项目限流，P2 接现有 quota |

---

## 14. 落地前需要你确认的 5 件事

1. **项目数量**：预计几个？是否长期会超过 20 个？（决定 silo 还是要提前 pool）
2. **项目内是否有角色差异**：运维平台里是否有人不该看某些文档？（决定 ACL 放 P2 还是 P3）
3. **官网内容来源**：CMS 导出、手工 Markdown，还是要爬？（决定入库管线的解析器）
4. **引用怎么展示**：只在接口返回，还是前端要做"点击跳原文"？（决定 citations 字段形态）
5. **向量库选型**：Qdrant（推荐，隔离和过滤都成熟）还是先用 pgvector（少一个组件）？

确认后按 P1 拆成具体任务，再动代码。
