-- ============================================================================
-- 013_projects_assistants_documents：多项目 RAG 知识库隔离
--
-- 隔离模型（silo）：
--   projects      项目实体，slug 全局唯一，用于 Qdrant collection 名 kb_<slug>
--   assistants    对外模型名 → 项目 + 提示词 + 召回参数（同一租户内 model_name 唯一）
--   documents     文档元数据（checksum 跳过未变更文档；index_epoch 由入库流程 +1）
--   usage_logs    增加项目 / 助手 / 检索维度，支持按项目对账
--
-- 上游模型映射仍由 abilities 表维护：为 assistants.model_name 插一条
-- abilities 记录即可复用现有 relay 路由，无需改 relay。
-- ============================================================================

CREATE TABLE IF NOT EXISTS projects (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id   BIGINT       NOT NULL,
    name        VARCHAR(128) NOT NULL COMMENT '项目名：运维平台 / 官网',
    slug        VARCHAR(64)  NOT NULL COMMENT '用于 collection 名与 URL，全局唯一',
    status      VARCHAR(16)  NOT NULL DEFAULT 'active' COMMENT 'active | disabled',
    index_epoch BIGINT       NOT NULL DEFAULT 0 COMMENT '知识库版本：文档入库/删除 +1，缓存 key 携带',
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_projects_slug (slug),
    UNIQUE KEY uk_projects_tenant_name (tenant_id, name),
    INDEX idx_projects_tenant (tenant_id, status),
    CONSTRAINT fk_projects_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS assistants (
    id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id          BIGINT       NOT NULL,
    project_id         BIGINT       NOT NULL,
    model_name         VARCHAR(64)  NOT NULL COMMENT '对外入口模型名：ops-assistant',
    system_prompt      TEXT         NULL,
    top_k              INT          NOT NULL DEFAULT 5 COMMENT '召回条数',
    score_threshold    DECIMAL(4,3) NOT NULL DEFAULT 0.300 COMMENT '相似度下限',
    max_context_tokens INT          NOT NULL DEFAULT 2000 COMMENT '检索资料 token 预算',
    enabled            TINYINT(1)   NOT NULL DEFAULT 1,
    created_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_assistants_entry (tenant_id, model_name),
    INDEX idx_assistants_project (project_id),
    CONSTRAINT fk_assistants_project FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS documents (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    project_id  BIGINT       NOT NULL,
    doc_key     VARCHAR(256) NOT NULL COMMENT '源系统唯一标识：URL / 文件路径 / 工单号',
    title       VARCHAR(512) NOT NULL DEFAULT '',
    source_type VARCHAR(32)  NOT NULL DEFAULT 'md' COMMENT 'md | txt | html | pdf | manual',
    checksum    CHAR(64)     NOT NULL COMMENT '内容 SHA256：未变则跳过重建索引',
    chunk_count INT          NOT NULL DEFAULT 0,
    embed_model VARCHAR(64)  NOT NULL DEFAULT '' COMMENT 'embedding 模型版本',
    status      VARCHAR(16)  NOT NULL DEFAULT 'pending' COMMENT 'pending | indexed | failed | deleted',
    indexed_at  DATETIME(3)  NULL,
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_documents_project_key (project_id, doc_key),
    INDEX idx_documents_status (project_id, status),
    CONSTRAINT fk_documents_project FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE usage_logs
    ADD COLUMN project_id      BIGINT NULL AFTER api_key_id,
    ADD COLUMN assistant_id    BIGINT NULL AFTER project_id,
    ADD COLUMN retrieved_count INT    NOT NULL DEFAULT 0 COMMENT 'RAG 召回条数' AFTER model,
    ADD COLUMN retrieval_ms    INT    NOT NULL DEFAULT 0 COMMENT 'RAG 检索耗时（毫秒）' AFTER model,
    ADD INDEX idx_usage_project_time (project_id, created_at DESC);
