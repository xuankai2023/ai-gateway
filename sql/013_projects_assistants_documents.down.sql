-- ============================================================================
-- 013_projects_assistants_documents（回滚）
-- ============================================================================

ALTER TABLE usage_logs
    DROP INDEX idx_usage_project_time,
    DROP COLUMN retrieval_ms,
    DROP COLUMN retrieved_count,
    DROP COLUMN assistant_id,
    DROP COLUMN project_id;

DROP TABLE IF EXISTS documents;
DROP TABLE IF EXISTS assistants;
DROP TABLE IF EXISTS projects;
