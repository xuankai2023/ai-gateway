-- 回滚 API Key 的项目绑定
ALTER TABLE api_keys
    DROP INDEX idx_api_keys_project,
    DROP COLUMN project_id;
