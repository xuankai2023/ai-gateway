-- 回滚脚本（与 up.sql 相反顺序删除）
-- usage_logs → models → api_keys → tenants

SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS usage_logs;
DROP TABLE IF EXISTS models;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS tenants;
SET FOREIGN_KEY_CHECKS = 1;
