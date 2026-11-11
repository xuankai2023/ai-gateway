-- 回滚账本三件套

ALTER TABLE usage_logs
    DROP COLUMN final_cost,
    DROP COLUMN reserved_cost;

DROP TABLE IF EXISTS ledger_entries;

ALTER TABLE tenants
    DROP COLUMN trust_quota,
    DROP COLUMN reserved,
    DROP COLUMN balance;
