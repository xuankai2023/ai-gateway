# SQL 迁移规范

- 所有表结构变更必须创建一对 up/down SQL 文件，放在 sql/ 目录
- 命名规则：`{序号}_{描述}.up.sql` + `{序号}_{描述}.down.sql`
- 执行命令：`cd apps/migrate && go run . up`
- **禁止手动改生产数据库表结构**，必须走迁移
