#!/bin/bash
# 本地开发环境一键初始化（占位）
#
# 1. cd infra && docker compose up -d
# 2. 等待所有服务健康
# 3. 跑迁移：cd apps/migrate && go run . up
# 4. 造测试数据：psql -f infra/scripts/seed_test_data.sql
# 5. 启动 Gateway：cd apps/gateway && go run .
# 6. 打开 http://localhost:3000 看 Console
#    打开 http://localhost:16686 看 Jaeger
#    打开 http://localhost:9090 看 Prometheus
#    打开 http://localhost:3001 看 Grafana（admin/admin）
