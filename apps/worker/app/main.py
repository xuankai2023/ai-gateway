# Worker 入口
#
# 启动 Celery / Kafka Consumer
# 消费 Kafka topic "ai-gateway-batch"
# 分发到 services/ 下各处理器
# 完成后调 CallbackURL 通知业务方
