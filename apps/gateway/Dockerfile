# Gateway 生产镜像：多阶段构建 → 静态二进制 → 非 root 运行
#
# 构建：
#   docker build -f apps/gateway/Dockerfile -t ai-gateway:latest \
#     --build-arg VERSION=$(git describe --tags --always) .
#
# 运行（配置文件由 ConfigMap / 挂载提供，镜像内不含任何密钥）：
#   docker run -p 8080:8080 -p 8081:8081 -p 8082:8082 \
#     -v $(pwd)/configs:/app/configs:ro --env-file ./gateway.env ai-gateway:latest
#
# 端口：8080 API / 8081 metrics / 8082 admin（admin 仅内网可达）

# ---------- 构建阶段 ----------
FROM golang:1.26-alpine AS builder

WORKDIR /src

# git 供 go mod 拉取依赖，ca-certificates 供构建期 HTTPS
RUN apk add --no-cache git ca-certificates

# 先只拷依赖清单，源码变更不会击穿依赖下载缓存
COPY go.mod go.sum ./
RUN go mod download

# 只拷 gateway 实际依赖的目录（不含 .git / docs / tests / 测试二进制）
COPY apps/gateway ./apps/gateway
COPY pkg ./pkg

# VERSION 由 CI 注入（git describe 或 commit sha），通过 -X 覆盖 main.version
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/gateway ./apps/gateway/cmd

# ---------- 运行阶段 ----------
FROM alpine:3.21

# ca-certificates：访问 HTTPS 上游必需
# tzdata：MySQL DSN loc=Local 与日志时间戳依赖时区数据
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S gateway \
    && adduser -S -G gateway -H -s /sbin/nologin gateway \
    && mkdir -p /app/configs \
    && chown -R gateway:gateway /app

WORKDIR /app

COPY --from=builder /out/gateway /app/gateway

EXPOSE 8080 8081 8082

# 健康检查走 /livez：与 k8s liveness 语义一致（进程存活即通过，不查依赖）
HEALTHCHECK --interval=10s --timeout=2s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/livez || exit 1

# 非 root 运行；配置目录只读挂载
USER gateway

ENTRYPOINT ["/app/gateway"]
CMD ["--config", "/app/configs/config.yaml"]
