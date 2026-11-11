// Command main Go 网关启动入口
//
// 启动顺序：
//  1. config.Load 读取本地 yaml（--config 可指定，默认 configs/config.yaml）
//  2. observability.Init 初始化结构化日志
//  3. db.InitPool 初始化 MySQL 连接池并 Ping（失败即退出）
//  4. redis.InitPool 初始化 Redis 连接池并 Ping（失败即退出，限流硬依赖）
//  5. app.New 组装 App（熔断器 / 异步执行器 / 用量写入器 / Provider 注册表 /
//     路由器 / 中继全部实例级创建，挂到 App 生命周期）
//  6. 按 config.providers 注册 Provider 到 App.Providers（未知类型即退出）
//  7. gin.New() + 中间件链（全部 func(app) 注入）+ /livez /readyz（/healthz 为 readyz 别名）
//  8. 监听 :8080，SIGINT/SIGTERM 优雅退出
//
// 关停序列（收到信号后严格按序执行）：
//  1. ready 置 false → /readyz 立即 503（k8s 摘流量）
//  2. srv.Shutdown 等在途请求收尾（30s，覆盖流式长回答）
//  3. app.Async.Wait 排空后台任务（渠道健康写库等）
//  4. app.Close 排空用量日志队列并 flush 最后一批 + 关闭中间件/路由缓存
//  5. 关闭 Redis / MySQL（defer，晚于 app.Close：写日志依赖 DB）
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"ai-gateway/apps/gateway/internal/admin"
	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/apps/gateway/internal/cache"
	"ai-gateway/apps/gateway/internal/handler"
	"ai-gateway/apps/gateway/internal/middleware"
	"ai-gateway/apps/gateway/internal/rag"
	"ai-gateway/pkg/apperr"
	"ai-gateway/pkg/broadcast"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/db"
	"ai-gateway/pkg/observability"
	"ai-gateway/pkg/provider"
	"ai-gateway/pkg/provider/anthropic"
	"ai-gateway/pkg/provider/openai"
	"ai-gateway/pkg/provider/volcengine"
	"ai-gateway/pkg/redis"
	"ai-gateway/pkg/vault"
	"ai-gateway/pkg/vector"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// shutdownTimeout 优雅退出最长时间（覆盖流式收尾）
	shutdownTimeout = 30 * time.Second
	// asyncDrainTimeout 关停时排空后台异步任务的最长等待
	asyncDrainTimeout = 10 * time.Second
	// appCloseTimeout 关停时 App 收尾（排空用量日志队列）的最长等待
	appCloseTimeout = 10 * time.Second
	// healthCheckTimeout 就绪探针 Ping 依赖的超时
	healthCheckTimeout = time.Second
	// maxBodySize 请求体最大 10MB，防止 DoS
	maxBodySize = 10 << 20
)

// version 二进制版本号，构建时可通过 -ldflags "-X main.version=..." 覆盖
// （必须是 var：链接器无法覆盖 const）
var version = "dev"

// ready 就绪标志：启动完成置 true，收到关停信号立即置 false（/readyz 摘流量）
var ready atomic.Bool

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gateway start failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", config.DefaultPath, "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	if err := observability.Init("info"); err != nil {
		return err
	}
	defer observability.Sync()

	// 初始化 OTel Tracer（全链路 span）；未配置 exporter 时保持 Noop
	tracerCfg := cfg.Observability.Tracing
	if err := observability.InitTracer(context.Background(), observability.TracerOptions{
		Enabled:     tracerCfg.Enabled,
		Endpoint:    tracerCfg.Endpoint,
		Insecure:    tracerCfg.Insecure,
		SampleRatio: tracerCfg.SampleRatio,
	}); err != nil {
		observability.L().Warn("tracer init failed, continuing without tracing", zap.Error(err))
	} else if tracerCfg.Enabled {
		defer observability.ShutdownTracer(context.Background())
	}

	observability.L().Info("config loaded",
		zap.String("path", *configPath),
		zap.String("listen_addr", cfg.Gateway.ListenAddr),
	)

	stage := observability.BeginStage("mysql")
	pool, err := db.InitPool(context.Background(), cfg.MySQL.DSN, cfg.MySQL.PoolSize)
	if err != nil {
		return err
	}
	defer db.Close()
	stage.Done("db connected")

	stage = observability.BeginStage("redis")
	redisDialTimeout, err := time.ParseDuration(cfg.Redis.DialTimeout)
	if cfg.Redis.DialTimeout == "" {
		redisDialTimeout = 0 // 用 pkg/redis 内置默认
	} else if err != nil {
		return fmt.Errorf("redis.dial_timeout 格式非法: %w", err)
	}
	redisClient, err := redis.InitPool(context.Background(), redis.Options{
		Addr:        cfg.Redis.Addr,
		Password:    cfg.Redis.Password,
		DB:          cfg.Redis.DB,
		PoolSize:    cfg.Redis.PoolSize,
		DialTimeout: redisDialTimeout,
	})
	if err != nil {
		return err
	}
	defer redis.Close()
	stage.Done("redis connected")

	// 阶段 12：语义缓存（精确哈希层必开；Qdrant + embedding 同时配置才启用向量层）
	semanticCache := newSemanticCache(cfg, redisClient)

	// 多项目 RAG 知识库（rag.enabled=false 时为 nil，handler 退化为纯 LLM 转发）
	ragService := newRAGService(cfg, pool)

	// 跨节点广播总线（Redis Pub/Sub），先建总线再组装 App（熔断器/路由器挂接用）
	bus := broadcast.Init(redisClient)
	broadcast.SetLogger(observability.L().Warn)

	// 阶段 14：组装 App（DB / Redis / 熔断 / 广播 / 异步执行器 / 用量写入器 /
	// Provider 注册表 / 路由器 / 中继全部实例级收敛；关停时在 db.Close 前 Close）
	a := app.New(app.Options{
		Config:        cfg,
		DB:            pool,
		Redis:         redisClient,
		Bus:           bus,
		SemanticCache: semanticCache,
		RAG:           ragService,
	})
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), appCloseTimeout)
		defer closeCancel()
		if err := a.Close(closeCtx); err != nil {
			observability.L().Warn("app close timeout, entries may be lost", zap.Error(err))
		}
	}()

	// 注册事件处理器：其他节点的状态变更 → 本节点同步失效缓存
	bus.On(broadcast.TypeChannelDisabled, func(_ context.Context, ev broadcast.Event) error {
		observability.L().Info("received cross-node channel_disabled",
			zap.Int64("ability_id", ev.AbilityID),
			zap.String("origin", ev.Origin))
		a.InvalidateCache("abilities")
		return nil
	})
	bus.On(broadcast.TypeChannelCooldown, func(_ context.Context, ev broadcast.Event) error {
		observability.L().Info("received cross-node channel_cooldown",
			zap.Int64("ability_id", ev.AbilityID),
			zap.String("origin", ev.Origin))
		a.InvalidateCache("abilities")
		return nil
	})
	bus.On(broadcast.TypeCacheInvalidate, func(_ context.Context, ev broadcast.Event) error {
		observability.L().Info("received cross-node cache_invalidate",
			zap.String("cache_prefix", ev.CachePrefix),
			zap.String("origin", ev.Origin))
		// 阶段 15：prefix 定向失效（Admin 写操作）；空前缀 = 全量
		switch ev.CachePrefix {
		case "":
			a.InvalidateCache("")
		case "channel_keys":
			// 渠道 key 池是进程内状态（非 cache.Cache），需显式重载；路由缓存附带失效
			a.Providers.ReloadChannelKeys(context.Background(), a.DB)
			a.InvalidateCache("abilities")
		default:
			a.InvalidateCache(ev.CachePrefix)
		}
		return nil
	})
	bus.On(broadcast.TypeBreakerOpen, func(_ context.Context, ev broadcast.Event) error {
		// 本地熔断器已是真实来源（其他节点本地 gobreaker 独立检测），
		// 广播主要用于日志观测；其他节点继续走本地 IsProviderOpen 过滤即可。
		observability.L().Info("received cross-node breaker_open",
			zap.String("breaker_key", ev.BreakerKey),
			zap.String("origin", ev.Origin))
		return nil
	})
	{
		subCtx, subCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := bus.Subscribe(subCtx); err != nil {
			subCancel()
			observability.L().Warn("broadcast subscribe failed, continuing without cross-node sync",
				zap.Error(err))
		} else {
			subCancel()
			defer bus.Close()
			observability.L().Info("broadcast subscribed",
				zap.String("instance_id", bus.InstanceID()))
		}
	}

	// 阶段 7：启动周期采集器（DB 池 / Redis 池 / 进程内缓存）
	observability.InitPeriodicCollector(context.Background(), pool, redisClient)

	stage = observability.BeginStage("providers")
	if len(cfg.Providers) == 0 {
		return errors.New("at least one provider must be configured")
	}
	// 阶段 16：key_ref 解析器装配（env:// 默认；vault.addr 已配置时叠加 vault:// scheme）
	resolver := buildKeyRefResolver(cfg)
	a.Providers.SetKeyRefResolver(resolver)
	if err := registerProviders(cfg.Providers, a.Providers, resolver); err != nil {
		return err
	}
	stage.Done("providers registered", zap.Strings("types", a.Providers.AllRegistered()))

	// 阶段 8：注入 provider type → baseURL 映射 + 从 DB 加载 channel_keys 多 key 池
	a.Providers.SetBaseURLs(providerTypeBaseURLMap(cfg.Providers))
	if err := a.Providers.LoadChannelKeys(context.Background(), pool); err != nil {
		// channel_keys 加载失败不应阻塞启动（fallback 到单 key 模式）
		observability.L().Warn("channel_keys load failed, falling back to single-key mode",
			zap.Error(err))
	}

	ln, err := net.Listen("tcp", cfg.Gateway.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s failed: %w", cfg.Gateway.ListenAddr, err)
	}

	srv := &http.Server{
		Handler:           newRouter(a),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()
	ready.Store(true)
	observability.L().Info("gateway listening",
		zap.String("addr", cfg.Gateway.ListenAddr),
		zap.String("version", version),
	)

	// Prometheus metrics server（独立端口，与 API 流量隔离）
	var metricsSrv *http.Server
	if cfg.Gateway.ListenMetrics != "" {
		metricsLn, metricsErr := net.Listen("tcp", cfg.Gateway.ListenMetrics)
		if metricsErr != nil {
			observability.L().Warn("metrics server listen failed",
				zap.String("addr", cfg.Gateway.ListenMetrics),
				zap.Error(metricsErr))
		} else {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"status":"ok"}`))
			})
			metricsSrv = &http.Server{Handler: mux}
			go func() {
				if e := metricsSrv.Serve(metricsLn); e != nil && !errors.Is(e, http.ErrServerClosed) {
					observability.L().Warn("metrics server stopped", zap.Error(e))
				}
			}()
			observability.L().Info("metrics server listening",
				zap.String("addr", cfg.Gateway.ListenMetrics))
		}
	}

	// 阶段 15：管理面 server（独立端口 + X-Admin-Token，生产经 NetworkPolicy 仅内网可达）
	var adminSrv *http.Server
	if adminToken := resolveAdminToken(cfg.Admin.Token); adminToken != "" {
		if adminRouter := admin.NewRouter(a, adminToken); adminRouter != nil {
			adminLn, adminErr := net.Listen("tcp", cfg.Admin.ListenAddr)
			if adminErr != nil {
				observability.L().Warn("admin server listen failed",
					zap.String("addr", cfg.Admin.ListenAddr), zap.Error(adminErr))
			} else {
				adminSrv = &http.Server{Handler: adminRouter, ReadHeaderTimeout: 5 * time.Second}
				go func() {
					if e := adminSrv.Serve(adminLn); e != nil && !errors.Is(e, http.ErrServerClosed) {
						observability.L().Warn("admin server stopped", zap.Error(e))
					}
				}()
				observability.L().Info("admin server listening", zap.String("addr", cfg.Admin.ListenAddr))
			}
		}
	} else {
		observability.L().Warn("admin token empty, admin API disabled (set admin.token / env:// reference to enable)")
	}

	// 阶段 15：配置热更新双通道
	// 通道 1：Redis Pub/Sub（gw:config）——Admin PUT /config 推送，全节点不重进程生效
	stopConfigSub := startConfigSubscriber(a)
	defer stopConfigSub()
	// 通道 2：SIGHUP 重读配置文件（应用热更新项，非热更新项变更记 warn 需重启）
	stopSighup := startSighupReloader(*configPath, a)
	defer stopSighup()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case err := <-serveErr:
		observability.L().Error("server stopped unexpectedly", zap.Error(err))
		return fmt.Errorf("serve %s failed: %w", cfg.Gateway.ListenAddr, err)
	case sig := <-sigCh:
		observability.L().Info("shutdown signal received", zap.String("signal", sig.String()))
	}

	// 关停序列第 1 步：/readyz 立即 503 摘流量（k8s endpoints 摘除有传播延迟，
	// 期间新请求仍可被在途配额正常处理，Shutdown 会等它们收尾）
	ready.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		observability.L().Error("shutdown failed", zap.Error(err))
	}
	if metricsSrv != nil {
		_ = metricsSrv.Shutdown(ctx)
	}
	if adminSrv != nil {
		_ = adminSrv.Shutdown(ctx)
	}

	// 第 3 步：排空 App 后台异步任务（渠道健康写库等），超时兜底
	drainCtx, drainCancel := context.WithTimeout(context.Background(), asyncDrainTimeout)
	defer drainCancel()
	if err := a.Async.Wait(drainCtx); err != nil {
		observability.L().Warn("async tasks drain timeout", zap.Error(err))
	}

	// 第 4 步（App 收尾：用量日志 flush + 缓存关闭）与第 5 步（关闭 Redis/MySQL）由 defer 按注册逆序执行
	observability.L().Info("shutdown complete")
	return nil
}

// newRouter 组装路由与中间件链（阶段 14：全部中间件 / handler 经 App 注入）
func newRouter(a *app.App) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	// 全局中间件（所有路由都走）
	// 顺序：Recover 最外层兜 panic → Trace 注入 trace_id（必须先于一切错误出口，
	// 保证所有错误响应都带 request_id）→ bodyLimit → Gzip → Logger → CORS
	r.Use(middleware.Recover())
	r.Use(middleware.Trace())
	r.Use(bodyLimit(maxBodySize))
	// Gzip 只压缩非 SSE 响应（writer 首写时按 Content-Type 决策），流式转发不受影响
	r.Use(middleware.Gzip())
	r.Use(middleware.Logger())
	// CORS 必须在 Auth 之前：预检请求不带 Authorization
	r.Use(middleware.CORS(a.Config.Gateway.CORSAllowedOrigins))

	// 探针（不经过 LoadShed/Auth：过载与关停期间探活必须可用）
	r.GET("/livez", livez)
	r.GET("/readyz", func(c *gin.Context) { readyz(c, a) })
	// 旧端点保留为 readyz 别名，一个版本后删除
	r.GET("/healthz", func(c *gin.Context) { readyz(c, a) })

	// API 路由组（/v1/*）
	// LoadShed 在 Auth 之前：未鉴权流量也占在途名额，过载时全局快速拒绝；
	// Timeout 在 RateLimit 之前：Redis 调用与 relay 都包进请求级时限；
	// RateLimit 在 Auth 之后：缓存命中（后续阶段）也要先过限流，防止绕过
	v1 := r.Group("/v1")
	v1.Use(middleware.LoadShed(a))
	v1.Use(middleware.Auth(a))
	v1.Use(middleware.Timeout(a))
	v1.Use(middleware.RateLimit(a))
	// 阶段 11：幂等键（早于缓存/账本，重复 key 不产生额外上游调用与扣费）
	v1.Use(middleware.Idempotency(a))
	// 阶段 12：语义缓存（命中直接短路，跳过 relay 与账本，cost=0）
	v1.Use(middleware.SemanticCache(a))
	// 阶段 13：模型灰度（缓存之后：命中短路不处理灰度；命中灰度则改按 target_model 路由）
	v1.Use(middleware.Gray(a))
	// 账本预扣押金（Auth / RateLimit 都在前面 —— 只有合法请求才去扣账本）
	v1.Use(middleware.Ledger(a))
	// LedgerCapture 包在 handler 外层：defer 在 handler 返回后做 Settles / Refund
	v1.POST("/chat/completions", middleware.LedgerCapture(a), handler.ChatCompletion(a))

	return r
}

// livez 存活探针：进程能响应即 200，不查任何依赖
// （k8s liveness 失败会重启 Pod，依赖抖动绝不能传导到这里）
func livez(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "alive",
		"version": version,
	})
}

// readyz 就绪探针：收到关停信号立即 503（摘流量）；
// 正常时 Ping DB + Redis（1s 超时），任一依赖不可用即 503
func readyz(c *gin.Context, a *app.App) {
	if !ready.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "not_ready",
			"reason":  "shutting_down",
			"version": version,
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), healthCheckTimeout)
	defer cancel()

	if a.DB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "unavailable",
			"db":      "uninitialized",
			"version": version,
		})
		return
	}
	if err := a.DB.PingContext(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "unavailable",
			"db":      "down",
			"version": version,
		})
		return
	}

	if a.Redis == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "unavailable",
			"db":      "ok",
			"redis":   "uninitialized",
			"version": version,
		})
		return
	}
	if err := a.Redis.Ping(ctx).Err(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "unavailable",
			"db":      "ok",
			"redis":   "down",
			"version": version,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"db":      "ok",
		"redis":   "ok",
		"version": version,
	})
}

// registerProviders 按配置注册各 provider 类型
//
// 凭证解析（阶段 16：删除"必须环境变量"的单一路径）：api_key_ref（env:// 或 vault://）优先，
// 缺省回落 api_key_env 环境变量；两者皆空 → 空 key 注册（转发时上游 401，错误照常落账）
//
// 未知类型直接返回错误
//
// 同时注册渠道级 KeyPool 构造器（阶段 8 多 key 池用）：ChannelProvider 在首次选中某把 key 时，
// 需要按 providerType + baseURL + 该 key 的 api_key 动态构造 Provider 实例。
func registerProviders(list []config.ProviderConfig, registry *provider.Registry, resolveRef func(string) string) error {
	clients := registry.Clients()
	resolveKey := func(pc config.ProviderConfig) string {
		if pc.APIKeyRef != "" && resolveRef != nil {
			if k := resolveRef(pc.APIKeyRef); k != "" {
				return k
			}
			observability.L().Warn("provider api_key_ref resolved empty, falling back to api_key_env",
				zap.String("type", pc.Type))
		}
		return os.Getenv(pc.APIKeyEnv)
	}
	for _, pc := range list {
		constructor, ok := providerConstructors(pc.Type)
		if !ok {
			return fmt.Errorf("unknown provider type: %s", pc.Type)
		}
		registry.Register(pc.Type, func() provider.Provider {
			return constructor(pc.BaseURL, resolveKey(pc), clients)
		})
		registry.RegisterChannelConstructor(pc.Type, constructor)
	}
	return nil
}

// providerConstructors 返回 provider 类型对应的构造函数。
// DeepSeek 官方接口与 OpenAI 协议兼容，复用 openai 实现。
func providerConstructors(providerType string) (func(baseURL, apiKey string, c *provider.Clients) provider.Provider, bool) {
	switch providerType {
	case "openai", "deepseek":
		return func(b, k string, c *provider.Clients) provider.Provider { return openai.New(b, k, c) }, true
	case "anthropic":
		return func(b, k string, c *provider.Clients) provider.Provider { return anthropic.New(b, k, c) }, true
	case "volcengine":
		return func(b, k string, c *provider.Clients) provider.Provider { return volcengine.New(b, k, c) }, true
	default:
		return nil, false
	}
}

// buildKeyRefResolver 构造 key_ref 组合解析器（阶段 16）：
//   - env://VAR / 纯字符串 → 包级默认（环境变量 / 透传）
//   - vault://path         → Vault KV v2 读取（vault.addr 未配置时返回空并 warn）
func buildKeyRefResolver(cfg *config.Config) func(string) string {
	if cfg.Vault.Addr == "" {
		return provider.ResolveKeyRef
	}
	timeout, err := time.ParseDuration(cfg.Vault.Timeout)
	if err != nil || timeout <= 0 {
		timeout = 3 * time.Second
	}
	vc := vault.New(cfg.Vault.Addr, os.Getenv(cfg.Vault.TokenEnv), timeout)
	return func(ref string) string {
		if strings.HasPrefix(ref, "vault://") {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			secret, err := vc.ReadSecret(ctx, strings.TrimPrefix(ref, "vault://"))
			if err != nil {
				observability.L().Warn("vault key_ref resolve failed", zap.Error(err))
				return ""
			}
			return secret
		}
		return provider.ResolveKeyRef(ref)
	}
}

// resolveAdminToken 解析管理面令牌（支持 env://VAR_NAME 引用；空 = 管理面不启动）
func resolveAdminToken(tokenCfg string) string {
	tokenCfg = strings.TrimSpace(tokenCfg)
	if strings.HasPrefix(tokenCfg, "env://") {
		return strings.TrimSpace(os.Getenv(strings.TrimPrefix(tokenCfg, "env://")))
	}
	return tokenCfg
}

// startConfigSubscriber 配置热更新通道 1：订阅 Redis gw:config 频道，应用到本节点（阶段 15）
// 返回停止函数；Redis 缺失时返回 no-op
func startConfigSubscriber(a *app.App) func() {
	if a.Redis == nil || a.Hot == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	sub := a.Redis.Subscribe(ctx, admin.ConfigChannel)
	go func() {
		ch := sub.Channel()
		for msg := range ch {
			var m map[string]any
			if err := json.Unmarshal([]byte(msg.Payload), &m); err != nil {
				observability.L().Warn("config hot-update: bad payload", zap.Error(err))
				continue
			}
			applied, err := a.Hot.ApplyMap(m)
			if err != nil {
				observability.L().Warn("config hot-update rejected", zap.Error(err))
				continue
			}
			observability.L().Info("config hot-update applied", zap.Strings("keys", applied))
		}
	}()
	return func() {
		cancel()
		_ = sub.Close()
	}
}

// startSighupReloader 配置热更新通道 2：SIGHUP 重读配置文件（阶段 15）
// 热更新项立即生效；非热更新项变更记 warn（需重启生效）
// 返回停止函数
func startSighupReloader(configPath string, a *app.App) func() {
	if a.Hot == nil {
		return func() {}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ch:
				cfg, err := config.Load(configPath) // Load 内部已 applyDefaults + validate
				if err != nil {
					observability.L().Warn("sighup reload: config load failed, ignored", zap.Error(err))
					continue
				}
				a.Hot.ApplyConfig(cfg)
				observability.L().Info("sighup reload: hot config applied",
					zap.Any("snapshot", a.Hot.Snapshot()))
				// 非热更新项变更提示（listen_addr / attempt_timeout / max_in_flight / dsn / redis）
				if cfg.Gateway.ListenAddr != a.Config.Gateway.ListenAddr ||
					cfg.Gateway.AttemptTimeout != a.Config.Gateway.AttemptTimeout ||
					cfg.Gateway.MaxInFlight != a.Config.Gateway.MaxInFlight {
					observability.L().Warn("sighup reload: non-hot fields changed (listen_addr/attempt_timeout/max_in_flight), restart required")
				}
			}
		}
	}()
	return func() {
		signal.Stop(ch)
		close(done)
	}
}

// newSemanticCache 构建语义缓存：Redis 精确哈希层必开；
// qdrant.addr + qdrant.embedding_url 同时配置时启用 Qdrant 向量相似层，否则退化为单层精确缓存
func newSemanticCache(cfg *config.Config, cli *goredis.Client) *cache.Cache {
	opts := cache.Options{
		Redis:          cli,
		TTL:            time.Hour,
		ScoreThreshold: cfg.Qdrant.ScoreThreshold,
	}
	if cfg.Qdrant.Addr != "" && cfg.Qdrant.EmbeddingURL != "" {
		opts.Embedder = vector.NewEmbeddingClient(
			cfg.Qdrant.EmbeddingURL,
			cfg.Qdrant.EmbeddingModel,
			os.Getenv(cfg.Qdrant.EmbeddingAPIKeyEnv),
		)
		opts.VectorIndex = cache.NewVectorIndex(vector.NewQdrantClient(cfg.Qdrant.Addr))
		observability.L().Info("semantic cache vector layer enabled",
			zap.String("qdrant", cfg.Qdrant.Addr),
			zap.String("embedding_model", cfg.Qdrant.EmbeddingModel),
		)
	} else {
		observability.L().Info("semantic cache exact-hash only (qdrant.addr / embedding_url not configured)")
	}
	return cache.New(opts)
}

// newRAGService 构建 RAG 检索服务（多项目知识库）
// 依赖同上：qdrant.addr + qdrant.embedding_url 同时配置且 rag.enabled=true 才构建；
// 任何依赖缺失时返回 nil，网关退化为纯 LLM 转发（不阻塞启动）。
func newRAGService(cfg *config.Config, pool *sql.DB) *rag.Service {
	if !cfg.RAG.Enabled {
		observability.L().Info("rag disabled (rag.enabled=false)")
		return nil
	}
	if cfg.Qdrant.Addr == "" || cfg.Qdrant.EmbeddingURL == "" {
		observability.L().Warn("rag enabled but qdrant.addr / qdrant.embedding_url missing, rag disabled")
		return nil
	}
	observability.L().Info("rag enabled",
		zap.Int("top_k", cfg.RAG.TopK),
		zap.Int("max_context_tokens", cfg.RAG.MaxContextTokens),
		zap.String("retrieval_timeout", cfg.RAG.RetrievalTimeout),
		zap.String("collection_prefix", cfg.RAG.CollectionPrefix),
	)
	return rag.NewService(
		pool,
		vector.NewEmbeddingClient(
			cfg.Qdrant.EmbeddingURL,
			cfg.Qdrant.EmbeddingModel,
			os.Getenv(cfg.Qdrant.EmbeddingAPIKeyEnv),
		),
		vector.NewQdrantClient(cfg.Qdrant.Addr),
		rag.Options{
			CollectionPrefix: cfg.RAG.CollectionPrefix,
			TopK:             cfg.RAG.TopK,
			ScoreThreshold:   cfg.RAG.ScoreThreshold,
			MaxContextTokens: cfg.RAG.MaxContextTokens,
			RetrievalTimeout: cfg.RAG.RetrievalTimeoutDuration(),
			ChunkSize:        cfg.RAG.ChunkSize,
			ChunkOverlap:     cfg.RAG.ChunkOverlap,
			EmbedModel:       cfg.Qdrant.EmbeddingModel,
		},
	)
}

// bodyLimit 限制请求体大小，超过返回 413
func bodyLimit(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.ContentLength > maxBytes {
			apperr.Abort(c, http.StatusRequestEntityTooLarge, "invalid_request_error", "request_body_too_large", "request body too large")
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		c.Next()
	}
}

// providerTypeBaseURLMap 从 config.providers 提取 provider type → baseURL 映射
// 供阶段 8 LoadChannelKeys 使用（ChannelProvider 构造时需要知道 baseURL）
func providerTypeBaseURLMap(providers []config.ProviderConfig) map[string]string {
	m := make(map[string]string, len(providers))
	for _, p := range providers {
		m[p.Type] = p.BaseURL
	}
	return m
}
