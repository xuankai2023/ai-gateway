// Command main Go 网关启动入口
//
// 启动顺序：
//  1. config.Load 读取本地 yaml（--config 可指定，默认 configs/config.yaml）
//  2. observability.Init 初始化结构化日志
//  3. db.InitPool 初始化 MySQL 连接池并 Ping（失败即退出）
//  4. 按 config.providers 注册 Provider（未知类型即退出）
//  5. gin.New() + 中间件链 + /healthz
//  6. 监听 :8080，SIGINT/SIGTERM 优雅退出
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ai-gateway/apps/gateway/internal/handler"
	"ai-gateway/apps/gateway/internal/middleware"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/db"
	"ai-gateway/pkg/observability"
	"ai-gateway/pkg/provider"
	"ai-gateway/pkg/provider/anthropic"
	"ai-gateway/pkg/provider/openai"
	"ai-gateway/pkg/provider/volcengine"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	// shutdownTimeout 优雅退出最长时间
	shutdownTimeout = 5 * time.Second
	// healthCheckTimeout 健康检查 Ping DB 的超时
	healthCheckTimeout = time.Second
	// version 二进制版本号，构建时可通过 -ldflags 覆盖
	version = "dev"
	// maxBodySize 请求体最大 10MB，防止 DoS
	maxBodySize = 10 << 20
)

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

	// 初始化 OTel Tracer（全链路 span）
	if err := observability.InitTracer(context.Background()); err != nil {
		observability.L().Warn("tracer init failed, continuing without tracing", zap.Error(err))
	} else {
		defer observability.ShutdownTracer(context.Background())
	}

	observability.L().Info("config loaded",
		zap.String("path", *configPath),
		zap.String("listen_addr", cfg.Gateway.ListenAddr),
	)

	stage := observability.BeginStage("mysql")
	if _, err := db.InitPool(context.Background(), cfg.MySQL.DSN, cfg.MySQL.PoolSize); err != nil {
		return err
	}
	defer db.Close()
	stage.Done("db connected")

	stage = observability.BeginStage("providers")
	if len(cfg.Providers) == 0 {
		return errors.New("at least one provider must be configured")
	}
	if err := registerProviders(cfg.Providers); err != nil {
		return err
	}
	stage.Done("providers registered", zap.Strings("types", provider.Factory.AllRegistered()))

	ln, err := net.Listen("tcp", cfg.Gateway.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s failed: %w", cfg.Gateway.ListenAddr, err)
	}

	srv := &http.Server{
		Handler:           newRouter(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()
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

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		observability.L().Error("shutdown failed", zap.Error(err))
	}
	if metricsSrv != nil {
		_ = metricsSrv.Shutdown(ctx)
	}
	observability.L().Info("shutdown complete")
	return nil
}

// newRouter 组装路由与中间件链
func newRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	// 全局中间件（所有路由都走）
	r.Use(bodyLimit(maxBodySize))
	r.Use(middleware.Recover())
	r.Use(middleware.Trace())
	r.Use(middleware.Logger())

	// 公共路由（不需要鉴权）
	r.GET("/healthz", healthz)

	// API 路由组（/v1/*，需要 Auth）
	v1 := r.Group("/v1")
	v1.Use(middleware.Auth())
	v1.POST("/chat/completions", handler.ChatCompletion())

	return r
}

// healthz 健康检查：DB 可 Ping 才返回 200
func healthz(c *gin.Context) {
	pool := db.GetPool()
	if pool == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "unavailable",
			"db":      "uninitialized",
			"version": version,
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), healthCheckTimeout)
	defer cancel()
	if err := pool.PingContext(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "unavailable",
			"db":      "down",
			"version": version,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"db":      "ok",
		"version": version,
	})
}

// registerProviders 按配置注册 Provider；未知类型直接返回错误
func registerProviders(list []config.ProviderConfig) error {
	for _, pc := range list {
		switch pc.Type {
		case "openai":
			baseURL, apiKey := pc.BaseURL, os.Getenv(pc.APIKeyEnv)
			provider.Factory.Register(pc.Type, func() provider.Provider {
				return openai.New(baseURL, apiKey)
			})
		case "anthropic":
			baseURL, apiKey := pc.BaseURL, os.Getenv(pc.APIKeyEnv)
			provider.Factory.Register(pc.Type, func() provider.Provider {
				return anthropic.New(baseURL, apiKey)
			})
		case "volcengine":
			baseURL, apiKey := pc.BaseURL, os.Getenv(pc.APIKeyEnv)
			provider.Factory.Register(pc.Type, func() provider.Provider {
				return volcengine.New(baseURL, apiKey)
			})
		default:
			return fmt.Errorf("unknown provider type: %s", pc.Type)
		}
	}
	return nil
}

// bodyLimit 限制请求体大小，超过返回 413
func bodyLimit(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.ContentLength > maxBytes {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{
				"error": gin.H{
					"message": "request body too large",
					"type":    "request_error",
				},
			})
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		c.Next()
	}
}
