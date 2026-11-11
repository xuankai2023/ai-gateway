// Package app 依赖聚合（阶段 14：DI 收拢）
//
// App 结构聚合网关全部运行时依赖：DB / Redis / 熔断管理器 / 广播总线 /
// 异步执行器 / 用量写入器 / Provider 注册表 / 路由器 / 中继 / 语义缓存 / 配置。
//
// 纪律：
//   - handler / middleware 不再引用 db.GetPool()、provider.Factory 等全局单例，
//     全部经 *App 注入（func(app *App) gin.HandlerFunc）
//   - pkg/db、pkg/redis 保留函数式 API，但只被 App 组装（main）引用
//   - 同一进程可起两个互不干扰的 App（缓存 / 熔断器 / 广播订阅全部实例级）
//   - 本包不 import handler / middleware（避免环依赖：它们反过来 import app）
package app

import (
	"context"
	"database/sql"
	"sync"
	"time"

	semcache "ai-gateway/apps/gateway/internal/cache"
	"ai-gateway/apps/gateway/internal/diagnostics"
	"ai-gateway/apps/gateway/internal/rag"
	"ai-gateway/apps/gateway/internal/relay"
	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/apps/gateway/internal/usagelog"
	"ai-gateway/pkg/async"
	"ai-gateway/pkg/broadcast"
	"ai-gateway/pkg/circuitbreaker"
	"ai-gateway/pkg/config"
	"ai-gateway/pkg/pricing"
	"ai-gateway/pkg/provider"
	goredis "github.com/redis/go-redis/v9"
)

// AttemptResolver 候选计划解析接口（*relay.Relay 实现；测试注入 fake）
type AttemptResolver interface {
	ResolveAttempts(ctx context.Context, tenantID int64, modelName string, maxAttempts int) ([]routing.Ability, error)
}

// ModelPricingResolver 模型定价快照解析接口（*relay.Relay 实现；测试注入 fake）
type ModelPricingResolver interface {
	ResolveModelPricing(ctx context.Context, tenantID int64, modelName string) (*pricing.ModelRow, error)
}

// Options App 组装入参（main 一次性传入；Config 为 nil 时回落 config.Default()）
type Options struct {
	Config        *config.Config
	DB            *sql.DB
	Redis         *goredis.Client
	Bus           *broadcast.Bus  // nil = 仅本节点缓存失效，不跨节点广播
	SemanticCache *semcache.Cache // nil = 语义缓存中间件直通
	RAG           *rag.Service    // nil = RAG 关闭（handler 不解析助手）
}

// App 网关运行时依赖聚合体
type App struct {
	Config *config.Config
	// Hot 运行期热更新配置（阶段 15：Redis Pub/Sub 推送 + SIGHUP 重读双通道生效）
	Hot   *config.HotValues
	DB    *sql.DB
	Redis *goredis.Client
	Bus   *broadcast.Bus

	Breakers  *circuitbreaker.Manager
	Async     *async.Executor
	Usage     *usagelog.Writer
	Providers *provider.Registry
	Router    *routing.Router
	Relay     *relay.Relay

	// Diagnostics 请求级诊断包（异步采样落库；DB 为 nil 时静默跳过）
	Diagnostics *diagnostics.Recorder

	// Resolver 候选计划解析（默认 = Relay；测试注入 fake 路由）
	Resolver AttemptResolver

	// Pricing 定价引擎（nil 时使用 pricing.Default）
	Pricing *pricing.Engine
	// PricingResolver 模型定价快照解析（默认 = Relay；测试注入 fake）
	PricingResolver ModelPricingResolver

	// SemanticCache 语义缓存实例（nil = 中间件直通）
	SemanticCache *semcache.Cache

	// RAG 多项目知识库检索（nil = 关闭；项目隔离与降级策略在 rag 包内）
	RAG *rag.Service

	// LoadShedQueueTimeout load_shed 排队上限（<=0 走中间件默认 2s；测试可覆盖）
	LoadShedQueueTimeout time.Duration

	mu           sync.Mutex
	closers      []func()
	invalidators map[string]func()
}

// New 组装 App：熔断管理器 / 异步执行器 / 用量写入器 / Provider 注册表 / 路由器 / 中继
// 全部实例级创建并互相挂接（bus 挂到熔断器与路由器；providers 注入共享 HTTP clients）
func New(opts Options) *App {
	cfg := opts.Config
	if cfg == nil {
		cfg = config.Default()
	}
	_, attemptTimeout, _ := cfg.Gateway.TimeoutDurations()

	a := &App{
		Config:        cfg,
		Hot:           config.NewHotValues(cfg),
		DB:            opts.DB,
		Redis:         opts.Redis,
		Bus:           opts.Bus,
		SemanticCache: opts.SemanticCache,
		RAG:           opts.RAG,
		invalidators:  make(map[string]func()),
	}
	a.Async = &async.Executor{}
	a.Breakers = circuitbreaker.NewManager(circuitbreaker.Options{})
	if opts.Bus != nil {
		a.Breakers.SetBus(opts.Bus)
	}
	a.Usage = usagelog.New(opts.DB, usagelog.Options{})
	a.Providers = provider.NewRegistry()
	a.Providers.SetClients(provider.NewClients(attemptTimeout))
	a.Router = routing.NewRouter(opts.DB, a.Breakers, opts.Bus, a.Async)
	a.Relay = relay.New(a.Router, a.Breakers, a.Providers, opts.DB, attemptTimeout)
	a.Resolver = a.Relay
	a.Pricing = pricing.Default
	a.PricingResolver = a.Relay
	a.Diagnostics = diagnostics.New(opts.DB, a.Async, cfg.Diagnostics.SampleRate)

	a.RegisterInvalidator("abilities", a.Router.InvalidateAllAbilitiesCache)
	a.RegisterInvalidator("model", a.Relay.InvalidateModelCache)
	if a.RAG != nil {
		a.RegisterInvalidator("assistants", a.RAG.InvalidateAssistants)
		a.AddCloser(a.RAG.Close)
	}
	return a
}

// AddCloser 注册一个收尾回调（中间件持有的本地缓存等挂到 App 生命周期）
func (a *App) AddCloser(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closers = append(a.closers, fn)
}

// RegisterInvalidator 注册缓存命名空间的失效入口（中间件 / 路由器在构造时挂接）
func (a *App) RegisterInvalidator(namespace string, fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.invalidators == nil {
		a.invalidators = make(map[string]func())
	}
	a.invalidators[namespace] = fn
}

// InvalidateCache 失效本节点缓存：ns 指定命名空间；ns="" 全部失效
// （跨节点 cache_invalidate 广播事件由 main 订阅后调用本方法）
func (a *App) InvalidateCache(ns string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if ns == "" {
		for _, fn := range a.invalidators {
			fn()
		}
		return
	}
	if fn, ok := a.invalidators[ns]; ok {
		fn()
	}
}

// Close 关停 App 内部组件：失效器注册的中间件缓存 → 路由/中继缓存 → 用量写入器排空
// 调用方需先停流量（HTTP Shutdown）再 Close；幂等
func (a *App) Close(ctx context.Context) error {
	a.mu.Lock()
	closers := a.closers
	a.closers = nil
	a.mu.Unlock()

	for _, fn := range closers {
		fn()
	}
	if a.Router != nil {
		a.Router.Close()
	}
	if a.Relay != nil {
		a.Relay.Close()
	}
	if a.Usage != nil {
		return a.Usage.Close(ctx)
	}
	return nil
}
