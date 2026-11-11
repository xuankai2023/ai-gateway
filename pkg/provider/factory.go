package provider

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// Registry Provider 注册表（阶段 14：全局单例 Factory / keypool 状态收敛为实例，随 App 生命周期创建）
//
// 聚合三类注册信息：
//   - provider type → 构造器（main 按 config.providers 注册）
//   - provider type → 渠道构造器 / baseURL（channelProvider 惰性构造用）
//   - abilityID → ChannelConfig（channel_keys 多 key 池）
type Registry struct {
	mu       sync.RWMutex
	registry map[string]func() Provider

	constructorsMu sync.RWMutex
	constructors   map[string]providerConstructor

	baseURLsMu sync.RWMutex
	baseURLs   map[string]string

	clients atomic.Pointer[Clients] // 共享 HTTP client 对（channelProvider 惰性构造用）

	channelConfigs sync.Map // abilityID → *ChannelConfig

	// keyRefResolver key_ref 解析器（阶段 16：main 注入 env://+vault:// 组合解析；
	// nil 时回落包级 resolveKeyRef —— env:// + 纯字符串）
	keyRefResolver atomic.Value // func(ref string) string
}

// SetKeyRefResolver 注入 key_ref 解析器（env:// 与 vault:// 双 scheme；main 启动时按 vault 配置注入）
func (r *Registry) SetKeyRefResolver(fn func(ref string) string) {
	r.keyRefResolver.Store(fn)
}

// resolveKeyRef 经注入的解析器解析；未注入时走包级默认
func (r *Registry) resolveKeyRef(ref string) string {
	if fn, ok := r.keyRefResolver.Load().(func(ref string) string); ok && fn != nil {
		return fn(ref)
	}
	return resolveKeyRef(ref)
}

// NewRegistry 创建空的 Provider 注册表
func NewRegistry() *Registry {
	return &Registry{
		registry:     make(map[string]func() Provider),
		constructors: make(map[string]providerConstructor),
		baseURLs:     make(map[string]string),
	}
}

// Register 注册一个 Provider 实现，重复注册会覆盖
func (r *Registry) Register(providerType string, constructor func() Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registry[providerType] = constructor
}

// Get 根据类型创建 Provider 实例；未知类型返回 error
func (r *Registry) Get(providerType string) (Provider, error) {
	r.mu.RLock()
	constructor, ok := r.registry[providerType]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown provider type: %s", providerType)
	}
	return constructor(), nil
}

// AllRegistered 返回所有已注册的 provider 类型（按字典序，管理台展示用）
func (r *Registry) AllRegistered() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.registry))
	for providerType := range r.registry {
		types = append(types, providerType)
	}
	sort.Strings(types)
	return types
}
