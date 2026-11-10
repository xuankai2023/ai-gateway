package provider

import (
	"fmt"
	"sort"
	"sync"
)

// Factory Provider 工厂
// 根据配置里的 providerType（如 "volcengine" / "openai"）动态创建 Provider 实例。
// 注册在 main 里按 config.providers 完成，业务代码零改动。
var Factory = &factoryImpl{}

type factoryImpl struct {
	mu       sync.RWMutex
	registry map[string]func() Provider
}

// Register 注册一个 Provider 实现，重复注册会覆盖
func (f *factoryImpl) Register(providerType string, constructor func() Provider) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.registry == nil {
		f.registry = make(map[string]func() Provider)
	}
	f.registry[providerType] = constructor
}

// Get 根据类型创建 Provider 实例；未知类型返回 error
func (f *factoryImpl) Get(providerType string) (Provider, error) {
	f.mu.RLock()
	constructor, ok := f.registry[providerType]
	f.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown provider type: %s", providerType)
	}
	return constructor(), nil
}

// AllRegistered 返回所有已注册的 provider 类型（按字典序，管理台展示用）
func (f *factoryImpl) AllRegistered() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	types := make([]string, 0, len(f.registry))
	for providerType := range f.registry {
		types = append(types, providerType)
	}
	sort.Strings(types)
	return types
}
