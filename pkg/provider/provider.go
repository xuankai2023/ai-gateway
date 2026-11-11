// Package provider 多供应商统一接口抽象
//
// 所有模型供应商（火山方舟 / OpenAI / Anthropic / 通义 / 本地部署）
// 必须实现 Provider 接口，Gateway 和 Worker 只依赖接口不依赖具体实现。
//
// 新增供应商步骤：
//  1. 在 pkg/provider/ 下新建子包（如 pkg/provider/qwen/）
//  2. 实现 Provider 接口的 4 个方法
//  3. 在 Factory 里注册到 providerType → struct 的映射表
package provider

import (
	"context"
	"errors"
)

// ErrNotImplemented 方法已定义但当前阶段尚未实现
var ErrNotImplemented = errors.New("provider: method not implemented")

// Provider 供应商统一接口（所有供应商必须实现）
type Provider interface {
	// ChatCompletion 聊天补全（最常用）
	ChatCompletion(ctx context.Context, req *ChatRequest) (*ChatResponse, error)

	// Embedding 向量嵌入（语义缓存 + RAG 用）
	Embedding(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error)

	// Moderation 内容审核（安全预检查用）
	Moderation(ctx context.Context, req *ModerationRequest) (*ModerationResponse, error)

	// Cost 成本计算（根据 models 表配置，不要硬编码）
	Cost(providerModel string, inputTokens, outputTokens int64) (float64, error)

	// Name 供应商标识（日志 / 指标 / UsageLog 里用）
	Name() string
}

// --- 请求 / 响应结构（所有供应商统一格式） ---

type ChatRequest struct {
	Model       string
	Messages    []Message
	Temperature float32
	MaxTokens   int
	Stream      bool
}

type Message struct {
	Role    string
	Content string
}

type ChatResponse struct {
	Content      string
	InputTokens  int64
	OutputTokens int64
	LatencyMs    int64
	Provider     string
}

type EmbeddingRequest struct {
	Model string
	Texts []string
}

type EmbeddingResponse struct {
	Vectors [][]float32
	Model   string
}

type ModerationRequest struct {
	Input string
}

type ModerationResponse struct {
	SafetyScore float32
	IsToxic     bool
	Categories  []string
}

// --- 流式扩展（可选接口，支持流式的 Provider 实现 Streamer） ---

// StreamChunk 流式响应的一个增量帧
// Content 是这一帧的文本增量；IsDone=true 表示最后一帧（usage 只在最后一帧有值）
type StreamChunk struct {
	Content      string
	IsDone       bool
	InputTokens  int64
	OutputTokens int64
}

// Streamer 可选扩展接口：支持流式的 Provider 实现它
// Handler 用类型断言判断：if s, ok := p.(provider.Streamer); ok { ... }
// 返回两个 channel：chunkChan 输出帧，errChan 输出错误（最多一个），都会在完成时 close
type Streamer interface {
	ChatCompletionStream(ctx context.Context, req *ChatRequest) (chunkChan <-chan StreamChunk, errChan <-chan error)
}

// Endpoint 可选扩展接口：暴露上游 base_url
// 熔断 key = provider type + base_url，同一类型的不同端点互不影响
type Endpoint interface {
	BaseURL() string
}
