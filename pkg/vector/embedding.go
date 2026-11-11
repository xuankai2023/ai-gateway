// Package vector 向量化与向量库基础设施
//
// 只做两件事：
//   - Embedder：OpenAI 兼容 /v1/embeddings 客户端（语义缓存 + RAG 共用）
//   - QdrantClient：Qdrant v1 REST 客户端（collection 管理 / 批量写入 / 检索 / 按条件删除）
//
// 复用方（语义缓存、RAG 检索）只依赖这两个类型，不再各自复制 HTTP 客户端。
package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Embedder 文本向量化接口
type Embedder interface {
	// Embed 单条文本向量化
	Embed(ctx context.Context, text string) ([]float32, error)
	// EmbedBatch 批量文本向量化，返回顺序与入参一致
	EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)
}

// EmbeddingClient OpenAI 兼容 /v1/embeddings 客户端（无第三方依赖）
type EmbeddingClient struct {
	url    string
	model  string
	apiKey string
	http   *http.Client
}

// NewEmbeddingClient 创建向量化客户端；apiKey 为空时不带 Authorization 头
func NewEmbeddingClient(url, model, apiKey string) *EmbeddingClient {
	return &EmbeddingClient{
		url:    url,
		model:  model,
		apiKey: apiKey,
		http:   &http.Client{Timeout: 3 * time.Second},
	}
}

// Embed 单条文本向量化（复用批量实现）
func (e *EmbeddingClient) Embed(ctx context.Context, text string) ([]float32, error) {
	vecs, err := e.EmbedBatch(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	return vecs[0], nil
}

// EmbedBatch 批量文本向量化；响应按 index 排序，保证与入参顺序一致
func (e *EmbeddingClient) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	reqBody, err := json.Marshal(map[string]any{"model": e.model, "input": texts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.apiKey)
	}
	resp, err := e.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("embedding http %d: %s", resp.StatusCode, string(raw))
	}
	var out struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("embedding decode failed: %w", err)
	}
	if len(out.Data) != len(texts) {
		return nil, fmt.Errorf("embedding response size mismatch: got %d, want %d", len(out.Data), len(texts))
	}
	vecs := make([][]float32, len(texts))
	for _, item := range out.Data {
		if item.Index < 0 || item.Index >= len(vecs) {
			return nil, fmt.Errorf("embedding response index out of range: %d", item.Index)
		}
		if len(item.Embedding) == 0 {
			return nil, fmt.Errorf("embedding response empty at index %d", item.Index)
		}
		vec := make([]float32, len(item.Embedding))
		for i, v := range item.Embedding {
			vec[i] = float32(v)
		}
		vecs[item.Index] = vec
	}
	for i, v := range vecs {
		if len(v) == 0 {
			return nil, fmt.Errorf("embedding response missing index %d", i)
		}
	}
	return vecs, nil
}
