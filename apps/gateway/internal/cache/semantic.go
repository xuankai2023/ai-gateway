// Package cache 语义缓存底层实现（阶段 12）
//
// 两级缓存：
//  1. 精确哈希（Redis）：key = semcache:exact:{tenant}:{project}:{epoch}:{sha256(model+messages 规范化)}，TTL 1h；
//  2. 向量相似（可选 Qdrant）：collection semcache_{tenant}_{project}，余弦相似度 ≥ 0.95 命中。
//
// 隔离：
//   - Redis key 带 tenant + project 前缀；
//   - Qdrant 每个租户的每个项目一个 collection（不使用共享 collection + payload 过滤，避免串数据）；
//   - epoch = projects.index_epoch：知识库文档更新后 +1，旧 key 自然失效（缓存最长 60s 内感知）。
//
// 向量层是可选增强：只有同时配置 embedder + vector index 时才启用；
// 未配置时退化为精确哈希单层缓存（功能仍完整闭环，只是不做近似语义召回）。
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"ai-gateway/pkg/vector"
	goredis "github.com/redis/go-redis/v9"
)

// DefaultTTL 缓存条目默认过期时间（计划口径 1h）
const DefaultTTL = time.Hour

// DefaultScoreThreshold 向量相似命中阈值（计划口径 ≥ 0.95）
const DefaultScoreThreshold = 0.95

// Message 规范化后的对话消息（与请求体 message 结构对齐）
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Entry 一条缓存响应
type Entry struct {
	Body        []byte `json:"body"`
	ContentType string `json:"content_type"`
	CreatedAt   int64  `json:"created_at"`
	ExpiresAt   int64  `json:"expires_at"`
}

// expired 是否已过期
func (e *Entry) expired(now time.Time) bool {
	return e.ExpiresAt > 0 && now.Unix() >= e.ExpiresAt
}

// Embedder 文本向量化接口（OpenAI 兼容 /v1/embeddings；pkg/vector.EmbeddingClient 实现）
type Embedder = vector.Embedder

// VectorIndex 向量索引接口（Qdrant 实现；payload 只存 fingerprint，响应体仍以 Redis 精确层为准）
type VectorIndex interface {
	Search(ctx context.Context, collection string, vector []float32, threshold float64) (fingerprint string, score float64, ok bool, err error)
	Upsert(ctx context.Context, collection string, id, fingerprint string, vector []float32) error
}

// Options 语义缓存配置
type Options struct {
	// Redis 精确哈希层（必填）
	Redis *goredis.Client
	// TTL 缓存条目过期时间（<=0 用 DefaultTTL）
	TTL time.Duration
	// Embedder + VectorIndex 同时非空才启用向量层
	Embedder    Embedder
	VectorIndex VectorIndex
	// ScoreThreshold 向量相似阈值（<=0 用 DefaultScoreThreshold）
	ScoreThreshold float64
}

// Cache 两级语义缓存
type Cache struct {
	redis     *goredis.Client
	ttl       time.Duration
	embedder  Embedder
	index     VectorIndex
	threshold float64
}

// New 创建语义缓存实例
func New(opts Options) *Cache {
	if opts.TTL <= 0 {
		opts.TTL = DefaultTTL
	}
	if opts.ScoreThreshold <= 0 {
		opts.ScoreThreshold = DefaultScoreThreshold
	}
	return &Cache{
		redis:     opts.Redis,
		ttl:       opts.TTL,
		embedder:  opts.Embedder,
		index:     opts.VectorIndex,
		threshold: opts.ScoreThreshold,
	}
}

// TTL 缓存条目过期时间
func (c *Cache) TTL() time.Duration { return c.ttl }

// VectorEnabled 向量层是否启用
func (c *Cache) VectorEnabled() bool { return c.embedder != nil && c.index != nil }

// Layer 缓存命中层级
const (
	LayerExact    = "exact"
	LayerSemantic = "semantic"
)

// 精确层 key 前缀
const exactKeyPrefix = "semcache:exact:"

// Fingerprint 计算规范化指纹（model + 有序 messages）
func Fingerprint(model string, messages []Message) string {
	payload := struct {
		Model    string    `json:"model"`
		Messages []Message `json:"messages"`
	}{Model: model, Messages: messages}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// promptText 拼出用于向量化的纯文本（角色 + 内容，保持顺序）
func promptText(model string, messages []Message) string {
	var b strings.Builder
	b.WriteString("model:")
	b.WriteString(model)
	for _, m := range messages {
		b.WriteString("\n")
		b.WriteString(m.Role)
		b.WriteString(":")
		b.WriteString(m.Content)
	}
	return b.String()
}

// exactKey 精确层 Redis key（租户 + 项目 + 知识库版本隔离）
func exactKey(tenantID, projectID, epoch int64, fingerprint string) string {
	return exactKeyPrefix + strconv.FormatInt(tenantID, 10) + ":" +
		strconv.FormatInt(projectID, 10) + ":" +
		strconv.FormatInt(epoch, 10) + ":" + fingerprint
}

// collection 租户 + 项目独立 Qdrant collection（projectID=0 表示租户级）
func collection(tenantID, projectID int64) string {
	return "semcache_" + strconv.FormatInt(tenantID, 10) + "_" + strconv.FormatInt(projectID, 10)
}

// Lookup 查询缓存；layer 为 exact / semantic，未命中 layer 为空
func (c *Cache) Lookup(ctx context.Context, tenantID, projectID, epoch int64, model string, messages []Message) (*Entry, string, error) {
	fp := Fingerprint(model, messages)

	entry, err := c.getExact(ctx, tenantID, projectID, epoch, fp)
	if err != nil {
		return nil, "", err
	}
	if entry != nil {
		return entry, LayerExact, nil
	}

	if !c.VectorEnabled() {
		return nil, "", nil
	}

	vec, err := c.embedder.Embed(ctx, promptText(model, messages))
	if err != nil {
		return nil, "", fmt.Errorf("semantic cache embed failed: %w", err)
	}
	hitFP, _, ok, err := c.index.Search(ctx, collection(tenantID, projectID), vec, c.threshold)
	if err != nil {
		return nil, "", fmt.Errorf("semantic cache vector search failed: %w", err)
	}
	if !ok || hitFP == "" {
		return nil, "", nil
	}

	// 向量命中后回源精确层取响应体（Qdrant 只存指纹，避免大响应体进向量库）
	entry, err = c.getExact(ctx, tenantID, projectID, epoch, hitFP)
	if err != nil {
		return nil, "", err
	}
	if entry == nil {
		return nil, "", nil // 精确层已过期（TTL 到期 / 知识库版本变化），视为未命中
	}

	// 写穿：同 prompt 下次直接走精确层，命中 P99 稳定在毫秒级
	_ = c.setExact(ctx, tenantID, projectID, epoch, fp, entry)
	return entry, LayerSemantic, nil
}

// Store 写回缓存：精确层同步写，向量层 best-effort（调用方放在异步执行器里）
func (c *Cache) Store(ctx context.Context, tenantID, projectID, epoch int64, model string, messages []Message, entry *Entry) error {
	fp := Fingerprint(model, messages)
	if err := c.setExact(ctx, tenantID, projectID, epoch, fp, entry); err != nil {
		return err
	}
	if !c.VectorEnabled() {
		return nil
	}
	vec, err := c.embedder.Embed(ctx, promptText(model, messages))
	if err != nil {
		return fmt.Errorf("semantic cache embed failed: %w", err)
	}
	if err := c.index.Upsert(ctx, collection(tenantID, projectID), vector.PointID(fp), fp, vec); err != nil {
		return fmt.Errorf("semantic cache vector upsert failed: %w", err)
	}
	return nil
}

// getExact 读精确层；过期条目惰性删除
func (c *Cache) getExact(ctx context.Context, tenantID, projectID, epoch int64, fp string) (*Entry, error) {
	if c.redis == nil {
		return nil, fmt.Errorf("semantic cache: redis client is nil")
	}
	key := exactKey(tenantID, projectID, epoch, fp)
	raw, err := c.redis.Get(ctx, key).Bytes()
	if err == goredis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("semantic cache exact get failed: %w", err)
	}
	var entry Entry
	if err := json.Unmarshal(raw, &entry); err != nil {
		_ = c.redis.Del(ctx, key).Err()
		return nil, fmt.Errorf("semantic cache exact decode failed: %w", err)
	}
	if entry.expired(time.Now()) {
		_ = c.redis.Del(ctx, key).Err()
		return nil, nil
	}
	return &entry, nil
}

// setExact 写精确层
func (c *Cache) setExact(ctx context.Context, tenantID, projectID, epoch int64, fp string, entry *Entry) error {
	if c.redis == nil {
		return fmt.Errorf("semantic cache: redis client is nil")
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("semantic cache exact encode failed: %w", err)
	}
	return c.redis.Set(ctx, exactKey(tenantID, projectID, epoch, fp), raw, c.ttl).Err()
}

// ---------------------------------------------------------------------------
// Qdrant 适配器：把 pkg/vector 客户端收敛为 VectorIndex（payload 只存 fingerprint）
// ---------------------------------------------------------------------------

// qdrantIndex VectorIndex 的 Qdrant 实现
type qdrantIndex struct {
	q *vector.QdrantClient
}

// NewVectorIndex 用 pkg/vector.QdrantClient 构造语义缓存向量索引
func NewVectorIndex(q *vector.QdrantClient) VectorIndex {
	return &qdrantIndex{q: q}
}

// Search 相似检索 top1；命中返回指纹与相似度
func (a *qdrantIndex) Search(ctx context.Context, name string, vec []float32, threshold float64) (string, float64, bool, error) {
	points, err := a.q.Search(ctx, name, vec, 1, threshold, nil)
	if err != nil {
		return "", 0, false, err
	}
	if len(points) == 0 {
		return "", 0, false, nil
	}
	fp, _ := points[0].Payload["fingerprint"].(string)
	if fp == "" {
		return "", 0, false, nil
	}
	return fp, points[0].Score, true, nil
}

// Upsert 写入/覆盖向量点（id 由指纹派生，天然幂等）
func (a *qdrantIndex) Upsert(ctx context.Context, name, id, fingerprint string, vec []float32) error {
	return a.q.UpsertPoints(ctx, name, []vector.Point{{
		ID:      id,
		Vector:  vec,
		Payload: map[string]any{"fingerprint": fingerprint},
	}})
}

// ---------------------------------------------------------------------------
// 说明：EmbeddingClient / QdrantClient 已收敛到 pkg/vector（语义缓存与 RAG 共用）
// ---------------------------------------------------------------------------
