// Package rag 多项目知识库检索（RAG）
//
// 隔离模型（与 docs/rag-multi-project.md 一致）：
//   - 项目归属只看 API Key 注入的 project_id，绝不从请求体读取；
//   - 对外模型名（如 ops-assistant）在 assistants 表映射到唯一 project_id + 提示词 + 召回参数；
//   - Qdrant 每项目独立 collection：kb_<project.slug>，检索同时带 project_id 过滤（双保险）；
//   - 检索硬超时（默认 800ms），失败 / 超时一律降级为"无资料回答"，不返回 5xx。
//
// 本包不 import handler / middleware，只依赖 DB、写入器与向量基础设施。
package rag

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"ai-gateway/pkg/cache"
	"ai-gateway/pkg/observability"
	"ai-gateway/pkg/provider"
	"ai-gateway/pkg/vector"
	"go.uber.org/zap"
)

// assistantTTL 助手配置本地缓存过期时间（Admin 修改后靠广播失效，最差 60s 生效）
const assistantTTL = 60 * time.Second

// ErrAssistantNotFound 模型名未配置为助手（按普通模型路由处理）
var ErrAssistantNotFound = errors.New("rag: assistant not found")

// ErrProjectInactive 项目已停用，禁止检索
var ErrProjectInactive = errors.New("rag: project inactive")

// Assistant 助手配置（对外模型名 → 项目 + 提示词 + 召回参数）
type Assistant struct {
	ID               int64
	TenantID         int64
	ProjectID        int64
	ModelName        string
	SystemPrompt     string
	TopK             int
	ScoreThreshold   float64
	MaxContextTokens int
	ProjectSlug      string
	ProjectStatus    string
}

// Citation 一条引用（只暴露标题与来源，不暴露向量分数与 chunk 原文）
type Citation struct {
	Index  int    `json:"index"`
	DocID  int64  `json:"doc_id"`
	Title  string `json:"title"`
	Source string `json:"source"`
}

// Context 检索拼装结果
type Context struct {
	// Messages 替换后的完整消息（system 提示词 + 编号资料 + 原始对话）
	Messages []provider.Message
	// Citations 引用列表（按编号顺序）
	Citations []Citation
	// RetrievedCount 实际召回条数
	RetrievedCount int
	// Elapsed 检索耗时
	Elapsed time.Duration
	// Fallback 检索失败 / 超时降级（仍可正常回答，只是没有资料）
	Fallback bool
}

// Options 检索参数（来自 config.rag）
type Options struct {
	CollectionPrefix string
	TopK             int
	ScoreThreshold   float64
	MaxContextTokens int
	RetrievalTimeout time.Duration
	ChunkSize        int
	ChunkOverlap     int
	EmbedModel       string
}

// Service RAG 检索服务（DB 元数据 + 向量检索 + 入库）
type Service struct {
	db      *sql.DB
	embed   vector.Embedder
	index   *vector.QdrantClient
	opts    Options
	assists *cache.Cache[*Assistant]
	misses  *cache.Cache[bool]
}

// NewService 创建检索服务；opts 零值字段按默认值补齐
func NewService(db *sql.DB, embed vector.Embedder, index *vector.QdrantClient, opts Options) *Service {
	if opts.TopK <= 0 {
		opts.TopK = 5
	}
	if opts.ScoreThreshold <= 0 {
		opts.ScoreThreshold = 0.3
	}
	if opts.MaxContextTokens <= 0 {
		opts.MaxContextTokens = 2000
	}
	if opts.RetrievalTimeout <= 0 {
		opts.RetrievalTimeout = 800 * time.Millisecond
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 400
	}
	if opts.ChunkOverlap < 0 || opts.ChunkOverlap >= opts.ChunkSize {
		opts.ChunkOverlap = opts.ChunkSize / 8
	}
	if opts.CollectionPrefix == "" {
		opts.CollectionPrefix = "kb_"
	}
	assists := cache.New[*Assistant](cache.Options{
		Name:       "assistants",
		MaxEntries: 10000,
		DefaultTTL: assistantTTL,
	})
	observability.RegisterCache("assistants", assists)
	// 未命中负缓存：避免每个普通模型请求都查一次 assistants 表
	misses := cache.New[bool](cache.Options{
		Name:       "assistants_miss",
		MaxEntries: 10000,
		DefaultTTL: assistantTTL,
	})
	observability.RegisterCache("assistants_miss", misses)
	return &Service{db: db, embed: embed, index: index, opts: opts, assists: assists, misses: misses}
}

// Close 停止助手缓存的后台清扫
func (s *Service) Close() {
	if s == nil {
		return
	}
	if s.assists != nil {
		s.assists.Close()
	}
	if s.misses != nil {
		s.misses.Close()
	}
}

// InvalidateAssistants 清空助手配置缓存（Admin 修改 / 跨节点广播后调用）
func (s *Service) InvalidateAssistants() {
	if s == nil {
		return
	}
	if s.assists != nil {
		s.assists.InvalidatePrefix("")
	}
	if s.misses != nil {
		s.misses.InvalidatePrefix("")
	}
}

// Collection 项目独立 collection 名（服务端拼接，不接受调用方传入）
func (s *Service) Collection(slug string) string {
	return s.opts.CollectionPrefix + slug
}

// EmbedModel 入库时记录的 embedding 模型版本
func (s *Service) EmbedModel() string { return s.opts.EmbedModel }

// ResolveAssistant 按租户 + 对外模型名查助手配置（本地缓存 60s）
// 未配置返回 ErrAssistantNotFound；项目停用返回 ErrProjectInactive。
func (s *Service) ResolveAssistant(ctx context.Context, tenantID int64, modelName string) (*Assistant, error) {
	if s == nil || s.db == nil {
		return nil, ErrAssistantNotFound
	}
	key := strconv.FormatInt(tenantID, 10) + ":" + modelName
	if a, ok := s.assists.Get(key); ok {
		if a.ProjectStatus != "active" {
			return nil, ErrProjectInactive
		}
		return a, nil
	}
	if s.misses != nil {
		if _, ok := s.misses.Get(key); ok {
			return nil, ErrAssistantNotFound
		}
	}

	var a Assistant
	err := s.db.QueryRowContext(ctx, `
		SELECT a.id, a.tenant_id, a.project_id, a.model_name, COALESCE(a.system_prompt, ''),
		       a.top_k, a.score_threshold, a.max_context_tokens, p.slug, p.status
		FROM assistants a
		JOIN projects p ON p.id = a.project_id
		WHERE a.tenant_id = ? AND a.model_name = ? AND a.enabled = 1
	`, tenantID, modelName).Scan(
		&a.ID, &a.TenantID, &a.ProjectID, &a.ModelName, &a.SystemPrompt,
		&a.TopK, &a.ScoreThreshold, &a.MaxContextTokens, &a.ProjectSlug, &a.ProjectStatus,
	)
	if errors.Is(err, sql.ErrNoRows) {
		if s.misses != nil {
			s.misses.Set(key, true)
		}
		return nil, ErrAssistantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("rag: resolve assistant failed: %w", err)
	}
	// 参数兜底：DB 中为 0 的列回落到全局默认
	if a.TopK <= 0 {
		a.TopK = s.opts.TopK
	}
	if a.ScoreThreshold <= 0 {
		a.ScoreThreshold = s.opts.ScoreThreshold
	}
	if a.MaxContextTokens <= 0 {
		a.MaxContextTokens = s.opts.MaxContextTokens
	}
	if a.ProjectStatus != "active" {
		s.assists.Set(key, &a)
		return nil, ErrProjectInactive
	}
	s.assists.Set(key, &a)
	return &a, nil
}

// retrievedChunk 一条召回结果
type retrievedChunk struct {
	DocID   int64
	ChunkNo int
	Title   string
	Source  string
	Text    string
	Score   float64
}

// Assemble 检索并把资料拼进消息列表。
//
// 检索失败 / 超时 / 未配置向量层一律降级为"无资料回答"（Fallback=true），
// 只有配置性错误才返回 error，调用方永远不需要因为检索失败向客户端返回 5xx。
func (s *Service) Assemble(ctx context.Context, asst *Assistant, messages []provider.Message) (*Context, error) {
	out := &Context{Messages: append([]provider.Message(nil), messages...)}
	if s == nil || asst == nil {
		return out, nil
	}

	var chunks []retrievedChunk
	query := lastUserContent(messages)
	if query != "" && s.embed != nil && s.index != nil && asst.ProjectID > 0 && asst.ProjectSlug != "" {
		start := time.Now()
		var err error
		chunks, err = s.retrieve(ctx, asst, query)
		out.Elapsed = time.Since(start)
		observability.RecordRAGRetrieval(asst.ProjectID, out.Elapsed.Milliseconds())
		if err != nil {
			out.Fallback = true
			observability.RecordRAGFallback(asst.ProjectID)
			observability.L().Warn("rag: retrieval failed, fallback to no-context",
				zap.Int64("project_id", asst.ProjectID),
				zap.String("model", asst.ModelName),
				zap.Error(err),
			)
			chunks = nil
		}
	}
	if len(chunks) == 0 {
		observability.RecordRAGEmpty(asst.ProjectID)
	}

	out.RetrievedCount, out.Citations = len(chunks), make([]Citation, 0, len(chunks))
	system := buildSystemMessage(asst, chunks, asst.MaxContextTokens, &out.Citations)
	out.RetrievedCount = len(out.Citations)
	if system != "" {
		out.Messages = append([]provider.Message{{Role: "system", Content: system}}, out.Messages...)
	}
	return out, nil
}

// retrieve 向量检索（硬超时），并按项目 + slug 隔离
func (s *Service) retrieve(ctx context.Context, asst *Assistant, query string) ([]retrievedChunk, error) {
	rctx, cancel := context.WithTimeout(ctx, s.opts.RetrievalTimeout)
	defer cancel()

	vec, err := s.embed.Embed(rctx, query)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	filter := map[string]any{"must": []map[string]any{
		{"key": "project_id", "match": map[string]any{"value": asst.ProjectID}},
	}}
	points, err := s.index.Search(rctx, s.Collection(asst.ProjectSlug), vec,
		asst.TopK, asst.ScoreThreshold, filter)
	if err != nil {
		return nil, fmt.Errorf("vector search: %w", err)
	}

	// 同文档相邻 chunk 去重：高分的先入，相邻编号视为重复上下文
	included := make(map[int64]map[int]bool)
	out := make([]retrievedChunk, 0, len(points))
	for _, p := range points {
		text := payloadString(p.Payload, "text")
		if text == "" {
			continue
		}
		c := retrievedChunk{
			DocID:   payloadInt64(p.Payload, "doc_id"),
			ChunkNo: int(payloadInt64(p.Payload, "chunk_no")),
			Title:   payloadString(p.Payload, "title"),
			Source:  payloadString(p.Payload, "source"),
			Text:    text,
			Score:   p.Score,
		}
		if c.DocID <= 0 {
			continue
		}
		if included[c.DocID] == nil {
			included[c.DocID] = make(map[int]bool)
		}
		if included[c.DocID][c.ChunkNo] ||
			included[c.DocID][c.ChunkNo-1] || included[c.DocID][c.ChunkNo+1] {
			continue
		}
		included[c.DocID][c.ChunkNo] = true
		out = append(out, c)
	}
	return out, nil
}

// buildSystemMessage 拼 system 消息：提示词 + 编号资料 + 回答约束；超出 token 预算按分数截断
func buildSystemMessage(asst *Assistant, chunks []retrievedChunk, maxTokens int, citations *[]Citation) string {
	if len(chunks) == 0 {
		if asst.SystemPrompt == "" {
			return ""
		}
		return asst.SystemPrompt + "\n\n知识库未检索到相关资料；若无法确定，请明确说明「资料中未找到相关内容」。"
	}

	remaining := maxTokens
	var b []rune
	if asst.SystemPrompt != "" {
		b = append(b, []rune(asst.SystemPrompt)...)
		b = append(b, []rune("\n\n")...)
		remaining -= runeLen(asst.SystemPrompt)
	}
	b = append(b, []rune("以下是从知识库检索到的资料，引用时请标注编号（如 [1]）：\n\n")...)

	for _, c := range chunks {
		idx := len(*citations) + 1
		header := fmt.Sprintf("[%d] 《%s》（%s）\n", idx, c.Title, c.Source)
		text := c.Text
		cost := runeLen(header) + runeLen(text)
		if cost > remaining {
			if len(*citations) == 0 && remaining > runeLen(header)+40 {
				text = string([]rune(text)[:remaining-runeLen(header)])
				cost = remaining
			} else {
				continue // 预算不足：按分数顺序截断
			}
		}
		*citations = append(*citations, Citation{
			Index: idx, DocID: c.DocID, Title: c.Title, Source: c.Source,
		})
		b = append(b, []rune(header)...)
		b = append(b, []rune(text)...)
		b = append(b, []rune("\n\n")...)
		remaining -= cost
		if remaining <= 0 {
			break
		}
	}
	if len(*citations) == 0 {
		// 一条都放不下：仍然给出提示词 + 约束（不塞半截资料）
		if asst.SystemPrompt == "" {
			return ""
		}
		return asst.SystemPrompt + "\n\n知识库资料超出上下文预算；若无法确定，请明确说明「资料中未找到相关内容」。"
	}
	b = append(b, []rune("回答要求：只依据上述资料回答；资料不足时明确说明「资料中未找到相关内容」；引用资料必须标注编号。")...)
	return string(b)
}

// lastUserContent 取最后一条 user 消息作为检索 query
func lastUserContent(messages []provider.Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == "user" && messages[i].Content != "" {
			return messages[i].Content
		}
	}
	return ""
}

func runeLen(s string) int { return len([]rune(s)) }

func payloadString(p map[string]any, key string) string {
	if v, ok := p[key].(string); ok {
		return v
	}
	return ""
}

func payloadInt64(p map[string]any, key string) int64 {
	switch v := p[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	default:
		return 0
	}
}
