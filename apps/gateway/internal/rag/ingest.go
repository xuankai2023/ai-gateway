package rag

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"ai-gateway/pkg/vector"
)

// IngestRequest 一次文档入库请求（CLI / 后台任务调用）
type IngestRequest struct {
	TenantID    int64  // >0 时校验项目归属，0 表示不校验（内部任务）
	ProjectSlug string // projects.slug（决定 collection）
	DocKey      string // 源唯一键：路径 / URL / 工单号
	Title       string
	SourceType  string
	Content     string
}

// IngestResult 入库结果
type IngestResult struct {
	DocID      int64
	ChunkCount int
	// Skipped checksum 未变，跳过了重建索引
	Skipped bool
}

// Ingest 文档入库：同步 documents 元数据 + 重建该文档向量点
//
// 幂等：documents(project_id, doc_key) 唯一；point id 由 doc_id + chunk_no 派生，
// 重建前先按 doc_id 删除旧点，避免残留脏数据。成功后将 projects.index_epoch + 1，
// 使语义缓存中的旧答案在 60s 内失效。
func (s *Service) Ingest(ctx context.Context, req IngestRequest) (*IngestResult, error) {
	if s == nil || s.db == nil || s.embed == nil || s.index == nil {
		return nil, errors.New("rag: ingest requires db + embedder + qdrant")
	}
	req.ProjectSlug = strings.TrimSpace(req.ProjectSlug)
	req.DocKey = strings.TrimSpace(req.DocKey)
	req.Title = strings.TrimSpace(req.Title)
	req.SourceType = strings.TrimSpace(req.SourceType)
	if req.ProjectSlug == "" || req.DocKey == "" {
		return nil, errors.New("rag: project slug and doc key are required")
	}
	if strings.TrimSpace(req.Content) == "" {
		return nil, errors.New("rag: content is empty")
	}
	if req.Title == "" {
		req.Title = req.DocKey
	}
	if req.SourceType == "" {
		req.SourceType = "md"
	}

	var projectID, tenantID int64
	var status string
	err := s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, status FROM projects WHERE slug = ?`, req.ProjectSlug).
		Scan(&projectID, &tenantID, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("rag: project %q not found", req.ProjectSlug)
	}
	if err != nil {
		return nil, fmt.Errorf("rag: query project failed: %w", err)
	}
	if status != "active" {
		return nil, ErrProjectInactive
	}
	if req.TenantID > 0 && req.TenantID != tenantID {
		return nil, fmt.Errorf("rag: project %q does not belong to tenant %d", req.ProjectSlug, req.TenantID)
	}

	sum := checksum(req.Content)
	docID, skipped, err := s.upsertDocument(ctx, projectID, req, sum)
	if err != nil {
		return nil, err
	}
	if skipped {
		return &IngestResult{DocID: docID, Skipped: true}, nil
	}

	// 失败兜底：任何后续步骤出错都把文档标记为 failed，避免永远 pending
	ok := false
	defer func() {
		if !ok {
			_, _ = s.db.ExecContext(ctx,
				`UPDATE documents SET status = 'failed' WHERE id = ?`, docID)
		}
	}()

	chunks := SplitMarkdown(req.Content, req.Title, s.opts.ChunkSize, s.opts.ChunkOverlap)
	if len(chunks) == 0 {
		return nil, errors.New("rag: no chunk produced")
	}

	texts := make([]string, len(chunks))
	for i, c := range chunks {
		texts[i] = c.Text
	}
	vectors, err := s.embed.EmbedBatch(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("rag: embed chunks failed: %w", err)
	}
	if len(vectors) != len(chunks) {
		return nil, errors.New("rag: embedding count mismatch")
	}

	collection := s.Collection(req.ProjectSlug)
	// 先删旧点再写新点：文档变短时避免残留历史 chunk
	if err := s.index.DeleteByFilter(ctx, collection, docFilter(docID)); err != nil {
		return nil, fmt.Errorf("rag: delete old points failed: %w", err)
	}

	points := make([]vector.Point, 0, len(chunks))
	for i, c := range chunks {
		points = append(points, vector.Point{
			ID:     vector.PointID(fmt.Sprintf("%d:%d", docID, c.Index)),
			Vector: vectors[i],
			Payload: map[string]any{
				"project_id": projectID,
				"doc_id":     docID,
				"chunk_no":   c.Index,
				"title":      req.Title,
				"source":     req.DocKey,
				"section":    c.Section,
				"text":       c.Text,
			},
		})
	}
	if err := s.index.UpsertPoints(ctx, collection, points); err != nil {
		return nil, fmt.Errorf("rag: upsert points failed: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		UPDATE documents
		SET title = ?, source_type = ?, checksum = ?, chunk_count = ?, embed_model = ?,
		    status = 'indexed', indexed_at = ?
		WHERE id = ?`,
		req.Title, req.SourceType, sum, len(chunks), s.opts.EmbedModel, time.Now(), docID); err != nil {
		return nil, fmt.Errorf("rag: update document failed: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`UPDATE projects SET index_epoch = index_epoch + 1 WHERE id = ?`, projectID); err != nil {
		return nil, fmt.Errorf("rag: bump project epoch failed: %w", err)
	}
	ok = true
	return &IngestResult{DocID: docID, ChunkCount: len(chunks)}, nil
}

// upsertDocument 写入/更新文档元数据；checksum 未变且已索引时返回 skipped
func (s *Service) upsertDocument(ctx context.Context, projectID int64,
	req IngestRequest, sum string) (docID int64, skipped bool, err error) {

	var oldSum, oldStatus string
	err = s.db.QueryRowContext(ctx,
		`SELECT id, checksum, status FROM documents WHERE project_id = ? AND doc_key = ?`,
		projectID, req.DocKey).Scan(&docID, &oldSum, &oldStatus)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		res, ierr := s.db.ExecContext(ctx, `
			INSERT INTO documents (project_id, doc_key, title, source_type, checksum, embed_model, status)
			VALUES (?, ?, ?, ?, ?, ?, 'pending')`,
			projectID, req.DocKey, req.Title, req.SourceType, sum, s.opts.EmbedModel)
		if ierr != nil {
			return 0, false, fmt.Errorf("rag: insert document failed: %w", ierr)
		}
		docID, ierr = res.LastInsertId()
		if ierr != nil {
			return 0, false, fmt.Errorf("rag: insert document id failed: %w", ierr)
		}
		return docID, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("rag: query document failed: %w", err)
	}

	if oldSum == sum && oldStatus == "indexed" {
		return docID, true, nil
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE documents SET title = ?, source_type = ?, checksum = ?, embed_model = ?, status = 'pending'
		WHERE id = ?`,
		req.Title, req.SourceType, sum, s.opts.EmbedModel, docID); err != nil {
		return 0, false, fmt.Errorf("rag: update document failed: %w", err)
	}
	return docID, false, nil
}

// docFilter 按 doc_id 精确匹配的 Qdrant 过滤条件
func docFilter(docID int64) map[string]any {
	return map[string]any{"must": []map[string]any{
		{"key": "doc_id", "match": map[string]any{"value": docID}},
	}}
}

// checksum 内容 SHA256（十六进制）
func checksum(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}
