package vector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Point 一条待写入的向量点
type Point struct {
	ID      string
	Vector  []float32
	Payload map[string]any
}

// ScoredPoint 一条检索结果
type ScoredPoint struct {
	ID      string
	Score   float64
	Payload map[string]any
}

// QdrantClient Qdrant v1 REST 客户端
type QdrantClient struct {
	baseURL string
	http    *http.Client

	mu      sync.Mutex
	ensured map[string]int // collection -> dim
}

// NewQdrantClient 创建 Qdrant 客户端；addr 可带或不带 http:// 前缀
func NewQdrantClient(addr string) *QdrantClient {
	base := strings.TrimRight(addr, "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	return &QdrantClient{
		baseURL: base,
		http:    &http.Client{Timeout: 3 * time.Second},
		ensured: make(map[string]int),
	}
}

// EnsureCollection 首次写入/查询前建 collection（余弦距离）；已存在视为成功
func (q *QdrantClient) EnsureCollection(ctx context.Context, name string, dim int) error {
	return q.ensureCollection(ctx, name, dim)
}

// ensureCollection 带进程内缓存：同一 collection 同维度只发一次 PUT
func (q *QdrantClient) ensureCollection(ctx context.Context, name string, dim int) error {
	q.mu.Lock()
	if known, ok := q.ensured[name]; ok && known == dim {
		q.mu.Unlock()
		return nil
	}
	q.mu.Unlock()

	body, _ := json.Marshal(map[string]any{
		"vectors": map[string]any{"size": dim, "distance": "Cosine"},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, q.baseURL+"/collections/"+name, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := q.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	// 200 = 新建成功；409 = 已存在（维度一致由调用方约定）
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("qdrant ensure collection %s: http %d: %s", name, resp.StatusCode, string(raw))
	}

	q.mu.Lock()
	q.ensured[name] = dim
	q.mu.Unlock()
	return nil
}

// UpsertPoints 批量写入/覆盖向量点（同 id 幂等覆盖）
func (q *QdrantClient) UpsertPoints(ctx context.Context, name string, points []Point) error {
	if len(points) == 0 {
		return nil
	}
	if err := q.ensureCollection(ctx, name, len(points[0].Vector)); err != nil {
		return err
	}
	payload := make([]map[string]any, 0, len(points))
	for _, p := range points {
		payload = append(payload, map[string]any{
			"id":      p.ID,
			"vector":  p.Vector,
			"payload": p.Payload,
		})
	}
	body, _ := json.Marshal(map[string]any{"points": payload})
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		q.baseURL+"/collections/"+name+"/points?wait=true", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := q.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("qdrant upsert http %d: %s", resp.StatusCode, string(raw))
	}
	return nil
}

// Search 相似检索；filter 为 Qdrant 原生过滤结构（nil = 不过滤）
func (q *QdrantClient) Search(ctx context.Context, name string, vector []float32, limit int,
	threshold float64, filter map[string]any) ([]ScoredPoint, error) {

	if limit <= 0 {
		limit = 5
	}
	if err := q.ensureCollection(ctx, name, len(vector)); err != nil {
		return nil, err
	}
	reqBody := map[string]any{
		"vector":       vector,
		"limit":        limit,
		"with_payload": true,
	}
	if threshold > 0 {
		reqBody["score_threshold"] = threshold
	}
	if filter != nil {
		reqBody["filter"] = filter
	}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		q.baseURL+"/collections/"+name+"/points/search", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := q.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("qdrant search http %d: %s", resp.StatusCode, string(raw))
	}
	var out struct {
		Result []struct {
			ID      any            `json:"id"`
			Score   float64        `json:"score"`
			Payload map[string]any `json:"payload"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("qdrant search decode failed: %w", err)
	}
	points := make([]ScoredPoint, 0, len(out.Result))
	for _, r := range out.Result {
		points = append(points, ScoredPoint{
			ID:      fmt.Sprint(r.ID),
			Score:   r.Score,
			Payload: r.Payload,
		})
	}
	return points, nil
}

// DeleteByFilter 按 payload 条件删除向量点
func (q *QdrantClient) DeleteByFilter(ctx context.Context, name string, filter map[string]any) error {
	if filter == nil {
		return fmt.Errorf("qdrant delete: filter is required")
	}
	body, _ := json.Marshal(map[string]any{"filter": filter})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		q.baseURL+"/collections/"+name+"/points/delete?wait=true", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := q.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("qdrant delete http %d: %s", resp.StatusCode, string(raw))
	}
	return nil
}

// PointID 由业务唯一键派生稳定 UUID（Qdrant point id 只接受 UUID / uint64）
// 同一 key 重复入库得到同一 id，天然幂等覆盖
func PointID(key string) string {
	sum := sha256.Sum256([]byte(key))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
