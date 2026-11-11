package vector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// EmbeddingClient：批量请求体 + 按 index 乱序返回也能对齐入参顺序
func TestEmbedBatchOrderAndAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
		}
		var req struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Model != "emb-test" || len(req.Input) != 2 {
			t.Errorf("请求体异常: %+v", req)
		}
		// 故意乱序返回，验证按 index 对齐
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"index": 1, "embedding": []float64{0, 2}},
				{"index": 0, "embedding": []float64{1, 0}},
			},
		})
	}))
	defer server.Close()

	cli := NewEmbeddingClient(server.URL, "emb-test", "test-key")
	vecs, err := cli.EmbedBatch(context.Background(), []string{"a", "b"})
	if err != nil {
		t.Fatalf("EmbedBatch: %v", err)
	}
	if len(vecs) != 2 || vecs[0][0] != 1 || vecs[1][1] != 2 {
		t.Fatalf("批量结果与入参顺序不一致: %+v", vecs)
	}
}

// EmbeddingClient：数量不一致直接报错（避免 chunk 与向量错位）
func TestEmbedBatchSizeMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"index": 0, "embedding": []float64{1}}},
		})
	}))
	defer server.Close()

	cli := NewEmbeddingClient(server.URL, "emb", "")
	if _, err := cli.EmbedBatch(context.Background(), []string{"a", "b"}); err == nil {
		t.Fatal("数量不一致应返回错误")
	}
}

// QdrantClient：建 collection / 批量 upsert / 带 filter 检索 / 按 filter 删除
func TestQdrantClientContract(t *testing.T) {
	var upsertBody struct {
		Points []struct {
			ID      string         `json:"id"`
			Vector  []float32      `json:"vector"`
			Payload map[string]any `json:"payload"`
		} `json:"points"`
	}
	var deleted bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/collections/kb_ops":
			_, _ = w.Write([]byte(`{"result":true}`))
		case r.Method == http.MethodPut && r.URL.Path == "/collections/kb_ops/points":
			if r.URL.Query().Get("wait") != "true" {
				t.Errorf("upsert 应同步等待 wait=true")
			}
			_ = json.NewDecoder(r.Body).Decode(&upsertBody)
			_, _ = w.Write([]byte(`{"result":{}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/collections/kb_ops/points/search":
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req["limit"] != float64(3) {
				t.Errorf("limit = %v, want 3", req["limit"])
			}
			if _, ok := req["filter"]; !ok {
				t.Error("检索必须携带 filter（项目隔离双保险）")
			}
			_, _ = w.Write([]byte(`{"result":[{"id":"p1","score":0.91,"payload":{"doc_id":12,"text":"磁盘处理"}}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/collections/kb_ops/points/delete":
			var req struct {
				Filter map[string]any `json:"filter"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.Filter == nil {
				t.Error("删除必须带 filter")
			}
			deleted = true
			_, _ = w.Write([]byte(`{"result":{}}`))
		default:
			t.Errorf("未预期请求: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cli := NewQdrantClient(server.URL)
	ctx := context.Background()
	points := []Point{{
		ID:     PointID("12:0"),
		Vector: []float32{1, 0},
		Payload: map[string]any{
			"project_id": 7, "doc_id": 12, "text": "磁盘处理",
		},
	}}
	if err := cli.UpsertPoints(ctx, "kb_ops", points); err != nil {
		t.Fatalf("UpsertPoints: %v", err)
	}
	if len(upsertBody.Points) != 1 || upsertBody.Points[0].Payload["doc_id"] != float64(12) {
		t.Errorf("upsert 请求体异常: %+v", upsertBody)
	}

	filter := map[string]any{"must": []map[string]any{
		{"key": "project_id", "match": map[string]any{"value": 7}},
	}}
	got, err := cli.Search(ctx, "kb_ops", []float32{1, 0}, 3, 0.3, filter)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != 1 || got[0].Score < 0.9 || got[0].Payload["text"] != "磁盘处理" {
		t.Errorf("Search 结果异常: %+v", got)
	}

	if err := cli.DeleteByFilter(ctx, "kb_ops", filter); err != nil {
		t.Fatalf("DeleteByFilter: %v", err)
	}
	if !deleted {
		t.Error("DeleteByFilter 未发出删除请求")
	}
}

// DeleteByFilter 必须显式给 filter，禁止误删整个 collection
func TestQdrantDeleteRequiresFilter(t *testing.T) {
	cli := NewQdrantClient("http://127.0.0.1:1")
	if err := cli.DeleteByFilter(context.Background(), "kb_x", nil); err == nil {
		t.Fatal("缺 filter 应返回错误")
	}
}

// PointID 稳定且符合 UUID 格式
func TestPointIDStableUUID(t *testing.T) {
	id := PointID("12:0")
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		t.Fatalf("PointID 不是 UUID 格式: %s", id)
	}
	if id != PointID("12:0") {
		t.Error("PointID 应稳定（重复入库幂等覆盖）")
	}
	if id == PointID("12:1") {
		t.Error("不同 chunk 的 PointID 应不同")
	}
}
