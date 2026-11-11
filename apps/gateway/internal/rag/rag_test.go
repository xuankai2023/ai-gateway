package rag

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ai-gateway/pkg/dbtest"
	"ai-gateway/pkg/provider"
	"ai-gateway/pkg/vector"
)

// stubEmbedder 固定维度向量；可注入错误
type stubEmbedder struct {
	err error
}

func (s *stubEmbedder) Embed(context.Context, string) ([]float32, error) {
	if s.err != nil {
		return nil, s.err
	}
	return []float32{1, 0, 0}, nil
}

func (s *stubEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = []float32{1, 0, 0}
	}
	return out, nil
}

// newQdrantStub 起一个只处理 RAG 所需接口的 Qdrant stub
func newQdrantStub(t *testing.T, search func(body map[string]any) (int, string)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/points"):
			_, _ = w.Write([]byte(`{"result":{}}`))
		case r.Method == http.MethodPut:
			_, _ = w.Write([]byte(`{"result":true}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/points/search"):
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			code, resp := search(body)
			w.WriteHeader(code)
			_, _ = w.Write([]byte(resp))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/points/delete"):
			_, _ = w.Write([]byte(`{"result":{}}`))
		default:
			t.Errorf("未预期 Qdrant 请求: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func testAssistant() *Assistant {
	return &Assistant{
		ID: 3, TenantID: 1, ProjectID: 7, ModelName: "ops-assistant",
		SystemPrompt: "你是运维助手。", TopK: 5, ScoreThreshold: 0.2,
		MaxContextTokens: 1000, ProjectSlug: "ops", ProjectStatus: "active",
	}
}

// 检索命中：system 提示词 + 编号资料 + citations
func TestAssembleInjectsContextAndCitations(t *testing.T) {
	var gotFilter map[string]any
	srv := newQdrantStub(t, func(body map[string]any) (int, string) {
		gotFilter, _ = body["filter"].(map[string]any)
		return http.StatusOK, `{"result":[
			{"id":"p1","score":0.92,"payload":{"project_id":7,"doc_id":11,"chunk_no":0,"title":"磁盘告警","source":"ops/disk.md","text":"磁盘告警处理：清理日志"}},
			{"id":"p2","score":0.88,"payload":{"project_id":7,"doc_id":12,"chunk_no":0,"title":"内存告警","source":"ops/mem.md","text":"内存告警处理：释放缓存"}}
		]}`
	})
	svc := NewService(nil, &stubEmbedder{}, vector.NewQdrantClient(srv.URL), Options{})
	defer svc.Close()

	ctx, err := svc.Assemble(context.Background(), testAssistant(), []provider.Message{
		{Role: "user", Content: "磁盘满了怎么办"},
	})
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	if len(ctx.Messages) != 2 || ctx.Messages[0].Role != "system" {
		t.Fatalf("应插入一条 system 消息: %+v", ctx.Messages)
	}
	sys := ctx.Messages[0].Content
	for _, want := range []string{"你是运维助手", "[1] 《磁盘告警》", "[2] 《内存告警》", "引用资料必须标注编号"} {
		if !strings.Contains(sys, want) {
			t.Errorf("system 消息缺少 %q:\n%s", want, sys)
		}
	}
	if ctx.RetrievedCount != 2 || len(ctx.Citations) != 2 {
		t.Fatalf("召回/citations = %d/%d, want 2/2", ctx.RetrievedCount, len(ctx.Citations))
	}
	if ctx.Citations[0].DocID != 11 || ctx.Citations[0].Index != 1 {
		t.Errorf("citation[0] = %+v", ctx.Citations[0])
	}
	if ctx.Fallback {
		t.Error("正常检索不应标记 Fallback")
	}

	// 项目过滤必须下推到 Qdrant（collection 已隔离，这里是双保险）
	must, _ := gotFilter["must"].([]any)
	if len(must) != 1 {
		t.Fatalf("filter.must = %+v", gotFilter)
	}
	cond, _ := must[0].(map[string]any)
	if cond["key"] != "project_id" {
		t.Errorf("过滤字段 = %v, want project_id", cond["key"])
	}
}

// 同文档相邻 chunk 去重（避免同一段资料重复占预算）
func TestAssembleDedupAdjacentChunks(t *testing.T) {
	srv := newQdrantStub(t, func(map[string]any) (int, string) {
		return http.StatusOK, `{"result":[
			{"id":"p1","score":0.9,"payload":{"doc_id":11,"chunk_no":0,"title":"A","source":"a.md","text":"第一段"}},
			{"id":"p2","score":0.89,"payload":{"doc_id":11,"chunk_no":1,"title":"A","source":"a.md","text":"第二段（相邻，应去重）"}},
			{"id":"p3","score":0.8,"payload":{"doc_id":12,"chunk_no":0,"title":"B","source":"b.md","text":"另一文档"}}
		]}`
	})
	svc := NewService(nil, &stubEmbedder{}, vector.NewQdrantClient(srv.URL), Options{})
	defer svc.Close()

	ctx, _ := svc.Assemble(context.Background(), testAssistant(), []provider.Message{{Role: "user", Content: "问题"}})
	if len(ctx.Citations) != 2 {
		t.Fatalf("相邻 chunk 未去重: %+v", ctx.Citations)
	}
	if ctx.Citations[1].DocID != 12 {
		t.Errorf("第二篇应来自另一文档: %+v", ctx.Citations[1])
	}
}

// 检索失败降级：不返回 error、不塞资料，仍保留提示词
func TestAssembleFallbackOnSearchError(t *testing.T) {
	srv := newQdrantStub(t, func(map[string]any) (int, string) {
		return http.StatusInternalServerError, `{"status":"boom"}`
	})
	svc := NewService(nil, &stubEmbedder{}, vector.NewQdrantClient(srv.URL), Options{})
	defer svc.Close()

	ctx, err := svc.Assemble(context.Background(), testAssistant(), []provider.Message{{Role: "user", Content: "问题"}})
	if err != nil {
		t.Fatalf("检索失败不应向上返回错误: %v", err)
	}
	if !ctx.Fallback || ctx.RetrievedCount != 0 || len(ctx.Citations) != 0 {
		t.Fatalf("降级标记异常: %+v", ctx)
	}
	if len(ctx.Messages) != 2 || !strings.Contains(ctx.Messages[0].Content, "你是运维助手") {
		t.Fatalf("降级后仍应保留 system 提示词: %+v", ctx.Messages)
	}
	if !strings.Contains(ctx.Messages[0].Content, "资料中未找到相关内容") {
		t.Error("降级后应提示资料不足")
	}
}

// 无 user 消息：不检索，消息原样返回
func TestAssembleNoUserMessage(t *testing.T) {
	svc := NewService(nil, &stubEmbedder{}, vector.NewQdrantClient("http://127.0.0.1:1"), Options{})
	defer svc.Close()

	msgs := []provider.Message{{Role: "assistant", Content: "hi"}}
	ctx, err := svc.Assemble(context.Background(), &Assistant{ProjectID: 7, ProjectSlug: "ops"}, msgs)
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	if len(ctx.Messages) != 1 || ctx.Messages[0].Content != "hi" {
		t.Fatalf("无 user 消息不应改写 messages: %+v", ctx.Messages)
	}
}

// 检索为空：system 消息仍提示未找到资料
func TestAssembleEmptyResult(t *testing.T) {
	srv := newQdrantStub(t, func(map[string]any) (int, string) {
		return http.StatusOK, `{"result":[]}`
	})
	svc := NewService(nil, &stubEmbedder{}, vector.NewQdrantClient(srv.URL), Options{})
	defer svc.Close()

	ctx, _ := svc.Assemble(context.Background(), testAssistant(), []provider.Message{{Role: "user", Content: "问题"}})
	if ctx.RetrievedCount != 0 || ctx.Fallback {
		t.Fatalf("空结果不应标记 Fallback: %+v", ctx)
	}
	if !strings.Contains(ctx.Messages[0].Content, "知识库未检索到相关资料") {
		t.Errorf("空结果提示缺失: %s", ctx.Messages[0].Content)
	}
}

// ResolveAssistant：DB 查询 + 本地缓存 + 失效 + 错误分支
func TestResolveAssistantAndCache(t *testing.T) {
	db, fake := dbtest.New(t)
	fake.ExpectQuery("FROM assistants").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "model_name", "system_prompt",
			"top_k", "score_threshold", "max_context_tokens", "slug", "status"},
		[]driver.Value{int64(3), int64(1), int64(7), "ops-assistant", "提示词",
			int64(4), []byte("0.250"), int64(800), "ops", "active"},
	)
	svc := NewService(db, &stubEmbedder{}, nil, Options{})
	defer svc.Close()

	a, err := svc.ResolveAssistant(context.Background(), 1, "ops-assistant")
	if err != nil {
		t.Fatalf("ResolveAssistant: %v", err)
	}
	if a.ID != 3 || a.ProjectID != 7 || a.ProjectSlug != "ops" || a.TopK != 4 {
		t.Errorf("解析结果异常: %+v", a)
	}
	if a.ScoreThreshold != 0.25 {
		t.Errorf("score_threshold = %v, want 0.25", a.ScoreThreshold)
	}

	// 第二次命中本地缓存：不再查 DB（fake 无剩余期望 → 查了就会报错）
	if _, err := svc.ResolveAssistant(context.Background(), 1, "ops-assistant"); err != nil {
		t.Fatalf("缓存命中失败: %v", err)
	}

	// 失效后需重新查库
	svc.InvalidateAssistants()
	fake.ExpectQuery("FROM assistants").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "model_name", "system_prompt",
			"top_k", "score_threshold", "max_context_tokens", "slug", "status"},
		[]driver.Value{int64(3), int64(1), int64(7), "ops-assistant", "提示词2",
			int64(5), []byte("0.300"), int64(2000), "ops", "active"},
	)
	if _, err := svc.ResolveAssistant(context.Background(), 1, "ops-assistant"); err != nil {
		t.Fatalf("失效后重查失败: %v", err)
	}
	fake.AssertExpectations(t)
}

func TestResolveAssistantNotFound(t *testing.T) {
	db, fake := dbtest.New(t)
	fake.ExpectQuery("FROM assistants").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "model_name", "system_prompt",
			"top_k", "score_threshold", "max_context_tokens", "slug", "status"},
	)
	svc := NewService(db, &stubEmbedder{}, nil, Options{})
	defer svc.Close()

	if _, err := svc.ResolveAssistant(context.Background(), 1, "gpt-4o"); !errors.Is(err, ErrAssistantNotFound) {
		t.Fatalf("首次 err = %v, want ErrAssistantNotFound", err)
	}
	// 负缓存命中：第二次不再查库（fake 无剩余期望，查了即失败）
	if _, err := svc.ResolveAssistant(context.Background(), 1, "gpt-4o"); !errors.Is(err, ErrAssistantNotFound) {
		t.Fatalf("负缓存 err = %v, want ErrAssistantNotFound", err)
	}
	// 失效后重新查库
	svc.InvalidateAssistants()
	fake.ExpectQuery("FROM assistants").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "model_name", "system_prompt",
			"top_k", "score_threshold", "max_context_tokens", "slug", "status"},
	)
	if _, err := svc.ResolveAssistant(context.Background(), 1, "gpt-4o"); !errors.Is(err, ErrAssistantNotFound) {
		t.Fatalf("失效后 err = %v, want ErrAssistantNotFound", err)
	}
	fake.AssertExpectations(t)
}

func TestResolveAssistantProjectInactive(t *testing.T) {
	db, fake := dbtest.New(t)
	fake.ExpectQuery("FROM assistants").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "model_name", "system_prompt",
			"top_k", "score_threshold", "max_context_tokens", "slug", "status"},
		[]driver.Value{int64(3), int64(1), int64(7), "ops-assistant", "",
			int64(5), []byte("0.300"), int64(2000), "ops", "disabled"},
	)
	svc := NewService(db, &stubEmbedder{}, nil, Options{})
	defer svc.Close()

	_, err := svc.ResolveAssistant(context.Background(), 1, "ops-assistant")
	if !errors.Is(err, ErrProjectInactive) {
		t.Fatalf("err = %v, want ErrProjectInactive", err)
	}
}

// Ingest：新文档 → 建 collection / 删旧点 / 写点 / 更新元数据 / epoch +1
func TestIngestNewDocument(t *testing.T) {
	var upsertPayload map[string]any
	var deleted bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/points"):
			var body struct {
				Points []struct {
					ID      string         `json:"id"`
					Vector  []float32      `json:"vector"`
					Payload map[string]any `json:"payload"`
				} `json:"points"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if len(body.Points) > 0 {
				upsertPayload = body.Points[0].Payload
			}
			_, _ = w.Write([]byte(`{"result":{}}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/points/delete"):
			deleted = true
			_, _ = w.Write([]byte(`{"result":{}}`))
		case r.Method == http.MethodPut:
			_, _ = w.Write([]byte(`{"result":true}`))
		default:
			t.Errorf("未预期请求: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	db, fake := dbtest.New(t)
	fake.ExpectQuery("FROM projects").WillReturnRows(
		[]string{"id", "tenant_id", "status"}, []driver.Value{int64(5), int64(1), "active"},
	)
	fake.ExpectQuery("FROM documents").WillReturnRows([]string{"id", "checksum", "status"})
	fake.ExpectExec("INSERT INTO documents").WillReturnResult(77, 1)
	fake.ExpectExec("UPDATE documents").WillReturnResult(0, 1)
	fake.ExpectExec("index_epoch = index_epoch + 1").WillReturnResult(0, 1)

	svc := NewService(db, &stubEmbedder{}, vector.NewQdrantClient(srv.URL), Options{ChunkSize: 200})
	defer svc.Close()

	content := "# 手册\n\n## 磁盘告警\n\n处理步骤：清理日志。\n\n## 内存告警\n\n处理步骤：释放缓存。"
	res, err := svc.Ingest(context.Background(), IngestRequest{
		TenantID: 1, ProjectSlug: "ops", DocKey: "ops/manual.md", Title: "运维手册", Content: content,
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.DocID != 77 || res.ChunkCount != 2 || res.Skipped {
		t.Fatalf("入库结果异常: %+v", res)
	}
	if !deleted {
		t.Error("重建索引前应先按 doc_id 删除旧点")
	}
	if upsertPayload["project_id"] != float64(5) || upsertPayload["doc_id"] != float64(77) {
		t.Errorf("point payload 隔离字段异常: %+v", upsertPayload)
	}
	if !strings.Contains(upsertPayload["text"].(string), "磁盘告警") {
		t.Errorf("point text 缺少章节内容: %v", upsertPayload["text"])
	}
	fake.AssertExpectations(t)
}

// Ingest：checksum 未变 → 跳过（不产生任何写入）
func TestIngestSkipsUnchangedDocument(t *testing.T) {
	db, fake := dbtest.New(t)
	fake.ExpectQuery("FROM projects").WillReturnRows(
		[]string{"id", "tenant_id", "status"}, []driver.Value{int64(5), int64(1), "active"},
	)
	content := "不变的内容"
	fake.ExpectQuery("FROM documents").WillReturnRows(
		[]string{"id", "checksum", "status"}, []driver.Value{int64(77), checksum(content), "indexed"},
	)
	svc := NewService(db, &stubEmbedder{}, vector.NewQdrantClient("http://127.0.0.1:1"), Options{})
	defer svc.Close()

	res, err := svc.Ingest(context.Background(), IngestRequest{
		TenantID: 1, ProjectSlug: "ops", DocKey: "a.md", Content: content,
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if !res.Skipped || res.DocID != 77 {
		t.Fatalf("应跳过未变化文档: %+v", res)
	}
	fake.AssertExpectations(t)
}

// Ingest：embedding 失败 → 文档标记 failed 并返回错误
func TestIngestFailureMarksDocumentFailed(t *testing.T) {
	db, fake := dbtest.New(t)
	fake.ExpectQuery("FROM projects").WillReturnRows(
		[]string{"id", "tenant_id", "status"}, []driver.Value{int64(5), int64(1), "active"},
	)
	fake.ExpectQuery("FROM documents").WillReturnRows([]string{"id", "checksum", "status"})
	fake.ExpectExec("INSERT INTO documents").WillReturnResult(88, 1)
	fake.ExpectExec("SET status = 'failed'").WillReturnResult(0, 1)

	svc := NewService(db, &stubEmbedder{err: errors.New("embedding down")},
		vector.NewQdrantClient("http://127.0.0.1:1"), Options{})
	defer svc.Close()

	_, err := svc.Ingest(context.Background(), IngestRequest{
		TenantID: 1, ProjectSlug: "ops", DocKey: "a.md", Content: "内容",
	})
	if err == nil {
		t.Fatal("embedding 失败应返回错误")
	}
	fake.AssertExpectations(t)
}

// 项目归属校验：租户不匹配直接拒绝
func TestIngestRejectsForeignTenant(t *testing.T) {
	db, fake := dbtest.New(t)
	fake.ExpectQuery("FROM projects").WillReturnRows(
		[]string{"id", "tenant_id", "status"}, []driver.Value{int64(5), int64(1), "active"},
	)
	svc := NewService(db, &stubEmbedder{}, vector.NewQdrantClient("http://127.0.0.1:1"), Options{})
	defer svc.Close()

	_, err := svc.Ingest(context.Background(), IngestRequest{
		TenantID: 2, ProjectSlug: "ops", DocKey: "a.md", Content: "内容",
	})
	if err == nil || !strings.Contains(err.Error(), "does not belong") {
		t.Fatalf("err = %v, want tenant mismatch", err)
	}
}
