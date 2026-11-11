package handler

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ai-gateway/apps/gateway/internal/app"
	"ai-gateway/apps/gateway/internal/rag"
	"ai-gateway/apps/gateway/internal/routing"
	"ai-gateway/pkg/dbtest"
	"ai-gateway/pkg/provider"
	"ai-gateway/pkg/vector"
	"github.com/gin-gonic/gin"
)

// handlerEmbedder 固定向量（handler 测试不关心向量值）
type handlerEmbedder struct{}

func (handlerEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}

func (handlerEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = []float32{1, 0}
	}
	return out, nil
}

// captureProvider 记录 relay 实际发出的 messages（验证 RAG 注入）
type captureProvider struct {
	response *provider.ChatResponse
	gotReq   *provider.ChatRequest
}

func (p *captureProvider) ChatCompletion(_ context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
	p.gotReq = req
	return p.response, nil
}

func (p *captureProvider) Embedding(context.Context, *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (p *captureProvider) Moderation(context.Context, *provider.ModerationRequest) (*provider.ModerationResponse, error) {
	return nil, provider.ErrNotImplemented
}

func (p *captureProvider) Cost(string, int64, int64) (float64, error) {
	return 0, provider.ErrNotImplemented
}

func (p *captureProvider) Name() string { return "fake-rag" }

// newRAGApp 构造带 RAG 服务的 handler App；assistant 期望值由调用方注册
func newRAGApp(t *testing.T, fakeQuery func(*dbtest.FakeDB), indexURL string) (*app.App, *provider.Registry) {
	t.Helper()
	a, registry := newHandlerTestApp(t, resolveFunc(func(context.Context, int64, string, int) ([]routing.Ability, error) {
		return []routing.Ability{{ID: 0, Provider: "fake-rag", ProviderModel: "upstream-model"}}, nil
	}), nil)
	if fakeQuery != nil {
		db, fake := dbtest.New(t)
		a.DB = db
		fakeQuery(fake)
		a.RAG = rag.NewService(db, handlerEmbedder{}, vector.NewQdrantClient(indexURL), rag.Options{})
		t.Cleanup(a.RAG.Close)
	}
	return a, registry
}

// assistantRow 助手查询返回行
func assistantRow(fake *dbtest.FakeDB) {
	fake.ExpectQuery("FROM assistants").WillReturnRows(
		[]string{"id", "tenant_id", "project_id", "model_name", "system_prompt",
			"top_k", "score_threshold", "max_context_tokens", "slug", "status"},
		[]driver.Value{int64(3), int64(1), int64(7), "test-model", "你是运维助手",
			int64(5), []byte("0.200"), int64(1000), "ops", "active"},
	)
}

func doRAGChat(t *testing.T, r *gin.Engine, stream bool) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"model":    "test-model",
		"messages": []map[string]string{{"role": "user", "content": "磁盘满了"}},
		"stream":   stream,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// 助手模型但 API Key 未绑定项目：403 project_required，不调上游
func TestChatCompletion_RAGProjectRequired(t *testing.T) {
	prov := &captureProvider{response: &provider.ChatResponse{Content: "ok"}}
	a, registry := newRAGApp(t, assistantRow, "http://127.0.0.1:1")
	registerFake(registry, "fake-rag", prov)

	w := doRAGChat(t, mountChat(a), false)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "project_required") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if prov.gotReq != nil {
		t.Error("项目校验失败时不应调用上游")
	}
}

// 项目不匹配：403 project_scope_mismatch（禁止跨项目读知识库）
func TestChatCompletion_RAGProjectMismatch(t *testing.T) {
	prov := &captureProvider{response: &provider.ChatResponse{Content: "ok"}}
	a, registry := newRAGApp(t, assistantRow, "http://127.0.0.1:1")
	registerFake(registry, "fake-rag", prov)

	w := doRAGChat(t, mountChat(a, func(c *gin.Context) { c.Set("project_id", int64(99)) }), false)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "project_scope_mismatch") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if prov.gotReq != nil {
		t.Error("跨项目请求不应调用上游")
	}
}

// 命中助手：system 提示词 + 资料注入 relay 请求，响应带 citations
func TestChatCompletion_RAGInjectsContextAndCitations(t *testing.T) {
	index := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			_, _ = w.Write([]byte(`{"result":true}`))
		case strings.HasSuffix(r.URL.Path, "/points/search"):
			_, _ = w.Write([]byte(`{"result":[
				{"id":"p1","score":0.95,"payload":{"doc_id":11,"chunk_no":0,"title":"磁盘告警","source":"ops/disk.md","text":"清理日志"}}
			]}`))
		default:
			t.Errorf("未预期请求: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer index.Close()

	prov := &captureProvider{response: &provider.ChatResponse{Content: "先清理日志", InputTokens: 10, OutputTokens: 4}}
	a, registry := newRAGApp(t, assistantRow, index.URL)
	registerFake(registry, "fake-rag", prov)

	w := doRAGChat(t, mountChat(a, func(c *gin.Context) { c.Set("project_id", int64(7)) }), false)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if prov.gotReq == nil || len(prov.gotReq.Messages) < 2 {
		t.Fatalf("relay 请求未注入 system 消息: %+v", prov.gotReq)
	}
	if prov.gotReq.Messages[0].Role != "system" || !strings.Contains(prov.gotReq.Messages[0].Content, "你是运维助手") {
		t.Errorf("system 消息异常: %+v", prov.gotReq.Messages[0])
	}
	if !strings.Contains(prov.gotReq.Messages[0].Content, "《磁盘告警》") {
		t.Errorf("system 消息缺少召回资料: %s", prov.gotReq.Messages[0].Content)
	}

	var resp struct {
		Choices []struct {
			Message struct {
				Content   string         `json:"content"`
				Citations []rag.Citation `json:"citations"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("响应不是合法 JSON: %v", err)
	}
	if len(resp.Choices) != 1 || len(resp.Choices[0].Message.Citations) != 1 ||
		resp.Choices[0].Message.Citations[0].DocID != 11 {
		t.Fatalf("citations 未返回: %s", w.Body.String())
	}
}

// 未命中助手（普通模型）：RAG 开启也不影响正常转发
func TestChatCompletion_RAGPlainModelUnaffected(t *testing.T) {
	prov := &captureProvider{response: &provider.ChatResponse{Content: "plain"}}
	a, registry := newRAGApp(t, func(fake *dbtest.FakeDB) {
		fake.ExpectQuery("FROM assistants").WillReturnRows(
			[]string{"id", "tenant_id", "project_id", "model_name", "system_prompt",
				"top_k", "score_threshold", "max_context_tokens", "slug", "status"},
		)
	}, "http://127.0.0.1:1")
	registerFake(registry, "fake-rag", prov)

	w := doRAGChat(t, mountChat(a), false)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "plain") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if prov.gotReq == nil || len(prov.gotReq.Messages) != 1 {
		t.Errorf("普通模型不应注入 system 消息: %+v", prov.gotReq)
	}
}

// 流式 citations 只挂在 finish_reason=stop 的最后一个 chunk 上
func TestBuildStreamFrameCitationsOnlyOnDone(t *testing.T) {
	citations := []rag.Citation{{Index: 1, DocID: 11, Title: "磁盘告警"}}
	normal := buildStreamFrame("id", 1, "m", provider.StreamChunk{Content: "片段"}, citations)
	if strings.Contains(normal, "citations") {
		t.Errorf("普通 chunk 不应带 citations: %s", normal)
	}
	done := buildStreamFrame("id", 1, "m", provider.StreamChunk{IsDone: true}, citations)
	if !strings.Contains(done, "citations") || !strings.Contains(done, `"finish_reason":"stop"`) {
		t.Errorf("结束 chunk 应带 citations: %s", done)
	}
}
