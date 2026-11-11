package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeConfig 在临时目录写一份配置，返回路径
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("写入临时配置失败: %v", err)
	}
	return path
}

func TestLoadFull(t *testing.T) {
	path := writeConfig(t, `
etcd:
  endpoints: ["localhost:2379"]
  dial_timeout: "5s"
mysql:
  dsn: "root:root@tcp(localhost:3306)/ai_gateway?charset=utf8mb4&parseTime=True&loc=Local"
  pool_size: 30
redis:
  addr: "localhost:6379"
  password: ""
kafka:
  brokers: ["localhost:9092"]
  group_id: "ai-gateway"
qdrant:
  addr: "localhost:6333"
gateway:
  listen_addr: ":9090"
  listen_metrics: ":9091"
  default_rps_limit: 10
  default_tpm_limit: 10000
providers:
  - type: openai
    base_url: "https://api.openai.com/v1"
    api_key_env: "OPENAI_API_KEY"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}

	if got, want := cfg.MySQL.DSN, "root:root@tcp(localhost:3306)/ai_gateway?charset=utf8mb4&parseTime=True&loc=Local"; got != want {
		t.Errorf("mysql.dsn = %q, want %q", got, want)
	}
	if got, want := cfg.MySQL.PoolSize, 30; got != want {
		t.Errorf("mysql.pool_size = %d, want %d", got, want)
	}
	if got, want := cfg.Etcd.DialTimeout, "5s"; got != want {
		t.Errorf("etcd.dial_timeout = %q, want %q", got, want)
	}
	if got, want := cfg.Gateway.ListenAddr, ":9090"; got != want {
		t.Errorf("gateway.listen_addr = %q, want %q", got, want)
	}
	if got, want := len(cfg.Providers), 1; got != want {
		t.Fatalf("providers 数量 = %d, want %d", got, want)
	}
	if got, want := cfg.Providers[0].Type, "openai"; got != want {
		t.Errorf("providers[0].type = %q, want %q", got, want)
	}
	if got, want := cfg.Providers[0].APIKeyEnv, "OPENAI_API_KEY"; got != want {
		t.Errorf("providers[0].api_key_env = %q, want %q", got, want)
	}
}

func TestLoadDefaultListenAddrAndPoolSize(t *testing.T) {
	path := writeConfig(t, `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
redis:
  addr: "localhost:6379"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if got, want := cfg.Gateway.ListenAddr, ":8080"; got != want {
		t.Errorf("默认 listen_addr = %q, want %q", got, want)
	}
	if got, want := cfg.MySQL.PoolSize, 20; got != want {
		t.Errorf("默认 mysql.pool_size = %d, want %d", got, want)
	}
	// 限流默认值：rps=10 / tpm=10000 / concurrency=50
	if got, want := cfg.Gateway.DefaultRPSLimit, 10; got != want {
		t.Errorf("默认 default_rps_limit = %d, want %d", got, want)
	}
	if got, want := cfg.Gateway.DefaultTPMLimit, 10000; got != want {
		t.Errorf("默认 default_tpm_limit = %d, want %d", got, want)
	}
	if got, want := cfg.Gateway.DefaultConcurrencyLimit, 50; got != want {
		t.Errorf("默认 default_concurrency_limit = %d, want %d", got, want)
	}
	if got, want := cfg.Gateway.MaxInFlight, 1000; got != want {
		t.Errorf("默认 max_in_flight = %d, want %d", got, want)
	}
	// 可选依赖（etcd / kafka / qdrant）缺失不应报错
	if cfg.Etcd.Endpoints != nil || cfg.Kafka.Brokers != nil {
		t.Errorf("可选依赖缺失时不应被填充: etcd=%v kafka=%v", cfg.Etcd.Endpoints, cfg.Kafka.Brokers)
	}
}

func TestLoadMissingRedisAddr(t *testing.T) {
	path := writeConfig(t, "mysql:\n  dsn: \"root@tcp(localhost:3306)/ai_gateway\"\n")

	_, err := Load(path)
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	if !strings.Contains(err.Error(), "redis.addr") {
		t.Errorf("错误信息应指出 redis.addr: %v", err)
	}
}

func TestLoadInvalidRedisDialTimeout(t *testing.T) {
	path := writeConfig(t, `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
redis:
  addr: "localhost:6379"
  dial_timeout: "not-a-duration"
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	if !strings.Contains(err.Error(), "redis.dial_timeout") {
		t.Errorf("错误信息应指出 redis.dial_timeout: %v", err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-exist.yaml")

	_, err := Load(path)
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	for _, want := range []string{"配置文件不存在", path, "config.example.yaml"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误信息缺少 %q: %v", want, err)
		}
	}
}

func TestLoadEmptyDSN(t *testing.T) {
	path := writeConfig(t, "mysql:\n  dsn: \"\"\n")

	_, err := Load(path)
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	if !strings.Contains(err.Error(), "mysql.dsn") {
		t.Errorf("错误信息应指出 mysql.dsn: %v", err)
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	path := writeConfig(t, "mysql: [")

	_, err := Load(path)
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	if !strings.Contains(err.Error(), "解析配置文件失败") {
		t.Errorf("错误信息应包含解析失败: %v", err)
	}
}

func TestLoadEmptyPathUsesDefault(t *testing.T) {
	// 工作目录下没有 configs/config.yaml，应命中默认路径的错误分支
	_, err := Load("")
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	if !strings.Contains(err.Error(), DefaultPath) {
		t.Errorf("错误信息应包含默认路径 %q: %v", DefaultPath, err)
	}
}

func TestLoadTimeoutDefaults(t *testing.T) {
	path := writeConfig(t, `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
redis:
  addr: "localhost:6379"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if got, want := cfg.Gateway.RequestTimeout, "60s"; got != want {
		t.Errorf("默认 request_timeout = %q, want %q", got, want)
	}
	if got, want := cfg.Gateway.AttemptTimeout, "30s"; got != want {
		t.Errorf("默认 attempt_timeout = %q, want %q", got, want)
	}
	if got, want := cfg.Gateway.StreamMaxDuration, "300s"; got != want {
		t.Errorf("默认 stream_max_duration = %q, want %q", got, want)
	}

	request, attempt, streamMax := cfg.Gateway.TimeoutDurations()
	if request != 60*time.Second || attempt != 30*time.Second || streamMax != 300*time.Second {
		t.Errorf("TimeoutDurations = %v/%v/%v, want 60s/30s/300s", request, attempt, streamMax)
	}
}

func TestLoadCustomTimeouts(t *testing.T) {
	path := writeConfig(t, `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
redis:
  addr: "localhost:6379"
gateway:
  request_timeout: "90s"
  attempt_timeout: "15s"
  stream_max_duration: "600s"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	request, attempt, streamMax := cfg.Gateway.TimeoutDurations()
	if request != 90*time.Second || attempt != 15*time.Second || streamMax != 600*time.Second {
		t.Errorf("TimeoutDurations = %v/%v/%v, want 90s/15s/600s", request, attempt, streamMax)
	}
}

func TestLoadInvalidAttemptTimeout(t *testing.T) {
	path := writeConfig(t, `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
redis:
  addr: "localhost:6379"
gateway:
  attempt_timeout: "abc"
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	if !strings.Contains(err.Error(), "gateway.attempt_timeout") {
		t.Errorf("错误信息应指出 gateway.attempt_timeout: %v", err)
	}
}

// RAG 未配置时使用默认值（检索超时 800ms，切分 400/50）
func TestLoadRAGDefaults(t *testing.T) {
	path := writeConfig(t, `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
redis:
  addr: "localhost:6379"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.RAG.Enabled {
		t.Error("默认应关闭 rag")
	}
	if cfg.RAG.TopK != 5 || cfg.RAG.ScoreThreshold != 0.3 ||
		cfg.RAG.MaxContextTokens != 2000 || cfg.RAG.ChunkSize != 400 ||
		cfg.RAG.ChunkOverlap != 50 || cfg.RAG.CollectionPrefix != "kb_" {
		t.Errorf("RAG 默认值异常: %+v", cfg.RAG)
	}
	if got := cfg.RAG.RetrievalTimeoutDuration(); got != 800*time.Millisecond {
		t.Errorf("默认 retrieval timeout = %v, want 800ms", got)
	}
}

// rag.enabled=true 时必须配置 Qdrant 与 embedding（避免启动后静默不检索）
func TestLoadRAGEnabledRequiresVectorDeps(t *testing.T) {
	path := writeConfig(t, `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
redis:
  addr: "localhost:6379"
rag:
  enabled: true
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "qdrant.addr") {
		t.Fatalf("期望 qdrant.addr 校验错误, got %v", err)
	}
}

func TestLoadRAGInvalidRetrievalTimeout(t *testing.T) {
	path := writeConfig(t, `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
redis:
  addr: "localhost:6379"
rag:
  retrieval_timeout: "0s"
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "rag.retrieval_timeout") {
		t.Fatalf("期望 rag.retrieval_timeout 校验错误, got %v", err)
	}
}
