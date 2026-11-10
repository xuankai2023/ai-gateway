package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	// 可选依赖（etcd / redis / kafka / qdrant）缺失不应报错
	if cfg.Etcd.Endpoints != nil || cfg.Kafka.Brokers != nil {
		t.Errorf("可选依赖缺失时不应被填充: etcd=%v kafka=%v", cfg.Etcd.Endpoints, cfg.Kafka.Brokers)
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
