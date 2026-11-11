package config

import (
	"strings"
	"testing"
)

// validBase 最小合法配置（各用例在此基础上叠加非法字段）
const validBase = `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
redis:
  addr: "localhost:6379"
`

// TestValidateStrict_RejectsNegative 负数显式配置启动即报错（不再被默认值吞掉）
func TestValidateStrict_RejectsNegative(t *testing.T) {
	cases := []struct {
		name    string
		snippet string
		want    string
	}{
		{"max_in_flight", "gateway:\n  max_in_flight: -1\n", "gateway.max_in_flight"},
		{"max_attempts", "gateway:\n  max_attempts: -2\n", "gateway.max_attempts"},
		{"rps_limit", "gateway:\n  default_rps_limit: -1\n", "gateway.default_rps_limit"},
		{"tpm_limit", "gateway:\n  default_tpm_limit: -1\n", "gateway.default_tpm_limit"},
		{"concurrency", "gateway:\n  default_concurrency_limit: -1\n", "gateway.default_concurrency_limit"},
		{"sample_ratio_over", "observability:\n  tracing:\n    sample_ratio: 1.5\n", "sample_ratio"},
		{"sample_ratio_negative", "observability:\n  tracing:\n    sample_ratio: -0.1\n", "sample_ratio"},
		{"qdrant_threshold", "qdrant:\n  score_threshold: 2.0\n", "qdrant.score_threshold"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, validBase+tc.snippet)
			_, err := Load(path)
			if err == nil {
				t.Fatalf("期望报错，实际为 nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误信息应包含 %q: %v", tc.want, err)
			}
		})
	}
}

// TestValidateStrict_RejectsNegativeTopLevel mysql/redis 段的负数（需自带完整配置避免键重复）
func TestValidateStrict_RejectsNegativeTopLevel(t *testing.T) {
	cases := []struct {
		name string
		full string
		want string
	}{
		{"mysql_pool", `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
  pool_size: -5
redis:
  addr: "localhost:6379"
`, "mysql.pool_size"},
		{"redis_db", `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
redis:
  addr: "localhost:6379"
  db: -1
`, "redis.db"},
		{"redis_pool", `
mysql:
  dsn: "root@tcp(localhost:3306)/ai_gateway"
redis:
  addr: "localhost:6379"
  pool_size: -10
`, "redis.pool_size"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, tc.full)
			_, err := Load(path)
			if err == nil {
				t.Fatalf("期望报错，实际为 nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误信息应包含 %q: %v", tc.want, err)
			}
		})
	}
}

// TestValidateStrict_Dependency 依赖校验：配置不完整即报错
func TestValidateStrict_Dependency(t *testing.T) {
	cases := []struct {
		name    string
		snippet string
		want    string
	}{
		{"embedding without addr",
			"qdrant:\n  embedding_url: \"http://localhost:8081/v1/embeddings\"\n", "qdrant.addr"},
		{"model without url",
			"qdrant:\n  addr: \"localhost:6333\"\n  embedding_model: \"text-embedding-3-small\"\n", "qdrant.embedding_url"},
		{"tracing without endpoint",
			"observability:\n  tracing:\n    enabled: true\n", "tracing.endpoint"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, validBase+tc.snippet)
			_, err := Load(path)
			if err == nil {
				t.Fatalf("期望报错，实际为 nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误信息应包含 %q: %v", tc.want, err)
			}
		})
	}
}

// TestValidateStrict_PortConflict 端口冲突提前到配置期暴露
func TestValidateStrict_PortConflict(t *testing.T) {
	path := writeConfig(t, validBase+`
gateway:
  listen_addr: ":8080"
admin:
  listen_addr: ":8080"
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "admin.listen_addr") {
		t.Fatalf("期望端口冲突错误, got %v", err)
	}

	path = writeConfig(t, validBase+`
gateway:
  listen_addr: ":8080"
  listen_metrics: ":8080"
`)
	_, err = Load(path)
	if err == nil || !strings.Contains(err.Error(), "listen_metrics") {
		t.Fatalf("期望 metrics 端口冲突错误, got %v", err)
	}
}

// TestValidateStrict_AggregatesErrors 多个错误一次性聚合返回
func TestValidateStrict_AggregatesErrors(t *testing.T) {
	path := writeConfig(t, validBase+`
gateway:
  max_in_flight: -1
  max_attempts: -1
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("期望报错，实际为 nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "max_in_flight") || !strings.Contains(msg, "max_attempts") {
		t.Errorf("应聚合两个错误: %v", err)
	}
}

// TestValidateStrict_AllowsZero 零值（未配置）不报错，走默认值
func TestValidateStrict_AllowsZero(t *testing.T) {
	path := writeConfig(t, validBase)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.Gateway.MaxInFlight != defaultMaxInFlight {
		t.Errorf("max_in_flight 默认 = %d, want %d", cfg.Gateway.MaxInFlight, defaultMaxInFlight)
	}
}
