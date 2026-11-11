// 配置 Schema 严格校验（启动期前置，applyDefaults 之前执行）
//
// 动机：applyDefaults 会把 <=0 的数值静默改成默认值，
// 配置写错（如 max_in_flight: -1）会被吞掉，运行期行为与配置预期不符。
// validateStrict 在默认值填充之前拒绝非法值，错误一次性聚合返回。
package config

import (
	"fmt"
	"strings"
)

// validateStrict 前置严格校验：范围 / 依赖 / 互斥
// 只拒绝"显式配置了非法值"的情况；零值（未配置）留给 applyDefaults 填默认
func (c *Config) validateStrict() error {
	var errs []string

	// --- 范围校验：数值字段不允许负数（0 = 未配置，走默认值） ---
	if c.MySQL.PoolSize < 0 {
		errs = append(errs, "mysql.pool_size must be >= 0")
	}
	if c.Redis.DB < 0 {
		errs = append(errs, "redis.db must be >= 0")
	}
	if c.Redis.PoolSize < 0 {
		errs = append(errs, "redis.pool_size must be >= 0")
	}
	if c.Gateway.MaxInFlight < 0 {
		errs = append(errs, "gateway.max_in_flight must be > 0")
	}
	if c.Gateway.MaxAttempts < 0 {
		errs = append(errs, "gateway.max_attempts must be > 0")
	}
	if c.Gateway.DefaultRPSLimit < 0 {
		errs = append(errs, "gateway.default_rps_limit must be > 0")
	}
	if c.Gateway.DefaultTPMLimit < 0 {
		errs = append(errs, "gateway.default_tpm_limit must be > 0")
	}
	if c.Gateway.DefaultConcurrencyLimit < 0 {
		errs = append(errs, "gateway.default_concurrency_limit must be > 0")
	}
	if c.Observability.Tracing.SampleRatio < 0 || c.Observability.Tracing.SampleRatio > 1 {
		errs = append(errs, fmt.Sprintf("observability.tracing.sample_ratio must be in [0,1], got %v",
			c.Observability.Tracing.SampleRatio))
	}
	if c.Qdrant.ScoreThreshold < 0 || c.Qdrant.ScoreThreshold > 1 {
		errs = append(errs, fmt.Sprintf("qdrant.score_threshold must be in [0,1], got %v",
			c.Qdrant.ScoreThreshold))
	}
	if c.Diagnostics.SampleRate < 0 || c.Diagnostics.SampleRate > 1 {
		errs = append(errs, fmt.Sprintf("diagnostics.sample_rate must be in [0,1], got %v",
			c.Diagnostics.SampleRate))
	}
	if c.RAG.ChunkOverlap < 0 || c.RAG.ChunkSize < 0 {
		errs = append(errs, "rag.chunk_size / chunk_overlap must be >= 0")
	}

	// --- 依赖校验 ---
	// 向量层配置不完整：只配 embedding 不配 addr（或反之）属于配置错误，启动即报
	if c.Qdrant.EmbeddingURL != "" && c.Qdrant.Addr == "" {
		errs = append(errs, "qdrant.addr required when qdrant.embedding_url is set")
	}
	if c.Qdrant.EmbeddingURL == "" && c.Qdrant.EmbeddingModel != "" {
		errs = append(errs, "qdrant.embedding_url required when qdrant.embedding_model is set")
	}
	// tracing 开启必须有 exporter endpoint
	if c.Observability.Tracing.Enabled && c.Observability.Tracing.Endpoint == "" {
		errs = append(errs, "observability.tracing.endpoint required when tracing.enabled")
	}

	// --- 互斥/冲突校验 ---
	// 管理面与业务面监听地址不能相同（端口冲突只在 listen 时才暴露，提前到配置期）
	if c.Admin.ListenAddr != "" && c.Gateway.ListenAddr != "" &&
		c.Admin.ListenAddr == c.Gateway.ListenAddr {
		errs = append(errs, "admin.listen_addr conflicts with gateway.listen_addr")
	}
	if c.Gateway.ListenMetrics != "" && c.Gateway.ListenAddr != "" &&
		c.Gateway.ListenMetrics == c.Gateway.ListenAddr {
		errs = append(errs, "gateway.listen_metrics conflicts with gateway.listen_addr")
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}
