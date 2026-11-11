// Package config 配置加载
//
// 开发环境：读本地 config.yaml
// 生产环境：Watch etcd /gateway/ 前缀热更新（后续阶段实现）
//
// 启动必需项：mysql.dsn、redis.addr（限流中间件硬依赖）、gateway.listen_addr；
// etcd / kafka / qdrant 解析但缺失不阻塞启动（可选依赖）。
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultPath 默认配置文件路径（相对 Gateway 工作目录）
const DefaultPath = "configs/config.yaml"

// defaultMaxAttempts gateway.max_attempts 缺省值：单次请求最多尝试 4 个渠道
const defaultMaxAttempts = 4

// 限流全局默认值（租户配额列为 NULL 时回落，与中间件兜底一致）
const (
	defaultRPSLimit         = 10
	defaultTPMLimit         = 10000
	defaultConcurrencyLimit = 50
)

// defaultMaxInFlight gateway.max_in_flight 缺省值：全局在途请求硬上限
const defaultMaxInFlight = 1000

// 超时默认值（分层超时体系：attempt < request < stream_max）
const (
	defaultRequestTimeout    = "60s"   // 非流式请求整链路上限
	defaultAttemptTimeout    = "30s"   // 单次渠道 attempt 上限
	defaultStreamMaxDuration = "300s"  // 流式请求总时长兜底
	defaultRetrievalTimeout  = "800ms" // RAG 检索硬超时（超时降级为无资料回答）
)

// RAG 默认值（assistants 行未显式配置时回落）
const (
	defaultRAGTopK           = 5
	defaultRAGScoreThreshold = 0.3
	defaultRAGMaxTokens      = 2000
	defaultRAGChunkSize      = 400
	defaultRAGChunkOverlap   = 50
	defaultRAGCollection     = "kb_"
)

// Config 网关配置
type Config struct {
	Etcd          EtcdConfig          `yaml:"etcd"`
	MySQL         MySQLConfig         `yaml:"mysql"`
	Redis         RedisConfig         `yaml:"redis"`
	Kafka         KafkaConfig         `yaml:"kafka"`
	Qdrant        QdrantConfig        `yaml:"qdrant"`
	RAG           RAGConfig           `yaml:"rag"`
	Gateway       GatewayConfig       `yaml:"gateway"`
	Admin         AdminConfig         `yaml:"admin"`
	Vault         VaultConfig         `yaml:"vault"`
	Observability ObservabilityConfig `yaml:"observability"`
	Providers     []ProviderConfig    `yaml:"providers"`

	// Diagnostics 请求级诊断包（采样落库路由决策链路；错误请求 100% 采样）
	Diagnostics DiagnosticsConfig `yaml:"diagnostics"`
}

// DiagnosticsConfig 诊断包配置
type DiagnosticsConfig struct {
	// SampleRate 正常请求采样率（0~1，默认 0.01；错误请求恒定 100% 采样）
	SampleRate float64 `yaml:"sample_rate"`
}

// EtcdConfig etcd 连接配置（可选，热更新阶段启用）
type EtcdConfig struct {
	Endpoints   []string `yaml:"endpoints"`
	DialTimeout string   `yaml:"dial_timeout"`
}

// MySQLConfig MySQL 连接配置（必填）
type MySQLConfig struct {
	DSN      string `yaml:"dsn"`
	PoolSize int    `yaml:"pool_size"`
}

// RedisConfig Redis 连接配置（限流中间件硬依赖，启动必填）
type RedisConfig struct {
	Addr        string `yaml:"addr"`
	Password    string `yaml:"password"`
	DB          int    `yaml:"db"`
	PoolSize    int    `yaml:"pool_size"`
	DialTimeout string `yaml:"dial_timeout"`
}

// KafkaConfig Kafka 连接配置（可选，异步记账阶段启用）
type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	GroupID string   `yaml:"group_id"`
}

// QdrantConfig 语义缓存配置（阶段 12）
//
// 只配 addr 时无法做向量化（缺 embedding），语义缓存退化为 Redis 精确哈希单层；
// addr + embedding_url 同时配置才启用向量相似层（Qdrant + OpenAI 兼容 /v1/embeddings）。
type QdrantConfig struct {
	Addr string `yaml:"addr"`
	// EmbeddingURL OpenAI 兼容 embedding 完整地址（如 http://127.0.0.1:8081/v1/embeddings）
	EmbeddingURL string `yaml:"embedding_url"`
	// EmbeddingModel embedding 模型名
	EmbeddingModel string `yaml:"embedding_model"`
	// EmbeddingAPIKeyEnv embedding 服务 API Key 所在环境变量名（可空）
	EmbeddingAPIKeyEnv string `yaml:"embedding_api_key_env"`
	// ScoreThreshold 向量相似命中阈值（默认 0.95）
	ScoreThreshold float64 `yaml:"score_threshold"`
}

// RAGConfig RAG 检索配置（多项目知识库）
//
// 向量化与 Qdrant 连接复用 qdrant 段（embedding_url / embedding_model / addr），
// 本段只配置检索与切分参数，避免出现第二份重复的 embedding 配置。
type RAGConfig struct {
	// Enabled 总开关；false 时 handler 不解析助手、不检索（网关退化为纯 LLM 转发）
	Enabled bool `yaml:"enabled"`
	// TopK 默认召回条数（assistants.top_k 优先）
	TopK int `yaml:"top_k"`
	// ScoreThreshold 默认相似度下限（assistants.score_threshold 优先）
	ScoreThreshold float64 `yaml:"score_threshold"`
	// MaxContextTokens 检索资料默认 token 预算（assistants.max_context_tokens 优先）
	MaxContextTokens int `yaml:"max_context_tokens"`
	// RetrievalTimeout 单次检索硬超时（超时降级为"无资料回答"，不返回 5xx）
	RetrievalTimeout string `yaml:"retrieval_timeout"`
	// ChunkSize 单 chunk 目标字数
	ChunkSize int `yaml:"chunk_size"`
	// ChunkOverlap 相邻 chunk 重叠字数
	ChunkOverlap int `yaml:"chunk_overlap"`
	// CollectionPrefix Qdrant collection 前缀（collection = prefix + slug）
	CollectionPrefix string `yaml:"collection_prefix"`
}

// RetrievalTimeoutDuration 检索超时解析结果（非法 / 空返回默认 800ms）
func (r *RAGConfig) RetrievalTimeoutDuration() time.Duration {
	if r.RetrievalTimeout != "" {
		if d, err := time.ParseDuration(r.RetrievalTimeout); err == nil && d > 0 {
			return d
		}
	}
	return 800 * time.Millisecond
}

// GatewayConfig 网关自身监听与默认配额
type GatewayConfig struct {
	ListenAddr      string `yaml:"listen_addr"`
	ListenMetrics   string `yaml:"listen_metrics"`
	DefaultRPSLimit int    `yaml:"default_rps_limit"`
	DefaultTPMLimit int    `yaml:"default_tpm_limit"`
	// DefaultConcurrencyLimit 租户默认并发数上限（tenants.concurrency_limit 为 NULL 时回落）
	DefaultConcurrencyLimit int `yaml:"default_concurrency_limit"`
	// MaxInFlight 全局在途请求硬上限（load_shed 信号量容量，超出排队 2s 后 503）
	MaxInFlight int `yaml:"max_in_flight"`
	// MaxAttempts 单次请求最大尝试渠道数（重试按渠道数计算，每渠道最多尝试一次）
	MaxAttempts int `yaml:"max_attempts"`
	// RequestTimeout 非流式请求整链路上限（含全部渠道重试），超时返回 504
	RequestTimeout string `yaml:"request_timeout"`
	// AttemptTimeout 单次渠道 attempt 上限（非流式整次调用 / 流式建连+首帧）
	AttemptTimeout string `yaml:"attempt_timeout"`
	// StreamMaxDuration 流式请求总时长兜底（长回答不被 request_timeout 误杀）
	StreamMaxDuration string `yaml:"stream_max_duration"`
	// CORSAllowedOrigins 允许跨域的前端来源；["*"] 表示允许任意来源
	CORSAllowedOrigins []string `yaml:"cors_allowed_origins"`

	// 解析后的超时（applyDefaults 填充，yaml 不直接映射；直接构造 Config 时为零值）
	requestTimeout    time.Duration
	attemptTimeout    time.Duration
	streamMaxDuration time.Duration
}

// TimeoutDurations 返回三个超时的解析结果（request / attempt / streamMax）
// 字符串字段非空时以字符串为准实时解析（直接构造 Config 的测试场景），否则用 applyDefaults 的预解析值
func (g *GatewayConfig) TimeoutDurations() (request, attempt, streamMax time.Duration) {
	request, attempt, streamMax = g.requestTimeout, g.attemptTimeout, g.streamMaxDuration
	if g.RequestTimeout != "" {
		if d, err := time.ParseDuration(g.RequestTimeout); err == nil {
			request = d
		}
	}
	if g.AttemptTimeout != "" {
		if d, err := time.ParseDuration(g.AttemptTimeout); err == nil {
			attempt = d
		}
	}
	if g.StreamMaxDuration != "" {
		if d, err := time.ParseDuration(g.StreamMaxDuration); err == nil {
			streamMax = d
		}
	}
	return request, attempt, streamMax
}

// ProviderConfig 供应商注册配置
// API Key 不写进配置文件：api_key_env 记录环境变量名（启动时从环境读取）；
// 或 api_key_ref 记录凭证引用（env://VAR / vault://path，阶段 16），二选一，api_key_ref 优先。
type ProviderConfig struct {
	Type      string `yaml:"type"`
	BaseURL   string `yaml:"base_url"`
	APIKeyEnv string `yaml:"api_key_env"`
	// APIKeyRef 凭证引用（阶段 16）：env://VAR_NAME 或 vault://secret/data/xxx
	APIKeyRef string `yaml:"api_key_ref"`
}

// AdminConfig 管理面配置（阶段 15）
//
// 管理面与业务面隔离：独立监听端口（默认 :8082，生产经 NetworkPolicy 仅内网可达），
// 鉴权用独立 X-Admin-Token（与业务 api_keys 体系完全隔离）。
// Token 不落明文配置：token 字段支持 env://VAR_NAME 引用（推荐）或直写（仅本地开发）。
// Token 解析后为空 → 管理面不启动（warn 日志），业务面不受影响。
type AdminConfig struct {
	// ListenAddr 管理面监听地址（默认 :8082；空串 = 不启动管理面）
	ListenAddr string `yaml:"listen_addr"`
	// Token 管理面令牌（X-Admin-Token 头比对）；支持 env:// 引用
	Token string `yaml:"token"`
}

// VaultConfig Vault 凭证托管配置（阶段 16）
//
// 配置 addr 后，channel_keys.key_ref / providers[].api_key_ref 中的
// vault:// 前缀引用经 Vault KV v2 HTTP API 解析；未配置时 vault:// 引用解析为空并 warn 跳过。
type VaultConfig struct {
	// Addr Vault 服务地址（如 http://127.0.0.1:8200），空 = 未启用
	Addr string `yaml:"addr"`
	// TokenEnv Vault token 所在环境变量名（默认 VAULT_TOKEN）
	TokenEnv string `yaml:"token_env"`
	// Timeout 单次 Vault 查询超时（默认 3s）
	Timeout string `yaml:"timeout"`
}

// ObservabilityConfig 可观测性配置
type ObservabilityConfig struct {
	Tracing TracingConfig `yaml:"tracing"`
}

// TracingConfig OTel tracing exporter 配置
type TracingConfig struct {
	Enabled     bool    `yaml:"enabled"`
	Endpoint    string  `yaml:"endpoint"`
	Insecure    bool    `yaml:"insecure"`
	SampleRatio float64 `yaml:"sample_ratio"`
}

// Load 读取并校验配置文件；path 为空时使用 DefaultPath
func Load(path string) (*Config, error) {
	if path == "" {
		path = DefaultPath
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("配置文件不存在: %s（请从 config.example.yaml 复制）", path)
		}
		return nil, fmt.Errorf("读取配置文件失败: %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %s: %w", path, err)
	}

	// 严格校验在默认值填充之前：拒绝显式配置的非法值（负数 / 依赖缺失 / 端口冲突）
	if err := cfg.validateStrict(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("配置校验失败: %w", err)
	}
	return &cfg, nil
}

// Default 返回只填充默认值的配置（测试与 App 组装兜底用；生产入口仍走 Load）
func Default() *Config {
	c := &Config{}
	c.applyDefaults()
	return c
}

// applyDefaults 给可省略的字段填默认值
func (c *Config) applyDefaults() {
	if c.Gateway.ListenAddr == "" {
		c.Gateway.ListenAddr = ":8080"
	}
	if c.MySQL.PoolSize <= 0 {
		c.MySQL.PoolSize = 20
	}
	if len(c.Gateway.CORSAllowedOrigins) == 0 {
		c.Gateway.CORSAllowedOrigins = []string{"*"}
	}
	if c.Gateway.MaxAttempts <= 0 {
		c.Gateway.MaxAttempts = defaultMaxAttempts
	}
	if c.Gateway.DefaultRPSLimit <= 0 {
		c.Gateway.DefaultRPSLimit = defaultRPSLimit
	}
	if c.Gateway.DefaultTPMLimit <= 0 {
		c.Gateway.DefaultTPMLimit = defaultTPMLimit
	}
	if c.Gateway.DefaultConcurrencyLimit <= 0 {
		c.Gateway.DefaultConcurrencyLimit = defaultConcurrencyLimit
	}
	if c.Gateway.MaxInFlight <= 0 {
		c.Gateway.MaxInFlight = defaultMaxInFlight
	}
	if c.Gateway.RequestTimeout == "" {
		c.Gateway.RequestTimeout = defaultRequestTimeout
	}
	if c.Gateway.AttemptTimeout == "" {
		c.Gateway.AttemptTimeout = defaultAttemptTimeout
	}
	if c.Gateway.StreamMaxDuration == "" {
		c.Gateway.StreamMaxDuration = defaultStreamMaxDuration
	}
	// 解析失败此处记零值，validate 会按字符串再校验并报错
	c.Gateway.requestTimeout, _ = time.ParseDuration(c.Gateway.RequestTimeout)
	c.Gateway.attemptTimeout, _ = time.ParseDuration(c.Gateway.AttemptTimeout)
	c.Gateway.streamMaxDuration, _ = time.ParseDuration(c.Gateway.StreamMaxDuration)
	if c.Observability.Tracing.SampleRatio <= 0 {
		c.Observability.Tracing.SampleRatio = 1.0
	}
	if c.Observability.Tracing.SampleRatio > 1.0 {
		c.Observability.Tracing.SampleRatio = 1.0
	}
	if c.Admin.ListenAddr == "" {
		c.Admin.ListenAddr = ":8082"
	}
	if c.Vault.TokenEnv == "" {
		c.Vault.TokenEnv = "VAULT_TOKEN"
	}
	if c.Vault.Timeout == "" {
		c.Vault.Timeout = "3s"
	}
	if c.RAG.TopK <= 0 {
		c.RAG.TopK = defaultRAGTopK
	}
	if c.RAG.ScoreThreshold <= 0 {
		c.RAG.ScoreThreshold = defaultRAGScoreThreshold
	}
	if c.RAG.MaxContextTokens <= 0 {
		c.RAG.MaxContextTokens = defaultRAGMaxTokens
	}
	if c.RAG.RetrievalTimeout == "" {
		c.RAG.RetrievalTimeout = defaultRetrievalTimeout
	}
	if c.RAG.ChunkSize <= 0 {
		c.RAG.ChunkSize = defaultRAGChunkSize
	}
	if c.RAG.ChunkOverlap <= 0 {
		c.RAG.ChunkOverlap = defaultRAGChunkOverlap
	}
	if c.RAG.ChunkOverlap >= c.RAG.ChunkSize {
		c.RAG.ChunkOverlap = c.RAG.ChunkSize / 8
	}
	if c.RAG.CollectionPrefix == "" {
		c.RAG.CollectionPrefix = defaultRAGCollection
	}
}

// validate 只校验启动必需项，可选依赖（etcd / kafka / qdrant）缺失不报错
func (c *Config) validate() error {
	if c.MySQL.DSN == "" {
		return errors.New("mysql.dsn 不能为空")
	}
	if c.Redis.Addr == "" {
		return errors.New("redis.addr 不能为空（限流中间件依赖 Redis）")
	}
	if c.Redis.DialTimeout != "" {
		if _, err := time.ParseDuration(c.Redis.DialTimeout); err != nil {
			return fmt.Errorf("redis.dial_timeout 格式非法: %w", err)
		}
	}
	if c.Gateway.ListenAddr == "" {
		return errors.New("gateway.listen_addr 不能为空")
	}
	for name, s := range map[string]string{
		"gateway.request_timeout":     c.Gateway.RequestTimeout,
		"gateway.attempt_timeout":     c.Gateway.AttemptTimeout,
		"gateway.stream_max_duration": c.Gateway.StreamMaxDuration,
	} {
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 {
			return fmt.Errorf("%s 必须是正时长（如 30s）: %q", name, s)
		}
	}
	if c.Vault.Timeout != "" {
		if d, err := time.ParseDuration(c.Vault.Timeout); err != nil || d <= 0 {
			return fmt.Errorf("vault.timeout 必须是正时长（如 3s）: %q", c.Vault.Timeout)
		}
	}
	if c.RAG.RetrievalTimeout != "" {
		if d, err := time.ParseDuration(c.RAG.RetrievalTimeout); err != nil || d <= 0 {
			return fmt.Errorf("rag.retrieval_timeout 必须是正时长（如 800ms）: %q", c.RAG.RetrievalTimeout)
		}
	}
	if c.RAG.Enabled {
		if c.Qdrant.Addr == "" {
			return errors.New("rag.enabled=true 时 qdrant.addr 不能为空")
		}
		if c.Qdrant.EmbeddingURL == "" || c.Qdrant.EmbeddingModel == "" {
			return errors.New("rag.enabled=true 时 qdrant.embedding_url / embedding_model 不能为空")
		}
	}
	return nil
}
