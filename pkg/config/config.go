// Package config 配置加载
//
// 开发环境：读本地 config.yaml
// 生产环境：Watch etcd /gateway/ 前缀热更新（后续阶段实现）
//
// 本阶段只强校验启动必需的 mysql.dsn 与 gateway.listen_addr；
// etcd / redis / kafka / qdrant 解析但缺失不阻塞启动（可选依赖）。
package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// DefaultPath 默认配置文件路径（相对 Gateway 工作目录）
const DefaultPath = "configs/config.yaml"

// Config 网关配置
type Config struct {
	Etcd      EtcdConfig       `yaml:"etcd"`
	MySQL     MySQLConfig      `yaml:"mysql"`
	Redis     RedisConfig      `yaml:"redis"`
	Kafka     KafkaConfig      `yaml:"kafka"`
	Qdrant    QdrantConfig     `yaml:"qdrant"`
	Gateway   GatewayConfig    `yaml:"gateway"`
	Providers []ProviderConfig `yaml:"providers"`
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

// RedisConfig Redis 连接配置（可选，限流/缓存阶段启用）
type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
}

// KafkaConfig Kafka 连接配置（可选，异步记账阶段启用）
type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	GroupID string   `yaml:"group_id"`
}

// QdrantConfig 向量库配置（可选，语义缓存阶段启用）
type QdrantConfig struct {
	Addr string `yaml:"addr"`
}

// GatewayConfig 网关自身监听与默认配额
type GatewayConfig struct {
	ListenAddr      string `yaml:"listen_addr"`
	ListenMetrics   string `yaml:"listen_metrics"`
	DefaultRPSLimit int    `yaml:"default_rps_limit"`
	DefaultTPMLimit int    `yaml:"default_tpm_limit"`
}

// ProviderConfig 供应商注册配置
// API Key 不写进配置文件，只记录环境变量名，启动时从环境读取
type ProviderConfig struct {
	Type      string `yaml:"type"`
	BaseURL   string `yaml:"base_url"`
	APIKeyEnv string `yaml:"api_key_env"`
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

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("配置校验失败: %w", err)
	}
	return &cfg, nil
}

// applyDefaults 给可省略的字段填默认值
func (c *Config) applyDefaults() {
	if c.Gateway.ListenAddr == "" {
		c.Gateway.ListenAddr = ":8080"
	}
	if c.MySQL.PoolSize <= 0 {
		c.MySQL.PoolSize = 20
	}
}

// validate 只校验本阶段启动必需项，可选依赖缺失不报错
func (c *Config) validate() error {
	if c.MySQL.DSN == "" {
		return errors.New("mysql.dsn 不能为空")
	}
	if c.Gateway.ListenAddr == "" {
		return errors.New("gateway.listen_addr 不能为空")
	}
	return nil
}
