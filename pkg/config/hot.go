// 阶段 15：运行期配置热更新
//
// 热更新双通道：
//  1. Redis Pub/Sub 推送：频道 gw:config，payload 为 JSON map（{"default_rps_limit": 20, ...}），
//     Admin API PUT /admin/v1/config 发布，所有节点订阅后 ApplyMap 生效；
//  2. SIGHUP 重读：main 收到 SIGHUP 后重新 Load 配置文件，ApplyConfig 应用热更新项。
//
// 热更新白名单（其他项变更需重启进程，ApplyConfig 返回 skipped 清单由调用方记日志）：
//   - max_attempts                单次请求最大尝试渠道数（handler 每请求读取）
//   - default_rps_limit           租户默认 RPS（ratelimit 中间件每请求读取）
//   - default_tpm_limit           租户默认 TPM
//   - default_concurrency_limit   租户默认并发数
//   - request_timeout             非流式整链路上限（Timeout 中间件每请求读取）
//   - stream_max_duration         流式整链路兜底（Timeout 中间件 + handler 每请求读取）
//
// 不可热更项（启动时定型）：listen_addr / listen_metrics / attempt_timeout（HTTP client
// 已按它构造）/ max_in_flight（信号量容量）/ mysql.dsn / redis.* / providers。
package config

import (
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

// HotValues 热更新值容器：全部字段原子读写，消费方每请求读取，零锁开销
type HotValues struct {
	maxAttempts            atomic.Int64
	defaultRPS             atomic.Int64
	defaultTPM             atomic.Int64
	defaultConcurrency     atomic.Int64
	requestTimeoutNanos    atomic.Int64
	streamMaxDurationNanos atomic.Int64
}

// 热更新项 key 名（与 yaml 字段名一致，Pub/Sub 推送与 SIGHUP 共用同一套 key）
const (
	HotKeyMaxAttempts        = "max_attempts"
	HotKeyDefaultRPS         = "default_rps_limit"
	HotKeyDefaultTPM         = "default_tpm_limit"
	HotKeyDefaultConcurrency = "default_concurrency_limit"
	HotKeyRequestTimeout     = "request_timeout"
	HotKeyStreamMaxDuration  = "stream_max_duration"
)

// NewHotValues 从已加载配置构造热更新值容器
func NewHotValues(c *Config) *HotValues {
	h := &HotValues{}
	h.ApplyConfig(c)
	return h
}

// ApplyConfig 从一份完整配置应用全部热更新项；返回 skipped（不可热更但被修改的项）供调用方告警
// cfg 为 nil 时不动
func (h *HotValues) ApplyConfig(c *Config) (skipped []string) {
	if c == nil {
		return nil
	}
	g := c.Gateway
	if g.MaxAttempts > 0 {
		h.maxAttempts.Store(int64(g.MaxAttempts))
	}
	if g.DefaultRPSLimit > 0 {
		h.defaultRPS.Store(int64(g.DefaultRPSLimit))
	}
	if g.DefaultTPMLimit > 0 {
		h.defaultTPM.Store(int64(g.DefaultTPMLimit))
	}
	if g.DefaultConcurrencyLimit > 0 {
		h.defaultConcurrency.Store(int64(g.DefaultConcurrencyLimit))
	}
	req, _, streamMax := g.TimeoutDurations()
	if req > 0 {
		h.requestTimeoutNanos.Store(int64(req))
	}
	if streamMax > 0 {
		h.streamMaxDurationNanos.Store(int64(streamMax))
	}
	return nil
}

// ApplyMap 应用 Pub/Sub 推送的部分更新；逐项校验，非法值整体拒绝（返回第一个错误）
// 返回 applied 已生效 key 清单（排序后，便于日志与测试断言）
func (h *HotValues) ApplyMap(m map[string]any) (applied []string, err error) {
	// 先校验全部 key，再统一应用（避免部分生效）
	type parsed struct {
		key   string
		intV  int64
		durV  time.Duration
		isDur bool
	}
	items := make([]parsed, 0, len(m))
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := m[k]
		switch k {
		case HotKeyMaxAttempts, HotKeyDefaultRPS, HotKeyDefaultTPM, HotKeyDefaultConcurrency:
			n, err := toInt64(v)
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("config hot-update %s 必须是正整数: %v", k, v)
			}
			items = append(items, parsed{key: k, intV: n})
		case HotKeyRequestTimeout, HotKeyStreamMaxDuration:
			d, err := toDuration(v)
			if err != nil || d <= 0 {
				return nil, fmt.Errorf("config hot-update %s 必须是正时长（如 30s）: %v", k, v)
			}
			items = append(items, parsed{key: k, durV: d, isDur: true})
		default:
			return nil, fmt.Errorf("config hot-update 不支持的项: %s（仅支持 max_attempts/default_rps_limit/default_tpm_limit/default_concurrency_limit/request_timeout/stream_max_duration）", k)
		}
	}
	for _, it := range items {
		switch it.key {
		case HotKeyMaxAttempts:
			h.maxAttempts.Store(it.intV)
		case HotKeyDefaultRPS:
			h.defaultRPS.Store(it.intV)
		case HotKeyDefaultTPM:
			h.defaultTPM.Store(it.intV)
		case HotKeyDefaultConcurrency:
			h.defaultConcurrency.Store(it.intV)
		case HotKeyRequestTimeout:
			h.requestTimeoutNanos.Store(int64(it.durV))
		case HotKeyStreamMaxDuration:
			h.streamMaxDurationNanos.Store(int64(it.durV))
		}
		applied = append(applied, it.key)
	}
	return applied, nil
}

// Snapshot 当前热更新值（Admin GET /config 与日志用）
func (h *HotValues) Snapshot() map[string]any {
	return map[string]any{
		HotKeyMaxAttempts:        h.MaxAttempts(),
		HotKeyDefaultRPS:         h.DefaultRPSLimit(),
		HotKeyDefaultTPM:         h.DefaultTPMLimit(),
		HotKeyDefaultConcurrency: h.DefaultConcurrencyLimit(),
		HotKeyRequestTimeout:     h.RequestTimeout().String(),
		HotKeyStreamMaxDuration:  h.StreamMaxDuration().String(),
	}
}

// MaxAttempts 单次请求最大尝试渠道数（0 = 未设置，消费方回落默认）
func (h *HotValues) MaxAttempts() int { return int(h.maxAttempts.Load()) }

// DefaultRPSLimit 租户默认 RPS（0 = 未设置）
func (h *HotValues) DefaultRPSLimit() int { return int(h.defaultRPS.Load()) }

// DefaultTPMLimit 租户默认 TPM（0 = 未设置）
func (h *HotValues) DefaultTPMLimit() int { return int(h.defaultTPM.Load()) }

// DefaultConcurrencyLimit 租户默认并发数（0 = 未设置）
func (h *HotValues) DefaultConcurrencyLimit() int { return int(h.defaultConcurrency.Load()) }

// RequestTimeout 非流式整链路上限（0 = 未设置）
func (h *HotValues) RequestTimeout() time.Duration {
	return time.Duration(h.requestTimeoutNanos.Load())
}

// StreamMaxDuration 流式整链路兜底（0 = 未设置）
func (h *HotValues) StreamMaxDuration() time.Duration {
	return time.Duration(h.streamMaxDurationNanos.Load())
}

// toInt64 宽容解析整数（JSON 数字是 float64）
func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int:
		return int64(t), nil
	case int64:
		return t, nil
	case float64:
		return int64(t), nil
	case string:
		return strconv.ParseInt(t, 10, 64)
	default:
		return 0, fmt.Errorf("not an integer: %v", v)
	}
}

// toDuration 解析时长：字符串（"45s"）或数字（秒）
func toDuration(v any) (time.Duration, error) {
	switch t := v.(type) {
	case string:
		return time.ParseDuration(t)
	case float64:
		return time.Duration(t * float64(time.Second)), nil
	case int64:
		return time.Duration(t) * time.Second, nil
	case int:
		return time.Duration(t) * time.Second, nil
	default:
		return 0, fmt.Errorf("not a duration: %v", v)
	}
}
