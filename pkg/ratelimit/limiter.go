// Package ratelimit 分布式限流器（Redis Lua 原子实现）
//
// 三种限流语义，全部按租户隔离（key 含 tenant_id）：
//
//	RPS  令牌桶：key=rl:rps:{tenant_id}，容量=rate（1 秒令牌），按经过时间补充
//	TPM  滑动窗口日志：key=rl:tpm:{tenant_id}，ZSET 记录窗口内每笔 token，求和限流
//	并发  INCR/DECR 计数：key=rl:conc:{tenant_id}，带 TTL 兜底防进程崩溃泄漏
//
// 为什么用 Lua：令牌补充 / 窗口求和 / 计数 + TTL 都必须原子完成，
// 否则并发请求下会出现超卖（放行数 > 限额）或计数泄漏。
//
// 时钟说明：脚本的时间基准由调用方传入（now 参数），而非 Redis 服务器时间，
// 便于测试注入假时钟；生产环境多副本间毫秒级时钟偏差不影响限流语义。
package ratelimit

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// 默认窗口与兜底常量
const (
	// DefaultTPMWindow TPM 滑动窗口长度（1 分钟）
	DefaultTPMWindow = time.Minute
	// DefaultConcurrencyTTL 并发计数 key 的兜底 TTL（防释放泄漏）
	DefaultConcurrencyTTL = 120 * time.Second
)

// luaRPS 令牌桶脚本
//
// KEYS[1] = rl:rps:{tenant_id}
// ARGV[1] = rate（令牌/秒，桶容量同 rate）
// ARGV[2] = now_ms
// ARGV[3] = requested（本次申请令牌数，通常为 1）
// 返回 {allowed(0/1), retry_after_ms}
const luaRPS = `
local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local rate = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local tokens = tonumber(bucket[1]) or rate
local ts = tonumber(bucket[2]) or now

local elapsed = math.max(0, now - ts)
tokens = math.min(rate, tokens + elapsed * rate / 1000)

local allowed = 0
local wait = 0
if tokens >= requested then
	tokens = tokens - requested
	allowed = 1
else
	wait = math.ceil((requested - tokens) * 1000 / rate)
end

redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 10000) -- 兜底 TTL：冷租户 key 不堆积
return {allowed, wait}
`

// luaTPM 滑动窗口日志脚本（ZSET：score=时间戳，member="{tokens}#{uniq}"）
//
// KEYS[1] = rl:tpm:{tenant_id}
// ARGV[1] = limit（窗口内 token 上限）
// ARGV[2] = now_ms
// ARGV[3] = window_ms
// ARGV[4] = tokens（本请求估算 token 数）
// ARGV[5] = uniq（成员去重后缀，一般用 trace_id）
// 返回 {allowed(0/1), retry_after_ms}
const luaTPM = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local window = tonumber(ARGV[3])
local tokens = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local members = redis.call('ZRANGE', key, 0, -1)
local sum = 0
local suffix = '#' .. ARGV[5]
for _, m in ipairs(members) do
	local i = string.find(m, '#')
	if i then
		-- 同一 uniq（同一次请求）的旧成员不计入：ZADD 将覆盖它，保证重入幂等
		if string.sub(m, i) ~= suffix then
			sum = sum + tonumber(string.sub(m, 1, i - 1))
		end
	end
end

if sum + tokens > limit then
	local wait = 1000
	local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
	if #oldest >= 2 then
		wait = math.max(1, tonumber(oldest[2]) + window - now)
	end
	return {0, wait}
end

redis.call('ZADD', key, now, tokens .. '#' .. ARGV[5])
redis.call('PEXPIRE', key, window + 10000) -- 兜底 TTL：窗口滑过后自动清 key
return {1, 0}
`

// luaConcAcquire 并发占用脚本：INCR 并刷新 TTL，超限立即回退并拒绝
//
// KEYS[1] = rl:conc:{tenant_id}
// ARGV[1] = limit
// ARGV[2] = ttl_ms
// 返回 1=占用成功 0=超限
const luaConcAcquire = `
local cur = redis.call('INCR', KEYS[1])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2])) -- 每次占用刷新 TTL：有流量期间 key 不过期，进程全灭后兜底回收
if cur > tonumber(ARGV[1]) then
	redis.call('DECR', KEYS[1])
	return 0
end
return 1
`

// luaConcRelease 并发释放脚本：计数为正才 DECR；归零即删 key
// （TTL 到期清零后再收到迟到释放时不能 DECR 出负数）
//
// KEYS[1] = rl:conc:{tenant_id}
const luaConcRelease = `
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
if cur <= 0 then
	return 0
end
if redis.call('DECR', KEYS[1]) <= 0 then
	redis.call('DEL', KEYS[1])
end
return 0
`

var (
	scriptRPS         = goredis.NewScript(luaRPS)
	scriptTPM         = goredis.NewScript(luaTPM)
	scriptConcAcquire = goredis.NewScript(luaConcAcquire)
	scriptConcRelease = goredis.NewScript(luaConcRelease)
)

// Limiter 分布式限流器；方法均并发安全
type Limiter struct {
	client *goredis.Client
	// TPMWindow TPM 滑动窗口长度，<=0 用 DefaultTPMWindow
	TPMWindow time.Duration
	// ConcurrencyTTL 并发计数兜底 TTL，<=0 用 DefaultConcurrencyTTL
	ConcurrencyTTL time.Duration
	// now 时钟源（测试可注入假时钟）；nil 用 time.Now
	now func() time.Time
}

// New 创建限流器
func New(client *goredis.Client) *Limiter {
	return &Limiter{client: client, now: time.Now}
}

// SetClock 替换时钟源（仅测试用）
func (l *Limiter) SetClock(now func() time.Time) { l.now = now }

// AllowRPS 申请 RPS 令牌；allowed=false 时 retryAfter 为建议重试等待时长
func (l *Limiter) AllowRPS(ctx context.Context, tenantID int64, rate int) (allowed bool, retryAfter time.Duration, err error) {
	if rate <= 0 {
		return true, 0, nil // 无配额配置 = 不限
	}
	res, err := scriptRPS.Run(ctx, l.client,
		[]string{rpsKey(tenantID)},
		rate, l.nowUnixMilli(), 1,
	).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	return res[0] == 1, time.Duration(res[1]) * time.Millisecond, nil
}

// AllowTPM 在滑动窗口内记账 tokens；allowed=false 时 retryAfter 为最早条目滑出窗口的等待时长
// uniq 用于成员去重（同一请求多次记账会覆盖而非累加），一般用 trace_id
func (l *Limiter) AllowTPM(ctx context.Context, tenantID int64, limit, tokens int, uniq string) (allowed bool, retryAfter time.Duration, err error) {
	if limit <= 0 || tokens <= 0 {
		return true, 0, nil // 无配额或零估算 = 不限
	}
	window := l.TPMWindow
	if window <= 0 {
		window = DefaultTPMWindow
	}
	res, err := scriptTPM.Run(ctx, l.client,
		[]string{tpmKey(tenantID)},
		limit, l.nowUnixMilli(), window.Milliseconds(), tokens, uniq,
	).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	return res[0] == 1, time.Duration(res[1]) * time.Millisecond, nil
}

// AcquireConcurrency 占用一个并发槽；allowed=false 表示超限（已被脚本回退，无需释放）
func (l *Limiter) AcquireConcurrency(ctx context.Context, tenantID int64, limit int) (allowed bool, err error) {
	if limit <= 0 {
		return true, nil
	}
	ttl := l.ConcurrencyTTL
	if ttl <= 0 {
		ttl = DefaultConcurrencyTTL
	}
	res, err := scriptConcAcquire.Run(ctx, l.client,
		[]string{concKey(tenantID)},
		limit, ttl.Milliseconds(),
	).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// ReleaseConcurrency 释放一个并发槽（与成功的 Acquire 一一对应）
func (l *Limiter) ReleaseConcurrency(ctx context.Context, tenantID int64) error {
	return scriptConcRelease.Run(ctx, l.client, []string{concKey(tenantID)}).Err()
}

func (l *Limiter) nowUnixMilli() int64 { return l.now().UnixMilli() }

func rpsKey(tenantID int64) string  { return fmt.Sprintf("rl:rps:%d", tenantID) }
func tpmKey(tenantID int64) string  { return fmt.Sprintf("rl:tpm:%d", tenantID) }
func concKey(tenantID int64) string { return fmt.Sprintf("rl:conc:%d", tenantID) }
