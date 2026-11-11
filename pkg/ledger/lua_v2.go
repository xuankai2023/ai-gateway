package ledger

import "github.com/redis/go-redis/v9"

// scriptReservesV2 预占脚本（整数版，A2 余额不足拦截）
//
//	KEYS[1] = ledger:tenant:{tenant_id}
//	ARGV[1] = estimated (μUSD)
//	ARGV[2] = trust_quota_default (μUSD)
//	ARGV[3] = reserved_at（unix_nano）
//	返回 {reserved_after, skipped, balance, available, blocked}
var scriptReservesV2 = redis.NewScript(`
local key          = KEYS[1]
local estimated    = tonumber(ARGV[1])
local trust_def    = tonumber(ARGV[2])
local reserved_at  = ARGV[3]
local balance      = tonumber(redis.call('HGET', key, 'balance') or '0')
local reserved     = tonumber(redis.call('HGET', key, 'reserved') or '0')
local trust        = tonumber(redis.call('HGET', key, 'trust_quota') or tostring(trust_def))
local available    = balance - reserved

-- A3：新租户 / 信任额度内，不做余额校验
if balance <= trust then
  return {reserved, 1, balance, available, 0}
end

-- A2：可用余额不足，拒绝（不修改 reserved）
if available < estimated then
  return {reserved, 0, balance, available, 1}
end

local reserved_after = redis.call('HINCRBY', key, 'reserved', estimated)
redis.call('HSET', key, 'reserved_at', reserved_at)
return {reserved_after, 0, balance, available, 0}
`)

// scriptSettlesV2 结算脚本（整数版）
//
//	KEYS[1] = ledger:tenant:{tenant_id}
//	ARGV[1] = reserved (μUSD)
//	ARGV[2] = final_cost (μUSD)
var scriptSettlesV2 = redis.NewScript(`
local key       = KEYS[1]
local reserved  = tonumber(ARGV[1])
local finalCost = tonumber(ARGV[2])
local balance

if reserved > 0 then
  local delta = finalCost - reserved
  if delta ~= 0 then
    balance = redis.call('HINCRBY', key, 'balance', -delta)
  else
    balance = tonumber(redis.call('HGET', key, 'balance') or '0')
  end
  redis.call('HSET', key, 'reserved', 0)
  redis.call('HDEL', key, 'reserved_at')
else
  balance = redis.call('HINCRBY', key, 'balance', -finalCost)
end

return balance
`)

// scriptRefundV2 退款脚本（整数版）
//
//	KEYS[1] = ledger:tenant:{tenant_id}
//	ARGV[1] = amount (μUSD)
var scriptRefundV2 = redis.NewScript(`
local key    = KEYS[1]
local amount = tonumber(ARGV[1])
local reserved = redis.call('HINCRBY', key, 'reserved', -amount)
if tonumber(reserved) <= 0 then
  redis.call('HSET', key, 'reserved', 0)
  redis.call('HDEL', key, 'reserved_at')
end
return reserved
`)
