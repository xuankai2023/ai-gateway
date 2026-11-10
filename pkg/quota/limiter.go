// Package quota 🔴 New API 借鉴：预扣押金模型
//
// 核心算法：
//
//   阶段 1：准入检查
//     - 令牌桶（RPS）: Redis Lua 原子扣减令牌
//     - 滑动窗口（TPM）: etcd + Redis 双保险
//     - 并发数: etcd 原子 Incr/Decr
//     - 预算: MySQL 聚合 + 缓存结果到 etcd（60s TTL）
//
//   阶段 2：预扣押金 🔴
//     - tokenkit 本地估算输入 token
//     - 定价 = input_tokens × input_price_1k/1000 × price_ratio
//     - 余额 > trust_quota → 跳过预扣（高并发优化）
//     - Redis Lua 原子扣减：
//         if Quota < amount then return 0 end
//         HINCRBY Quota -amount
//         HINCRBY Reserved +amount  // 冻结标记
//
//   阶段 3：结算 🔴
//     - 上游返回真实 usage → 整单重算 cost
//     - delta = cost - reserved（正数补扣 / 负数退款）
//     - HINCRBY Quota delta
//     - HSET Reserved 0（清零冻结）
//     - 两步提交防双退
//
// New API 借鉴点：
//   1. 预扣只算输入（输出未知），保守估计押金
//   2. 信任额度内用户跳过预扣（优化高并发账本压力）
//   3. 结算时整单重算，不是"预扣输入不动只补输出"
//   4. Redis Lua 原子性保证账本不出现双退
package quota
