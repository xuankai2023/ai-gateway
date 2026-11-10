// Routing 路由策略实现
//
//  - WeightedRoundRobin  权重轮询（火山方舟多个 endpoint）
//  - LatencyAware        延迟感知（选最快的 endpoint）
//  - Failover            故障转移（主 endpoint 挂了自动切备）
package routing
