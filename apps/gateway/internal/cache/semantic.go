// Cache 语义缓存底层实现
//
//  - QdrantClient    向量库客户端（Search / Upsert / Delete）
//  - HashCache       Redis 精确哈希缓存（SHA256 Prompt）
//  - PreWarm         Top N 高频 Prompt 预热（定时任务）
package cache
