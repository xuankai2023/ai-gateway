// Package model BodyStorage 🔴 New API 借鉴：一份请求体，全链路复用
//
// 参考：New API common/body_storage.go（约 80 行）
//
// 为什么需要它：
//   Gin 的 c.Request.Body 是 io.ReadCloser —— 读一次就没了
//   但 AuthMiddleware（取 model 名）、CacheMiddleware（取 Prompt）、
//   最终 Handler（取 Messages）都要读请求体
//   不包装就会出现：AuthMiddleware 读完 → CacheMiddleware 读到空 → Handler 读到空
//
// 实现：
//   type BodyStorage struct {
//     data     []byte     // < 1MB 驻内存（快）
//     tempFile *os.File   // ≥ 1MB 自动落临时文件（不撑爆内存）
//     current  int64
//   }
//
//   // 全生命周期只有一份副本
//   bodyStorage.Seek(0, 0)
//   c.Request.Body = io.NopCloser(bodyStorage)
//   // 任何中间件/Handler 读之前调 Seek 回起点
//
// 自动清理：Handler 结束后 defer os.Remove(tempFile.Name())
// 请求体在重试循环里：每轮重试前 Seek(0, 0) → 重新读 → 完整重放
//                    ↑ 这个特性让 New API 的"重试循环 = 换渠道"成为可能
//
// 接入点：
//   AuthMiddleware（第 4 层）第一步就把 c.Request.Body 换成 BodyStorage 包装
//   internal/middleware/auth.go 里有注释说明
package model
