// 阶段 16 安全加固：日志脱敏工具
//
// 纪律：
//   - Authorization 头、api key 明文、provider key 明文一律不落日志；
//     需要关联定位时只记 RedactKey 的哈希前缀 + 长度
//   - 请求体 messages 内容不落日志（现有 Logger 中间件本就不记 body，本文件提供
//     body 摘要工具供排障场景显式调用）
package observability

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// RedactKey 密钥脱敏：`sha256前8位…(len=N)`，可关联定位又不可还原
func RedactKey(plain string) string {
	if plain == "" {
		return "(empty)"
	}
	sum := sha256.Sum256([]byte(plain))
	return fmt.Sprintf("%s…(len=%d)", hex.EncodeToString(sum[:4]), len(plain))
}

// BodyDigest 请求体摘要：只保留长度与哈希（排障时确认两次请求体是否一致用，不落内容）
func BodyDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return fmt.Sprintf("len=%d sha256=%s", len(body), hex.EncodeToString(sum[:8]))
}
