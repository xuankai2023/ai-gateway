// 凭证/敏感 JSON 文件原子写入
//
// 写入语义：临时文件（同目录，0o600）→ 写入 → 原子 rename → chmod 0o600。
// 进程在任意时刻 kill -9，目标文件要么是旧内容要么是新内容，无半写状态。
// 用途：本地凭证文件（如 provider key 缓存）落盘。
package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// WritePrivateJSONFile 原子写入 JSON 文件（最终权限 0o600，父目录 0o700）
func WritePrivateJSONFile(path string, value any) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	// 临时文件：同目录（保证 rename 同文件系统）+ 随机后缀（防冲突）
	tmp := filepath.Join(dir, fmt.Sprintf(".%s.%d.%s.tmp",
		filepath.Base(path), os.Getpid(), randHex(6)))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}

	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("marshal: %w", err)
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close tmp: %w", err)
	}

	// 原子替换
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	// 兜底权限（rename 保留 tmp 的 0o600，但显式 chmod 防 umask 边界）
	return os.Chmod(path, 0o600)
}

// randHex 生成 n 字节随机 hex 字符串（2n 字符）
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 不可用属极端异常，退化为固定字符串（tmp 名仍有 pid 区分）
		return "fallback"
	}
	return hex.EncodeToString(b)
}
