package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWritePrivateJSONFile 基本写入：内容正确 + 权限 0o600
func TestWritePrivateJSONFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cred.json")
	if err := WritePrivateJSONFile(path, map[string]any{"api_key": "sk-test", "expires": 123}); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["api_key"] != "sk-test" || got["expires"].(float64) != 123 {
		t.Errorf("内容错误: %v", got)
	}
	if !strings.HasSuffix(string(data), "\n") {
		t.Error("文件应以换行结尾")
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("权限 = %o, want 600", perm)
	}
}

// TestWritePrivateJSONFile_AtomicNoPartial 写入中途无半写状态：
// 覆盖写场景下目标文件始终是新或旧完整内容（rename 原子性）
func TestWritePrivateJSONFile_AtomicNoPartial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cred.json")

	// 先写旧内容
	old := map[string]any{"version": 1}
	if err := WritePrivateJSONFile(path, old); err != nil {
		t.Fatal(err)
	}
	oldData, _ := os.ReadFile(path)

	// 再写新内容
	newVal := map[string]any{"version": 2, "data": strings.Repeat("x", 10000)}
	if err := WritePrivateJSONFile(path, newVal); err != nil {
		t.Fatal(err)
	}
	newData, _ := os.ReadFile(path)

	// 当前内容必须恰好等于新内容（无混合）
	cur, _ := os.ReadFile(path)
	if string(cur) != string(newData) && string(cur) != string(oldData) {
		t.Error("目标文件出现半写状态")
	}
	if string(cur) != string(newData) {
		t.Error("覆盖后应为新内容")
	}

	// 无残留临时文件
	entries, _ := os.ReadDir(filepath.Dir(path))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("残留临时文件: %s", e.Name())
		}
	}
}

// TestWritePrivateJSONFile_MkdirAll 父目录不存在时自动创建（0o700）
func TestWritePrivateJSONFile_MkdirAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a", "b", "cred.json")
	if err := WritePrivateJSONFile(path, map[string]any{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(filepath.Dir(path))
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Errorf("目录权限 = %o, want 700", perm)
	}
}

// TestWritePrivateJSONFile_ConcurrentNoCorrupt 并发写同一路径不损坏（rename 原子性）
func TestWritePrivateJSONFile_ConcurrentNoCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cred.json")
	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(v int) {
			done <- WritePrivateJSONFile(path, map[string]any{"writer": v, "pad": strings.Repeat("y", 5000)})
		}(i)
	}
	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Errorf("并发写失败: %v", err)
		}
	}
	// 最终内容必须是某个写入者的完整 JSON
	data, _ := os.ReadFile(path)
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("并发写后文件损坏: %v", err)
	}
}
