package config

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestCIWorkflowYAMLParses CI workflow 是合法 YAML（防手抖语法错误进 master）
func TestCIWorkflowYAMLParses(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Skipf("ci.yml 不存在: %v", err)
	}
	var v map[string]any
	if err := yaml.Unmarshal(data, &v); err != nil {
		t.Fatalf("ci.yml YAML 语法错误: %v", err)
	}
	jobs, ok := v["jobs"].(map[string]any)
	if !ok {
		t.Fatal("ci.yml 缺 jobs 段")
	}
	for _, name := range []string{"lint", "unit-test", "build", "audit", "secret-scan", "perf"} {
		if _, ok := jobs[name]; !ok {
			t.Errorf("ci.yml 缺 job: %s", name)
		}
	}
}
