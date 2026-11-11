package rag

import (
	"strings"
	"testing"
)

// 按 Markdown 标题切分：章节路径带文档标题，正文独立成块
func TestSplitMarkdownSections(t *testing.T) {
	content := `# 告警手册

## 磁盘告警

处理步骤：先看 df -h，再清理日志。

## 内存告警

处理步骤：先看 free -m。`
	chunks := SplitMarkdown(content, "运维手册", 200, 20)
	if len(chunks) != 2 {
		t.Fatalf("chunk 数 = %d, want 2: %+v", len(chunks), chunks)
	}
	if chunks[0].Section != "运维手册 > 告警手册 > 磁盘告警" {
		t.Errorf("section = %q", chunks[0].Section)
	}
	if !strings.Contains(chunks[0].Text, "df -h") || !strings.HasPrefix(chunks[0].Text, "运维手册 > 告警手册 > 磁盘告警") {
		t.Errorf("chunk 文本缺少章节头或正文: %q", chunks[0].Text)
	}
	if chunks[0].Index != 0 || chunks[1].Index != 1 {
		t.Errorf("chunk index 应连续: %d, %d", chunks[0].Index, chunks[1].Index)
	}
}

// 超长无标题文本：按字数窗口切分并保留重叠
func TestSplitMarkdownLongTextOverlap(t *testing.T) {
	content := strings.Repeat("测", 900)
	chunks := SplitMarkdown(content, "长文", 400, 50)
	if len(chunks) < 3 {
		t.Fatalf("chunk 数 = %d, want >= 3", len(chunks))
	}
	firstBody := []rune(strings.SplitN(chunks[0].Text, "\n", 2)[1])
	second := chunks[1].Text
	if !strings.Contains(second, string(firstBody[len(firstBody)-50:])) {
		t.Error("相邻 chunk 未保留 overlap")
	}
	for _, c := range chunks {
		body := []rune(strings.SplitN(c.Text, "\n", 2)[1])
		if len(body) > 400 {
			t.Errorf("chunk 超长: %d", len(body))
		}
	}
}

// 空正文 / 只有标题不产生空 chunk
func TestSplitMarkdownEmptyBody(t *testing.T) {
	if chunks := SplitMarkdown("# 只有标题\n\n## 子标题\n", "文档", 400, 50); len(chunks) != 0 {
		t.Fatalf("空正文不应产生 chunk: %+v", chunks)
	}
}

// 普通文本（无 # 标题）退化为单章节
func TestSplitMarkdownPlainText(t *testing.T) {
	chunks := SplitMarkdown("第一段\n\n第二段", "纯文本", 400, 50)
	if len(chunks) != 1 || chunks[0].Section != "纯文本" {
		t.Fatalf("纯文本切分异常: %+v", chunks)
	}
}
