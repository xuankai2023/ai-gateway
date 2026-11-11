package rag

import "strings"

// Chunk 一个切分单元
type Chunk struct {
	// Index 从 0 递增（同一文档内稳定，用于 PointID 幂等）
	Index int
	// Section 章节路径（文档标题 > 一级标题 > 二级标题）
	Section string
	// Text 实际入库文本（章节路径 + 正文），保证召回片段脱离原文也可读
	Text string
}

// SplitMarkdown 按 Markdown 标题切分，再按字数窗口滑动（带重叠）。
// 非 Markdown 纯文本没有标题时退化为「标题 + 全文滑动切分」。
func SplitMarkdown(content, title string, size, overlap int) []Chunk {
	if size <= 0 {
		size = 400
	}
	if overlap < 0 {
		overlap = 0
	}
	if overlap >= size {
		overlap = size / 4
	}
	content = strings.ReplaceAll(content, "\r\n", "\n")
	title = strings.TrimSpace(title)
	if title == "" {
		title = "未命名文档"
	}

	type section struct {
		path []string
		body []string
	}
	var sections []section
	var stack []string
	cur := section{path: []string{title}}

	for _, line := range strings.Split(content, "\n") {
		if level := headingLevel(line); level > 0 {
			if len(cur.body) > 0 {
				sections = append(sections, cur)
			}
			if level <= len(stack) {
				stack = stack[:level-1]
			}
			stack = append(stack, strings.TrimSpace(line[level:]))
			cur = section{path: append([]string{title}, stack...)}
			continue
		}
		cur.body = append(cur.body, line)
	}
	if len(cur.body) > 0 {
		sections = append(sections, cur)
	}

	var chunks []Chunk
	for _, sec := range sections {
		body := strings.TrimSpace(strings.Join(sec.body, "\n"))
		if body == "" {
			continue
		}
		prefix := strings.Join(sec.path, " > ")
		for _, piece := range splitRunes(body, size, overlap) {
			chunks = append(chunks, Chunk{
				Index:   len(chunks),
				Section: prefix,
				Text:    prefix + "\n" + piece,
			})
		}
	}
	return chunks
}

// headingLevel 返回 Markdown 标题级别（1~6），非标题返回 0
func headingLevel(line string) int {
	n := 0
	for n < len(line) && line[n] == '#' {
		n++
	}
	if n == 0 || n > 6 || n >= len(line) || line[n] != ' ' {
		return 0
	}
	return n
}

// splitRunes 按 rune 窗口切分，尽量在换行 / 句末断开；相邻窗口重叠 overlap 个字符
func splitRunes(text string, size, overlap int) []string {
	runes := []rune(text)
	if len(runes) <= size {
		return []string{text}
	}
	var out []string
	start := 0
	for start < len(runes) {
		end := start + size
		if end >= len(runes) {
			end = len(runes)
		} else {
			// 最多回退 20% 找自然断点，避免把句子劈开
			min := start + size*4/5
			for i := end; i > min; i-- {
				switch runes[i-1] {
				case '\n', '。', '！', '？', '.', '!', '?':
					end = i
				}
				if end != start+size {
					break
				}
			}
		}
		out = append(out, string(runes[start:end]))
		if end >= len(runes) {
			break
		}
		next := end - overlap
		if next <= start {
			next = end
		}
		start = next
	}
	return out
}
