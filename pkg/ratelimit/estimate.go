// token 估算：准入阶段（限流 TPM）按请求体粗算输入 token
//
// 启发式：英文/代码约 4 字符 1 token；每条 message 另有固定结构开销。
// 这是准入估计而非账单口径——精确计数（tiktoken）在阶段 10 账本落地时替换，
// 结算按上游真实 usage 整单重算，多退少补。
package ratelimit

import "encoding/json"

const (
	// charsPerToken 估算字符/ token 比
	charsPerToken = 4
	// perMessageOverhead 每条 message 的结构开销（role 等）
	perMessageOverhead = 4
)

// chatMessage 只取 content 字段；OpenAI 兼容请求中 content 可能是
// 字符串或多模态数组（[{type:"text",text:"..."}, ...]）
type chatMessage struct {
	Content json.RawMessage `json:"content"`
}

// EstimateTokens 按请求体估算输入 token；空体或解析失败返回 0（请求随后会被
// handler 的 JSON 校验拒绝，无需在准入阶段为它计 token）
func EstimateTokens(body []byte) int {
	if len(body) == 0 {
		return 0
	}
	var req struct {
		Messages []chatMessage `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil || len(req.Messages) == 0 {
		return 0
	}

	total := 0
	for _, m := range req.Messages {
		total += perMessageOverhead + contentChars(m.Content)/charsPerToken
	}
	return total
}

// contentChars 计算 message content 的文本字符数（字符串与多模态数组两种形态）
func contentChars(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return len(s)
	}
	var parts []struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return 0
	}
	n := 0
	for _, p := range parts {
		n += len(p.Text)
	}
	return n
}
