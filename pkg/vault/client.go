// Package vault Vault KV v2 凭证读取客户端（阶段 16 安全加固）
//
// 设计：
//   - key_ref 形如 vault://secret/data/ai-gateway/openai-1（完整 KV v2 data 路径）
//   - GET {addr}/v1/{path}，Header X-Vault-Token；响应取 data.data 下的
//     "api_key" 字段，缺省再取 "value"，都没有则取第一个字符串值
//   - 只读：网关只消费凭证，不写入；解析结果只在内存留存，不落日志不落库
package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client Vault KV v2 客户端
type Client struct {
	addr  string
	token string
	hc    *http.Client
}

// New 构造 Vault 客户端；timeout<=0 用 3s
func New(addr, token string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Client{
		addr:  strings.TrimRight(addr, "/"),
		token: token,
		hc:    &http.Client{Timeout: timeout},
	}
}

// kvV2Response Vault KV v2 GET 响应骨架（只取 data.data）
type kvV2Response struct {
	Data struct {
		Data map[string]any `json:"data"`
	} `json:"data"`
	Errors []string `json:"errors"`
}

// ReadSecret 读取 vault:// 引用对应的密钥；path 不带 vault:// 前缀
// 找不到 / 权限不足 / 网络错误都返回 error（调用方按"解析失败跳过"处理）
func (c *Client) ReadSecret(ctx context.Context, path string) (string, error) {
	if c == nil || c.addr == "" {
		return "", fmt.Errorf("vault: client not configured")
	}
	path = strings.TrimPrefix(strings.TrimSpace(path), "/")
	if path == "" {
		return "", fmt.Errorf("vault: empty path")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.addr+"/v1/"+path, nil)
	if err != nil {
		return "", fmt.Errorf("vault: build request: %w", err)
	}
	if c.token != "" {
		req.Header.Set("X-Vault-Token", c.token)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("vault: request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("vault: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("vault: status %d", resp.StatusCode)
	}

	var parsed kvV2Response
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("vault: decode response: %w", err)
	}
	if len(parsed.Errors) > 0 {
		return "", fmt.Errorf("vault: %s", strings.Join(parsed.Errors, "; "))
	}

	data := parsed.Data.Data
	if len(data) == 0 {
		return "", fmt.Errorf("vault: secret %s has no data", path)
	}
	// 优先约定字段，其次任意第一个字符串值
	for _, k := range []string{"api_key", "value"} {
		if v, ok := data[k].(string); ok && v != "" {
			return v, nil
		}
	}
	for _, v := range data {
		if s, ok := v.(string); ok && s != "" {
			return s, nil
		}
	}
	return "", fmt.Errorf("vault: secret %s has no string value", path)
}
