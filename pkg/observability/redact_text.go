// 通用文本/结构脱敏引擎（对齐 OpenCreator redaction.ts）
//
// 覆盖 6 类模式：赋值形式（KEY=xxx）、标签形式（password: xxx）、
// Authorization 头、Cookie 头、OpenAI 风格独立 key（sk-xxx）、agent capability token。
// RedactText 处理字符串；RedactValue 递归处理 map/slice/struct（审计日志与诊断包导出用）。
//
// 纪律：已脱敏内容（值以 [REDACTED 开头）不重复替换。
// 注：Go RE2 不支持否定前瞻，幂等性用 ReplaceAllStringFunc + 值前缀判断实现。
package observability

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// Redacted 脱敏占位符（导出：诊断包等调用方需要做幂等判断）
const Redacted = "[REDACTED]"

var (
	// 1. 赋值形式：KEY=xxx / TOKEN=xxx / SECRET=xxx / PASSWORD=xxx / COOKIE=xxx / AUTH=xxx
	assignmentSecretPattern = regexp.MustCompile(
		`(?i)([A-Z0-9_]*(?:KEY|TOKEN|SECRET|PASSWORD|COOKIE|AUTH)[A-Z0-9_]*)=([^\s"',}\]]+)`)

	// 2. 标签形式：password: xxx / token: xxx / api-key: xxx / secret: xxx（允许闭引号与值前引号）
	labeledSecretPattern = regexp.MustCompile(
		`(?i)(\b(?:password|passwd|token|api[\s_-]?key|secret)\b["']?\s*[:=]\s*["']?)([^\s"',}\]]+)`)

	// 3. HTTP Authorization 头：Authorization: Bearer xxx
	authorizationHeaderPattern = regexp.MustCompile(
		`(?i)(Authorization:\s*[A-Za-z][A-Za-z0-9._~-]*\s+)([^\s"',}\]]+)`)

	// 4. Cookie 头：Cookie: xxx / Set-Cookie: xxx（值到行尾）
	cookieHeaderPattern = regexp.MustCompile(
		`(?i)((?:Set-)?Cookie:\s*)([^\r\n]+)`)

	// 5. OpenAI 风格独立 key：sk-xxxxxxxxxxxxxxxx
	standaloneAPIKeyPattern = regexp.MustCompile(
		`\bsk-[A-Za-z0-9_-]{8,}\b`)

	// 6. Agent capability token：occap_xxx
	agentCapabilityPattern = regexp.MustCompile(
		`\boccap_[A-Za-z0-9_-]+\b`)
)

// alreadyRedacted 值已被脱敏（前缀判断：值捕获组可能不含闭合括号 ]）
func alreadyRedacted(val string) bool {
	return strings.HasPrefix(val, "[REDACTED")
}

// replaceGroups 对 re 的每个命中执行 f(groups)；f 返回替换文本
func replaceMatch(re *regexp.Regexp, s string, f func(groups []string, match string) string) string {
	return re.ReplaceAllStringFunc(s, func(m string) string {
		return f(re.FindStringSubmatch(m), m)
	})
}

// RedactText 对字符串逐模式脱敏（幂等：已含 [REDACTED] 的不再替换）
func RedactText(input string) string {
	if input == "" {
		return input
	}
	// 先处理整行/整段模式（Cookie 值到行尾，会吞掉行内其他模式的目标）
	input = replaceMatch(cookieHeaderPattern, input, func(g []string, m string) string {
		if alreadyRedacted(g[2]) {
			return m
		}
		return g[1] + Redacted
	})
	input = replaceMatch(authorizationHeaderPattern, input, func(g []string, m string) string {
		if alreadyRedacted(g[2]) {
			return m
		}
		return g[1] + Redacted
	})
	input = replaceMatch(assignmentSecretPattern, input, func(g []string, m string) string {
		if alreadyRedacted(g[2]) {
			return m
		}
		return g[1] + "=" + Redacted
	})
	input = replaceMatch(labeledSecretPattern, input, func(g []string, m string) string {
		if alreadyRedacted(g[2]) {
			return m
		}
		return g[1] + Redacted
	})
	input = standaloneAPIKeyPattern.ReplaceAllString(input, Redacted)
	input = agentCapabilityPattern.ReplaceAllString(input, Redacted)
	return input
}

// sensitiveFields 按字段名判定敏感（归一化：小写 + 去 - _ 空格 后匹配）
var sensitiveFields = map[string]bool{
	"accesstoken": true, "authorization": true, "cookie": true,
	"expiresat": true, "password": true, "passwd": true, "setcookie": true,
}

// sensitiveSuffixes 字段名归一化后的敏感后缀（apikey / callertoken 等家族）
var sensitiveSuffixes = []string{"apikey", "key", "token", "secret", "password", "passwd"}

// normalizeFieldName 字段名归一化：小写并去除分隔符（api-key / api_key / apiKey 归一到同名族）
func normalizeFieldName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, "-", "")
	name = strings.ReplaceAll(name, "_", "")
	name = strings.ReplaceAll(name, " ", "")
	return name
}

// isSensitiveField 判定字段名是否敏感（精确名单 + 后缀族匹配）
func isSensitiveField(name string) bool {
	n := normalizeFieldName(name)
	if sensitiveFields[n] {
		return true
	}
	for _, suffix := range sensitiveSuffixes {
		if strings.HasSuffix(n, suffix) {
			return true
		}
	}
	return false
}

// RedactValue 递归脱敏任意结构：
//   - string → RedactText
//   - slice/array → 逐项递归（返回 []any）
//   - map（字符串 key）→ key 命中敏感名单直接 [REDACTED]，否则 value 递归（返回新 map）
//   - struct → 遍历导出字段，字段名 / json tag 命中敏感名单直接替换为 [REDACTED]（返回 map）
//   - 指针 / 接口 → 解引用递归；其他类型原样返回
//
// 返回新值，不修改入参。
func RedactValue(input any) any {
	return redactValue(reflect.ValueOf(input), "")
}

func redactValue(v reflect.Value, fieldName string) any {
	if !v.IsValid() {
		return nil
	}

	// 字段名命中敏感名单：整个值替换（不再深入）
	if fieldName != "" && isSensitiveField(fieldName) {
		return Redacted
	}

	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return redactValue(v.Elem(), "")

	case reflect.String:
		return RedactText(v.String())

	case reflect.Slice, reflect.Array:
		out := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			out[i] = redactValue(v.Index(i), "")
		}
		return out

	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return v.Interface() // 非字符串 key 的 map 原样返回
		}
		out := make(map[string]any, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			k := iter.Key().String()
			out[k] = redactValue(iter.Value(), k)
		}
		return out

	case reflect.Struct:
		out := make(map[string]any, v.NumField())
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue // 未导出字段跳过
			}
			name := f.Name
			if tag := f.Tag.Get("json"); tag != "" {
				if idx := strings.IndexByte(tag, ','); idx >= 0 {
					tag = tag[:idx]
				}
				if tag == "-" {
					continue
				}
				if tag != "" {
					name = tag
				}
			}
			out[name] = redactValue(v.Field(i), name)
		}
		return out

	default:
		return v.Interface()
	}
}

// RedactTextf 便捷形式：fmt.Sprintf 后立即 RedactText（错误日志拼接上游响应体用）
func RedactTextf(format string, args ...any) string {
	return RedactText(fmt.Sprintf(format, args...))
}
