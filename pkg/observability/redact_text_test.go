package observability

import (
	"reflect"
	"testing"
)

// TestRedactText 表驱动覆盖 6 类脱敏模式 + 幂等性
func TestRedactText(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", ""},
		{"no secret", "hello world, status ok", "hello world, status ok"},

		// 1. 赋值形式
		{"assignment key", `API_KEY=abc123xyz`, `API_KEY=[REDACTED]`},
		{"assignment password", `DB_PASSWORD=mysecret123`, `DB_PASSWORD=[REDACTED]`},
		{"assignment token lowercase", `auth_token=tok-xyz-123`, `auth_token=[REDACTED]`},
		{"assignment in sentence", `connect failed: DB_PASSWORD=hunter2 retrying`, `connect failed: DB_PASSWORD=[REDACTED] retrying`},

		// 2. 标签形式
		{"labeled password", `password: hunter2`, `password: [REDACTED]`},
		{"labeled token", `token: abc.def.ghi`, `token: [REDACTED]`},
		{"labeled api-key", `api-key: kksshh`, `api-key: [REDACTED]`},
		{"labeled api_key spaced", `api_key = somevalue123`, `api_key = [REDACTED]`},
		{"labeled secret json-ish", `"secret": "val123"`, `"secret": "[REDACTED]"`},

		// 3. Authorization 头
		{"authorization bearer", `Authorization: Bearer sk-abc123def456`, `Authorization: Bearer [REDACTED]`},
		{"authorization basic", `authorization: Basic dXNlcjpwYXNz`, `authorization: Basic [REDACTED]`},

		// 4. Cookie 头
		{"cookie", "Cookie: session=abc123; user=bob", "Cookie: [REDACTED]"},
		{"set-cookie", "Set-Cookie: sid=xyz; HttpOnly", "Set-Cookie: [REDACTED]"},

		// 5. 独立 sk- key
		{"standalone sk", `key is sk-abc123def456 here`, `key is [REDACTED] here`},

		// 6. occap token
		{"occap", `capability occap_abc123XYZ-foo granted`, `capability [REDACTED] granted`},

		// 幂等：已脱敏不重复替换
		{"idempotent assignment", `API_KEY=[REDACTED]`, `API_KEY=[REDACTED]`},
		{"idempotent cookie", "Cookie: [REDACTED]", "Cookie: [REDACTED]"},
		{"idempotent auth", `Authorization: Bearer [REDACTED]`, `Authorization: Bearer [REDACTED]`},
		{"idempotent labeled", `password: [REDACTED]`, `password: [REDACTED]`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RedactText(tc.input); got != tc.want {
				t.Errorf("RedactText(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// TestRedactText_ShortKeyNotMatched 短 sk- 不命中独立 key 模式（避免误伤普通文本）
func TestRedactText_ShortKeyNotMatched(t *testing.T) {
	if got := RedactText("sk-abc"); got != "sk-abc" {
		t.Errorf("short sk- should not be redacted, got %q", got)
	}
}

// TestRedactText_Idempotent 二次脱敏结果不变
func TestRedactText_Idempotent(t *testing.T) {
	s := `Authorization: Bearer sk-topsecret123 DB_PASSWORD=foo password: bar`
	once := RedactText(s)
	twice := RedactText(once)
	if once != twice {
		t.Errorf("not idempotent:\nonce:  %q\ntwice: %q", once, twice)
	}
}

// TestRedactValue_Map map 递归脱敏：敏感字段名整体替换，普通字符串走 RedactText
func TestRedactValue_Map(t *testing.T) {
	in := map[string]any{
		"password": "x",
		"data":     "ok",
		"nested": map[string]any{
			"Authorization": "Bearer sk-abc12345678",
			"note":          "fine",
		},
		"list": []any{"token: abc123", "plain"},
	}
	got := RedactValue(in).(map[string]any)

	if got["password"] != Redacted {
		t.Errorf("password = %v, want %s", got["password"], Redacted)
	}
	if got["data"] != "ok" {
		t.Errorf("data = %v, want ok", got["data"])
	}
	nested := got["nested"].(map[string]any)
	if nested["Authorization"] != Redacted {
		t.Errorf("nested.Authorization = %v, want %s", nested["Authorization"], Redacted)
	}
	list := got["list"].([]any)
	if list[0] != "token: [REDACTED]" {
		t.Errorf("list[0] = %v", list[0])
	}
	if list[1] != "plain" {
		t.Errorf("list[1] = %v", list[1])
	}
	// 入参不被修改
	if in["password"] != "x" {
		t.Errorf("input mutated: %v", in["password"])
	}
}

// TestRedactValue_Struct struct 字段名/json tag 命中敏感名单即替换
func TestRedactValue_Struct(t *testing.T) {
	type Creds struct {
		User      string `json:"user"`
		Password  string `json:"password"`
		APIToken  string `json:"api_token"`
		CallerKey string `json:"caller_key"` // 后缀 *key 命中
		Note      string `json:"note"`
	}
	got := RedactValue(Creds{User: "bob", Password: "pw", APIToken: "tok", CallerKey: "ck", Note: "n"}).(map[string]any)

	if got["user"] != "bob" {
		t.Errorf("user should not be redacted, got %v", got["user"])
	}
	for _, k := range []string{"password", "api_token", "caller_key"} {
		if got[k] != Redacted {
			t.Errorf("%s = %v, want %s", k, got[k], Redacted)
		}
	}
	if got["note"] != "n" {
		t.Errorf("note = %v, want n", got["note"])
	}
}

// TestRedactValue_FieldNameVariants 字段名归一化：api-key / apiKey / API_KEY 同族命中
func TestRedactValue_FieldNameVariants(t *testing.T) {
	for _, name := range []string{"apiKey", "api-key", "API_KEY", "apikey", "accessToken", "Access-Token"} {
		got := RedactValue(map[string]any{name: "secret-value"}).(map[string]any)
		if got[name] != Redacted {
			t.Errorf("field %q = %v, want %s", name, got[name], Redacted)
		}
	}
}

// TestRedactValue_ScalarPassthrough 非敏感标量原样返回
func TestRedactValue_ScalarPassthrough(t *testing.T) {
	if got := RedactValue(42); got != 42 {
		t.Errorf("int = %v", got)
	}
	if got := RedactValue(true); got != true {
		t.Errorf("bool = %v", got)
	}
	if got := RedactValue(nil); got != nil {
		t.Errorf("nil = %v", got)
	}
}

// TestRedactValue_NilMapEntry nil 值不 panic
func TestRedactValue_NilMapEntry(t *testing.T) {
	got := RedactValue(map[string]any{"k": nil}).(map[string]any)
	if got["k"] != nil {
		t.Errorf("nil entry = %v", got["k"])
	}
}

// TestRedactValue_NoSharedReference 返回的 map 是新对象
func TestRedactValue_NoSharedReference(t *testing.T) {
	in := map[string]any{"a": "b"}
	out := RedactValue(in).(map[string]any)
	if reflect.ValueOf(in).Pointer() == reflect.ValueOf(out).Pointer() {
		t.Error("output map should be a copy")
	}
}
