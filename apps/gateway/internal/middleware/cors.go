// CORS 跨域中间件
//
// 位置：第 5 层（Logger 之后、Auth 之前）
//
// 浏览器预检请求（OPTIONS）不会带 Authorization，因此必须在 Auth 之前返回 204，
// 否则预检会被鉴权拦成 401，真实请求根本发不出去。
//
// 允许来源来自配置 gateway.cors_allowed_origins：
//   - ["*"]               允许任意来源（本地开发默认）
//   - ["http://a.com"]    仅允许列出的来源
package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// CORS 跨域中间件
func CORS(allowedOrigins []string) gin.HandlerFunc {
	allowAll := false
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, origin := range allowedOrigins {
		if origin == "*" {
			allowAll = true
		}
		allowed[strings.TrimRight(origin, "/")] = struct{}{}
	}

	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin == "" {
			c.Next()
			return
		}

		if allowAll {
			c.Header("Access-Control-Allow-Origin", "*")
		} else if _, ok := allowed[strings.TrimRight(origin, "/")]; ok {
			// 回显具体来源，配合 Vary 避免缓存串域
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
		} else {
			// 不在白名单：不返回 CORS 头，浏览器会拦截；预检直接结束
			if c.Request.Method == http.MethodOptions {
				c.AbortWithStatus(http.StatusNoContent)
				return
			}
			c.Next()
			return
		}

		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type")
		c.Header("Access-Control-Max-Age", "86400")

		// 预检请求到此为止，不进入业务链路
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}
