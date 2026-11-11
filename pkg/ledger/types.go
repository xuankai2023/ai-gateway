// Package ledger 账本引擎：基于 Redis Lua 的租户余额预占 / 结算 / 退款三件套
//
// 金额精度（D1）：所有账本 / 定价 / 计费金额统一使用 MicroUSD（1 USD = 1,000,000 μUSD），
// 全链路 int64 整数运算，彻底消灭 float64 累积误差。
package ledger

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// MicroUSD 微美元金额（1 USD = 1,000,000 μUSD）
type MicroUSD int64

// MicroUSDPerDollar 1 美元对应的微美元数
const MicroUSDPerDollar MicroUSD = 1_000_000

// FromDollars 美元浮点 → 微美元（四舍五入到最小单位）
func FromDollars(f float64) MicroUSD {
	return MicroUSD(math.Round(f * float64(MicroUSDPerDollar)))
}

// Dollars 微美元 → 美元浮点（仅展示 / 兼容外部 API 使用，内部计算禁止转回 float）
func (m MicroUSD) Dollars() float64 {
	return float64(m) / float64(MicroUSDPerDollar)
}

// String 微美元 → 保留 6 位小数的美元字符串（负数带前导 -）
func (m MicroUSD) String() string {
	v := int64(m)
	sign := ""
	if v < 0 {
		sign = "-"
		// 避免 math.MinInt64 取反溢出
		if v == math.MinInt64 {
			return "-9223372036854.775808"
		}
		v = -v
	}
	return fmt.Sprintf("%s%d.%06d", sign, v/int64(MicroUSDPerDollar), v%int64(MicroUSDPerDollar))
}

// ParseMicroUSD 解析字符串化的 int64 微美元
func ParseMicroUSD(s string) (MicroUSD, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ledger: parse MicroUSD %q: %w", s, err)
	}
	return MicroUSD(v), nil
}
