package ledger

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// trustQuotaDefault 新租户默认信任额度：$10 = 10,000,000 μUSD
const trustQuotaDefault MicroUSD = 10 * MicroUSDPerDollar

func ledgerKey(tenantID int64) string {
	return fmt.Sprintf("ledger:tenant:%d", tenantID)
}

// TenantKey 返回租户账本在 Redis 中的 HASH key（对账器复用，避免 key 格式漂移）。
func TenantKey(tenantID int64) string { return ledgerKey(tenantID) }

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case string:
		return strconv.ParseInt(x, 10, 64)
	case float64:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("ledger: unexpected lua return type %T", v)
	}
}

// Settles 结算：reserved 归零，按 finalCost 扣减真实余额，返回结算后余额。
func Settles(ctx context.Context, client *redis.Client, tenantID int64, reserved, finalCost MicroUSD) (balanceAfter MicroUSD, err error) {
	if client == nil {
		return 0, fmt.Errorf("ledger: redis client is nil")
	}
	if reserved < 0 || finalCost < 0 {
		return 0, fmt.Errorf("ledger: settle amounts must be non-negative")
	}
	res, err := scriptSettlesV2.Run(ctx, client, []string{ledgerKey(tenantID)},
		int64(reserved), int64(finalCost)).Result()
	if err != nil {
		return 0, err
	}
	n, err := toInt64(res)
	if err != nil {
		return 0, err
	}
	return MicroUSD(n), nil
}

// Refund 退款：释放预占金额。
func Refund(ctx context.Context, client *redis.Client, tenantID int64, amount MicroUSD) error {
	if client == nil {
		return fmt.Errorf("ledger: redis client is nil")
	}
	if amount < 0 {
		return fmt.Errorf("ledger: refund amount must be non-negative")
	}
	_, err := scriptRefundV2.Run(ctx, client, []string{ledgerKey(tenantID)}, int64(amount)).Result()
	return err
}

// GetBalance 读取账本快照。
func GetBalance(ctx context.Context, client *redis.Client, tenantID int64) (balance, reserved, trustQuota MicroUSD, err error) {
	if client == nil {
		return 0, 0, 0, fmt.Errorf("ledger: redis client is nil")
	}
	res, err := client.HGetAll(ctx, ledgerKey(tenantID)).Result()
	if err != nil {
		return 0, 0, 0, err
	}
	parse := func(s string) MicroUSD {
		v, _ := ParseMicroUSD(s)
		return v
	}
	trust := trustQuotaDefault
	if s, ok := res["trust_quota"]; ok {
		trust = parse(s)
	}
	return parse(res["balance"]), parse(res["reserved"]), trust, nil
}
