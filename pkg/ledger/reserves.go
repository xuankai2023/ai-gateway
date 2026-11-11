package ledger

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// ReservesV2 预占估算金额。
//
// 返回：
//   - reservedAfter：预占后的 reserved 总额（μUSD；跳过校验时等于原值）
//   - skipped：是否因信任额度跳过预占
//   - available：预占前的可用余额（μUSD）
//   - blocked：余额不足，调用方必须返回 402
func ReservesV2(ctx context.Context, client *redis.Client, tenantID int64, estimated MicroUSD) (reservedAfter MicroUSD, skipped bool, available MicroUSD, blocked bool, err error) {
	if client == nil {
		return 0, false, 0, false, fmt.Errorf("ledger: redis client is nil")
	}
	if estimated < 0 {
		return 0, false, 0, false, fmt.Errorf("ledger: estimated amount must be non-negative")
	}
	reservedAt := strconv.FormatInt(time.Now().UnixNano(), 10)
	res, err := scriptReservesV2.Run(ctx, client, []string{ledgerKey(tenantID)},
		int64(estimated), int64(trustQuotaDefault), reservedAt).Result()
	if err != nil {
		return 0, false, 0, false, err
	}
	vals, ok := res.([]any)
	if !ok || len(vals) != 5 {
		return 0, false, 0, false, fmt.Errorf("ledger: unexpected reserves result %#v", res)
	}
	nums := make([]int64, 5)
	for i := range vals {
		n, convErr := toInt64(vals[i])
		if convErr != nil {
			return 0, false, 0, false, convErr
		}
		nums[i] = n
	}
	return MicroUSD(nums[0]), nums[1] == 1, MicroUSD(nums[3]), nums[4] == 1, nil
}
