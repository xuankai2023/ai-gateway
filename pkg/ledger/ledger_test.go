package ledger

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newTestClient 启动 miniredis 并返回 go-redis 客户端，用 t.Cleanup 自动停
func newTestClient(t *testing.T) (*goredis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	cli := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cli.Close() })
	return cli, mr
}

// seedBalance 给租户预写一笔 balance（μUSD），方便直接测试特定场景
func seedBalance(ctx context.Context, cli *goredis.Client, tenantID int64, balance MicroUSD) {
	cli.HSet(ctx, TenantKey(tenantID),
		"balance", strconv.FormatInt(int64(balance), 10),
		"trust_quota", strconv.FormatInt(int64(trustQuotaDefault), 10),
	)
}

const (
	tidLarge   int64 = 1001 // balance 30 USD，远大于 trust_quota（10 USD）
	tidSmall   int64 = 1002 // balance 5 USD，小于 trust_quota → 跳过预占
	tidTrustEq int64 = 1003 // balance 10 USD，等于 trust_quota → 跳过（<=）
)

func TestReservesV2(t *testing.T) {
	ctx := context.Background()

	t.Run("balance>trust_quota 成功预占", func(t *testing.T) {
		cli, _ := newTestClient(t)
		seedBalance(ctx, cli, tidLarge, FromDollars(30))

		reservedAfter, skipped, available, blocked, err := ReservesV2(ctx, cli, tidLarge, FromDollars(1.5))
		require.NoError(t, err)
		require.False(t, skipped)
		require.False(t, blocked)
		require.Equal(t, FromDollars(1.5), reservedAfter)
		require.Equal(t, FromDollars(30), available)

		_, reserved, _, err := GetBalance(ctx, cli, tidLarge)
		require.NoError(t, err)
		require.Equal(t, FromDollars(1.5), reserved)
	})

	t.Run("balance<=trust_quota 跳过预占", func(t *testing.T) {
		cli, _ := newTestClient(t)
		seedBalance(ctx, cli, tidSmall, FromDollars(5))

		reservedAfter, skipped, _, blocked, err := ReservesV2(ctx, cli, tidSmall, FromDollars(1.5))
		require.NoError(t, err)
		require.True(t, skipped)
		require.False(t, blocked)
		require.Equal(t, MicroUSD(0), reservedAfter)
	})

	t.Run("balance==trust_quota 边界值也跳过", func(t *testing.T) {
		cli, _ := newTestClient(t)
		seedBalance(ctx, cli, tidTrustEq, FromDollars(10))

		_, skipped, _, _, err := ReservesV2(ctx, cli, tidTrustEq, FromDollars(1.5))
		require.NoError(t, err)
		require.True(t, skipped)
	})

	t.Run("A2 可用余额不足 → blocked 且不修改 reserved", func(t *testing.T) {
		cli, _ := newTestClient(t)
		// trust_quota=0，强制走余额校验：balance 5 USD，reserved 0
		cli.HSet(ctx, TenantKey(1004), "balance", "5000000", "trust_quota", "0")

		reservedAfter, skipped, available, blocked, err := ReservesV2(ctx, cli, 1004, FromDollars(6))
		require.NoError(t, err)
		require.False(t, skipped)
		require.True(t, blocked, "available 5 < estimated 6 必须拦截")
		require.Equal(t, MicroUSD(0), reservedAfter)
		require.Equal(t, FromDollars(5), available)

		_, reserved, _, err := GetBalance(ctx, cli, 1004)
		require.NoError(t, err)
		require.Equal(t, MicroUSD(0), reserved, "被拦截时不得修改 reserved")
	})

	t.Run("可用余额 = reserved 扣减后的值", func(t *testing.T) {
		cli, _ := newTestClient(t)
		cli.HSet(ctx, TenantKey(1005), "balance", "20000000", "reserved", "15000000", "trust_quota", "0")

		_, _, available, blocked, err := ReservesV2(ctx, cli, 1005, FromDollars(6))
		require.NoError(t, err)
		require.True(t, blocked)
		require.Equal(t, FromDollars(5), available)
	})

	t.Run("nil client 返回错误", func(t *testing.T) {
		_, _, _, _, err := ReservesV2(ctx, nil, tidLarge, FromDollars(1))
		require.Error(t, err)
	})
}

func TestSettles(t *testing.T) {
	ctx := context.Background()

	t.Run("预占多了，退回差额", func(t *testing.T) {
		cli, _ := newTestClient(t)
		seedBalance(ctx, cli, 1001, FromDollars(30))
		cli.HSet(ctx, TenantKey(1001), "reserved", strconv.FormatInt(int64(FromDollars(1)), 10))

		balAfter, err := Settles(ctx, cli, 1001, FromDollars(1), FromDollars(0.7))
		require.NoError(t, err)
		// delta = 0.7 - 1 = -0.3 → balance -= -0.3 → 30.3
		require.Equal(t, FromDollars(30.3), balAfter)

		_, reserved, _, err := GetBalance(ctx, cli, 1001)
		require.NoError(t, err)
		require.Equal(t, MicroUSD(0), reserved, "reserved 应该被清 0")
	})

	t.Run("预占不够，补扣差额", func(t *testing.T) {
		cli, _ := newTestClient(t)
		seedBalance(ctx, cli, 1002, FromDollars(30))
		cli.HSet(ctx, TenantKey(1002), "reserved", strconv.FormatInt(int64(FromDollars(1)), 10))

		balAfter, err := Settles(ctx, cli, 1002, FromDollars(1), FromDollars(1.5))
		require.NoError(t, err)
		require.Equal(t, FromDollars(29.5), balAfter)
	})

	t.Run("reserved=0 走 trust_quota 路径，直接扣", func(t *testing.T) {
		cli, _ := newTestClient(t)
		seedBalance(ctx, cli, 1003, FromDollars(5))

		balAfter, err := Settles(ctx, cli, 1003, 0, FromDollars(0.7))
		require.NoError(t, err)
		require.Equal(t, FromDollars(4.3), balAfter)
	})

	t.Run("负数金额拒绝", func(t *testing.T) {
		cli, _ := newTestClient(t)
		_, err := Settles(ctx, cli, 1001, -1, 1)
		require.Error(t, err)
	})

	t.Run("nil client 返回错误", func(t *testing.T) {
		_, err := Settles(ctx, nil, 1001, FromDollars(1), FromDollars(0.7))
		require.Error(t, err)
	})
}

func TestRefund(t *testing.T) {
	ctx := context.Background()

	t.Run("部分退回", func(t *testing.T) {
		cli, _ := newTestClient(t)
		seedBalance(ctx, cli, 2001, FromDollars(30))
		cli.HSet(ctx, TenantKey(2001), "reserved", strconv.FormatInt(int64(FromDollars(1)), 10))

		require.NoError(t, Refund(ctx, cli, 2001, FromDollars(0.4)))

		_, reserved, _, err := GetBalance(ctx, cli, 2001)
		require.NoError(t, err)
		require.Equal(t, FromDollars(0.6), reserved)
	})

	t.Run("全额退回并清 0 / reserved_at", func(t *testing.T) {
		cli, _ := newTestClient(t)
		seedBalance(ctx, cli, 2002, FromDollars(30))
		cli.HSet(ctx, TenantKey(2002), "reserved", strconv.FormatInt(int64(FromDollars(1)), 10), "reserved_at", "now")

		require.NoError(t, Refund(ctx, cli, 2002, FromDollars(1)))

		_, reserved, _, err := GetBalance(ctx, cli, 2002)
		require.NoError(t, err)
		require.Equal(t, MicroUSD(0), reserved)

		val, err := cli.HGet(ctx, TenantKey(2002), "reserved_at").Result()
		require.Error(t, err, "reserved_at 应该被 HDEL 掉")
		require.Empty(t, val)
	})

	t.Run("退多了不出现负 reserved", func(t *testing.T) {
		cli, _ := newTestClient(t)
		seedBalance(ctx, cli, 2003, FromDollars(30))
		cli.HSet(ctx, TenantKey(2003), "reserved", strconv.FormatInt(int64(FromDollars(0.5)), 10))

		require.NoError(t, Refund(ctx, cli, 2003, FromDollars(2)))

		_, reserved, _, err := GetBalance(ctx, cli, 2003)
		require.NoError(t, err)
		require.Equal(t, MicroUSD(0), reserved)
	})

	t.Run("负数金额拒绝", func(t *testing.T) {
		cli, _ := newTestClient(t)
		require.Error(t, Refund(ctx, cli, 2001, -1))
	})

	t.Run("nil client 返回错误", func(t *testing.T) {
		require.Error(t, Refund(ctx, nil, 2001, FromDollars(1)))
	})
}

func TestGetBalance(t *testing.T) {
	ctx := context.Background()

	t.Run("正常返回三字段", func(t *testing.T) {
		cli, _ := newTestClient(t)
		cli.HSet(ctx, TenantKey(3001),
			"balance", strconv.FormatInt(int64(FromDollars(100)), 10),
			"reserved", strconv.FormatInt(int64(FromDollars(2.5)), 10),
			"trust_quota", strconv.FormatInt(int64(FromDollars(20)), 10),
		)

		bal, res, tq, err := GetBalance(ctx, cli, 3001)
		require.NoError(t, err)
		require.Equal(t, FromDollars(100), bal)
		require.Equal(t, FromDollars(2.5), res)
		require.Equal(t, FromDollars(20), tq)
	})

	t.Run("新租户键不存在时 trust_quota 回落默认值", func(t *testing.T) {
		cli, _ := newTestClient(t)
		bal, res, tq, err := GetBalance(ctx, cli, 3002)
		require.NoError(t, err)
		require.Equal(t, MicroUSD(0), bal)
		require.Equal(t, MicroUSD(0), res)
		require.Equal(t, trustQuotaDefault, tq)
	})

	t.Run("nil client 返回错误", func(t *testing.T) {
		_, _, _, err := GetBalance(ctx, nil, 3001)
		require.Error(t, err)
	})
}

func TestReservesThenSettleFullFlow(t *testing.T) {
	ctx := context.Background()
	cli, _ := newTestClient(t)
	const tenantID int64 = 9999

	seedBalance(ctx, cli, tenantID, FromDollars(50))

	reservedAfter, skipped, _, _, err := ReservesV2(ctx, cli, tenantID, FromDollars(1.2))
	require.NoError(t, err)
	require.False(t, skipped)
	require.Equal(t, FromDollars(1.2), reservedAfter)

	balAfter, err := Settles(ctx, cli, tenantID, FromDollars(1.2), FromDollars(1))
	require.NoError(t, err)
	require.Equal(t, FromDollars(50.2), balAfter)

	_, reserved, _, err := GetBalance(ctx, cli, tenantID)
	require.NoError(t, err)
	require.Equal(t, MicroUSD(0), reserved)
}

func TestMultiReservesAggregate(t *testing.T) {
	ctx := context.Background()
	cli, _ := newTestClient(t)
	seedBalance(ctx, cli, 7777, FromDollars(100))

	for i := 0; i < 3; i++ {
		_, _, _, blocked, err := ReservesV2(ctx, cli, 7777, FromDollars(0.5))
		require.NoError(t, err)
		require.False(t, blocked)
	}

	_, reserved, _, err := GetBalance(ctx, cli, 7777)
	require.NoError(t, err)
	require.Equal(t, FromDollars(1.5), reserved)
}

// TestA1IntegerPrecisionNoDrift 预占 / 结算 100% 整数，任意次循环后余额精确归零。
//
// 默认 10,000 次（CI 快速）；LEDGER_STRESS=1 时执行验收要求的 1,000,000 次。
func TestA1IntegerPrecisionNoDrift(t *testing.T) {
	ctx := context.Background()
	cli, _ := newTestClient(t)

	iterations := int64(10_000)
	if os.Getenv("LEDGER_STRESS") == "1" {
		iterations = 1_000_000
	}

	const tenantID int64 = 424242
	// 每轮预占 1 μUSD、消费 1 μUSD，余额始终保持 1 USD
	// trust_quota=0：强制走预占校验，避免信任额度跳过
	cli.HSet(ctx, TenantKey(tenantID),
		"balance", "1000000",
		"trust_quota", "0",
	)

	for i := int64(0); i < iterations; i++ {
		_, skipped, _, blocked, err := ReservesV2(ctx, cli, tenantID, 1)
		if err != nil || skipped || blocked {
			t.Fatalf("第 %d 轮预占失败: skipped=%v blocked=%v err=%v", i+1, skipped, blocked, err)
		}
		if _, err := Settles(ctx, cli, tenantID, 1, 1); err != nil {
			t.Fatalf("第 %d 轮结算失败: %v", i+1, err)
		}
	}

	balance, reserved, _, err := GetBalance(ctx, cli, tenantID)
	require.NoError(t, err)
	require.Equal(t, MicroUSD(1_000_000), balance, "循环 %d 次后余额必须精确不变（无浮点漂移）", iterations)
	require.Equal(t, MicroUSD(0), reserved)
}
