package creditlots

import (
	"context"
	"database/sql/driver"
	"testing"

	"ai-gateway/pkg/dbtest"
	"ai-gateway/pkg/ledger"
	"github.com/stretchr/testify/require"
)

// A10：先到期批次先扣，永久额度最后扣
//
//	lot_A 本月底过期，remaining=3
//	lot_B 永久，remaining=10
//	扣 5 → A 归零、B 剩 8
func TestA10DeductOrderByExpiryThenFIFO(t *testing.T) {
	ctx := context.Background()
	db, fake := dbtest.New(t)

	fake.ExpectQuery("FROM credit_lots").WillReturnRows(
		[]string{"id", "remaining"},
		[]driver.Value{int64(11), int64(3)},  // lot_A：先到期
		[]driver.Value{int64(22), int64(10)}, // lot_B：永久
	)
	fake.ExpectExec("UPDATE credit_lots SET remaining").WillReturnResult(0, 1)
	fake.ExpectExec("UPDATE credit_lots SET remaining").WillReturnResult(0, 1)

	res, err := Deduct(ctx, db, 7, ledger.MicroUSD(5))
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(5), res.Deducted)
	require.Equal(t, 2, res.Lots)

	calls := fake.Calls()
	require.Len(t, calls, 3)
	require.Equal(t, "query", calls[0].Method)
	// 第一次 UPDATE 扣 lot_A 的 3
	require.Equal(t, int64(3), namedInt(t, calls[1].Args[0]))
	require.Equal(t, int64(11), namedInt(t, calls[1].Args[1]))
	// 第二次 UPDATE 扣 lot_B 的 2
	require.Equal(t, int64(2), namedInt(t, calls[2].Args[0]))
	require.Equal(t, int64(22), namedInt(t, calls[2].Args[1]))

	fake.AssertExpectations(t)
}

func TestDeductSkipsWhenInsufficientLots(t *testing.T) {
	ctx := context.Background()
	db, fake := dbtest.New(t)

	fake.ExpectQuery("FROM credit_lots").WillReturnRows(
		[]string{"id", "remaining"},
		[]driver.Value{int64(1), int64(3)},
	)
	fake.ExpectExec("UPDATE credit_lots SET remaining").WillReturnResult(0, 1)

	res, err := Deduct(ctx, db, 7, ledger.MicroUSD(5))
	require.NoError(t, err)
	// 余额是后付费的：批次不够时只扣掉现有部分，不报错
	require.Equal(t, ledger.MicroUSD(3), res.Deducted)
	require.Equal(t, 1, res.Lots)
	fake.AssertExpectations(t)
}

func TestDeductSkipsConcurrentModifiedLot(t *testing.T) {
	ctx := context.Background()
	db, fake := dbtest.New(t)

	fake.ExpectQuery("FROM credit_lots").WillReturnRows(
		[]string{"id", "remaining"},
		[]driver.Value{int64(1), int64(3)},
		[]driver.Value{int64(2), int64(10)},
	)
	// lot 1 并发被改：affected=0 → 跳过；lot 2 成功
	fake.ExpectExec("UPDATE credit_lots SET remaining").WillReturnResult(0, 0)
	fake.ExpectExec("UPDATE credit_lots SET remaining").WillReturnResult(0, 1)

	res, err := Deduct(ctx, db, 7, ledger.MicroUSD(5))
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(5), res.Deducted)
	require.Equal(t, 1, res.Lots)
	fake.AssertExpectations(t)
}

func TestDeductNoop(t *testing.T) {
	ctx := context.Background()
	db, fake := dbtest.New(t)

	res, err := Deduct(ctx, db, 7, 0)
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(0), res.Deducted)
	require.Empty(t, fake.Calls())

	res, err = Deduct(ctx, nil, 7, ledger.MicroUSD(5))
	require.NoError(t, err)
	require.Equal(t, ledger.MicroUSD(0), res.Deducted)
}

// A9：过期批次 remaining 清零（不影响 Redis 余额）
func TestA9BurnExpired(t *testing.T) {
	ctx := context.Background()
	db, fake := dbtest.New(t)

	fake.ExpectExec("UPDATE credit_lots SET remaining = 0").WillReturnResult(0, 2)

	n, err := BurnExpired(ctx, db, 7)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	calls := fake.Calls()
	require.Len(t, calls, 1)
	require.Contains(t, calls[0].SQL, "expires_at < NOW(3)")
	require.Equal(t, int64(7), namedInt(t, calls[0].Args[0]))
	fake.AssertExpectations(t)
}

func TestBurnExpiredAll(t *testing.T) {
	ctx := context.Background()
	db, fake := dbtest.New(t)

	fake.ExpectExec("UPDATE credit_lots SET remaining = 0").WillReturnResult(0, 5)

	n, err := BurnExpiredAll(ctx, db)
	require.NoError(t, err)
	require.Equal(t, int64(5), n)
	fake.AssertExpectations(t)

	// nil DB 安全返回
	n, err = BurnExpired(ctx, nil, 7)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
}

func namedInt(t *testing.T, v driver.NamedValue) int64 {
	t.Helper()
	n, ok := v.Value.(int64)
	require.True(t, ok, "参数类型 = %T, want int64", v.Value)
	return n
}
