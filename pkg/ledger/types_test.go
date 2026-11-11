package ledger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromDollarsExact(t *testing.T) {
	cases := []struct {
		usd  float64
		want MicroUSD
	}{
		{0, 0},
		{0.000001, 1},
		{0.0015, 1500},
		{0.01, 10_000},
		{1, 1_000_000},
		{10, 10_000_000},
		{0.123456, 123_456},
		{-0.0015, -1500},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, FromDollars(tc.usd), "FromDollars(%v)", tc.usd)
	}
}

func TestMicroUSDString(t *testing.T) {
	cases := []struct {
		m    MicroUSD
		want string
	}{
		{0, "0.000000"},
		{1, "0.000001"},
		{1500, "0.001500"},
		{1_000_000, "1.000000"},
		{-1500, "-0.001500"},
		{12_345_678, "12.345678"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, tc.m.String())
	}
}

func TestParseMicroUSD(t *testing.T) {
	v, err := ParseMicroUSD("1234567")
	require.NoError(t, err)
	require.Equal(t, MicroUSD(1_234_567), v)

	v, err = ParseMicroUSD("")
	require.NoError(t, err)
	require.Equal(t, MicroUSD(0), v)

	_, err = ParseMicroUSD("1.5")
	require.Error(t, err)
}

func TestDollarsRoundTrip(t *testing.T) {
	require.InDelta(t, 0.0015, FromDollars(0.0015).Dollars(), 1e-12)
	require.InDelta(t, 99.99, FromDollars(99.99).Dollars(), 1e-12)
}
