package otlpcommon

import (
	"errors"
	"math"
	"testing"
)

func TestPlainValueKeepsScalarsAndWritesArraysAsJSON(t *testing.T) {
	for _, tc := range []struct {
		in   any
		want any
	}{
		{"a.b", "a.b"},
		{true, true},
		{7, int64(7)},
		{uint64(math.MaxUint64), "18446744073709551615"},
		{1.5, 1.5},
		{math.NaN(), "NaN"},
		{math.Inf(-1), "-Infinity"},
		{[]string{"x", "<y>"}, `["x","<y>"]`},
		{[]any{1, 2.5, false, math.Inf(1)}, `[1,2.5,false,"Infinity"]`},
		{errors.New("boom"), "boom"},
	} {
		if got := PlainValue(tc.in); got != tc.want {
			t.Errorf("PlainValue(%#v) = %#v, want %#v", tc.in, got, tc.want)
		}
	}
}

func TestStringValueWritesEveryValueAsText(t *testing.T) {
	for _, tc := range []struct {
		in   any
		want string
	}{
		{"text", "text"},
		{false, "false"},
		{int64(-3), "-3"},
		{0.25, "0.25"},
		{1e21, "1e+21"},
		{math.NaN(), "NaN"},
		{[]int64{1, 2}, "[1,2]"},
	} {
		if got := StringValue(tc.in); got != tc.want {
			t.Errorf("StringValue(%#v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
