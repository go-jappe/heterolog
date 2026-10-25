package logger

import (
	"errors"
	"testing"
	"time"
)

// 0以下の待機時間は、ConfigFieldErrorの原因のSlackIntervalErrorで、Intervalsの中の位置を
// 特定できます。同じ値の要素が複数あっても、最初の不正な要素の位置を返します。
func TestValidateSlackIntervalsIdentifiesNonPositiveIntervalByIndex(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		intervals []time.Duration
		wantIndex int
	}{
		{name: "negative only interval", intervals: []time.Duration{-time.Second}, wantIndex: 0},
		{name: "zero first interval", intervals: []time.Duration{0, time.Minute}, wantIndex: 0},
		{name: "zero later interval", intervals: []time.Duration{time.Minute, 0}, wantIndex: 1},
		{name: "first of several invalid intervals", intervals: []time.Duration{time.Minute, 2 * time.Minute, 0, -time.Second}, wantIndex: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateSlackIntervals(tt.intervals)
			if fieldErr := findFieldError(err, ConfigFieldIntervals, ErrNonPositiveSetting); fieldErr == nil {
				t.Fatalf("ValidateSlackIntervals() error = %v, want non-positive %s", err, ConfigFieldIntervals)
			}
			var intervalErr *SlackIntervalError
			if !errors.As(err, &intervalErr) || intervalErr.Index != tt.wantIndex {
				t.Fatalf("ValidateSlackIntervals() error = %#v, want the interval at index %d", err, tt.wantIndex)
			}
		})
	}
}

// 空の一覧は要素の不正ではないため、位置を持ちません。
func TestValidateSlackIntervalsRejectsEmptyIntervalsWithoutIndex(t *testing.T) {
	t.Parallel()

	err := ValidateSlackIntervals(nil)
	if fieldErr := findFieldError(err, ConfigFieldIntervals, ErrEmptySetting); fieldErr == nil {
		t.Fatalf("ValidateSlackIntervals(nil) error = %v, want empty %s", err, ConfigFieldIntervals)
	}
	var intervalErr *SlackIntervalError
	if errors.As(err, &intervalErr) {
		t.Fatalf("ValidateSlackIntervals(nil) error = %#v, want no interval position", err)
	}
	if err := ValidateSlackIntervals([]time.Duration{time.Second, time.Minute}); err != nil {
		t.Fatalf("ValidateSlackIntervals(valid) error = %v", err)
	}
}

// 不正な要素を利用者が特定できるよう、messageへ位置と値を含めます（設定errorとして表示されます）。
func TestSlackIntervalErrorFormat(t *testing.T) {
	t.Parallel()

	err := ValidateSlackIntervals([]time.Duration{time.Minute, 0})
	if want := "interval at index 1 must be positive: 0s"; err == nil || err.Error() != want {
		t.Fatalf("Error() = %v, want %q", err, want)
	}
}
