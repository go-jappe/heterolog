package cloudwatchlogs

import (
	"errors"
	"testing"
)

func TestStreamRoutingNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value StreamRouting
		want  StreamRouting
	}{
		{name: "zero value defaults to none", want: StreamRoutingNone},
		{name: "blank defaults to none", value: " \t\n", want: StreamRoutingNone},
		{name: "canonical value", value: StreamRoutingDaily, want: StreamRoutingDaily},
		{name: "trims and folds case", value: "  HoUrLy  ", want: StreamRoutingHourly},
		{name: "unsupported value remains normalized", value: "  WeEkLy  ", want: "weekly"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.value.Normalize(); got != tt.want {
				t.Fatalf("Normalize(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestStreamRoutingValidate(t *testing.T) {
	t.Parallel()

	valid := []StreamRouting{"", StreamRoutingNone, StreamRoutingMonthly, StreamRoutingDaily, StreamRoutingHourly, "  DAILY  "}
	for _, granularity := range valid {
		granularity := granularity
		t.Run("accepts_"+string(granularity.Normalize()), func(t *testing.T) {
			t.Parallel()

			if err := granularity.Validate(); err != nil {
				t.Fatalf("StreamRouting(%q).Validate() error = %v", granularity, err)
			}
		})
	}

	err := StreamRouting("weekly").Validate()
	var invalid *InvalidStreamRoutingError
	if !errors.Is(err, ErrInvalidStreamRouting) || !errors.As(err, &invalid) || invalid.Granularity != "weekly" {
		t.Fatalf("StreamRouting(%q).Validate() error = %v, want InvalidStreamRoutingError", "weekly", err)
	}
}

// 設定値の誤りを利用者へ示すmessageとして、対応値の一覧を含めます。
func TestInvalidStreamRoutingErrorListsSupportedValues(t *testing.T) {
	t.Parallel()

	want := `invalid cloudwatch logs stream routing granularity "weekly": must be one of none, monthly, daily, hourly`
	if got := (&InvalidStreamRoutingError{Granularity: "weekly"}).Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}
