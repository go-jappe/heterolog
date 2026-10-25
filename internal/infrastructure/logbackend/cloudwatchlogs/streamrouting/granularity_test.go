package streamrouting

import (
	"errors"
	"testing"
	"time"
)

func TestNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value Granularity
		want  Granularity
	}{
		{name: "zero value defaults to none", want: None},
		{name: "blank defaults to none", value: " \t\n", want: None},
		{name: "canonical value", value: Daily, want: Daily},
		{name: "trims and folds case", value: "  HoUrLy  ", want: Hourly},
		{name: "unsupported value remains normalized", value: "  WeEkLy  ", want: "weekly"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := Normalize(tt.value); got != tt.want {
				t.Fatalf("Normalize(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestGranularityValidate(t *testing.T) {
	t.Parallel()

	valid := []Granularity{"", None, Monthly, Daily, Hourly, "  DAILY  "}
	for _, granularity := range valid {
		granularity := granularity
		t.Run("accepts_"+string(Normalize(granularity)), func(t *testing.T) {
			t.Parallel()

			if err := granularity.Validate(); err != nil {
				t.Fatalf("Granularity(%q).Validate() error = %v", granularity, err)
			}
		})
	}

	err := Granularity("weekly").Validate()
	var invalid *InvalidGranularityError
	if !errors.Is(err, ErrInvalidGranularity) || !errors.As(err, &invalid) || invalid.Granularity != "weekly" {
		t.Fatalf("Granularity(%q).Validate() error = %v, want InvalidGranularityError", "weekly", err)
	}
}

// 設定値の誤りを利用者へ示すmessageとして、対応値の一覧を含めます。
func TestInvalidGranularityErrorListsSupportedValues(t *testing.T) {
	t.Parallel()

	want := `invalid cloudwatch logs stream routing granularity "weekly": must be one of none, monthly, daily, hourly`
	if got := (&InvalidGranularityError{Granularity: "weekly"}).Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestSuffix(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("JST", 9*60*60)
	at := time.Date(2026, time.August, 13, 17, 42, 0, 0, location)
	tests := []struct {
		name        string
		granularity Granularity
		want        string
	}{
		{name: "zero value", want: ""},
		{name: "none", granularity: None, want: ""},
		{name: "monthly", granularity: Monthly, want: "2026/08"},
		{name: "daily", granularity: Daily, want: "2026/08/13"},
		{name: "hourly", granularity: Hourly, want: "2026/08/13/17"},
		{name: "normalizes value", granularity: " HOURLY ", want: "2026/08/13/17"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Suffix(tt.granularity, at)
			if err != nil {
				t.Fatalf("Suffix(%q, at) error = %v", tt.granularity, err)
			}
			if got != tt.want {
				t.Fatalf("Suffix(%q, at) = %q, want %q", tt.granularity, got, tt.want)
			}
		})
	}
}

func TestSuffixUsesProvidedTimeLocation(t *testing.T) {
	t.Parallel()

	instantUTC := time.Date(2026, time.August, 31, 15, 30, 0, 0, time.UTC)
	jst := time.FixedZone("JST", 9*60*60)

	utcSuffix, err := Suffix(Daily, instantUTC)
	if err != nil {
		t.Fatalf("Suffix(Daily, UTC) error = %v", err)
	}
	jstSuffix, err := Suffix(Daily, instantUTC.In(jst))
	if err != nil {
		t.Fatalf("Suffix(Daily, JST) error = %v", err)
	}
	if utcSuffix != "2026/08/31" {
		t.Fatalf("UTC suffix = %q, want %q", utcSuffix, "2026/08/31")
	}
	if jstSuffix != "2026/09/01" {
		t.Fatalf("JST suffix = %q, want %q", jstSuffix, "2026/09/01")
	}
}

func TestSuffixRejectsUnsupportedGranularity(t *testing.T) {
	t.Parallel()

	_, err := Suffix("weekly", time.Now())
	if !errors.Is(err, ErrInvalidGranularity) {
		t.Fatalf("Suffix(weekly, at) error = %v, want ErrInvalidGranularity", err)
	}
}
