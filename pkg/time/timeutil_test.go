package time_test

import (
	"testing"
	"time"

	pkgtime "github.com/go-jappe/heterolog/pkg/time"
)

// 指定した曜日の当日はその日を返し、それ以外の日は1〜6日前の同じ曜日を返します。
func TestGetLastWeekdayIncludesTheTargetDay(t *testing.T) {
	// 2036-01-06は日曜日です。
	sunday := time.Date(2036, time.January, 6, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		at   time.Time
		want string
	}{
		{name: "start of the target day", at: sunday, want: "20360106"},
		{name: "end of the target day", at: sunday.Add(24*time.Hour - time.Nanosecond), want: "20360106"},
		{name: "one day after", at: sunday.AddDate(0, 0, 1), want: "20360106"},
		{name: "six days after", at: sunday.AddDate(0, 0, 6), want: "20360106"},
		{name: "one week after", at: sunday.AddDate(0, 0, 7), want: "20360113"},
		{name: "the day before", at: sunday.Add(-time.Nanosecond), want: "20351230"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := pkgtime.GetLastWeekday(tc.at, time.Sunday)
			if date := got.Format("20060102"); date != tc.want {
				t.Fatalf("GetLastWeekday(%s) = %s, want %s", tc.at, date, tc.want)
			}
			if got.Weekday() != time.Sunday {
				t.Fatalf("GetLastWeekday(%s) weekday = %s, want Sunday", tc.at, got.Weekday())
			}
			if h, m, s := got.Clock(); h != tc.at.Hour() || m != tc.at.Minute() || s != tc.at.Second() || got.Location() != tc.at.Location() {
				t.Fatalf("GetLastWeekday(%s) = %s, want the clock time and location of the input", tc.at, got)
			}
		})
	}
}
