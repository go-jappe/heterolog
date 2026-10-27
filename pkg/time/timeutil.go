package time

import "time"

// IsOneWeekPassed reports whether the current time is at or after the same
// clock time seven days after targetTime, in the location of targetTime. Across
// a daylight saving time change, that week can be shorter or longer than 168
// hours.
func IsOneWeekPassed(targetTime time.Time) bool {
	now := time.Now()

	// targetTimeのlocationで、7日後の同じ時刻を計算します。
	oneWeekLater := targetTime.AddDate(0, 0, 7)

	// 現在時刻がその時刻以降かを判定します。
	return now.After(oneWeekLater) || now.Equal(oneWeekLater)
}

// IsDateDifferent reports whether the dates of t1 and t2 differ, each read in
// its own location.
func IsDateDifferent(t1, t2 time.Time) bool {
	// 年月日を比較
	return t1.Year() != t2.Year() || t1.Month() != t2.Month() || t1.Day() != t2.Day()
}

// IsMonthDifferent reports whether the year and month of t1 and t2 differ, each
// read in its own location.
func IsMonthDifferent(t1, t2 time.Time) bool {
	return t1.Year() != t2.Year() || t1.Month() != t2.Month()
}

// GetLastSunday returns the Sunday of the week of t.
func GetLastSunday(t time.Time) time.Time {
	weekday := t.Weekday()                       // 現在の曜日
	lastSunday := t.AddDate(0, 0, -int(weekday)) // 現在の日付から曜日分を引く
	return lastSunday
}

// GetLastWeekday returns the most recent targetWeekday on or before the date of
// t, at the clock time and in the location of t. When t falls on
// targetWeekday, it returns t itself; otherwise the result is one to six days
// earlier.
func GetLastWeekday(t time.Time, targetWeekday time.Weekday) time.Time {
	// (今日の曜日 - 取得したい曜日 + 7) % 7 で、今日を含めて最大6日前までの同じ曜日を求めます。
	// 今日が指定した曜日なら0になり、今日を返します。
	offset := (int(t.Weekday()) - int(targetWeekday) + 7) % 7
	return t.AddDate(0, 0, -offset)
}

// StartOfDay returns 0:00 on the date of t.
func StartOfDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}
