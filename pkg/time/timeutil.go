package time

import "time"

// 指定された時刻から日付ベースで1週間経過しているか判定
func IsOneWeekPassed(targetTime time.Time) bool {
	// 現在時刻（ローカルタイム）
	now := time.Now()

	// 7日後の日付を計算
	oneWeekLater := targetTime.AddDate(0, 0, 7)

	// 現在の日付が7日後以降かを判定
	return now.After(oneWeekLater) || now.Equal(oneWeekLater)
}

// 日付が異なるか判定する関数
func IsDateDifferent(t1, t2 time.Time) bool {
	// 年月日を比較
	return t1.Year() != t2.Year() || t1.Month() != t2.Month() || t1.Day() != t2.Day()
}

// 年月が異るか比較
func IsMonthDifferent(t1, t2 time.Time) bool {
	return t1.Year() != t2.Year() || t1.Month() != t2.Month()
}

// その週の日曜日を取得する関数
func GetLastSunday(t time.Time) time.Time {
	weekday := t.Weekday()                       // 現在の曜日
	lastSunday := t.AddDate(0, 0, -int(weekday)) // 現在の日付から曜日分を引く
	return lastSunday
}

// 直近の指定した曜日の日付を取得する関数（過去1週間以内）
func GetLastWeekday(t time.Time, targetWeekday time.Weekday) time.Time {
	currentWeekday := t.Weekday()

	// (今日の曜日 - 取得したい曜日 + 7) % 7 を使うことで、最大でも6日前の同じ曜日を取得
	offset := (int(currentWeekday) - int(targetWeekday) + 7) % 7

	// 今日が指定した曜日なら、1週間前を取得
	if offset == 0 {
		offset = 7
	}

	return t.AddDate(0, 0, -offset)
}

// 引数の日付の0:00を返す
func StartOfDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}
