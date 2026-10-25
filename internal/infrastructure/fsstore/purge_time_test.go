package fsstore

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestPurgeByFileNameDateIfNeededUsesProvidedTimezoneDayBoundary(t *testing.T) {
	location, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatalf("load timezone: %v", err)
	}
	// 東京では8月22日00:30ですが、同じ時刻のUTCはまだ8月21日です。
	// 期限日はnowのtimezoneの暦日から求め、ファイル名の日付と比べます。
	instant := time.Date(2026, time.August, 22, 0, 30, 0, 0, location)
	for _, test := range []struct {
		name          string
		now           time.Time
		removesOldest bool
	}{
		// 期限日は8月21日なので、8月20日のファイルを削除します。
		{name: "Asia/Tokyo", now: instant, removesOldest: true},
		// 期限日は8月20日なので、期限当日のファイルとして保持します。
		{name: "UTC", now: instant.In(time.UTC), removesOldest: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			// mtimeは名前の日付と逆の順序にし、判定に使わないことも確認します。
			oldest := createFileNamePurgeTestFile(t, directory, "app_20260820_1.log", test.now)
			retained := createFileNamePurgeTestFile(t, directory, "app_20260821_1.log", test.now.AddDate(0, 0, -100))

			result := PurgeByFileNameDateIfNeeded([]FileInfo{oldest, retained}, 1, test.now, time.Time{})
			if !result.Ran {
				t.Fatal("age purge did not run")
			}
			if err := JoinFileRemovalFailures(result.RemovalResults); err != nil {
				t.Fatalf("removal failures = %v", err)
			}
			wantRemoved, wantRemaining := []string{}, []FileInfo{oldest, retained}
			if test.removesOldest {
				wantRemoved, wantRemaining = []string{oldest.Path}, []FileInfo{retained}
			}
			if got := fileNamePurgeRemovedPaths(result); !reflect.DeepEqual(got, wantRemoved) {
				t.Fatalf("removed = %v, want %v", got, wantRemoved)
			}
			if !reflect.DeepEqual(result.Remaining, wantRemaining) {
				t.Fatalf("remaining = %+v, want %+v", result.Remaining, wantRemaining)
			}
			for _, file := range wantRemaining {
				if _, err := os.Stat(file.Path); err != nil {
					t.Fatalf("retained file %s: %v", file.Path, err)
				}
			}
			if _, err := os.Stat(oldest.Path); test.removesOldest != errors.Is(err, os.ErrNotExist) {
				t.Fatalf("oldest file after purge: %v, want removed=%t", err, test.removesOldest)
			}
		})
	}
}

func TestShouldPurgeByAgeTodayComparesInCurrentPurgeTimezone(t *testing.T) {
	location, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatalf("load timezone: %v", err)
	}
	now := time.Date(2026, time.August, 22, 0, 30, 0, 0, location)
	lastSameTokyoDateStoredAsUTC := time.Date(2026, time.August, 21, 15, 10, 0, 0, time.UTC)

	if ShouldPurgeByAgeToday(now, lastSameTokyoDateStoredAsUTC) {
		t.Fatal("ShouldPurgeByAgeToday = true, want false within the configured timezone day")
	}
	if !ShouldPurgeByAgeToday(now, now.Add(-time.Hour)) {
		t.Fatal("ShouldPurgeByAgeToday = false, want true after the configured timezone day changed")
	}
}
