package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestCurrentPeriodLatestFileNameUsesClockTimezone(t *testing.T) {
	utcTime := time.Date(2026, time.August, 21, 15, 30, 0, 0, time.UTC)
	for _, test := range []struct {
		name string
		at   time.Time
		date string
	}{
		{"UTC", utcTime, "20260821"},
		{"JST", utcTime.In(time.FixedZone("JST", 9*60*60)), "20260822"},
		{"zero", time.Time{}, "00010101"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			const target = "clock-period"
			logDir := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(logDir, 0o750))
			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
				Config: testFileSeriesManagerConfigWithRotation(
					root, target, pkgfsstore.FileRotationDaily, 10, 0, 0, false,
				),
				Clock: pkgclock.NewFixedClock(test.at),
			})
			// Clockの値を別のtimezoneへ変換せず、日付判定にもそのまま使用します。
			require.Equal(t, test.at, manager.Clock.Now())
			require.Same(t, test.at.Location(), manager.Clock.Now().Location())
			fileName, err := manager.fileNamePattern()
			require.NoError(t, err)
			got, err := manager.currentPeriodLatestFileName(fileName)
			require.NoError(t, err)
			require.Equal(t, filepath.Join(logDir, target+"_"+test.date+"_1.log"), got)
		})
	}
}

func TestFileSeriesManagerKeepsAgePurgeStateAcrossDirectoryLifecycleReconstruction(t *testing.T) {
	utcTime := time.Date(2026, time.August, 21, 15, 30, 0, 0, time.UTC)
	for _, location := range []*time.Location{time.UTC, time.FixedZone("JST", 9*60*60)} {
		t.Run(location.String(), func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			const target = "runner-age-clock-state"
			logDir := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(logDir, 0o750))
			firstAttempt := utcTime.In(location)
			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
				Config:   testFileSeriesManagerConfig(root, target, 10, 1, 0, false),
				Clock:    pkgclock.NewFixedClock(firstAttempt),
			})

			report, err := manager.RunOnce(context.Background())
			require.NoError(t, err)
			require.True(t, report.Purge.ByAge.Ran)
			require.Equal(t, firstAttempt, manager.lastAgePurgeAt)
			require.Same(t, location, manager.lastAgePurgeAt.Location())

			// RunOnceがDirectoryLifecycleを作り直しても、Clock上の同日には再実行しません。
			manager.Clock = pkgclock.NewFixedClock(firstAttempt.Add(time.Hour))
			report, err = manager.RunOnce(context.Background())
			require.NoError(t, err)
			require.False(t, report.Purge.ByAge.Ran)
			require.Equal(t, firstAttempt, manager.lastAgePurgeAt)

			// 同じ9時間後でもUTCは翌日、JSTは同日です。Clockの日付だけで判定します。
			nextAttempt := firstAttempt.Add(9 * time.Hour)
			manager.Clock = pkgclock.NewFixedClock(nextAttempt)
			report, err = manager.RunOnce(context.Background())
			require.NoError(t, err)
			require.Equal(t, location == time.UTC, report.Purge.ByAge.Ran)
			if report.Purge.ByAge.Ran {
				require.Equal(t, nextAttempt, manager.lastAgePurgeAt)
			} else {
				require.Equal(t, firstAttempt, manager.lastAgePurgeAt)
			}
		})
	}
}
