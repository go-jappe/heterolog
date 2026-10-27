package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 2036-01-05は土曜日、2036-01-06は日曜日です。日曜日に始まる週次rotationでは、
// 土曜日の書き込みは2035-12-30の期間のfileに入り、日曜日になった最初の書き込みで
// その日の日付のfileへ切り替わります。
var (
	weeklyRotationSaturday = time.Date(2036, time.January, 5, 23, 59, 0, 0, time.UTC)
	weeklyRotationSunday   = time.Date(2036, time.January, 6, 0, 0, 1, 0, time.UTC)
)

func TestFileLoggerStartsWeeklyFileOnRotationDay(t *testing.T) {
	config := newTestFileLoggerConfig(t.TempDir(), "weekly_rotation", "weekly")
	config.FsstoreConfig.Lifecycle.EnableRotate = true
	config.FsstoreConfig.Lifecycle.RotationType = pkgfsstore.FileRotationWeekly
	config.FsstoreConfig.Lifecycle.RotationDayOfWeek = time.Sunday

	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(config), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	write := func(at time.Time, message string) string {
		t.Helper()
		logger.Clock = clock.NewFixedClock(at)
		require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{
			OccurredAt: at,
			Level:      pkglogger.LogLevelInfo,
			Message:    message,
		}))
		return logger.Fsstore.ActiveFilePath
	}

	saturdayPath := write(weeklyRotationSaturday, "saturday entry")
	require.Equal(t, "weekly_20351230_1.log", filepath.Base(saturdayPath))
	sundayPath := write(weeklyRotationSunday, "sunday entry")
	require.Equal(t, "weekly_20360106_1.log", filepath.Base(sundayPath))
	require.Equal(t, "20360106", logger.Fsstore.FileName.Date)

	requireWeeklyRotationContents(t, saturdayPath, sundayPath)
}

func TestDeliveryFileLoggerStartsWeeklyFileOnRotationDay(t *testing.T) {
	config := newTestDeliveryFileLoggerConfig(t.TempDir(), "delivery_weekly_rotation", "delivery-weekly")
	config.FsstoreConfig.Rotation.EnableRotate = true
	config.FsstoreConfig.Rotation.RotationType = pkgfsstore.FileRotationWeekly
	config.FsstoreConfig.Rotation.RotationDayOfWeek = time.Sunday

	logger, err := NewDeliveryFileLogger(context.Background(), NewDeliveryFileLoggerConfigAdapter(config), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	write := func(at time.Time, message string) string {
		t.Helper()
		logger.Clock = clock.NewFixedClock(at)
		report, err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
			OccurredAt: at,
			Level:      pkglogger.LogLevelInfo,
			Message:    message,
		})
		requireDeliverySucceeded(t, report, err)
		return logger.Fsstore.ActiveFilePath
	}

	saturdayPath := write(weeklyRotationSaturday, "saturday entry")
	require.Equal(t, "delivery-weekly_20351230_1.log", filepath.Base(saturdayPath))
	sundayPath := write(weeklyRotationSunday, "sunday entry")
	require.Equal(t, "delivery-weekly_20360106_1.log", filepath.Base(sundayPath))
	require.Equal(t, "20360106", logger.Fsstore.FileName.Date)

	requireWeeklyRotationContents(t, saturdayPath, sundayPath)
}

// requireWeeklyRotationContentsは、前の週のfileに土曜日のentryだけが、新しい週のfileに
// 日曜日のentryだけが入っていることを確かめます。
func requireWeeklyRotationContents(t *testing.T, saturdayPath, sundayPath string) {
	t.Helper()
	saturday, err := os.ReadFile(saturdayPath)
	require.NoError(t, err)
	require.Contains(t, string(saturday), "saturday entry")
	require.NotContains(t, string(saturday), "sunday entry")
	sunday, err := os.ReadFile(sundayPath)
	require.NoError(t, err)
	require.Contains(t, string(sunday), "sunday entry")
	require.NotContains(t, string(sunday), "saturday entry")
}
