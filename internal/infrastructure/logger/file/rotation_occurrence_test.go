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

func TestFileLoggerUsesWriteTimeForRotationAndOccurrenceForDisplay(t *testing.T) {
	config := newTestFileLoggerConfig(t.TempDir(), "write_time_rotation", "regular-write-time")
	config.TimeConfig.IsLocalTimeEnabled = true
	config.TimeConfig.TimeZone = "Asia/Tokyo"
	config.FsstoreConfig.Lifecycle.EnableRotate = true
	config.FsstoreConfig.Lifecycle.RotationType = pkgfsstore.FileRotationDaily

	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(config),
		nil,
		nil,
	)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	writeTime := time.Date(2036, time.January, 2, 0, 30, 0, 0, time.FixedZone("JST", 9*60*60))
	logger.Clock = clock.NewFixedClock(writeTime)
	occurredAt := time.Date(2035, time.December, 31, 15, 30, 0, 0, time.UTC) // 2036-01-01 00:30 JST

	require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{
		OccurredAt: occurredAt,
		Level:      pkglogger.LogLevelInfo,
		Message:    "regular write-time rotation",
	}))

	require.Equal(t, "20360102", logger.Fsstore.FileName.Date)
	require.Equal(t, "regular-write-time_20360102_1.log", filepath.Base(logger.Fsstore.ActiveFilePath))
	content, err := os.ReadFile(logger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.Contains(t, string(content), "2036-01-01 00:30:00")
	require.Contains(t, string(content), "regular write-time rotation")
}

func TestDeliveryFileLoggerUsesWriteTimeForRotationAndOccurrenceForDisplay(t *testing.T) {
	config := newTestDeliveryFileLoggerConfig(t.TempDir(), "delivery_write_time_rotation", "delivery-write-time")
	config.TimeConfig.IsLocalTimeEnabled = true
	config.TimeConfig.TimeZone = "Asia/Tokyo"
	config.FsstoreConfig.Rotation.EnableRotate = true
	config.FsstoreConfig.Rotation.RotationType = pkgfsstore.FileRotationDaily

	logger, err := NewDeliveryFileLogger(
		context.Background(),
		NewDeliveryFileLoggerConfigAdapter(config),
		nil,
		nil,
	)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	writeTime := time.Date(2036, time.January, 2, 0, 30, 0, 0, time.FixedZone("JST", 9*60*60))
	logger.Clock = clock.NewFixedClock(writeTime)
	occurredAt := time.Date(2035, time.December, 31, 15, 30, 0, 0, time.UTC) // 2036-01-01 00:30 JST

	report, err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
		OccurredAt: occurredAt,
		Level:      pkglogger.LogLevelInfo,
		Message:    "delivery write-time rotation",
	})
	requireDeliverySucceeded(t, report, err)

	require.Equal(t, "20360102", logger.Fsstore.FileName.Date)
	require.Equal(t, "delivery-write-time_20360102_1.log", filepath.Base(logger.Fsstore.ActiveFilePath))
	content, err := os.ReadFile(logger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.Contains(t, string(content), "2036-01-01 00:30:00")
	require.Contains(t, string(content), "delivery write-time rotation")
}

func TestFileLoggerSizeRotationKeepsWriteTimePeriodForDelayedOccurrence(t *testing.T) {
	config := newTestFileLoggerConfig(t.TempDir(), "delayed_size_rotation", "regular-delayed")
	config.TimeConfig.IsLocalTimeEnabled = true
	config.TimeConfig.TimeZone = "Asia/Tokyo"
	config.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
	config.FsstoreConfig.Lifecycle.EnableRotate = true
	config.FsstoreConfig.Lifecycle.RotationType = pkgfsstore.FileRotationDaily

	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(config),
		nil,
		nil,
	)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	writeTime := time.Date(2036, time.January, 2, 12, 0, 0, 0, time.FixedZone("JST", 9*60*60))
	logger.Clock = clock.NewFixedClock(writeTime)
	require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{
		OccurredAt: writeTime.Add(-24 * time.Hour),
		Level:      pkglogger.LogLevelInfo,
		Message:    "establish write period",
	}))
	require.Equal(t, "regular-delayed_20360102_1.log", filepath.Base(logger.Fsstore.ActiveFilePath))
	require.NoError(t, os.Truncate(
		logger.Fsstore.ActiveFilePath,
		int64(config.FsstoreConfig.Lifecycle.MaxFileSize),
	))

	require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{
		OccurredAt: writeTime.Add(-72 * time.Hour),
		Level:      pkglogger.LogLevelInfo,
		Message:    "delayed entry stays in write period",
	}))

	require.Equal(t, "20360102", logger.Fsstore.FileName.Date)
	require.Equal(t, 2, logger.Fsstore.FileName.Index)
	require.Equal(t, "regular-delayed_20360102_2.log", filepath.Base(logger.Fsstore.ActiveFilePath))
	backwardFiles, err := filepath.Glob(filepath.Join(logger.Fsstore.FileName.Dir, "regular-delayed_20351230_*.log"))
	require.NoError(t, err)
	require.Empty(t, backwardFiles)
}

func TestDeliveryFileLoggerSizeRotationKeepsWriteTimePeriodForDelayedOccurrence(t *testing.T) {
	config := newTestDeliveryFileLoggerConfig(t.TempDir(), "delivery_delayed_size_rotation", "delivery-delayed")
	config.TimeConfig.IsLocalTimeEnabled = true
	config.TimeConfig.TimeZone = "Asia/Tokyo"
	config.FsstoreConfig.Rotation.EnableRotate = true
	config.FsstoreConfig.Rotation.RotationType = pkgfsstore.FileRotationDaily

	logger, err := NewDeliveryFileLogger(
		context.Background(),
		NewDeliveryFileLoggerConfigAdapter(config),
		nil,
		nil,
	)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	writeTime := time.Date(2036, time.January, 2, 12, 0, 0, 0, time.FixedZone("JST", 9*60*60))
	logger.Clock = clock.NewFixedClock(writeTime)
	report, err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
		OccurredAt: writeTime.Add(-24 * time.Hour),
		Level:      pkglogger.LogLevelInfo,
		Message:    "establish delivery write period",
	})
	requireDeliverySucceeded(t, report, err)
	require.Equal(t, "delivery-delayed_20360102_1.log", filepath.Base(logger.Fsstore.ActiveFilePath))
	require.NoError(t, os.Truncate(
		logger.Fsstore.ActiveFilePath,
		int64(config.FsstoreConfig.Rotation.MaxFileSize),
	))

	report, err = logger.HandleLogEntry(context.Background(), &logging.LogEntry{
		OccurredAt: writeTime.Add(-72 * time.Hour),
		Level:      pkglogger.LogLevelInfo,
		Message:    "delayed delivery entry stays in write period",
	})
	requireDeliverySucceeded(t, report, err)

	require.Equal(t, "20360102", logger.Fsstore.FileName.Date)
	require.Equal(t, 2, logger.Fsstore.FileName.Index)
	require.Equal(t, "delivery-delayed_20360102_2.log", filepath.Base(logger.Fsstore.ActiveFilePath))
	backwardFiles, err := filepath.Glob(filepath.Join(logger.Fsstore.FileName.Dir, "delivery-delayed_20351230_*.log"))
	require.NoError(t, err)
	require.Empty(t, backwardFiles)
}
