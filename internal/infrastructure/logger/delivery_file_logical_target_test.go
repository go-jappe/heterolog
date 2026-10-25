package logger

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestDeliveryFileSplitRolesReportOneRegisteredLogicalTarget(t *testing.T) {
	fileCfg := newCloseOwnershipDeliveryFileConfig(t.TempDir(), "delivery-logical-file")
	fileCfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	fileCfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
	cfg := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		Timeout:               time.Second,
		MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
		File:                  fileCfg,
	}

	deliveryLogger, err := NewDeliveryLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	closer, ok := deliveryLogger.(interface{ Close() error })
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, closer.Close()) })

	tests := []struct {
		name    string
		message string
		deliver func() (pkglogger.DeliveryReport, error)
	}{
		{
			name:    "default",
			message: "logical file info",
			deliver: func() (pkglogger.DeliveryReport, error) {
				return deliveryLogger.Info(context.Background(), "logical file info")
			},
		},
		{
			name:    "error",
			message: "logical file error",
			deliver: func() (pkglogger.DeliveryReport, error) {
				return deliveryLogger.Error(context.Background(), "logical file error")
			},
		},
		{
			name:    "debug",
			message: "logical file debug",
			deliver: func() (pkglogger.DeliveryReport, error) {
				return deliveryLogger.Debug(context.Background(), "logical file debug")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report, err := tt.deliver()
			require.NoError(t, err)
			require.Len(t, report.Targets, 1)
			require.Equal(t, pkglogger.LogOutputFile.String(), report.Targets[0].Backend)
			require.Equal(t, pkglogger.DeliverySucceeded, report.Targets[0].Status)
			require.False(t, report.HasUnregisteredTarget())
		})
	}

	owned, ok := deliveryLogger.(*topLevelDeliveryLoggerRuntime)
	require.True(t, ok)
	defaultLogger, err := owned.fileLoggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	errorLogger, err := owned.fileLoggerSet.NewErrorFileLogger(context.Background())
	require.NoError(t, err)
	debugLogger, err := owned.fileLoggerSet.NewDebugFileLogger(context.Background())
	require.NoError(t, err)

	for path, message := range map[string]string{
		defaultLogger.Fsstore.ActiveFilePath: tests[0].message,
		errorLogger.Fsstore.ActiveFilePath:   tests[1].message,
		debugLogger.Fsstore.ActiveFilePath:   tests[2].message,
	} {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Contains(t, string(content), message)
	}
}
