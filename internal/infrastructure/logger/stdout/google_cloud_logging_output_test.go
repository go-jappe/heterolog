package stdout_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// requireSeverityは、出力が1行のJSONで、指定の重大度を持つことを確かめます。
func requireSeverity(t *testing.T, output, want string) {
	t.Helper()
	line := strings.TrimSuffix(output, "\n")
	require.NotContains(t, line, "\n")
	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &decoded), "output: %q", output)
	require.Equal(t, want, decoded["severity"])
}

// Cloud Loggingはstdoutとstderrのどちらも読むため、両方のstreamが同じ形式になることを確かめます。
func TestStdoutLoggerWritesGoogleCloudLoggingFormatToBothStreams(t *testing.T) {
	logger, err := stdout.NewStdoutLogger(context.Background(), stdout.NewConfigAdapter(pkglogger.StdoutLoggerConfig{
		RuntimeConfig:               pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelInfo},
		UseGoogleCloudLoggingFormat: true,
	}), nil, nil, nil)
	require.NoError(t, err)

	stdoutOutput, stderrOutput := captureStdoutAndStderr(t, func() {
		require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{
			Level: pkglogger.LogLevelInfo, Message: "info",
		}))
		require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{
			Level: pkglogger.LogLevelWarn, Message: "warn",
		}))
	})

	requireSeverity(t, stdoutOutput, "INFO")
	requireSeverity(t, stderrOutput, "WARNING")
}

func TestDeliveryStdoutLoggerWritesGoogleCloudLoggingFormat(t *testing.T) {
	logger, err := stdout.NewDeliveryStdoutLogger(context.Background(), stdout.NewDeliveryConfigAdapter(pkglogger.DeliveryStdoutLoggerConfig{
		UseGoogleCloudLoggingFormat: true,
	}), nil, nil, nil)
	require.NoError(t, err)

	_, stderrOutput := captureStdoutAndStderr(t, func() {
		_, err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
			Level: pkglogger.LogLevelError, Message: "error",
		})
		require.NoError(t, err)
	})

	requireSeverity(t, stderrOutput, "ERROR")
}
