package stdout

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errorを渡さないErrorWithStackも拒否せず、要求されたstackを持つERRORとして記録します。
// errorの欄は出力せず、配送結果は通常のERRORと同じく成功です。
func TestDeliveryStdoutLoggerErrorWithStackWithoutErrorRecordsErrorLog(t *testing.T) {
	readFile, writeFile, err := os.Pipe()
	require.NoError(t, err)
	originalStderr := os.Stderr
	os.Stderr = writeFile
	t.Cleanup(func() { os.Stderr = originalStderr })
	output := make(chan string, 1)
	go func() {
		content, _ := io.ReadAll(readFile)
		output <- string(content)
	}()

	logger := newDeliveryStdoutLoggerForReasonTest(t)
	report, err := logger.testErrorWithStack(context.Background(), "missing error", nil)
	os.Stderr = originalStderr
	require.NoError(t, writeFile.Close())
	written := <-output
	require.NoError(t, readFile.Close())

	require.NoError(t, err)
	result := requireSingleDeliveryStdoutTarget(t, report)
	assert.Equal(t, pkglogger.DeliverySucceeded, result.Status)
	assert.NoError(t, result.Err)
	assert.True(t, strings.HasPrefix(written, "[ERROR] "), "output = %q", written)
	assert.Contains(t, written, " - missing error (stacktrace): (1) at: (File: ")
	assert.NotContains(t, written, " - err: ")
}

func TestDeliveryStdoutLoggerWriteFailureReportsFailed(t *testing.T) {
	readFile, writeFile, err := os.Pipe()
	require.NoError(t, err)
	require.NoError(t, writeFile.Close())
	t.Cleanup(func() { require.NoError(t, readFile.Close()) })

	originalStdout := os.Stdout
	os.Stdout = writeFile
	t.Cleanup(func() { os.Stdout = originalStdout })

	logger := newDeliveryStdoutLoggerForReasonTest(t)
	report, err := logger.testInfo(context.Background(), "write to closed stdout")

	require.Error(t, err)
	result := requireSingleDeliveryStdoutTarget(t, report)
	require.Equal(t, err, result.Err)
	assert.Equal(t, pkglogger.DeliveryFailed, result.Status)
}

func TestDeliveryStdoutLoggerStderrWriteFailureReportsFailed(t *testing.T) {
	readFile, writeFile, err := os.Pipe()
	require.NoError(t, err)
	require.NoError(t, writeFile.Close())
	t.Cleanup(func() { require.NoError(t, readFile.Close()) })

	originalStderr := os.Stderr
	os.Stderr = writeFile
	t.Cleanup(func() { os.Stderr = originalStderr })

	logger := newDeliveryStdoutLoggerForReasonTest(t)
	report, err := logger.testError(context.Background(), "write to closed stderr")

	require.Error(t, err)
	result := requireSingleDeliveryStdoutTarget(t, report)
	require.Equal(t, err, result.Err)
	assert.Equal(t, pkglogger.DeliveryFailed, result.Status)
}

func requireSingleDeliveryStdoutTarget(t *testing.T, report pkglogger.DeliveryReport) pkglogger.DeliveryResult {
	t.Helper()

	require.Len(t, report.Targets, 1)
	return report.Targets[0]
}

func newDeliveryStdoutLoggerForReasonTest(t *testing.T) *DeliveryStdoutLogger {
	t.Helper()

	cfg := NewDeliveryConfigAdapter(pkglogger.DeliveryStdoutLoggerConfig{
		TimeConfig: pkglogger.TimeConfig{
			TimeZone: "UTC",
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
	})
	logger, err := NewDeliveryStdoutLogger(
		context.Background(),
		cfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	return logger
}
