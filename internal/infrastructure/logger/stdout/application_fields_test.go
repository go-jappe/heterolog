package stdout_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

const stdoutTypedFieldsJSON = `fields:{"attempt":2,"enabled":true,"labels":["primary","audit"]}`

func applicationFieldsForStdoutTest(t *testing.T) []pkglogger.Field {
	t.Helper()
	labels, err := pkglogger.Any("labels", []string{"primary", "audit"})
	require.NoError(t, err)

	return []pkglogger.Field{
		pkglogger.WithRequestID("field-request"),
		pkglogger.MustFieldKey("trace_id").String("field-trace"),
		pkglogger.MustFieldKey("span_id").String("field-span"),
		pkglogger.MustFieldKey("attempt").Int(2),
		pkglogger.MustFieldKey("enabled").Bool(true),
		labels,
	}
}

func testStdoutConfig() pkglogger.StdoutLoggerConfig {
	return pkglogger.StdoutLoggerConfig{
		TimeConfig: pkglogger.TimeConfig{TimeZone: "UTC"},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "stdout-fields-test",
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		SamplingConfig: pkglogger.SamplingConfig{IsSamplingEnabled: false},
	}
}

func TestStdoutLoggerHandleLogEntryPreservesApplicationFields(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "context-request")
	physicalLogger, err := stdout.NewStdoutLogger(
		ctx,
		stdout.NewConfigAdapter(testStdoutConfig()),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	logger := newStdoutTestMultiLogger(t, physicalLogger)

	fields := applicationFieldsForStdoutTest(t)
	tests := []struct {
		name   string
		stderr bool
		call   func() error
	}{
		{name: "debug", call: func() error { return logger.Debug(ctx, "stdout debug fields", fields...) }},
		{name: "info", call: func() error { return logger.Info(ctx, "stdout info fields", fields...) }},
		{name: "warn", stderr: true, call: func() error { return logger.Warn(ctx, "stdout warn fields", fields...) }},
		{name: "error", stderr: true, call: func() error { return logger.Error(ctx, "stdout error fields", fields...) }},
		{
			name:   "error_with_stack",
			stderr: true,
			call: func() error {
				return logger.ErrorWithStack(ctx, "stdout stack fields", errors.New("stack failure"), fields...)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var callErr error
			capture := captureOutput
			if tt.stderr {
				capture = captureErrorOutput
			}
			output, captureErr := capture(func() { callErr = tt.call() })
			require.NoError(t, captureErr)
			require.NoError(t, callErr)
			require.Contains(t, output, "request_id: field-request")
			require.Contains(t, output, "trace_id: field-trace")
			require.Contains(t, output, "span_id: field-span")
			require.Contains(t, output, stdoutTypedFieldsJSON)
			require.NotContains(t, output, "context-request")
		})
	}
}

func TestDeliveryStdoutLoggerHandleLogEntryPreservesApplicationFields(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "context-request")
	base := testStdoutConfig()
	cfg := pkglogger.DeliveryStdoutLoggerConfig{
		TimeConfig:    base.TimeConfig,
		RuntimeConfig: pkglogger.DeliveryRuntimeConfig{AppName: base.RuntimeConfig.AppName},
		LogBodyConfig: base.LogBodyConfig,
	}
	physicalLogger, err := stdout.NewDeliveryStdoutLogger(
		ctx,
		stdout.NewDeliveryConfigAdapter(cfg),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	logger := newStdoutTestDeliveryMultiLogger(t, physicalLogger)

	fields := applicationFieldsForStdoutTest(t)
	tests := []struct {
		name   string
		stderr bool
		call   func() (pkglogger.DeliveryReport, error)
	}{
		{name: "debug", call: func() (pkglogger.DeliveryReport, error) {
			return logger.Debug(ctx, "delivery stdout debug fields", fields...)
		}},
		{name: "info", call: func() (pkglogger.DeliveryReport, error) {
			return logger.Info(ctx, "delivery stdout info fields", fields...)
		}},
		{name: "warn", stderr: true, call: func() (pkglogger.DeliveryReport, error) {
			return logger.Warn(ctx, "delivery stdout warn fields", fields...)
		}},
		{name: "error", stderr: true, call: func() (pkglogger.DeliveryReport, error) {
			return logger.Error(ctx, "delivery stdout error fields", fields...)
		}},
		{
			name:   "error_with_stack",
			stderr: true,
			call: func() (pkglogger.DeliveryReport, error) {
				return logger.ErrorWithStack(ctx, "delivery stdout stack fields", errors.New("stack failure"), fields...)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var report pkglogger.DeliveryReport
			var callErr error
			capture := captureOutput
			if tt.stderr {
				capture = captureErrorOutput
			}
			output, captureErr := capture(func() { report, callErr = tt.call() })
			require.NoError(t, captureErr)
			require.NoError(t, callErr)
			require.Len(t, report.Targets, 1)
			require.Equal(t, pkglogger.DeliverySucceeded, report.Targets[0].Status)
			require.Contains(t, output, "request_id: field-request")
			require.Contains(t, output, "trace_id: field-trace")
			require.Contains(t, output, "span_id: field-span")
			require.Contains(t, output, stdoutTypedFieldsJSON)
			require.NotContains(t, output, "context-request")
		})
	}
}
