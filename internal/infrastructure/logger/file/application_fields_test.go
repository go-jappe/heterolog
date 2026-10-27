package file

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

const typedFieldsJSON = `fields:{"attempt":2,"enabled":true,"labels":["primary","audit"]}`

func applicationFieldsForFileTest(t *testing.T) []pkglogger.Field {
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

func TestFileLoggerHandleLogEntryPreservesApplicationFields(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "context-request")
	cfg := newTestFileLoggerConfig(t.TempDir(), "application_fields", "file-fields")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)

	fields := applicationFieldsForFileTest(t)
	calls := []struct {
		message string
		call    func() error
	}{
		{message: "file debug fields", call: func() error { return logger.testDebug(ctx, "file debug fields", fields...) }},
		{message: "file info fields", call: func() error { return logger.testInfo(ctx, "file info fields", fields...) }},
		{message: "file warn fields", call: func() error { return logger.testWarn(ctx, "file warn fields", fields...) }},
		{message: "file error fields", call: func() error { return logger.testError(ctx, "file error fields", fields...) }},
		{
			message: "file stack fields",
			call: func() error {
				return logger.testErrorWithStack(ctx, "file stack fields", errors.New("stack failure"), fields...)
			},
		},
	}
	for _, call := range calls {
		require.NoError(t, call.call())
	}
	path := logger.Fsstore.ActiveFilePath
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))

	contentBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	content := string(contentBytes)
	for _, call := range calls {
		require.Contains(t, content, call.message)
	}
	require.Equal(t, len(calls), strings.Count(content, "request_id: field-request"))
	require.Equal(t, len(calls), strings.Count(content, "trace_id: field-trace"))
	require.Equal(t, len(calls), strings.Count(content, "span_id: field-span"))
	require.Equal(t, len(calls), strings.Count(content, typedFieldsJSON))
	require.NotContains(t, content, "context-request")
}

func TestDeliveryFileLoggerHandleLogEntryPreservesApplicationFields(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "context-request")
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "delivery_application_fields", "delivery-file-fields")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	logger, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)

	fields := applicationFieldsForFileTest(t)
	calls := []struct {
		message string
		call    func() (pkglogger.DeliveryReport, error)
	}{
		{message: "delivery file debug fields", call: func() (pkglogger.DeliveryReport, error) {
			return logger.testDebug(ctx, "delivery file debug fields", fields...)
		}},
		{message: "delivery file info fields", call: func() (pkglogger.DeliveryReport, error) {
			return logger.testInfo(ctx, "delivery file info fields", fields...)
		}},
		{message: "delivery file warn fields", call: func() (pkglogger.DeliveryReport, error) {
			return logger.testWarn(ctx, "delivery file warn fields", fields...)
		}},
		{message: "delivery file error fields", call: func() (pkglogger.DeliveryReport, error) {
			return logger.testError(ctx, "delivery file error fields", fields...)
		}},
		{
			message: "delivery file stack fields",
			call: func() (pkglogger.DeliveryReport, error) {
				return logger.testErrorWithStack(ctx, "delivery file stack fields", errors.New("stack failure"), fields...)
			},
		},
	}
	for _, call := range calls {
		report, callErr := call.call()
		requireDeliverySucceeded(t, report, callErr)
	}
	path := logger.Fsstore.ActiveFilePath
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))

	contentBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	content := string(contentBytes)
	for _, call := range calls {
		require.Contains(t, content, call.message)
	}
	require.Equal(t, len(calls), strings.Count(content, "request_id: field-request"))
	require.Equal(t, len(calls), strings.Count(content, "trace_id: field-trace"))
	require.Equal(t, len(calls), strings.Count(content, "span_id: field-span"))
	require.Equal(t, len(calls), strings.Count(content, typedFieldsJSON))
	require.NotContains(t, content, "context-request")
}
