package slack

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	slackgo "github.com/slack-go/slack"
	"github.com/stretchr/testify/require"
)

const slackTypedFieldsJSON = `fields:{"attempt":2,"enabled":true,"labels":["primary","audit"]}`

func applicationFieldsForSlackTest(t *testing.T) []pkglogger.Field {
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

func TestSlackLoggerHandleLogEntryPreservesApplicationFields(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "context-request")
	cfg := normalSlackLifecycleConfig([]time.Duration{time.Hour})
	cfg.MaxMessageLength = 10_000
	logger, err := NewSlackLogger(
		ctx,
		NewConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	client := &recordingSlackHTTPClient{}
	logger.slackAPI = slackgo.New(cfg.Token, slackgo.OptionHTTPClient(client))
	// Fieldの検証中に即時timerがbufferを取り出さないよう、次回通知を通常間隔にします。
	logger.nextNotificationImmediate = false

	fields := applicationFieldsForSlackTest(t)
	calls := []func() error{
		func() error { return logger.testDebug(ctx, "slack debug fields", fields...) },
		func() error { return logger.testInfo(ctx, "slack info fields", fields...) },
		func() error { return logger.testWarn(ctx, "slack warn fields", fields...) },
		func() error { return logger.testError(ctx, "slack error fields", fields...) },
		func() error {
			return logger.testErrorWithStack(ctx, "slack stack fields", errors.New("stack failure"), fields...)
		},
	}
	for _, call := range calls {
		require.NoError(t, call())
	}

	logger.mu.Lock()
	entries := append([]*logging.LogEntry(nil), logger.logs...)
	logger.mu.Unlock()
	require.Len(t, entries, len(calls))
	for _, entry := range entries {
		require.Equal(t, "field-request", entry.RequestID)
		require.Equal(t, "field-trace", entry.TraceID)
		require.Equal(t, "field-span", entry.SpanID)
		formatted := logger.LogFormatter.Format(entry)
		require.Contains(t, formatted, slackTypedFieldsJSON)
		require.NotContains(t, formatted, "context-request")
	}

	require.NoError(t, logger.Close())
	require.Equal(t, 4, client.callCount())
	client.requireTextContains(t, slackTypedFieldsJSON)
}

func TestDeliverySlackLoggerHandleLogEntryPreservesApplicationFields(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "context-request")
	cfg := pkglogger.DeliverySlackLoggerConfig{
		TimeConfig: pkglogger.TimeConfig{TimeZone: "UTC"},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "delivery-slack-fields-test",
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		Timeout:          time.Second,
		Token:            "dummy-token",
		Channel:          "dummy-channel",
		ErrorChannel:     "dummy-error-channel",
		WarnChannel:      "dummy-warn-channel",
		InfoChannel:      "dummy-info-channel",
		DebugChannel:     "dummy-debug-channel",
		MaxMessageLength: 10_000,
	}
	logger, err := NewDeliverySlackLogger(
		ctx,
		NewDeliveryConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	client := &recordingSlackHTTPClient{}
	logger.slackAPI = slackgo.New(cfg.Token, slackgo.OptionHTTPClient(client))

	fields := applicationFieldsForSlackTest(t)
	calls := []func() (pkglogger.DeliveryReport, error){
		func() (pkglogger.DeliveryReport, error) {
			return logger.testDebug(ctx, "delivery slack debug fields", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return logger.testInfo(ctx, "delivery slack info fields", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return logger.testWarn(ctx, "delivery slack warn fields", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return logger.testError(ctx, "delivery slack error fields", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return logger.testErrorWithStack(ctx, "delivery slack stack fields", errors.New("stack failure"), fields...)
		},
	}
	for _, call := range calls {
		report, callErr := call()
		require.NoError(t, callErr)
		require.Len(t, report.Targets, 1)
		require.Equal(t, pkglogger.DeliverySucceeded, report.Targets[0].Status)
	}
	require.Equal(t, len(calls), client.callCount())
	client.requireTextContains(t, "request_id: field-request")
	client.requireTextContains(t, "trace_id: field-trace")
	client.requireTextContains(t, "span_id: field-span")
	client.requireTextContains(t, slackTypedFieldsJSON)
}
