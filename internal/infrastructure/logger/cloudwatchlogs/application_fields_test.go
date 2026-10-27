package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestCloudWatchLogsLoggerHandleLogEntryPreservesApplicationFieldsInPutLogEventsMessages(t *testing.T) {
	client := &recordingCloudWatchLogsClient{}
	logger, err := NewCloudWatchLogsLogger(
		context.Background(),
		newTestCloudWatchLogsLoggerConfig(1, time.Hour, true),
		client,
		newTestCloudWatchLogsFlushManagerConfig(),
		newTestCloudWatchLogsResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
		WithClock(fixedCloudWatchLogsClock{t: time.Date(2026, 8, 16, 1, 2, 3, 0, time.UTC)}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logger.Close(context.Background())) })

	ctx := context.Background()
	fields := cloudWatchLogsApplicationFields(t)
	tests := []struct {
		name string
		call func() error
	}{
		{name: "debug", call: func() error { return handleTestDebug(ctx, logger, "cloudwatch debug fields", fields...) }},
		{name: "info", call: func() error { return handleTestInfo(ctx, logger, "cloudwatch info fields", fields...) }},
		{name: "warn", call: func() error { return handleTestWarn(ctx, logger, "cloudwatch warn fields", fields...) }},
		{name: "error", call: func() error { return handleTestError(ctx, logger, "cloudwatch error fields", fields...) }},
		{
			name: "error_with_stack",
			call: func() error {
				return handleTestErrorWithStack(
					ctx,
					logger,
					"cloudwatch stack fields",
					errors.New("stack failure"),
					fields...,
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previousCalls := len(client.calls)
			require.NoError(t, tt.call())
			require.Len(t, client.calls, previousCalls+1)
			putCall := client.calls[previousCalls]
			require.Len(t, putCall.events, 1)
			assertCloudWatchLogsApplicationFieldsMessage(t, putCall.events[0].Message)
		})
	}
}

func TestDeliveryCloudWatchLogsLoggerHandleLogEntryPreservesApplicationFieldsInPutLogEventsMessages(t *testing.T) {
	client := &recordingCloudWatchLogsClient{}
	logger, err := NewDeliveryCloudWatchLogsLogger(
		context.Background(),
		NewDeliveryCloudWatchLogsLoggerConfigAdapter(pkglogger.DeliveryCloudWatchLogsLoggerBaseConfig{
			TimeConfig: pkglogger.TimeConfig{TimeZone: "UTC"},
			RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
				AppName: "delivery-cloudwatch-application-fields-test",
				Target:  "cloudwatchlogs",
			},
			LogBodyConfig: pkglogger.LogBodyConfig{
				LogLineDateFormat:        time.RFC3339Nano,
				ErrorTraceDepth:          1,
				ErrorTraceDepthDebugMode: 1,
			},
		}),
		client,
		NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{
			LogGroupName:  "/app/test",
			LogStreamName: "stream-a",
			Resource: pkglogger.Resource{
				ServiceName: "delivery-cloudwatch-application-fields-test",
				Cloud:       &pkglogger.CloudResourceInfo{},
			},
		}, pkgcloudwatchlogs.StreamRoutingNone),
		newTestCloudWatchLogsResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
		WithClock(fixedCloudWatchLogsClock{t: time.Date(2026, 8, 16, 1, 2, 3, 0, time.UTC)}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logger.Close()) })

	ctx := context.Background()
	fields := cloudWatchLogsApplicationFields(t)
	tests := []struct {
		name string
		call func() (pkglogger.DeliveryReport, error)
	}{
		{name: "debug", call: func() (pkglogger.DeliveryReport, error) {
			return handleTestDeliveryDebug(ctx, logger, "delivery cloudwatch debug fields", fields...)
		}},
		{name: "info", call: func() (pkglogger.DeliveryReport, error) {
			return handleTestDeliveryInfo(ctx, logger, "delivery cloudwatch info fields", fields...)
		}},
		{name: "warn", call: func() (pkglogger.DeliveryReport, error) {
			return handleTestDeliveryWarn(ctx, logger, "delivery cloudwatch warn fields", fields...)
		}},
		{name: "error", call: func() (pkglogger.DeliveryReport, error) {
			return handleTestDeliveryError(ctx, logger, "delivery cloudwatch error fields", fields...)
		}},
		{
			name: "error_with_stack",
			call: func() (pkglogger.DeliveryReport, error) {
				return handleTestDeliveryErrorWithStack(
					ctx,
					logger,
					"delivery cloudwatch stack fields",
					errors.New("stack failure"),
					fields...,
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previousCalls := len(client.calls)
			report, callErr := tt.call()
			require.NoError(t, callErr)
			require.Len(t, report.Targets, 1)
			require.Equal(t, pkglogger.DeliverySucceeded, report.Targets[0].Status)
			require.Len(t, client.calls, previousCalls+1)
			putCall := client.calls[previousCalls]
			require.Len(t, putCall.events, 1)
			assertCloudWatchLogsApplicationFieldsMessage(t, putCall.events[0].Message)
		})
	}
}

func cloudWatchLogsApplicationFields(t *testing.T) []pkglogger.Field {
	t.Helper()

	contextField, err := pkglogger.Any("context", map[string]any{
		"labels": []string{"primary", "audit"},
		"retry": map[string]any{
			"enabled": false,
			"limit":   3,
		},
	})
	require.NoError(t, err)

	return []pkglogger.Field{
		pkglogger.WithRequestID("field-request"),
		pkglogger.MustFieldKey("trace_id").String("field-trace"),
		pkglogger.MustFieldKey("span_id").String("field-span"),
		pkglogger.MustFieldKey("region").String("ap-northeast-1"),
		pkglogger.MustFieldKey("enabled").Bool(true),
		pkglogger.MustFieldKey("attempt").Int(2),
		contextField,
	}
}

func assertCloudWatchLogsApplicationFieldsMessage(t *testing.T, message string) {
	t.Helper()

	require.Contains(t, message, "request_id: field-request")
	require.Contains(t, message, "trace_id: field-trace")
	require.Contains(t, message, "span_id: field-span")

	const marker = " fields:"
	markerIndex := strings.Index(message, marker)
	require.NotEqual(t, -1, markerIndex, "PutLogEvents message does not contain structured fields: %q", message)

	decoder := json.NewDecoder(strings.NewReader(message[markerIndex+len(marker):]))
	decoder.UseNumber()
	var fields map[string]any
	require.NoError(t, decoder.Decode(&fields), "decode fields from PutLogEvents message %q", message)

	require.Equal(t, "ap-northeast-1", fields["region"])
	require.Equal(t, true, fields["enabled"])
	require.Equal(t, json.Number("2"), fields["attempt"])
	require.Equal(t, map[string]any{
		"labels": []any{"primary", "audit"},
		"retry": map[string]any{
			"enabled": false,
			"limit":   json.Number("3"),
		},
	}, fields["context"])
	require.NotContains(t, fields, "request_id")
	require.NotContains(t, fields, "trace_id")
	require.NotContains(t, fields, "span_id")
}
