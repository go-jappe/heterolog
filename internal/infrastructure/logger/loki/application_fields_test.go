package loki

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"

	"github.com/stretchr/testify/require"
)

func TestLokiLoggerHandleLogEntryPreservesApplicationFieldsInPushLine(t *testing.T) {
	client := &recordingFlushManagerLokiClient{}
	logger, err := NewLokiLogger(
		context.Background(),
		newTestLokiLoggerConfig(100, time.Hour, true),
		client,
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
		WithClock(fixedLokiClock{t: time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logger.Close(context.Background())) })

	fields := lokiApplicationFields(t)
	calls := []func() error{
		func() error { return logger.testDebug(context.Background(), "debug fields", fields...) },
		func() error { return logger.testInfo(context.Background(), "info fields", fields...) },
		func() error { return logger.testWarn(context.Background(), "warn fields", fields...) },
		func() error { return logger.testError(context.Background(), "error fields", fields...) },
		func() error {
			return logger.testErrorWithStack(
				context.Background(),
				"stack fields",
				errors.New("stack failure"),
				fields...,
			)
		},
	}
	for _, call := range calls {
		require.NoError(t, call())
	}

	require.Equal(t, len(calls), client.pushCalls)
	assertLokiPushRequestsPreserveApplicationFields(t, client.pushRequests)
}

func TestDeliveryLokiLoggerHandleLogEntryPreservesApplicationFieldsInPushLine(t *testing.T) {
	client := &recordingFlushManagerLokiClient{}
	resourceProvider := resource.NewResourceAdapter(pkglogger.Resource{
		ServiceName: "delivery-loki-field-test",
		Environment: "test",
		Cloud:       &pkglogger.CloudResourceInfo{},
	})
	logger, err := NewDeliveryLokiLogger(
		context.Background(),
		NewDeliveryLokiLoggerConfigAdapter(pkglogger.DeliveryLokiLoggerBaseConfig{
			RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
				AppName: "delivery-loki-field-test",
				Target:  "loki",
			},
			LogBodyConfig: pkglogger.LogBodyConfig{
				LogLineDateFormat:        time.RFC3339Nano,
				ErrorTraceDepth:          3,
				ErrorTraceDepthDebugMode: 10,
			},
			MaxLineBytes:        256 * 1024,
			MaxPushRequestBytes: 4 * 1024 * 1024,
		}),
		client,
		NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryLokiFlushManagerConfig{
			Resource: pkglogger.Resource{
				ServiceName: "delivery-loki-field-test",
				Environment: "test",
				Cloud:       &pkglogger.CloudResourceInfo{},
			},
		}),
		resourceProvider,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
		WithClock(fixedLokiClock{t: time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logger.Close()) })

	fields := lokiApplicationFields(t)
	calls := []func() (pkglogger.DeliveryReport, error){
		func() (pkglogger.DeliveryReport, error) {
			return logger.testDebug(context.Background(), "delivery debug fields", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return logger.testInfo(context.Background(), "delivery info fields", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return logger.testWarn(context.Background(), "delivery warn fields", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return logger.testError(context.Background(), "delivery error fields", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return logger.testErrorWithStack(
				context.Background(),
				"delivery stack fields",
				errors.New("stack failure"),
				fields...,
			)
		},
	}
	for _, call := range calls {
		report, callErr := call()
		require.NoError(t, callErr)
		require.Len(t, report.Targets, 1)
		require.Equal(t, pkglogger.DeliverySucceeded, report.Targets[0].Status)
	}

	require.Equal(t, len(calls), client.pushCalls)
	assertLokiPushRequestsPreserveApplicationFields(t, client.pushRequests)
}

func lokiApplicationFields(t *testing.T) []applogging.Field {
	t.Helper()

	contextField, err := applogging.Any("context", map[string]any{
		"roles": []string{"buyer", "reviewer"},
	})
	require.NoError(t, err)

	return []applogging.Field{
		applogging.MustFieldKey("user_id").String("hogehoge"),
		applogging.MustFieldKey("authenticated").Bool(true),
		applogging.MustFieldKey("attempt").Int(2),
		contextField,
	}
}

func assertLokiPushRequestsPreserveApplicationFields(
	t *testing.T,
	requests [][]pkgloki.Stream,
) {
	t.Helper()

	for _, request := range requests {
		require.Len(t, request, 1)
		require.Len(t, request[0].Entries, 1)

		line := request[0].Entries[0].Line
		fieldsIndex := strings.LastIndex(line, " fields:")
		require.NotEqual(t, -1, fieldsIndex, "Push line does not contain fields: %q", line)

		var got map[string]any
		decoder := json.NewDecoder(strings.NewReader(line[fieldsIndex+len(" fields:"):]))
		decoder.UseNumber()
		require.NoError(t, decoder.Decode(&got))
		require.Equal(t, "hogehoge", got["user_id"])
		require.Equal(t, true, got["authenticated"])
		require.Equal(t, json.Number("2"), got["attempt"])
		require.Equal(t, map[string]any{
			"roles": []any{"buyer", "reviewer"},
		}, got["context"])
	}
}
