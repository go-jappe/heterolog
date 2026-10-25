package opensearch

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestOpenSearchLoggerApplicationFieldsReachPlainAndOtelDocuments(t *testing.T) {
	for _, useOtel := range []bool{false, true} {
		format := "plain"
		if useOtel {
			format = "otel"
		}
		t.Run(format, func(t *testing.T) {
			cfg := NewMockOpenSearchConfigProvider()
			cfg.noBulkFlushModeEnabled = true
			logger, client := newTestLogger(t, cfg, nil)

			converter := NewLogEntryConverter(
				useOtel,
				NewMockResourceProvider(),
				logger.TimestampFormatter,
			)
			logger.entryConverter = converter
			logger.flushManager.(*OpenSearchFlushManager).converter = converter

			require.NoError(t, logger.testInfo(
				context.Background(),
				"typed application fields",
				openSearchApplicationFields(t)...,
			))

			client.mu.RLock()
			require.Len(t, client.IndexDocumentArgs, 1)
			document := client.IndexDocumentArgs[0].Doc
			client.mu.RUnlock()

			assertOpenSearchApplicationFieldDocument(t, document, useOtel)
		})
	}
}

func TestDeliveryOpenSearchLoggerApplicationFieldsReachPlainAndOtelDocuments(t *testing.T) {
	for _, useOtel := range []bool{false, true} {
		format := "plain"
		if useOtel {
			format = "otel"
		}
		t.Run(format, func(t *testing.T) {
			client := NewMockOpenSearchClient()
			tracer := observer.NopTracer{}
			meter := observer.NopMeter{}
			loggerConfig := NewDeliveryOpenSearchLoggerConfigAdapter(pkglogger.DeliveryOpenSearchLoggerBaseConfig{
				RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
					AppName: "delivery-field-test",
					Target:  "opensearch",
				},
				LogBodyConfig: pkglogger.LogBodyConfig{
					LogLineDateFormat: time.RFC3339Nano,
				},
			})
			flushManagerConfig := NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryOpenSearchFlushManagerConfig{
				Index:                      "delivery-field-index",
				UseOpenTelemetryFormat:     useOtel,
				PostWarnThresholdDuration:  time.Hour,
				PostErrorThresholdDuration: 2 * time.Hour,
				Resource: pkglogger.Resource{
					Cloud: &pkglogger.CloudResourceInfo{},
				},
			})
			logger := NewDeliveryOpenSearchLogger(
				context.Background(),
				loggerConfig,
				client,
				flushManagerConfig,
				NewMockResourceProvider(),
				nil,
				tracer,
				meter,
				&LoggerBase{
					Clock: pkgclock.NewUTCClock(),
				},
			)
			t.Cleanup(func() { require.NoError(t, logger.Close()) })

			report, err := logger.testInfo(
				context.Background(),
				"typed delivery fields",
				openSearchApplicationFields(t)...,
			)
			require.NoError(t, err)
			require.Len(t, report.Targets, 1)
			require.Equal(t, pkglogger.DeliverySucceeded, report.Targets[0].Status)

			client.mu.RLock()
			require.Len(t, client.IndexDocumentArgs, 1)
			document := client.IndexDocumentArgs[0].Doc
			client.mu.RUnlock()

			assertOpenSearchApplicationFieldDocument(t, document, useOtel)
		})
	}
}

func openSearchApplicationFields(t *testing.T) []applogging.Field {
	t.Helper()
	contextField, err := applogging.Any("context", map[string]any{"region": "ap-northeast-1"})
	require.NoError(t, err)

	return []applogging.Field{
		applogging.WithRequestID("request-123"),
		applogging.MustFieldKey("trace_id").String("trace-456"),
		applogging.MustFieldKey("span_id").String("span-789"),
		applogging.MustFieldKey("success").Bool(true),
		applogging.MustFieldKey("attempt").Int(2),
		contextField,
	}
}

func assertOpenSearchApplicationFieldDocument(
	t *testing.T,
	document map[string]any,
	useOtel bool,
) {
	t.Helper()

	var attributes map[string]any
	if useOtel {
		var ok bool
		attributes, ok = document["attributes"].(map[string]any)
		require.True(t, ok)
	} else {
		attributes = document
	}

	require.Equal(t, "request-123", attributes["request_id"])
	require.Equal(t, "trace-456", attributes["trace_id"])
	require.Equal(t, "span-789", attributes["span_id"])

	var extra map[string]any
	if useOtel {
		var ok bool
		extra, ok = attributes["fields"].(map[string]any)
		require.True(t, ok)
	} else {
		encoded, ok := attributes["fields"].(string)
		require.True(t, ok)
		decoder := json.NewDecoder(strings.NewReader(encoded))
		decoder.UseNumber()
		require.NoError(t, decoder.Decode(&extra))
	}

	require.Equal(t, true, extra["success"])
	if useOtel {
		// OTel documentはbackend送信時のJSON encodingまでFieldのprimitive型を保持します。
		require.Equal(t, 2, extra["attempt"])
	} else {
		// Plain documentではfields自体をJSON文字列として格納します。
		require.Equal(t, json.Number("2"), extra["attempt"])
	}
	require.Equal(t, map[string]any{"region": "ap-northeast-1"}, extra["context"])
	require.NotContains(t, extra, "request_id")
	require.NotContains(t, extra, "trace_id")
	require.NotContains(t, extra, "span_id")
}
