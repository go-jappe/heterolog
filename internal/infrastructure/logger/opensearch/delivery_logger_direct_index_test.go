package opensearch

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/require"
)

func TestDeliveryOpenSearchLoggerIndexesImmediatelyWithoutBufferBulkOrTimer(t *testing.T) {
	ctx := context.Background()
	client := NewMockOpenSearchClient()
	resourceProvider := NewMockResourceProvider()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	loggerCfg := NewDeliveryOpenSearchLoggerConfigAdapter(pkglogger.DeliveryOpenSearchLoggerBaseConfig{
		RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
			AppName: "delivery-test",
			Target:  "opensearch",
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat: time.RFC3339Nano,
		},
	})
	if _, ok := any(loggerCfg).(OpenSearchBulkFlushManagerConfigProvider); ok {
		t.Fatal("Delivery logger config must not expose bulk retry settings")
	}
	flushManagerCfg := NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryOpenSearchFlushManagerConfig{
		Index:                      "delivery-index",
		PostWarnThresholdDuration:  time.Hour,
		PostErrorThresholdDuration: 2 * time.Hour,
		Resource: pkglogger.Resource{
			Cloud: &pkglogger.CloudResourceInfo{},
		},
	})
	loggerBase := &LoggerBase{
		Clock:  pkgclock.NewUTCClock(),
		Tracer: tracer,
		Meter:  meter,
	}

	logger := NewDeliveryOpenSearchLogger(
		ctx,
		loggerCfg,
		client,
		flushManagerCfg,
		resourceProvider,
		nil,
		tracer,
		meter,
		loggerBase,
	)

	report, err := logger.testInfo(ctx, "send this document directly")
	require.NoError(t, err)
	require.Len(t, report.Targets, 1)
	require.Equal(t, pkglogger.DeliverySucceeded, report.Targets[0].Status)

	client.mu.RLock()
	indexDocumentArgs := append([]pkgopensearch.DocumentWriteRequest(nil), client.IndexDocumentArgs...)
	indexDocumentsCalled := client.IndexDocumentsCalled
	indexDocumentsArgs := append([]pkgopensearch.DocumentWriteRequest(nil), client.IndexDocumentsArgs...)
	client.mu.RUnlock()

	require.Len(t, indexDocumentArgs, 1)
	require.False(t, indexDocumentsCalled)
	require.Empty(t, indexDocumentsArgs)
	request := indexDocumentArgs[0]

	require.Equal(t, "delivery-index", request.Index)
	require.NotEmpty(t, request.ID)
	require.NotEmpty(t, request.Doc)

	// Close時にもbuffer flushは存在せず、送信回数は増えません。
	require.NoError(t, logger.Close())
	client.mu.RLock()
	indexDocumentCallCount := len(client.IndexDocumentArgs)
	indexDocumentsCalled = client.IndexDocumentsCalled
	client.mu.RUnlock()
	require.Equal(t, 1, indexDocumentCallCount)
	require.False(t, indexDocumentsCalled)
}
