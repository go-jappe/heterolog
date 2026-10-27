package opensearch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/fsarchivehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenSearchLoggerDurableArchiveFailureDropsDetachedSnapshotは、Loggerから
// FlushManager、CompositeFailureHandler、filesystem failure archiveまでのproduction経路を
// 結合して検証します。backend送信後のdurable archiveにも失敗したsnapshotは通常bufferへ
// 戻さず、Closeでも再送しません。
func TestOpenSearchLoggerDurableArchiveFailureDropsDetachedSnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	var backendRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendRequests.Add(1)
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Contains(t, r.URL.Path, "/test-index/_create/")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"type":"unavailable","reason":"temporary failure"},"status":503}`))
	}))
	t.Cleanup(server.Close)

	client, err := infraopensearch.NewClient(
		infraopensearch.NewOpenSearchClientConfigAdapter(&pkgopensearch.Config{
			Endpoint: server.URL,
			Timeout:  time.Second,
			AuthConfig: pkgopensearch.AuthConfig{
				Type: pkgopensearch.AuthTypeNone,
			},
		}),
		nil,
		tracer,
		meter,
	)
	require.NoError(t, err)

	archiveRoot := t.TempDir()
	failureStoreConfig := pkgdurable.FailureStoreConfig{
		MaxConcurrent: 1,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive},
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			FileArchiveConfig: pkgdurable.FileArchiveConfig{
				Target:                     "test-app",
				FailureFileArchivePath:     archiveRoot,
				Dir:                        "opensearch-failure",
				DirCaseRetryable:           "retryable",
				DirCasePermanent:           "permanent",
				FileNameFormat:             pkgfsstore.FailureArchiveFileNameFormat,
				FileDateFormat:             pkgfsstore.FileDateFormat,
				LifecycleMode:              pkgfsstore.FileLifecycleModeDisable,
				RotationType:               pkgfsstore.FileRotationDaily,
				MaxFileSize:                infrafsstore.MinAllowedFileSizeBytes,
				MaxDirSize:                 infrafsstore.MinAllowedMaxDirSize,
				MaxFileAge:                 infrafsstore.MinAllowedMaxFileAge,
				MaxFileBackups:             infrafsstore.MinAllowedMaxFileBackups,
				UsageCacheTTL:              time.Hour,
				CompressFormat:             pkgfsstore.CompressGzip,
				MaxWriteRetries:            0,
				FileOperationRetryWaitTime: time.Millisecond,
				OverloadPolicyConfig: pkgdurable.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:        pkgdurable.OverloadPolicyDrop,
					MinAvailableDiskSpace: pkgdurable.MinFileArchiveAvailableDiskSpace,
				},
			},
		},
	}

	resourceConfig := pkglogger.Resource{
		ServiceName:    "opensearch-durable-integration",
		ServiceVersion: "test",
		HostName:       "localhost",
		OSType:         "linux",
		Environment:    "test",
		RuntimeType:    "go",
		Cloud:          &pkglogger.CloudResourceInfo{},
	}
	fmConfig := NewFlushManagerConfigAdapter(pkglogger.OpenSearchFlushManagerConfig{
		Index:              "test-index",
		FailureStoreConfig: failureStoreConfig,
		Resource:           resourceConfig,
		MaxItemRetries:     0,
	})
	failureHandler, err := BuildFailureHandler(
		ctx,
		clock.NewUTCClock(),
		fmConfig,
		nil,
		tracer,
		meter,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, failureHandler.Handlers, 1)
	archivePipeline, ok := failureHandler.Handlers[0].(*fsarchivehandler.FailureArchivePipeline)
	require.True(t, ok, "failure handler type = %T, want *fsarchivehandler.FailureArchivePipeline", failureHandler.Handlers[0])
	failureHandler.Handlers[0] = newOpenSearchFailFirstArchiveHandler(archivePipeline)

	loggerConfig := NewMockOpenSearchConfigProvider()
	loggerConfig.maxBulkDocument = 1
	loggerConfig.flushInterval = time.Hour
	resourceProvider := resource.NewResourceAdapter(resourceConfig)
	loggerBase := &LoggerBase{
		Clock:  clock.NewUTCClock(),
		Tracer: tracer,
		Meter:  meter,
	}
	logger, err := NewOpenSearchLogger(
		ctx,
		loggerConfig,
		client,
		fmConfig,
		resourceProvider,
		failureHandler,
		tracer,
		meter,
		loggerBase,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = logger.Close(context.Background())
	})

	const message = "drop after durable archive failure"
	err = logger.testInfo(ctx, message)
	require.Error(t, err, "Info must report the failed durable archive write")
	require.ErrorIs(t, err, fsarchivehandler.ErrFailureArchiveWrite)
	assert.Equal(t, 0, entryCount(logger), "failed threshold flush must not restore its snapshot")
	assert.Equal(t, int32(1), backendRequests.Load())

	require.NoError(t, logger.Close(ctx))
	assert.Equal(t, 0, entryCount(logger))
	assert.Equal(t, int32(1), backendRequests.Load(), "Close must not retry the detached snapshot")
}

type openSearchFailFirstArchiveHandler struct {
	delegate *fsarchivehandler.FailureArchivePipeline
	failNext atomic.Bool
}

var (
	_ flushfailure.FailureHandler  = (*openSearchFailFirstArchiveHandler)(nil)
	_ flushfailure.HandlerShutdown = (*openSearchFailFirstArchiveHandler)(nil)
)

func newOpenSearchFailFirstArchiveHandler(
	delegate *fsarchivehandler.FailureArchivePipeline,
) *openSearchFailFirstArchiveHandler {
	handler := &openSearchFailFirstArchiveHandler{delegate: delegate}
	handler.failNext.Store(true)
	return handler
}

func (h *openSearchFailFirstArchiveHandler) HandleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	if h.failNext.Swap(false) {
		result = openSearchUnencodableRetryableResult(result)
	}
	return h.delegate.HandleFailure(ctx, result)
}

func (h *openSearchFailFirstArchiveHandler) Shutdown(ctx context.Context) error {
	return h.delegate.Shutdown(ctx)
}

func openSearchUnencodableRetryableResult(
	result loggerfailure.SendResult,
) loggerfailure.SendResult {
	result.RetryableRecords = append(
		[]loggerfailure.FailedRecord(nil),
		result.RetryableRecords...,
	)
	if len(result.RetryableRecords) == 0 {
		return result
	}
	result.RetryableRecords[0].Payload = map[string]any{
		"unencodable": make(chan struct{}),
	}
	return result
}
