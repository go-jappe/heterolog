package cloudwatchlogs

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/config/failure"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/fsarchivehandler"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCloudWatchLogsLoggerDurableWriteFailureDropsSnapshotWithoutCloseRetry は、Loggerから
// CloudWatch Logs送信、failure分類、Composite、failure archiveへの実書き込みまでを
// 結合して検証します。archive保存にも失敗したsnapshotを通常bufferへ戻さず、Closeで
// backend送信を繰り返さないことを確認します。
func TestCloudWatchLogsLoggerDurableWriteFailureDropsSnapshotWithoutCloseRetry(t *testing.T) {
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	archiveClock := fixedCloudWatchLogsClock{
		t: time.Date(2026, 8, 11, 18, 5, 0, 0, time.UTC),
	}
	spoolCfg := cloudWatchLogsLoggerSpoolTestConfig{
		spoolPath: t.TempDir(),
		target:    "cloudwatchlogs-durable-buffer-test",
		dir:       "cloudwatchlogs",
	}
	fmCfg := cloudWatchLogsFailureStoreFlushManagerConfig(
		t.TempDir(),
		spoolCfg,
		[]pkgdurable.DurableType{failure.DurableTypeFsstore},
	)
	failureHandler, err := BuildFailureHandler(
		ctx,
		archiveClock,
		fmCfg,
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
	require.True(t, ok, "failure handler type = %T", failureHandler.Handlers[0])
	failureHandler.Handlers[0] = newCloudWatchLogsFailFirstArchiveHandler(archivePipeline)

	client := &recordingCloudWatchLogsClient{
		err: &pkgcloudwatchlogs.HTTPStatusError{
			Code:   http.StatusTooManyRequests,
			Status: http.StatusText(http.StatusTooManyRequests),
		},
	}
	logger, err := NewCloudWatchLogsLogger(
		ctx,
		newTestCloudWatchLogsLoggerConfig(1, time.Hour, false),
		client,
		fmCfg,
		newTestCloudWatchLogsResourceProvider(),
		failureHandler,
		tracer,
		meter,
		&LoggerBase{},
		WithClock(archiveClock),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = logger.Close(context.Background())
	})

	err = handleTestInfo(ctx, logger, "cloudwatchlogs drop after durable failure")
	require.Error(t, err)
	assert.ErrorIs(t, err, fsarchivehandler.ErrFailureArchiveWrite)
	assert.Equal(t, 0, lifecycleCloudWatchLogsEntryCount(logger))
	assert.Len(t, client.events, 1)

	require.NoError(t, logger.Close(ctx))
	assert.Equal(t, 0, lifecycleCloudWatchLogsEntryCount(logger))
	assert.Len(t, client.events, 1)
}

type cloudWatchLogsFailFirstArchiveHandler struct {
	delegate *fsarchivehandler.FailureArchivePipeline
	failNext atomic.Bool
}

var (
	_ flushfailure.FailureHandler  = (*cloudWatchLogsFailFirstArchiveHandler)(nil)
	_ flushfailure.HandlerShutdown = (*cloudWatchLogsFailFirstArchiveHandler)(nil)
)

func newCloudWatchLogsFailFirstArchiveHandler(
	delegate *fsarchivehandler.FailureArchivePipeline,
) *cloudWatchLogsFailFirstArchiveHandler {
	handler := &cloudWatchLogsFailFirstArchiveHandler{delegate: delegate}
	handler.failNext.Store(true)
	return handler
}

func (h *cloudWatchLogsFailFirstArchiveHandler) HandleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	if h.failNext.Swap(false) {
		result = cloudWatchLogsUnencodableRetryableResult(result)
	}
	return h.delegate.HandleFailure(ctx, result)
}

func (h *cloudWatchLogsFailFirstArchiveHandler) Shutdown(ctx context.Context) error {
	return h.delegate.Shutdown(ctx)
}

func cloudWatchLogsUnencodableRetryableResult(
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
