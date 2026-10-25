package clickhouse

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
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClickHouseLoggerDurableWriteFailureDropsSnapshotWithoutCloseRetry は、backend送信から
// failure archiveへの実書き込みまでproduction実装を結合し、durable保存にも失敗した
// snapshotを通常bufferへ戻さず、Closeでbackend送信を繰り返さないことを確認します。
func TestClickHouseLoggerDurableWriteFailureDropsSnapshotWithoutCloseRetry(t *testing.T) {
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	archiveClock := fixedClickHouseClock{
		t: time.Date(2026, 8, 11, 18, 0, 0, 0, time.UTC),
	}
	fmCfg := newClickHouseFailureArchiveFlushManagerConfig(
		t.TempDir(),
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
	failureHandler.Handlers[0] = newClickHouseFailFirstArchiveHandler(archivePipeline)

	client := &recordingClickHouseClient{
		err: &pkgclickhouse.HTTPStatusError{
			Code:   http.StatusServiceUnavailable,
			Status: http.StatusText(http.StatusServiceUnavailable),
		},
	}
	loggerCfg := newTestClickHouseLoggerBaseConfig()
	loggerCfg.FlushInterval = time.Hour
	loggerCfg.MaxEntries = 1
	logger, err := NewClickHouseLogger(
		ctx,
		NewClickHouseLoggerConfigAdapter(loggerCfg),
		client,
		fmCfg,
		newTestClickHouseResourceProvider(),
		failureHandler,
		tracer,
		meter,
		newTestClickHouseLoggerBase(),
		WithClock(archiveClock),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = logger.Close(context.Background())
	})

	err = handleTestInfo(ctx, logger, "clickhouse drop after durable failure")
	require.Error(t, err)
	assert.ErrorIs(t, err, fsarchivehandler.ErrFailureArchiveWrite)
	assert.Equal(t, 0, lifecycleClickHouseEntryCount(logger))
	assert.Equal(t, 1, client.batchCount())

	require.NoError(t, logger.Close(ctx))
	assert.Equal(t, 0, lifecycleClickHouseEntryCount(logger))
	assert.Equal(t, 1, client.batchCount())
}

type clickHouseFailFirstArchiveHandler struct {
	delegate *fsarchivehandler.FailureArchivePipeline
	failNext atomic.Bool
}

var (
	_ flushfailure.FailureHandler  = (*clickHouseFailFirstArchiveHandler)(nil)
	_ flushfailure.HandlerShutdown = (*clickHouseFailFirstArchiveHandler)(nil)
)

func newClickHouseFailFirstArchiveHandler(
	delegate *fsarchivehandler.FailureArchivePipeline,
) *clickHouseFailFirstArchiveHandler {
	handler := &clickHouseFailFirstArchiveHandler{delegate: delegate}
	handler.failNext.Store(true)
	return handler
}

func (h *clickHouseFailFirstArchiveHandler) HandleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	if h.failNext.Swap(false) {
		result = clickHouseUnencodableRetryableResult(result)
	}
	return h.delegate.HandleFailure(ctx, result)
}

func (h *clickHouseFailFirstArchiveHandler) Shutdown(ctx context.Context) error {
	return h.delegate.Shutdown(ctx)
}

func clickHouseUnencodableRetryableResult(
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
