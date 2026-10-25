package loki

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
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLokiLoggerDurableWriteFailureDropsDetachedSnapshotは、LoggerからLoki送信、
// failure分類、Composite、failure archiveへの実書き込みまでを結合し、durable保存にも
// 失敗したsnapshotを通常bufferへ戻さず、Closeでも再送しないことを確認します。
func TestLokiLoggerDurableWriteFailureDropsDetachedSnapshot(t *testing.T) {
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	archiveClock := fixedLokiClock{
		t: time.Date(2026, 6, 23, 12, 34, 56, 0, time.UTC),
	}
	fmCfg := newLokiFailureStoreTestFlushManagerConfig(
		t.TempDir(),
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
	failureHandler.Handlers[0] = newLokiFailFirstArchiveHandler(archivePipeline)

	client := &recordingFlushManagerLokiClient{
		pushErr: &pkgloki.HTTPStatusError{
			Code:   http.StatusServiceUnavailable,
			Status: http.StatusText(http.StatusServiceUnavailable),
		},
	}
	logger, err := NewLokiLogger(
		ctx,
		newTestLokiLoggerConfig(1, time.Hour, false),
		client,
		fmCfg,
		newTestLokiResourceProvider(),
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

	err = logger.testInfo(ctx, "loki drop after durable failure")
	require.Error(t, err)
	assert.ErrorIs(t, err, fsarchivehandler.ErrFailureArchiveWrite)
	assert.Equal(t, 0, lifecycleLokiEntryCount(logger))
	assert.Equal(t, 1, client.pushCalls)

	require.NoError(t, logger.Close(ctx))
	assert.Equal(t, 0, lifecycleLokiEntryCount(logger))
	assert.Equal(t, 1, client.pushCalls, "Close must not retry the detached snapshot")
}

type lokiFailFirstArchiveHandler struct {
	delegate *fsarchivehandler.FailureArchivePipeline
	failNext atomic.Bool
}

var (
	_ flushfailure.FailureHandler  = (*lokiFailFirstArchiveHandler)(nil)
	_ flushfailure.HandlerShutdown = (*lokiFailFirstArchiveHandler)(nil)
)

func newLokiFailFirstArchiveHandler(
	delegate *fsarchivehandler.FailureArchivePipeline,
) *lokiFailFirstArchiveHandler {
	handler := &lokiFailFirstArchiveHandler{delegate: delegate}
	handler.failNext.Store(true)
	return handler
}

func (h *lokiFailFirstArchiveHandler) HandleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	if h.failNext.Swap(false) {
		result = lokiUnencodableRetryableResult(result)
	}
	return h.delegate.HandleFailure(ctx, result)
}

func (h *lokiFailFirstArchiveHandler) Shutdown(ctx context.Context) error {
	return h.delegate.Shutdown(ctx)
}

func lokiUnencodableRetryableResult(
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
