package clickhouse

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClickHouseFlushManagerHandlesFailureBeforeReturning(t *testing.T) {
	handled := make(chan loggerfailure.SendResult, 1)
	handleErr := errors.New("clickhouse durable handling failed")
	failureHandler, err := flushfailure.NewCompositeFailureHandler(
		[]flushfailure.FailureHandler{
			&clickHouseFailureHandler{handled: handled, err: handleErr},
		},
		1,
	)
	require.NoError(t, err)
	client := &recordingClickHouseClient{
		err: &pkgclickhouse.HTTPStatusError{
			Code:   http.StatusTooManyRequests,
			Status: http.StatusText(http.StatusTooManyRequests),
		},
	}
	flushManager := NewClickHouseFlushManager(
		newTestClickHouseFlushManagerBase(t),
		client,
		newTestClickHouseFlushManagerConfig(),
		failureHandler,
		newTestClickHouseResourceProvider(),
		observer.NopTracer{},
		observer.NopMeter{},
	)

	entry := &logging.LogEntry{
		OccurredAt: logging.ResolveOccurredAt(flushManager.Clock),
		Level:      pkglogger.LogLevelError,
		Message:    "dispatch without delivery report",
	}
	require.ErrorIs(t, flushManager.Flush(context.Background(), []*logging.LogEntry{entry}), handleErr)

	result := waitForClickHouseHandledFailure(t, handled)
	require.Len(t, result.RetryableRecords, 1)
	assert.Equal(t, "dispatch without delivery report", result.RetryableRecords[0].Payload["message"])
}

func TestClickHouseFlushManagerReturnsNoFailureHandlerForFailedNormalFlush(t *testing.T) {
	client := &recordingClickHouseClient{
		err: &pkgclickhouse.HTTPStatusError{
			Code:   http.StatusTooManyRequests,
			Status: http.StatusText(http.StatusTooManyRequests),
		},
	}
	flushManager := NewClickHouseFlushManager(
		newTestClickHouseFlushManagerBase(t),
		client,
		newTestClickHouseFlushManagerConfig(),
		nil,
		newTestClickHouseResourceProvider(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	entry := &logging.LogEntry{
		OccurredAt: logging.ResolveOccurredAt(flushManager.Clock),
		Level:      pkglogger.LogLevelError,
		Message:    "normal flush requires a failure handler",
	}

	err := flushManager.Flush(context.Background(), []*logging.LogEntry{entry})

	require.ErrorIs(t, err, flushfailure.ErrNoFailureHandler)
	require.Equal(t, 1, client.batchCount())
}

func TestClickHouseFlushManagerWaitsForDurableFailureHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handled := make(chan loggerfailure.SendResult, 1)
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
		// 失敗時もbubble内のgoroutineを停止させるため、待機中のfailure処理を解放します。
		t.Cleanup(releaseHandler)
		failureHandler, err := flushfailure.NewCompositeFailureHandler(
			[]flushfailure.FailureHandler{
				&clickHouseFailureHandler{
					handled: handled,
					release: release,
				},
			},
			1,
		)
		require.NoError(t, err)
		client := &recordingClickHouseClient{
			err: &pkgclickhouse.HTTPStatusError{
				Code:   http.StatusTooManyRequests,
				Status: http.StatusText(http.StatusTooManyRequests),
			},
		}
		flushManager := NewClickHouseFlushManager(
			newTestClickHouseFlushManagerBase(t),
			client,
			newTestDeliveryClickHouseFlushManagerConfig(),
			failureHandler,
			newTestClickHouseResourceProvider(),
			observer.NopTracer{},
			observer.NopMeter{},
		)

		entry := &logging.LogEntry{
			OccurredAt: logging.ResolveOccurredAt(flushManager.Clock),
			Level:      pkglogger.LogLevelError,
			Message:    "durable failure handler remains running",
		}
		flushDone := make(chan error, 1)
		go func() {
			flushDone <- flushManager.Flush(context.Background(), []*logging.LogEntry{entry})
		}()

		synctest.Wait()
		select {
		case err := <-flushDone:
			t.Fatalf("Flush returned before the durable failure handler completed: %v", err)
		default:
		}

		result := waitForClickHouseHandledFailure(t, handled)
		require.Len(t, result.RetryableRecords, 1)
		assert.Equal(t, "durable failure handler remains running", result.RetryableRecords[0].Payload["message"])
		releaseHandler()
		require.NoError(t, <-flushDone)
	})
}

func TestDeliveryClickHouseLoggerCloseWaitsForAcceptedDurableFailureHandling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handled := make(chan loggerfailure.SendResult, 1)
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
		// 失敗時もbubble内のgoroutineを停止させるため、待機中のfailure処理を解放します。
		t.Cleanup(releaseHandler)
		failureHandler, err := flushfailure.NewReportingCompositeFailureHandler(
			[]flushfailure.FailureHandlerWithReport{
				&clickHouseFailureHandler{
					handled: handled,
					release: release,
				},
			},
			1,
		)
		require.NoError(t, err)
		client := &recordingClickHouseClient{
			err: &pkgclickhouse.HTTPStatusError{
				Code:   http.StatusTooManyRequests,
				Status: http.StatusText(http.StatusTooManyRequests),
			},
		}
		baseCfg := newTestDeliveryClickHouseLoggerBaseConfig()
		baseCfg.FlushInterval = time.Hour
		baseCfg.MaxEntries = 100
		logger, err := NewDeliveryClickHouseLogger(
			context.Background(),
			NewDeliveryClickHouseLoggerConfigAdapter(baseCfg),
			client,
			newTestDeliveryClickHouseFlushManagerConfig(),
			newTestClickHouseResourceProvider(),
			failureHandler,
			observer.NopTracer{},
			observer.NopMeter{},
			newTestClickHouseLoggerBase(),
		)
		require.NoError(t, err)
		// cleanupは登録と逆順に実行するため、失敗時も先にfailure処理を解放してからCloseし、
		// bubble内のworkerをdeadlockではなく停止させます。
		t.Cleanup(func() { _ = logger.Close() })
		t.Cleanup(releaseHandler)

		report, err := handleTestDeliveryInfo(context.Background(), logger, "do not requeue for internal handling")
		require.NoError(t, err)
		require.Len(t, report.Targets, 1)
		require.Equal(t, pkglogger.DeliveryEnqueued, report.Targets[0].Status)

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- logger.Close()
		}()

		result := waitForClickHouseHandledFailure(t, handled)
		require.Len(t, result.RetryableRecords, 1)

		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before the accepted failure handling completed: %v", err)
		default:
		}

		releaseHandler()
		select {
		case err := <-closeDone:
			require.NoError(t, err)
		case <-time.After(lifecycleHangTimeout):
			t.Fatal("Close did not return after the failure handler completed")
		}

		require.Equal(t, 1, client.batchCount())
		logger.mu.Lock()
		defer logger.mu.Unlock()
		require.Empty(t, logger.entries)
	})
}

func waitForClickHouseHandledFailure(
	t *testing.T,
	handled <-chan loggerfailure.SendResult,
) loggerfailure.SendResult {
	t.Helper()

	select {
	case result := <-handled:
		return result
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("failure handler was not invoked")
		return loggerfailure.SendResult{}
	}
}

type clickHouseFailureHandler struct {
	handled chan<- loggerfailure.SendResult
	release <-chan struct{}
	err     error
}

func (h *clickHouseFailureHandler) HandleFailure(
	_ context.Context,
	result loggerfailure.SendResult,
) error {
	if h.handled != nil {
		h.handled <- result
	}
	if h.release != nil {
		<-h.release
	}
	return h.err
}

func (h *clickHouseFailureHandler) HandleFailureWithReport(
	ctx context.Context,
	result loggerfailure.SendResult,
) (pkglogger.DeliveryReport, error) {
	return pkglogger.DeliveryReport{}, h.HandleFailure(ctx, result)
}

var _ flushfailure.FailureHandlerWithReport = (*clickHouseFailureHandler)(nil)
