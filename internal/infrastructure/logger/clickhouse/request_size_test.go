package clickhouse

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestClickHouseLoggerRejectsNonPositiveRequestByteLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure func(*pkglogger.ClickHouseLoggerBaseConfig)
		wantField pkglogger.ConfigField
	}{
		{
			name: "buffer flush bytes",
			configure: func(cfg *pkglogger.ClickHouseLoggerBaseConfig) {
				cfg.BufferFlushBytes = 0
			},
			wantField: pkglogger.ConfigFieldBufferFlushBytes,
		},
		{
			name: "max insert request bytes",
			configure: func(cfg *pkglogger.ClickHouseLoggerBaseConfig) {
				cfg.MaxInsertRequestBytes = 0
			},
			wantField: pkglogger.ConfigFieldMaxInsertRequestBytes,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := newTestClickHouseLoggerBaseConfig()
			test.configure(&cfg)
			_, err := NewClickHouseLogger(
				context.Background(),
				NewClickHouseLoggerConfigAdapter(cfg),
				nil,
				nil,
				nil,
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
				nil,
			)
			requireClickHouseConfigFieldError(t, err, pkglogger.ErrNonPositiveSetting, test.wantField, 0)
		})
	}
}

func TestClickHouseLoggerAllowsSoftThresholdAboveHardRequestLimit(t *testing.T) {
	t.Parallel()

	cfg := newTestClickHouseLoggerBaseConfig()
	cfg.BufferFlushBytes = 2 * 1024
	cfg.MaxInsertRequestBytes = 1024
	logger, err := NewClickHouseLogger(
		context.Background(),
		NewClickHouseLoggerConfigAdapter(cfg),
		nil,
		nil,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, logger.Close(context.Background()))
}

func TestClickHouseLoggerFlushesAtBufferByteThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestClickHouseLoggerBaseConfig()
		cfg.MaxEntries = 100
		cfg.BufferFlushBytes = 1
		cfg.MaxInsertRequestBytes = 10 * 1024 * 1024
		client := &recordingClickHouseClient{}
		logger := newTestClickHouseLoggerWithConfigAndOptions(
			t,
			client,
			cfg,
			WithClock(fixedClickHouseClock{t: time.Unix(1, 0).UTC()}),
		)
		t.Cleanup(func() { require.NoError(t, logger.Close(context.Background())) })

		require.NoError(t, handleTestInfo(context.Background(), logger, "byte threshold"))
		synctest.Wait()
		require.Equal(t, 1, client.batchCount())
	})
}

func TestClickHouseFallbackFlushesAtBufferByteThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestClickHouseLoggerBaseConfig()
		cfg.MaxEntries = 100
		cfg.BufferFlushBytes = 1
		cfg.MaxInsertRequestBytes = 10 * 1024 * 1024
		client := &recordingClickHouseClient{}
		fallback := newTimerTestClickHouseFallback(t, client, cfg)

		fallback.enqueueProcess(context.Background(), &logging.LogEntry{
			OccurredAt: time.Unix(1, 0).UTC(),
			Level:      pkglogger.LogLevelInfo,
			Message:    "fallback byte threshold",
		})
		// byte閾値のwakeでfallback workerが送信します。
		synctest.Wait()
		require.Equal(t, 1, client.batchCount())
		require.NoError(t, fallback.Close(context.Background()))
	})
}

func TestClickHouseFlushManagerSplitsHardLimitWithDistinctStableTokens(t *testing.T) {
	logger, client := newClickHouseRequestBoundaryTestLogger(t, nil)
	first := newTestLogEntry(pkglogger.LogLevelInfo, "first physical request", nil)
	second := newTestLogEntry(pkglogger.LogLevelInfo, "second physical request", nil)
	hardLimit := maxClickHouseEntryBytes(t, logger, first, second)
	cfg := logger.config.(*clickHouseLoggerConfigAdapter)
	cfg.cfg.MaxEntries = 2
	cfg.cfg.BufferFlushBytes = 10 * 1024 * 1024
	cfg.cfg.MaxInsertRequestBytes = hardLimit

	require.NoError(t, logger.enqueue(context.Background(), first))
	require.NoError(t, logger.enqueue(context.Background(), second))
	require.Len(t, client.batches, 2)
	require.Len(t, client.batches[0], 1)
	require.Len(t, client.batches[1], 1)
	require.Len(t, client.insertOptions, 2)
	firstToken := client.insertOptions[0].InsertDeduplicationToken
	secondToken := client.insertOptions[1].InsertDeduplicationToken
	require.NotEmpty(t, firstToken)
	require.NotEmpty(t, secondToken)
	require.NotEqual(t, firstToken, secondToken)
	require.NoError(t, logger.Close(context.Background()))
}

func TestClickHouseSplitFailurePayloadKeepsItsPhysicalRequestToken(t *testing.T) {
	handled := make(chan loggerfailure.SendResult, 2)
	failureHandler, err := flushfailure.NewCompositeFailureHandler(
		[]flushfailure.FailureHandler{&clickHouseFailureHandler{handled: handled}},
		1,
	)
	require.NoError(t, err)
	logger, client := newClickHouseRequestBoundaryTestLogger(t, failureHandler)
	client.errs = []error{
		&pkgclickhouse.HTTPStatusError{
			Code:   http.StatusTooManyRequests,
			Status: http.StatusText(http.StatusTooManyRequests),
		},
		&pkgclickhouse.HTTPStatusError{
			Code:   http.StatusTooManyRequests,
			Status: http.StatusText(http.StatusTooManyRequests),
		},
	}
	first := newTestLogEntry(pkglogger.LogLevelInfo, "first failed physical request", nil)
	second := newTestLogEntry(pkglogger.LogLevelInfo, "second failed physical request", nil)
	hardLimit := maxClickHouseEntryBytes(t, logger, first, second)
	cfg := logger.config.(*clickHouseLoggerConfigAdapter)
	cfg.cfg.MaxEntries = 2
	cfg.cfg.BufferFlushBytes = 10 * 1024 * 1024
	cfg.cfg.MaxInsertRequestBytes = hardLimit

	require.NoError(t, logger.enqueue(context.Background(), first))
	require.NoError(t, logger.enqueue(context.Background(), second))
	require.Len(t, client.insertOptions, 2)

	firstResult := waitForClickHouseHandledFailure(t, handled)
	secondResult := waitForClickHouseHandledFailure(t, handled)
	results := []loggerfailure.SendResult{firstResult, secondResult}
	for index, result := range results {
		require.Len(t, result.RetryableRecords, 1)
		sentToken := client.insertOptions[index].InsertDeduplicationToken
		require.NotEmpty(t, sentToken)
		require.Equal(
			t,
			sentToken,
			result.RetryableRecords[0].Payload[pkgclickhouse.SettingInsertDeduplicationToken],
		)
	}
	require.NotEqual(
		t,
		client.insertOptions[0].InsertDeduplicationToken,
		client.insertOptions[1].InsertDeduplicationToken,
	)
	require.NoError(t, logger.Close(context.Background()))
}

func TestClickHouseSingleOversizedRowIsQuarantinedBeforeClientSend(t *testing.T) {
	handled := make(chan loggerfailure.SendResult, 1)
	failureHandler, err := flushfailure.NewCompositeFailureHandler(
		[]flushfailure.FailureHandler{&clickHouseFailureHandler{handled: handled}},
		1,
	)
	require.NoError(t, err)
	logger, client := newClickHouseRequestBoundaryTestLogger(t, failureHandler)
	cfg := logger.config.(*clickHouseLoggerConfigAdapter)
	cfg.cfg.MaxEntries = 100
	cfg.cfg.BufferFlushBytes = 1
	cfg.cfg.MaxInsertRequestBytes = 1

	require.NoError(t, handleTestInfo(context.Background(), logger, "oversized before send"))
	require.Equal(t, 0, client.batchCount())
	var result loggerfailure.SendResult
	select {
	case result = <-handled:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("the oversized row was not passed to the failure handler")
	}
	require.Empty(t, result.RetryableRecords)
	require.Empty(t, result.DeadRecords)
	require.Len(t, result.QuarantineRecords, 1)
	require.True(t, result.Meta.AttemptedAt.IsZero())
	require.Equal(
		t,
		FailureCodeLogRecordTooLarge,
		result.QuarantineRecords[0].FailureCode,
	)
	require.ErrorIs(t, result.TerminalError, infraclickhouse.ErrLogRecordTooLarge)
	require.NoError(t, logger.Close(context.Background()))
}

func TestClickHouseUnencodableRowIsQuarantinedBeforeClientSend(t *testing.T) {
	handled := make(chan loggerfailure.SendResult, 1)
	failureHandler, err := flushfailure.NewCompositeFailureHandler(
		[]flushfailure.FailureHandler{&clickHouseFailureHandler{handled: handled}},
		1,
	)
	require.NoError(t, err)
	logger, client := newClickHouseRequestBoundaryTestLogger(t, failureHandler)
	entry := newTestLogEntry(pkglogger.LogLevelInfo, "unencodable before send", nil)
	entry.Fields = map[string]any{"unsupported": make(chan struct{})}

	require.NoError(t, logger.flushManager.Flush(context.Background(), []*logging.LogEntry{entry}))
	require.Equal(t, 0, client.batchCount())
	result := waitForClickHouseHandledFailure(t, handled)
	require.Empty(t, result.RetryableRecords)
	require.Empty(t, result.DeadRecords)
	require.Len(t, result.QuarantineRecords, 1)
	require.True(t, result.Meta.AttemptedAt.IsZero())
	require.Equal(t, FailureCodeLogRecordEncoding, result.QuarantineRecords[0].FailureCode)
	require.NoError(t, logger.Close(context.Background()))
}

func TestClickHouseLoggerDropsSnapshotWhenPhysicalRequestHandlingFails(t *testing.T) {
	handleErr := errors.New("durable handling failed")
	failureHandler, err := flushfailure.NewCompositeFailureHandler(
		[]flushfailure.FailureHandler{&clickHouseFailureHandler{err: handleErr}},
		1,
	)
	require.NoError(t, err)
	logger, client := newClickHouseRequestBoundaryTestLogger(t, failureHandler)
	client.errs = []error{
		nil,
		&pkgclickhouse.HTTPStatusError{
			Code:   http.StatusTooManyRequests,
			Status: http.StatusText(http.StatusTooManyRequests),
		},
		nil,
	}
	first := newTestLogEntry(pkglogger.LogLevelInfo, "completed first chunk", nil)
	second := newTestLogEntry(pkglogger.LogLevelInfo, "retry second chunk", nil)
	hardLimit := maxClickHouseEntryBytes(t, logger, first, second)
	cfg := logger.config.(*clickHouseLoggerConfigAdapter)
	cfg.cfg.MaxEntries = 2
	cfg.cfg.BufferFlushBytes = 10 * 1024 * 1024
	cfg.cfg.MaxInsertRequestBytes = hardLimit

	require.NoError(t, logger.enqueue(context.Background(), first))
	err = logger.enqueue(context.Background(), second)
	require.ErrorIs(t, err, handleErr)
	logger.mu.Lock()
	buffered := append([]*logging.LogEntry(nil), logger.entries...)
	logger.mu.Unlock()
	require.Empty(t, buffered)
	require.Len(t, client.batches, 2)
	require.Equal(t, "completed first chunk", client.batches[0][0].Message)
	require.Equal(t, "retry second chunk", client.batches[1][0].Message)

	require.NoError(t, logger.Close(context.Background()))
	require.Len(t, client.batches, 2)
}

func newClickHouseRequestBoundaryTestLogger(
	t *testing.T,
	failureHandler *flushfailure.CompositeFailureHandler,
) (*ClickHouseLogger, *recordingClickHouseClient) {
	t.Helper()
	client := &recordingClickHouseClient{}
	cfg := NewClickHouseLoggerConfigAdapter(newTestClickHouseLoggerBaseConfig())
	base := newTestClickHouseLoggerBase()
	logger, err := NewClickHouseLogger(
		context.Background(),
		cfg,
		client,
		newTestClickHouseFlushManagerConfig(),
		newTestClickHouseResourceProvider(),
		failureHandler,
		observer.NopTracer{},
		observer.NopMeter{},
		base,
		WithClock(fixedClickHouseClock{t: time.Unix(1, 0).UTC()}),
	)
	require.NoError(t, err)
	return logger, client
}

func maxClickHouseEntryBytes(
	t *testing.T,
	logger *ClickHouseLogger,
	entries ...*logging.LogEntry,
) int {
	t.Helper()
	maxBytes := 0
	for _, entry := range entries {
		entryBytes, err := measureClickHouseLogEntryJSONEachRowBytes(
			logger.LoggerBase,
			logger.resourceProvider,
			entry,
		)
		require.NoError(t, err)
		if entryBytes > maxBytes {
			maxBytes = entryBytes
		}
	}
	return maxBytes
}
