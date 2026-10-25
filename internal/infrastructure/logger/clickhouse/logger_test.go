package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClickHouseLoggerInfoQueuesUntilFlushWithoutPerCallBackendInsert(t *testing.T) {
	client := &recordingClickHouseClient{}
	logger := newTestClickHouseLogger(t, client)

	err := handleTestInfo(context.Background(), logger, "queued regular log")
	require.NoError(t, err)
	assert.Equal(t, 0, client.batchCount())

	require.NoError(t, logger.Close(context.Background()))
	require.Equal(t, 1, client.batchCount())
	require.Equal(t, "queued regular log", client.batches[0][0].Message)
}

func TestClickHouseFallbackLoggerBatchesWithoutPerCallBackendInsert(t *testing.T) {
	client := &recordingClickHouseClient{}
	logger := &FallbackLogger{ClickHouseLogger: newTestClickHouseLogger(t, client)}
	entry := newTestLogEntry(pkglogger.LogLevelInfo, "queued fallback log", nil)

	logger.enqueueProcess(context.Background(), entry)
	assert.Equal(t, 0, client.batchCount())

	require.NoError(t, logger.Close(context.Background()))
	require.Equal(t, 1, client.batchCount())
	require.Equal(t, "queued fallback log", client.batches[0][0].Message)
}

func TestDeliveryClickHouseLoggerInfoReportsEnqueuedWithoutBackendSuccess(t *testing.T) {
	client := &recordingClickHouseClient{}
	logger := newTestDeliveryClickHouseLogger(t, client)

	report, err := handleTestDeliveryInfo(context.Background(), logger, "queued delivery log")
	require.NoError(t, err)
	require.Len(t, report.Targets, 1)
	result := report.Targets[0]
	assert.Equal(t, deliveryBackendClickHouse, result.Backend)
	assert.Equal(t, pkglogger.DeliveryEnqueued, result.Status)
	assert.False(t, result.Status.IsRegistered())
	assert.Equal(t, 0, client.batchCount())

	require.NoError(t, logger.Close())
	require.Equal(t, 1, client.batchCount())
	require.Equal(t, "queued delivery log", client.batches[0][0].Message)
}

func TestDeliveryClickHouseLoggerMaxEntriesTriggersBackgroundFlushWithoutBackendSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &recordingClickHouseClient{}
		cfg := newTestDeliveryClickHouseLoggerBaseConfig()
		cfg.MaxEntries = 1
		logger := newTestDeliveryClickHouseLoggerWithConfig(t, client, cfg)

		report, err := handleTestDeliveryInfo(context.Background(), logger, "queued by max entries")
		require.NoError(t, err)
		require.Len(t, report.Targets, 1)
		result := report.Targets[0]
		assert.Equal(t, deliveryBackendClickHouse, result.Backend)
		assert.Equal(t, pkglogger.DeliveryEnqueued, result.Status)
		assert.False(t, result.Status.IsRegistered())

		// 件数閾値のwakeでbackground workerが送信します。
		synctest.Wait()
		require.Equal(t, 1, client.batchCount())
		require.Equal(t, "queued by max entries", client.batches[0][0].Message)
		require.NoError(t, logger.Close())
	})
}

// errorを渡さないErrorWithStackも拒否せず、要求されたstackを持つERRORとしてqueueへ受理します。
// 送信はClose時のbatchで行い、rowのerrorは空のままstack traceだけを持ちます。
func TestDeliveryClickHouseLoggerErrorWithStackWithoutErrorEnqueuesStack(t *testing.T) {
	client := &recordingClickHouseClient{}
	logger := newTestDeliveryClickHouseLogger(t, client)

	report, err := handleTestDeliveryErrorWithStack(context.Background(), logger, "missing error", nil)
	require.NoError(t, err)
	require.Len(t, report.Targets, 1)
	result := report.Targets[0]
	assert.Equal(t, deliveryBackendClickHouse, result.Backend)
	assert.Equal(t, pkglogger.DeliveryEnqueued, result.Status)
	assert.NoError(t, result.Err)
	assert.False(t, result.Status.IsRegistered())
	assert.Equal(t, 0, client.batchCount())

	require.NoError(t, logger.Close())
	requireClickHouseStackRowWithoutError(t, client, "missing error")
}

// 通常APIでもerrorを渡さないErrorWithStackを拒否せず、stackを持つERRORのrowとして送信します。
func TestClickHouseLoggerErrorWithStackWithoutErrorInsertsStack(t *testing.T) {
	client := &recordingClickHouseClient{}
	logger := newTestClickHouseLogger(t, client)

	require.NoError(t, handleTestErrorWithStack(context.Background(), logger, "missing error", nil))
	require.NoError(t, logger.Close(context.Background()))
	requireClickHouseStackRowWithoutError(t, client, "missing error")
}

// requireClickHouseStackRowWithoutErrorは、errorのないErrorWithStackが一件のERROR rowとして
// stack trace付きで送信され、errorの値を持たないことを確認します。
func requireClickHouseStackRowWithoutError(t *testing.T, client *recordingClickHouseClient, message string) {
	t.Helper()

	client.mu.Lock()
	batches := append([][]pkgclickhouse.LogRecord(nil), client.batches...)
	client.mu.Unlock()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 1)
	row := batches[0][0]
	assert.Equal(t, pkglogger.LogLevelError.String(), row.Level)
	assert.Equal(t, message, row.Message)
	assert.Empty(t, row.Error)
	assert.NotEmpty(t, row.StackTrace)
}

// FlushManagerがない場合はqueueへ受理せず送信も試みないため、backendの明示的な拒否を表す
// DeliveryFailedではなく、送信前の拒否としてDeliveryRejectedを返します。
func TestDeliveryClickHouseLoggerMissingFlushManagerReportsInternalError(t *testing.T) {
	client := &recordingClickHouseClient{}
	logger := newTestDeliveryClickHouseLogger(t, client)
	logger.mu.Lock()
	logger.flushManager = nil
	logger.mu.Unlock()

	report, err := handleTestDeliveryInfo(context.Background(), logger, "missing flush manager")

	require.Error(t, err)
	require.Len(t, report.Targets, 1)
	result := report.Targets[0]
	assert.Equal(t, deliveryBackendClickHouse, result.Backend)
	assert.Equal(t, pkglogger.DeliveryRejected, result.Status)
	assert.ErrorIs(t, result.Err, err)
	assert.False(t, result.Status.IsRegistered())
	assert.Equal(t, 0, lifecycleClickHouseEntryCount(logger.ClickHouseLogger))
	assert.Equal(t, 0, client.batchCount())
}

func TestClickHouseLoggerOptionsUseClockAndTimeProvider(t *testing.T) {
	client := &recordingClickHouseClient{}
	clockTime := time.Date(2026, 6, 24, 1, 2, 3, 0, time.UTC)
	providerTime := time.Date(2026, 6, 24, 4, 5, 6, 0, time.UTC)
	cfg := newTestClickHouseLoggerBaseConfig()
	cfg.MaxEntries = 1

	logger := newTestClickHouseLoggerWithConfigAndOptions(
		t,
		client,
		cfg,
		WithClock(fixedClickHouseClock{t: clockTime}),
		WithTimeProvider(func() time.Time { return providerTime }),
	)

	err := handleTestInfo(context.Background(), logger, "option clock message")
	require.NoError(t, err)
	require.Equal(t, 1, client.batchCount())
	require.Len(t, client.batches[0], 1)
	assert.True(t, client.batches[0][0].Timestamp.Equal(clockTime))

	logger.updateLastFlushTime()
	assert.True(t, logger.lastFlushTime.Equal(providerTime))
	require.NoError(t, logger.Close(context.Background()))
}

func TestClickHouseFlushManagerFailureDocumentKeepsInsertedRecord(t *testing.T) {
	client := &recordingClickHouseClient{
		err: &pkgclickhouse.HTTPStatusError{Code: http.StatusTooManyRequests, Status: "429 Too Many Requests"},
	}
	logger := newTestClickHouseLogger(t, client)
	flushManager := logger.flushManager.(*ClickHouseFlushManager)
	entry := newTestLogEntry(pkglogger.LogLevelError, "retryable clickhouse", nil)

	res, err := insertClickHouseLogEntriesAsSingleRequestForTest(context.Background(), flushManager, []*logging.LogEntry{entry})

	require.NoError(t, err)
	require.Equal(t, 1, client.batchCount())
	require.Len(t, client.batches[0], 1)
	require.Len(t, res.RetryableRecords, 1)
	insertedRecord := client.batches[0][0]
	doc := res.RetryableRecords[0].Payload
	assert.Equal(t, insertedRecord.Timestamp.Format(time.RFC3339Nano), doc["timestamp"])
	assert.Equal(t, insertedRecord.Line, doc["line"])
	assert.Equal(t, insertedRecord.Message, doc["message"])
	require.Len(t, client.insertOptions, 1)
	token := client.insertOptions[0].InsertDeduplicationToken
	require.NotEmpty(t, token)
	assert.Equal(t, token, doc[pkgclickhouse.SettingInsertDeduplicationToken])
}

func TestClickHouseFlushManagerClassifiesCircuitBreakerOpenAsRetryable(t *testing.T) {
	circuitBreakerErr := retryable.NewCircuitBreakerOpenError(
		"clickhouse insert",
		time.Now().Add(time.Minute),
	)
	client := &recordingClickHouseClient{err: circuitBreakerErr}
	logger := newTestClickHouseLogger(t, client)
	flushManager := logger.flushManager.(*ClickHouseFlushManager)
	entry := newTestLogEntry(pkglogger.LogLevelError, "circuit breaker open", nil)

	res, err := insertClickHouseLogEntriesAsSingleRequestForTest(context.Background(), flushManager, []*logging.LogEntry{entry})

	require.NoError(t, err)
	assert.ErrorIs(t, res.TerminalError, circuitBreakerErr)
	require.Len(t, res.RetryableRecords, 1)
	require.Empty(t, res.DeadRecords)
	require.Empty(t, res.QuarantineRecords)
	assert.Equal(
		t,
		loggerfailure.FailureCode(retryable.RetryReasonCircuitBreakerOpen),
		res.RetryableRecords[0].FailureCode,
	)
}

func TestClickHouseFlushManagerQuarantinesUnknownError(t *testing.T) {
	unknownErr := errors.New("unclassified result")
	client := &recordingClickHouseClient{err: unknownErr}
	logger := newTestClickHouseLogger(t, client)
	flushManager := logger.flushManager.(*ClickHouseFlushManager)
	entry := newTestLogEntry(pkglogger.LogLevelError, "unknown failure", nil)

	res, err := insertClickHouseLogEntriesAsSingleRequestForTest(context.Background(), flushManager, []*logging.LogEntry{entry})

	require.NoError(t, err)
	assert.ErrorIs(t, res.TerminalError, unknownErr)
	require.Empty(t, res.DeadRecords)
	require.Len(t, res.QuarantineRecords, 1)
	require.Empty(t, res.RetryableRecords)
}

func TestClickHouseFlushManagerClassifiesSchemaInsertErrorPermanent(t *testing.T) {
	client := &recordingClickHouseClient{
		err: &pkgclickhouse.HTTPStatusError{
			Code:   http.StatusInternalServerError,
			Status: "500 Internal Server Error",
			Body: []byte(
				"Code: 60. DB::Exception: Table default.logs does not exist. (UNKNOWN_TABLE) (version 26.5.3.52)",
			),
		},
	}
	logger := newTestClickHouseLogger(t, client)
	flushManager := logger.flushManager.(*ClickHouseFlushManager)
	entry := newTestLogEntry(pkglogger.LogLevelError, "permanent clickhouse", nil)

	res, err := insertClickHouseLogEntriesAsSingleRequestForTest(context.Background(), flushManager, []*logging.LogEntry{entry})

	require.NoError(t, err)
	require.Equal(t, 1, client.batchCount())
	require.Empty(t, res.RetryableRecords)
	require.Len(t, res.DeadRecords, 1)
	require.Empty(t, res.QuarantineRecords)
	doc := res.DeadRecords[0].Payload
	assert.Equal(t, "permanent clickhouse", doc["message"])
}

func TestClickHouseFlushManagerMarksRecordInputFailureForIsolatedDurableReplay(t *testing.T) {
	client := &recordingClickHouseClient{
		err: &pkgclickhouse.HTTPStatusError{
			Code:          http.StatusInternalServerError,
			Status:        "500 Internal Server Error",
			ExceptionType: pkgclickhouse.ErrorTypeCannotParseNumber,
		},
	}
	logger := newTestClickHouseLogger(t, client)
	flushManager := logger.flushManager.(*ClickHouseFlushManager)
	entries := []*logging.LogEntry{
		newTestLogEntry(pkglogger.LogLevelInfo, "first", nil),
		newTestLogEntry(pkglogger.LogLevelInfo, "second", nil),
	}

	result, err := insertClickHouseLogEntriesAsSingleRequestForTest(context.Background(), flushManager, entries)

	require.NoError(t, err)
	require.Len(t, result.RetryableRecords, 2)
	require.Empty(t, result.DeadRecords)
	require.Empty(t, result.QuarantineRecords)
	first := result.RetryableRecords[0]
	second := result.RetryableRecords[1]
	assert.Equal(t, loggerfailure.FailureCode(infraclickhouse.RetryReasonRecordIsolationRequired), first.FailureCode)
	assert.Equal(t, infraclickhouse.DurableReplayModeRecordIsolation, first.Payload[infraclickhouse.DurablePayloadFieldReplayMode])
	assert.Equal(t, infraclickhouse.DurableReplayModeRecordIsolation, second.Payload[infraclickhouse.DurablePayloadFieldReplayMode])
	assert.NotEmpty(t, first.Payload[infraclickhouse.DurablePayloadFieldRecordID])
	assert.NotEmpty(t, second.Payload[infraclickhouse.DurablePayloadFieldRecordID])
	assert.NotEqual(t, first.Payload[infraclickhouse.DurablePayloadFieldRecordID], second.Payload[infraclickhouse.DurablePayloadFieldRecordID])
	assert.Equal(t, first.Payload[pkgclickhouse.SettingInsertDeduplicationToken], second.Payload[pkgclickhouse.SettingInsertDeduplicationToken])
}

func TestClickHouseFlushManagerSeparatesClientReportedEncodingFailureBeforeDurableReplay(t *testing.T) {
	client := &recordingClickHouseClient{
		err: &pkgclickhouse.LogRecordMarshalError{
			Index: 1,
			Err:   errors.New("simulated log record encoding failure"),
		},
	}
	logger := newTestClickHouseLogger(t, client)
	flushManager := logger.flushManager.(*ClickHouseFlushManager)
	valid := newTestLogEntry(pkglogger.LogLevelInfo, "valid", nil)
	locallyRejected := newTestLogEntry(pkglogger.LogLevelInfo, "locally rejected", nil)
	_, locallyRejected.Fields = field.ExtractIDsAndExtraFields(clickHouseApplicationFields(t))

	result, err := insertClickHouseLogEntriesAsSingleRequestForTest(
		context.Background(),
		flushManager,
		[]*logging.LogEntry{valid, locallyRejected},
	)

	require.NoError(t, err)
	require.Len(t, result.QuarantineRecords, 1)
	require.Empty(t, result.DeadRecords)
	require.Len(t, result.RetryableRecords, 1)
	assert.True(t, result.Meta.AttemptedAt.IsZero())
	quarantined := result.QuarantineRecords[0]
	assert.Equal(t, FailureCodeLogRecordEncoding, quarantined.FailureCode)
	assert.Equal(t, infraclickhouse.DurableReplayModeNone, quarantined.Payload[infraclickhouse.DurablePayloadFieldReplayMode])
	durableFields, ok := quarantined.Payload["fields"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "hogehoge", durableFields["user_id"])
	assert.Equal(t, true, durableFields["authenticated"])
	assert.Equal(t, 2, durableFields["attempt"])
	_, marshalErr := json.Marshal(quarantined.Payload)
	require.NoError(t, marshalErr)

	retryableRecord := result.RetryableRecords[0]
	assert.Equal(t, "valid", retryableRecord.Payload["message"])
	assert.Equal(t, infraclickhouse.DurableReplayModeRecordIsolation, retryableRecord.Payload[infraclickhouse.DurablePayloadFieldReplayMode])
}

func TestClickHouseLoggerOptionUsesFlushIntervalGenerator(t *testing.T) {
	client := &recordingClickHouseClient{}
	flushInterval := 30 * time.Second
	var gotDuration time.Duration
	cfg := newTestClickHouseLoggerBaseConfig()
	cfg.FlushInterval = flushInterval

	logger := newTestClickHouseLoggerWithConfigAndOptions(
		t,
		client,
		cfg,
		WithFlushIntervalGenerator(func(d time.Duration) *time.Ticker {
			gotDuration = d
			return time.NewTicker(time.Hour)
		}),
	)
	defer func() {
		require.NoError(t, logger.Close(context.Background()))
	}()

	assert.Equal(t, flushInterval, gotDuration)
}

func newTestClickHouseLogger(t *testing.T, client Client) *ClickHouseLogger {
	t.Helper()

	return newTestClickHouseLoggerWithConfigAndOptions(t, client, newTestClickHouseLoggerBaseConfig())
}

func newTestClickHouseLoggerBaseConfig() pkglogger.ClickHouseLoggerBaseConfig {
	return pkglogger.ClickHouseLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "test-app",
			Target:   "test-target",
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			ErrorTraceDepth:          3,
			ErrorTraceDepthDebugMode: 10,
		},
		FlushInterval:           time.Hour,
		MaxEntries:              10000,
		BufferFlushBytes:        5 * 1024 * 1024,
		FallbackQueueMaxEntries: 10000,
		MaxInsertRequestBytes:   10 * 1024 * 1024,
	}
}

func newTestClickHouseLoggerWithConfigAndOptions(
	t *testing.T,
	client Client,
	baseCfg pkglogger.ClickHouseLoggerBaseConfig,
	opts ...ClickHouseLoggerOption,
) *ClickHouseLogger {
	t.Helper()

	cfg := NewClickHouseLoggerConfigAdapter(baseCfg)
	fmCfg := newTestClickHouseFlushManagerConfig()
	loggerBase := newTestClickHouseLoggerBase()

	logger, err := NewClickHouseLogger(
		context.Background(),
		cfg,
		client,
		fmCfg,
		newTestClickHouseResourceProvider(),
		nil,
		nil,
		nil,
		loggerBase,
		opts...,
	)
	require.NoError(t, err)
	closeClickHouseTestLoggerOnCleanup(t, func() error { return logger.Close(context.Background()) })
	return logger
}

func newTestDeliveryClickHouseLogger(t *testing.T, client Client) *DeliveryClickHouseLogger {
	t.Helper()

	return newTestDeliveryClickHouseLoggerWithConfig(t, client, newTestDeliveryClickHouseLoggerBaseConfig())
}

func newTestDeliveryClickHouseLoggerBaseConfig() pkglogger.DeliveryClickHouseLoggerBaseConfig {
	return pkglogger.DeliveryClickHouseLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{},
		RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
			AppName: "test-app",
			Target:  "test-target",
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			ErrorTraceDepth:          3,
			ErrorTraceDepthDebugMode: 10,
		},
		FlushInterval:           time.Hour,
		MaxEntries:              10000,
		BufferFlushBytes:        5 * 1024 * 1024,
		FallbackQueueMaxEntries: 10000,
		MaxInsertRequestBytes:   10 * 1024 * 1024,
	}
}

func newTestDeliveryClickHouseLoggerWithConfig(
	t *testing.T,
	client Client,
	baseCfg pkglogger.DeliveryClickHouseLoggerBaseConfig,
) *DeliveryClickHouseLogger {
	t.Helper()

	cfg := NewDeliveryClickHouseLoggerConfigAdapter(baseCfg)
	fmCfg := newTestDeliveryClickHouseFlushManagerConfig()
	loggerBase := newTestClickHouseLoggerBase()

	logger, err := NewDeliveryClickHouseLogger(
		context.Background(),
		cfg,
		client,
		fmCfg,
		newTestClickHouseResourceProvider(),
		nil,
		nil,
		nil,
		loggerBase,
	)
	require.NoError(t, err)
	closeClickHouseTestLoggerOnCleanup(t, logger.Close)
	return logger
}

// closeClickHouseTestLoggerOnCleanupは、試験が明示的にCloseしない場合や途中で失敗した場合も
// 構築時に開始したticker workerを停止します。Closeは完了後の再呼び出しで何もしないため、
// 明示的にCloseする試験とも両立します。cleanupは登録と逆順に実行するため、flushをblockする
// 試験はlogger構築後に解放用のcleanupを登録し、このCloseより先に解放させます。
func closeClickHouseTestLoggerOnCleanup(t *testing.T, closeLogger func() error) {
	t.Helper()
	t.Cleanup(func() {
		if err := closeLogger(); err != nil {
			t.Errorf("close ClickHouse test logger: %v", err)
		}
	})
}

func newTestClickHouseFlushManagerConfig() ClickHouseFlushManagerConfigProvider {
	return NewFlushManagerConfigAdapter(pkglogger.ClickHouseFlushManagerConfig{
		Resource: pkglogger.Resource{
			ServiceName: "test-app",
			Cloud:       &pkglogger.CloudResourceInfo{},
		},
	})
}

func newTestDeliveryClickHouseFlushManagerConfig() DeliveryClickHouseFlushManagerConfigProvider {
	return NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryClickHouseFlushManagerConfig{
		Resource: pkglogger.Resource{
			ServiceName: "test-app",
			Cloud:       &pkglogger.CloudResourceInfo{},
		},
	})
}

func newTestClickHouseLoggerBase() *LoggerBase {
	return &LoggerBase{
		Clock:  clock.NewUTCClock(),
		config: NewClickHouseLoggerConfigAdapter(newTestClickHouseLoggerBaseConfig()),
	}
}

func newTestClickHouseFlushManagerBase(t *testing.T) *LoggerBase {
	t.Helper()
	base := newTestClickHouseLoggerBase()
	timestampFormatter, err := NewConfiguredTimestampFormatter(base.config)
	require.NoError(t, err)
	base.LogFormatter = formatter.NewPlainTextFormatter(timestampFormatter)
	return base
}

// insertClickHouseLogEntriesAsSingleRequestForTestは、failure分類の単一request試験だけで
// 使用します。productionのFlushは必ずPlanInsertRequestsを通してhard limit内へ分割します。
func insertClickHouseLogEntriesAsSingleRequestForTest(
	ctx context.Context,
	flushManager *ClickHouseFlushManager,
	entries []*logging.LogEntry,
) (loggerfailure.SendResult, error) {
	if len(entries) == 0 {
		return loggerfailure.SendResult{}, nil
	}
	return flushManager.insertEntryRecords(
		ctx,
		flushManager.convertLogEntriesToEntryRecords(entries),
		newClickHouseInsertDeduplicationToken(),
	)
}

func newTestClickHouseResourceProvider() resource.ResourceProvider {
	return resource.NewResourceAdapter(pkglogger.Resource{
		ServiceName: "test-app",
		Cloud:       &pkglogger.CloudResourceInfo{},
	})
}

type recordingClickHouseClient struct {
	mu            sync.Mutex
	err           error
	errs          []error
	batches       [][]pkgclickhouse.LogRecord
	insertOptions []pkgclickhouse.InsertOptions
}

func (c *recordingClickHouseClient) InsertLogs(
	ctx context.Context,
	records []pkgclickhouse.LogRecord,
) (*pkgclickhouse.InsertResponse, error) {
	return c.InsertLogsWithOptions(ctx, records, pkgclickhouse.InsertOptions{})
}

func (c *recordingClickHouseClient) InsertLogsWithOptions(
	ctx context.Context,
	records []pkgclickhouse.LogRecord,
	opts pkgclickhouse.InsertOptions,
) (*pkgclickhouse.InsertResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	copied := append([]pkgclickhouse.LogRecord(nil), records...)
	c.batches = append(c.batches, copied)
	c.insertOptions = append(c.insertOptions, opts)
	callIndex := len(c.batches) - 1
	err := c.err
	if callIndex < len(c.errs) {
		err = c.errs[callIndex]
	}
	if err != nil {
		return nil, err
	}
	return &pkgclickhouse.InsertResponse{StatusCode: 200}, nil
}

func (c *recordingClickHouseClient) InsertLog(
	ctx context.Context,
	record pkgclickhouse.LogRecord,
) (*pkgclickhouse.InsertResponse, error) {
	return c.InsertLogs(ctx, []pkgclickhouse.LogRecord{record})
}

func (c *recordingClickHouseClient) InsertLogWithOptions(
	ctx context.Context,
	record pkgclickhouse.LogRecord,
	opts pkgclickhouse.InsertOptions,
) (*pkgclickhouse.InsertResponse, error) {
	return c.InsertLogsWithOptions(ctx, []pkgclickhouse.LogRecord{record}, opts)
}

func (c *recordingClickHouseClient) batchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.batches)
}

type fixedClickHouseClock struct {
	t time.Time
}

func (c fixedClickHouseClock) Now() time.Time {
	return c.t
}

var _ clock.Clock = fixedClickHouseClock{}

type advancingClickHouseClock struct {
	mu   sync.Mutex
	next time.Time
}

func (c *advancingClickHouseClock) Now() time.Time {
	return c.nextTime()
}

func (c *advancingClickHouseClock) nextTime() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := c.next
	c.next = c.next.Add(time.Second)
	return t
}

var _ clock.Clock = (*advancingClickHouseClock)(nil)
