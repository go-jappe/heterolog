package opensearch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	appotel "github.com/go-jappe/heterolog/internal/application/otel"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/require"
	iootel "go.opentelemetry.io/otel"
)

type MockOpenSearchClient struct {
	mu sync.RWMutex

	IndexDocumentCalled  bool
	IndexDocumentsCalled bool

	IndexDocumentArgs  []pkgopensearch.DocumentWriteRequest
	IndexDocumentsArgs []pkgopensearch.DocumentWriteRequest

	returnError bool
}

var _ pkgopensearch.Client = (*MockOpenSearchClient)(nil)

func NewMockOpenSearchClient() *MockOpenSearchClient {
	return &MockOpenSearchClient{}
}

func (m *MockOpenSearchClient) IndexDocument(
	ctx context.Context,
	doc pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.IndexResponse, error) {
	m.mu.Lock()
	m.IndexDocumentCalled = true
	m.IndexDocumentArgs = append(m.IndexDocumentArgs, doc)
	returnError := m.returnError
	m.mu.Unlock()

	if returnError {
		return nil, errors.New("mock error")
	}

	return &pkgopensearch.IndexResponse{
		Index:   doc.Index,
		ID:      doc.ID,
		Version: 1,
		Result:  "created",
		Status:  201,
	}, nil
}

func (m *MockOpenSearchClient) IndexDocuments(
	ctx context.Context,
	docs []pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.BulkResponse, error) {
	m.mu.Lock()
	m.IndexDocumentsCalled = true
	m.IndexDocumentsArgs = append(m.IndexDocumentsArgs, docs...)
	returnError := m.returnError
	m.mu.Unlock()

	if returnError {
		return nil, errors.New("mock error")
	}

	items := make([]map[string]pkgopensearch.BulkItemResult, 0, len(docs))
	for _, doc := range docs {
		action, err := doc.BulkActionKey()
		if err != nil {
			return nil, err
		}
		items = append(items, map[string]pkgopensearch.BulkItemResult{
			action: {
				Index:   doc.Index,
				ID:      doc.ID,
				Status:  201,
				Result:  "created",
				Version: 1,
			},
		})
	}

	return &pkgopensearch.BulkResponse{
		Took:   10,
		Errors: false,
		Items:  items,
	}, nil
}

func (m *MockOpenSearchClient) indexDocumentWasCalled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.IndexDocumentCalled
}

type MockOpenSearchConfigProvider struct {
	timeConfig                 pkglogger.TimeConfig
	isDebug                    bool
	appName                    string
	target                     string
	index                      string
	logLevel                   pkglogger.LogLevel
	logLineDateFormat          string
	errorTraceDepth            int
	errorTraceDepthDebugMode   int
	maxBulkDocument            int
	bufferFlushBytes           int
	maxBulkRequestBytes        int
	isSamplingEnabled          bool
	enableLoggingByRequestID   bool
	loggingByRequestIDRate     int
	samplingDebugRate          int
	samplingInfoRate           int
	samplingWarnRate           int
	samplingErrorRate          int
	postWarnThresholdDuration  time.Duration
	postErrorThresholdDuration time.Duration
	flushInterval              time.Duration
	noBulkFlushModeEnabled     bool
	indexLatencyEnabled        bool
	maxItemRetries             int
	retryWaitTime              time.Duration
	onErrorConfig              []string
	onWarnConfig               []string
	onInfoConfig               []string
	onDebugConfig              []string
	openTelemetryFormatEnabled bool
}

var _ OpenSearchLoggerConfigProvider = (*MockOpenSearchConfigProvider)(nil)
var _ OpenSearchFlushManagerConfigProvider = (*MockOpenSearchConfigProvider)(nil)
var _ RuntimeConfigProvider = (*MockOpenSearchConfigProvider)(nil)
var _ LogBodyConfigProvider = (*MockOpenSearchConfigProvider)(nil)

func NewMockOpenSearchConfigProvider() *MockOpenSearchConfigProvider {
	return &MockOpenSearchConfigProvider{
		isDebug:                    false,
		appName:                    "test-app",
		target:                     "test-target",
		index:                      "test-index",
		logLevel:                   pkglogger.LogLevelInfo,
		logLineDateFormat:          time.RFC3339Nano,
		errorTraceDepth:            3,
		errorTraceDepthDebugMode:   10,
		maxBulkDocument:            100,
		bufferFlushBytes:           5 * 1024 * 1024,
		maxBulkRequestBytes:        15 * 1024 * 1024,
		isSamplingEnabled:          false,
		enableLoggingByRequestID:   false,
		loggingByRequestIDRate:     10,
		samplingDebugRate:          1,
		samplingInfoRate:           1,
		samplingWarnRate:           1,
		samplingErrorRate:          1,
		postWarnThresholdDuration:  time.Second,
		postErrorThresholdDuration: 2 * time.Second,
		flushInterval:              5 * time.Second,
		indexLatencyEnabled:        true,
		maxItemRetries:             2,
		retryWaitTime:              time.Second,
	}
}

func (m *MockOpenSearchConfigProvider) Time() loggertime.TimeConfigProvider {
	return loggertime.NewTimeConfigAdapter(m.timeConfig)
}
func (m *MockOpenSearchConfigProvider) Runtime() RuntimeConfigProvider { return m }
func (m *MockOpenSearchConfigProvider) LogBody() LogBodyConfigProvider { return m }
func (m *MockOpenSearchConfigProvider) Sampling() sampling.SamplingConfigProvider {
	return m
}
func (m *MockOpenSearchConfigProvider) Handler() handler.ConfigProvider { return nil }
func (m *MockOpenSearchConfigProvider) FailureStoreConfig() durable.FailureStoreConfigProvider {
	return nil
}

func (m *MockOpenSearchConfigProvider) MaxItemRetries() int          { return m.maxItemRetries }
func (m *MockOpenSearchConfigProvider) RetryWaitTime() time.Duration { return m.retryWaitTime }
func (m *MockOpenSearchConfigProvider) IsDebug() bool                { return m.isDebug }
func (m *MockOpenSearchConfigProvider) AppName() string              { return m.appName }
func (m *MockOpenSearchConfigProvider) Target() string               { return m.target }
func (m *MockOpenSearchConfigProvider) Index() string                { return m.index }
func (m *MockOpenSearchConfigProvider) LogLevel() pkglogger.LogLevel { return m.logLevel }
func (m *MockOpenSearchConfigProvider) LogLineDateFormat() string    { return m.logLineDateFormat }
func (m *MockOpenSearchConfigProvider) ErrorTraceDepth() int         { return m.errorTraceDepth }
func (m *MockOpenSearchConfigProvider) ErrorTraceDepthDebugMode() int {
	return m.errorTraceDepthDebugMode
}
func (m *MockOpenSearchConfigProvider) MaxBulkDocument() int { return m.maxBulkDocument }
func (m *MockOpenSearchConfigProvider) BufferFlushBytes() int {
	return m.bufferFlushBytes
}
func (m *MockOpenSearchConfigProvider) MaxBulkRequestBytes() int {
	return m.maxBulkRequestBytes
}
func (m *MockOpenSearchConfigProvider) NoBulkFlushModeEnabled() bool {
	return m.noBulkFlushModeEnabled
}
func (m *MockOpenSearchConfigProvider) IsSamplingEnabled() bool { return m.isSamplingEnabled }
func (m *MockOpenSearchConfigProvider) EnableLoggingByRequestID() bool {
	return m.enableLoggingByRequestID
}
func (m *MockOpenSearchConfigProvider) LoggingByRequestIDRate() int { return m.loggingByRequestIDRate }
func (m *MockOpenSearchConfigProvider) SamplingDebugRate() int      { return m.samplingDebugRate }
func (m *MockOpenSearchConfigProvider) SamplingInfoRate() int       { return m.samplingInfoRate }
func (m *MockOpenSearchConfigProvider) SamplingWarnRate() int       { return m.samplingWarnRate }
func (m *MockOpenSearchConfigProvider) SamplingErrorRate() int      { return m.samplingErrorRate }
func (m *MockOpenSearchConfigProvider) PostWarnThresholdDuration() time.Duration {
	return m.postWarnThresholdDuration
}
func (m *MockOpenSearchConfigProvider) PostErrorThresholdDuration() time.Duration {
	return m.postErrorThresholdDuration
}
func (m *MockOpenSearchConfigProvider) FlushInterval() time.Duration { return m.flushInterval }
func (m *MockOpenSearchConfigProvider) IndexLatencyEnabled() bool    { return m.indexLatencyEnabled }
func (m *MockOpenSearchConfigProvider) OnErrorConfig() []string      { return m.onErrorConfig }
func (m *MockOpenSearchConfigProvider) OnWarnConfig() []string       { return m.onWarnConfig }
func (m *MockOpenSearchConfigProvider) OnInfoConfig() []string       { return m.onInfoConfig }
func (m *MockOpenSearchConfigProvider) OnDebugConfig() []string      { return m.onDebugConfig }
func (m *MockOpenSearchConfigProvider) OpenTelemetryFormatEnabled() bool {
	return m.openTelemetryFormatEnabled
}
func (m *MockOpenSearchConfigProvider) UseOpenTelemetryFormat() bool {
	return m.openTelemetryFormatEnabled
}

// MockResourceProvider は converter と OpenSearchFlushManager の両方で使えるテスト専用リソースです。
// resource.NewOtelResourceAdapter は CloudProviderRegion などの ResourceProvider 追加メソッドを
// 満たさない環境があるため、テストでは必要な全メソッドを持つ mock を使います。
type MockResourceProvider struct{}

func NewMockResourceProvider() *MockResourceProvider { return &MockResourceProvider{} }

func (r *MockResourceProvider) ServiceName() string         { return "mock-service" }
func (r *MockResourceProvider) ServiceVersion() string      { return "v0.1.0" }
func (r *MockResourceProvider) HostName() string            { return "localhost" }
func (r *MockResourceProvider) OSType() string              { return "linux" }
func (r *MockResourceProvider) Environment() string         { return "test" }
func (r *MockResourceProvider) RuntimeType() string         { return "go" }
func (r *MockResourceProvider) RuntimeInstanceID() string   { return "test-instance" }
func (r *MockResourceProvider) CloudProvider() string       { return "local" }
func (r *MockResourceProvider) CloudProviderRegion() string { return "local" }

var _ resource.ResourceProvider = (*MockResourceProvider)(nil)
var _ resource.OtelLogResourceProvider = (*MockResourceProvider)(nil)

func newTestObserver() (pkgobserver.Tracer, pkgobserver.Meter) {
	return appotel.NewOtelTracer(iootel.Tracer("opensearch-logger-test")),
		appotel.NewOtelMeter(iootel.Meter("opensearch-logger-test"))
}

func newTestLogger(
	t *testing.T,
	cfg *MockOpenSearchConfigProvider,
	client *MockOpenSearchClient,
) (*OpenSearchLogger, *MockOpenSearchClient) {
	t.Helper()

	if cfg == nil {
		cfg = NewMockOpenSearchConfigProvider()
	}
	if client == nil {
		client = NewMockOpenSearchClient()
	}

	tracer, meter := newTestObserver()
	loggerBase := &LoggerBase{
		Clock:              pkgclock.NewUTCClock(),
		TimestampFormatter: timestamp.RFC3339Formatter(),
		config:             cfg,
		Tracer:             tracer,
		Meter:              meter,
	}

	resourceProvider := NewMockResourceProvider()
	converter := NewLogEntryConverter(false, resourceProvider, loggerBase.TimestampFormatter)

	logger := &OpenSearchLogger{
		LoggerBase:       loggerBase,
		entries:          make([]*logging.LogEntry, 0, cfg.MaxBulkDocument()),
		stopCh:           make(chan struct{}),
		timeProvider:     time.Now,
		flushIntervalGen: time.NewTicker,
		entryConverter:   converter,
		Tracer:           tracer,
		Meter:            meter,
	}
	logger.lastFlushTime = logger.timeProvider()
	flushManager, err := NewOpenSearchFlushManager(
		loggerBase, client, cfg, converter, nil, resourceProvider, tracer, meter,
	)
	require.NoError(t, err)
	logger.flushManager = flushManager

	return logger, client
}

func entryCount(logger *OpenSearchLogger) int {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return len(logger.entries)
}

func TestLoggerBase_InternalEntries(t *testing.T) {
	base := &LoggerBase{
		Clock: pkgclock.NewUTCClock(),
	}

	t.Run("debug/info/warn entries", func(t *testing.T) {
		tests := []struct {
			name  string
			entry *logging.LogEntry
			level pkglogger.LogLevel
		}{
			{"debug", base.InternalDebugEntry("debug message", "req-debug"), pkglogger.LogLevelDebug},
			{"info", base.InternalInfoEntry("info message", "req-info"), pkglogger.LogLevelInfo},
			{"warn", base.InternalWarnEntry("warn message", "req-warn"), pkglogger.LogLevelWarn},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				require.Equal(t, tt.level, tt.entry.Level)
				require.True(t, tt.entry.Internal)
				require.Equal(t, common.SourceNameOpenSearchLogger, tt.entry.Source)
				require.False(t, tt.entry.OccurredAt.IsZero())
				require.NotEmpty(t, tt.entry.RequestID)
				require.NotEmpty(t, tt.entry.Message)
			})
		}
	})

	t.Run("error entry", func(t *testing.T) {
		err := errors.New("boom")
		entry := base.InternalErrorEntry("error message", "req-error", err)

		require.Equal(t, pkglogger.LogLevelError, entry.Level)
		require.Equal(t, err, entry.Err)
		require.True(t, entry.Internal)
		require.Equal(t, common.SourceNameOpenSearchLogger, entry.Source)
	})

	t.Run("error entry with stack", func(t *testing.T) {
		err := errors.New("stack boom")
		entry := base.InternalErrorEntryWithStack("error with stack", "req-stack", err, 1)

		require.Equal(t, pkglogger.LogLevelError, entry.Level)
		require.Equal(t, err, entry.Err)
		require.True(t, entry.Internal)
		require.Equal(t, common.SourceNameOpenSearchLogger, entry.Source)
		require.NotEmpty(t, entry.FuncName)
		require.NotEmpty(t, entry.File)
		require.NotEmpty(t, entry.Line)
	})
}

func TestOpenSearchLogger_LogFiltersByLevel(t *testing.T) {
	ctx := context.Background()
	cfg := NewMockOpenSearchConfigProvider()
	cfg.logLevel = pkglogger.LogLevelInfo
	logger, _ := newTestLogger(t, cfg, nil)

	tests := []struct {
		name       string
		logFunc    func(string) error
		wantLogged bool
		wantLevel  pkglogger.LogLevel
	}{
		{"error", func(msg string) error { return logger.testError(ctx, msg) }, true, pkglogger.LogLevelError},
		{"warn", func(msg string) error { return logger.testWarn(ctx, msg) }, true, pkglogger.LogLevelWarn},
		{"info", func(msg string) error { return logger.testInfo(ctx, msg) }, true, pkglogger.LogLevelInfo},
		{"debug_filtered", func(msg string) error { return logger.testDebug(ctx, msg) }, false, pkglogger.LogLevelDebug},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger.mu.Lock()
			logger.entries = logger.entries[:0]
			logger.mu.Unlock()

			require.NoError(t, tt.logFunc("test "+tt.name))

			logger.mu.Lock()
			defer logger.mu.Unlock()
			if !tt.wantLogged {
				require.Empty(t, logger.entries)
				return
			}
			require.Len(t, logger.entries, 1)
			require.Equal(t, tt.wantLevel, logger.entries[0].Level)
			require.Contains(t, logger.entries[0].Message, tt.name)
		})
	}
}
func TestOpenSearchLogger_FlushSingleEntry_UsesDocumentWriteRequestAndClearsEntries(t *testing.T) {
	ctx := context.Background()
	cfg := NewMockOpenSearchConfigProvider()
	logger, client := newTestLogger(t, cfg, nil)

	logger.mu.Lock()
	logger.entries = []*logging.LogEntry{
		{
			OccurredAt: logger.Clock.Now(),
			Level:      pkglogger.LogLevelError,
			Message:    "test error",
		},
	}
	logger.mu.Unlock()

	err := logger.doFlush(ctx)
	require.NoError(t, err)

	logger.mu.Lock()
	require.Empty(t, logger.entries)
	logger.mu.Unlock()

	require.True(t, client.IndexDocumentCalled)
	require.False(t, client.IndexDocumentsCalled)
	require.Len(t, client.IndexDocumentArgs, 1)

	req := client.IndexDocumentArgs[0]
	require.Equal(t, cfg.index, req.Index)
	require.NotEmpty(t, req.ID)
	require.Equal(t, pkgopensearch.WriteOpCreate, req.Op)
	require.NotEmpty(t, req.Doc)
}
func TestOpenSearchLogger_FlushBulk_UsesDocumentWriteRequestsAndClearsEntries(t *testing.T) {
	ctx := context.Background()
	cfg := NewMockOpenSearchConfigProvider()
	logger, client := newTestLogger(t, cfg, nil)

	logger.mu.Lock()
	logger.entries = []*logging.LogEntry{
		{OccurredAt: logger.Clock.Now(), Level: pkglogger.LogLevelInfo, Message: "bulk 1"},
		{OccurredAt: logger.Clock.Now(), Level: pkglogger.LogLevelInfo, Message: "bulk 2"},
	}
	logger.mu.Unlock()

	err := logger.doFlush(ctx)
	require.NoError(t, err)

	require.Equal(t, 0, entryCount(logger))
	require.False(t, client.IndexDocumentCalled)
	require.True(t, client.IndexDocumentsCalled)
	require.Len(t, client.IndexDocumentsArgs, 2)

	for _, req := range client.IndexDocumentsArgs {
		require.Equal(t, cfg.index, req.Index)
		require.NotEmpty(t, req.ID)
		require.Equal(t, pkgopensearch.WriteOpCreate, req.Op)
		require.NotEmpty(t, req.Doc)
	}
}

func TestOpenSearchLogger_NoBulkFlushModeLogMethodIndexesImmediately(t *testing.T) {
	ctx := context.Background()
	cfg := NewMockOpenSearchConfigProvider()
	cfg.noBulkFlushModeEnabled = true
	cfg.maxBulkDocument = 2
	logger, client := newTestLogger(t, cfg, nil)

	require.NoError(t, logger.testInfo(ctx, "no bulk one"))
	require.NoError(t, logger.testInfo(ctx, "no bulk two"))

	require.Equal(t, 0, entryCount(logger))
	require.True(t, client.IndexDocumentCalled)
	require.False(t, client.IndexDocumentsCalled)
	require.Len(t, client.IndexDocumentArgs, 2)

	for _, req := range client.IndexDocumentArgs {
		require.Equal(t, cfg.index, req.Index)
		require.Equal(t, pkgopensearch.WriteOpCreate, req.Op)
		require.NotEmpty(t, req.Doc)
	}
}

// log methodによるenqueueが件数閾値に達した時点で、呼び出し内でbulk flushすることを確認します。
func TestOpenSearchLogger_EnqueueTriggersFlush(t *testing.T) {
	ctx := context.Background()
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 2
	// intervalではなく件数閾値だけを契機にします。
	cfg.flushInterval = time.Hour
	logger, client := newTestLogger(t, cfg, nil)

	require.NoError(t, logger.testInfo(ctx, "one"))
	require.Equal(t, 1, entryCount(logger))
	require.False(t, client.IndexDocumentCalled)
	require.False(t, client.IndexDocumentsCalled, "the buffer is below MaxBulkDocument")

	require.NoError(t, logger.testInfo(ctx, "two"))

	require.Equal(t, 0, entryCount(logger))
	require.False(t, client.IndexDocumentCalled)
	require.True(t, client.IndexDocumentsCalled)
	require.Len(t, client.IndexDocumentsArgs, 2)

	for i, req := range client.IndexDocumentsArgs {
		require.Equal(t, cfg.index, req.Index)
		require.NotEmpty(t, req.ID)
		require.Equal(t, pkgopensearch.WriteOpCreate, req.Op)
		require.Equal(t, []string{"one", "two"}[i], req.Doc["message"])
	}
}

// Closeがbatch workerの停止channelを閉じてworkerを止め、閾値未満で残ったentryを送信することを確認します。
func TestOpenSearchLogger_CloseFlushesAndClosesStopChannel(t *testing.T) {
	ctx := context.Background()
	cfg := NewMockOpenSearchConfigProvider()
	cfg.flushInterval = time.Hour
	logger, client := newTestLogger(t, cfg, nil)
	logger.startBatchFlushWorker(ctx, nil, logger.flush)
	logger.mu.Lock()
	stopCh := logger.stopCh
	logger.mu.Unlock()

	require.NoError(t, logger.testError(ctx, "close flush"))
	require.Equal(t, 1, entryCount(logger))
	require.False(t, client.indexDocumentWasCalled(), "the buffer is below the flush thresholds")

	require.NoError(t, logger.Close(ctx))

	select {
	case <-stopCh:
	default:
		t.Fatal("Close did not close the batch worker stop channel")
	}
	require.Equal(t, 0, entryCount(logger))
	require.True(t, client.IndexDocumentCalled)
	require.False(t, client.IndexDocumentsCalled)
	require.Len(t, client.IndexDocumentArgs, 1)

	req := client.IndexDocumentArgs[0]
	require.Equal(t, cfg.index, req.Index)
	require.NotEmpty(t, req.ID)
	require.Equal(t, pkgopensearch.WriteOpCreate, req.Op)
	require.Equal(t, "close flush", req.Doc["message"])
	require.ErrorIs(t, logger.testInfo(ctx, "after close"), pkglogger.ErrLoggerClosed)
}

// 通常APIでもerrorを渡さないErrorWithStackを拒否せず、stackを持つERROR documentとして記録します。
func TestOpenSearchLogger_ErrorWithStackWithoutErrorIndexesStack(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.noBulkFlushModeEnabled = true
	logger, client := newTestLogger(t, cfg, nil)

	require.NoError(t, logger.testErrorWithStack(context.Background(), "missing error", nil))

	requireOpenSearchStackDocumentWithoutError(t, client, "missing error")
}

func TestOpenSearchLogger_ClientNotInitialized(t *testing.T) {
	ctx := context.Background()
	cfg := NewMockOpenSearchConfigProvider()
	logger, _ := newTestLogger(t, cfg, nil)

	tracer, meter := newTestObserver()
	resourceProvider := NewMockResourceProvider()

	logger.flushManager = &OpenSearchFlushManager{
		LoggerBase: logger.LoggerBase,
		osClient:   nil,
		config:     cfg,
		converter:  NewLogEntryConverter(false, resourceProvider, logger.TimestampFormatter),
		resource:   resourceProvider,
		Tracer:     tracer,
		Meter:      meter,
	}

	logger.entries = []*logging.LogEntry{
		{
			OccurredAt: logger.Clock.Now(),
			Level:      pkglogger.LogLevelError,
			Message:    "no client",
		},
	}

	err := logger.doFlush(ctx)

	require.ErrorIs(t, err, ErrClientNotInitialized)
}

func TestOpenSearchLogger_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 100

	logger, client := newTestLogger(t, cfg, nil)

	const goroutines = 10

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		i := i
		wg.Add(1)

		go func() {
			defer wg.Done()
			errCh <- logger.testInfo(ctx, fmt.Sprintf("test message %d", i))
		}()
	}

	// 呼び出しがhangした場合もtestを止めないよう、完了を通知で待ちます。
	writersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(writersDone)
	}()
	waitLifecycleSignal(t, writersDone, "concurrent Info calls did not finish")
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	require.Equal(t, goroutines, entryCount(logger))

	err := logger.doFlush(ctx)
	require.NoError(t, err)

	require.Equal(t, 0, entryCount(logger))
	require.False(t, client.IndexDocumentCalled)
	require.True(t, client.IndexDocumentsCalled)
	require.Len(t, client.IndexDocumentsArgs, goroutines)

	for _, req := range client.IndexDocumentsArgs {
		require.Equal(t, cfg.index, req.Index)
		require.NotEmpty(t, req.ID)
		require.Equal(t, pkgopensearch.WriteOpCreate, req.Op)
		require.NotEmpty(t, req.Doc)
	}
}

func TestOpenSearchLogger_TimerFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := NewMockOpenSearchConfigProvider()
		cfg.flushInterval = time.Second
		cfg.maxBulkDocument = 100
		logger, client := newTestLogger(t, cfg, nil)
		logger.startBatchFlushWorker(context.Background(), nil, logger.flush)
		t.Cleanup(func() { require.NoError(t, logger.Close(context.Background())) })
		require.NoError(t, logger.testInfo(context.Background(), "timer flush"))
		synctest.Wait()
		require.False(t, client.indexDocumentWasCalled(), "buffer is below the size trigger")

		time.Sleep(time.Second)
		synctest.Wait()
		require.True(t, client.indexDocumentWasCalled(), "the timer must deliver the message before Close")
		require.Equal(t, 0, entryCount(logger))
		client.mu.RLock()
		defer client.mu.RUnlock()
		require.Len(t, client.IndexDocumentArgs, 1)
		req := client.IndexDocumentArgs[0]
		require.Equal(t, cfg.index, req.Index)
		require.NotEmpty(t, req.ID)
		require.Equal(t, pkgopensearch.WriteOpCreate, req.Op)
		require.Equal(t, "timer flush", req.Doc["message"])
	})
}
