package opensearch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	appotel "github.com/go-jappe/heterolog/internal/application/otel"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	iootel "go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type mockTestResourceProvider struct {
	serviceName         string
	serviceVersion      string
	hostName            string
	osType              string
	environment         string
	runtimeType         string
	runtimeInstanceID   string
	cloudProvider       string
	cloudProviderRegion string
}

var _ resource.ResourceProvider = (*mockTestResourceProvider)(nil)
var _ resource.OtelLogResourceProvider = (*mockTestResourceProvider)(nil)

func newTestResourceProvider() resource.ResourceProvider {
	return &mockTestResourceProvider{
		serviceName:         "mock-service",
		serviceVersion:      "v0.1.0",
		hostName:            "localhost",
		osType:              "linux",
		environment:         "test",
		runtimeType:         "go",
		runtimeInstanceID:   "test-runtime-instance-id",
		cloudProvider:       "aws",
		cloudProviderRegion: "ap-northeast-1",
	}
}

func (r *mockTestResourceProvider) ServiceName() string {
	return r.serviceName
}

func (r *mockTestResourceProvider) ServiceVersion() string {
	return r.serviceVersion
}

func (r *mockTestResourceProvider) HostName() string {
	return r.hostName
}

func (r *mockTestResourceProvider) OSType() string {
	return r.osType
}

func (r *mockTestResourceProvider) Environment() string {
	return r.environment
}

func (r *mockTestResourceProvider) RuntimeType() string {
	return r.runtimeType
}

func (r *mockTestResourceProvider) RuntimeInstanceID() string {
	return r.runtimeInstanceID
}

func (r *mockTestResourceProvider) CloudProvider() string {
	return r.cloudProvider
}

func (r *mockTestResourceProvider) CloudProviderRegion() string {
	return r.cloudProviderRegion
}

// MockOpenSearchFlushManagerConfig は、テスト用の設定です。
// LoggerBase.config は OpenSearchLoggerConfigProvider、FlushManager.config は
// OpenSearchFlushManagerConfigProvider なので、この mock は両方を満たします。
type MockOpenSearchFlushManagerConfig struct {
	timeConfig                 pkglogger.TimeConfig
	maxRetryAttempts           int
	retryWaitTime              time.Duration
	postWarnThresholdDuration  time.Duration
	postErrorThresholdDuration time.Duration
	target                     string
	indexLatencyEnabled        bool
	useOpenTelemetryFormat     bool
	appName                    string
	isDebug                    bool
	logLevel                   pkglogger.LogLevel
	logLineDateFormat          string
	errorTraceDepth            int
	errorTraceDepthDebugMode   int
	maxBulkDocument            int
	bufferFlushBytes           int
	maxBulkRequestBytes        int
	flushInterval              time.Duration
	noBulkFlushModeEnabled     bool
	isSamplingEnabled          bool
	enableLoggingByRequestID   bool
	loggingByRequestIDRate     int
	samplingDebugRate          int
	samplingInfoRate           int
	samplingWarnRate           int
	samplingErrorRate          int
	onErrorConfig              []string
	onWarnConfig               []string
	onInfoConfig               []string
	onDebugConfig              []string
}

var _ OpenSearchFlushManagerConfigProvider = (*MockOpenSearchFlushManagerConfig)(nil)
var _ OpenSearchLoggerConfigProvider = (*MockOpenSearchFlushManagerConfig)(nil)
var _ RuntimeConfigProvider = (*MockOpenSearchFlushManagerConfig)(nil)
var _ LogBodyConfigProvider = (*MockOpenSearchFlushManagerConfig)(nil)

func NewMockOpenSearchFlushManagerConfig() *MockOpenSearchFlushManagerConfig {
	return &MockOpenSearchFlushManagerConfig{
		maxRetryAttempts:           3,
		retryWaitTime:              time.Millisecond,
		postWarnThresholdDuration:  time.Second,
		postErrorThresholdDuration: 2 * time.Second,
		target:                     "test-logs",
		indexLatencyEnabled:        true,
		useOpenTelemetryFormat:     false,
		appName:                    "test-app",
		isDebug:                    false,
		logLevel:                   pkglogger.LogLevelInfo,
		logLineDateFormat:          time.RFC3339Nano,
		errorTraceDepth:            3,
		errorTraceDepthDebugMode:   10,
		maxBulkDocument:            100,
		bufferFlushBytes:           5 * 1024 * 1024,
		maxBulkRequestBytes:        15 * 1024 * 1024,
		flushInterval:              5 * time.Second,
		isSamplingEnabled:          false,
		enableLoggingByRequestID:   false,
		loggingByRequestIDRate:     10,
		samplingDebugRate:          1,
		samplingInfoRate:           1,
		samplingWarnRate:           1,
		samplingErrorRate:          1,
	}
}

// OpenSearchLoggerConfigProvider
func (m *MockOpenSearchFlushManagerConfig) Time() loggertime.TimeConfigProvider {
	return loggertime.NewTimeConfigAdapter(m.timeConfig)
}
func (m *MockOpenSearchFlushManagerConfig) Runtime() RuntimeConfigProvider { return m }
func (m *MockOpenSearchFlushManagerConfig) LogBody() LogBodyConfigProvider { return m }
func (m *MockOpenSearchFlushManagerConfig) FlushInterval() time.Duration   { return m.flushInterval }
func (m *MockOpenSearchFlushManagerConfig) MaxBulkDocument() int           { return m.maxBulkDocument }
func (m *MockOpenSearchFlushManagerConfig) BufferFlushBytes() int          { return m.bufferFlushBytes }
func (m *MockOpenSearchFlushManagerConfig) MaxBulkRequestBytes() int {
	return m.maxBulkRequestBytes
}
func (m *MockOpenSearchFlushManagerConfig) NoBulkFlushModeEnabled() bool {
	return m.noBulkFlushModeEnabled
}
func (m *MockOpenSearchFlushManagerConfig) Sampling() sampling.SamplingConfigProvider {
	return nil
}
func (m *MockOpenSearchFlushManagerConfig) Handler() handler.ConfigProvider {
	return nil
}

// RuntimeConfigProvider
func (m *MockOpenSearchFlushManagerConfig) AppName() string              { return m.appName }
func (m *MockOpenSearchFlushManagerConfig) IsDebug() bool                { return m.isDebug }
func (m *MockOpenSearchFlushManagerConfig) Target() string               { return m.target }
func (m *MockOpenSearchFlushManagerConfig) LogLevel() pkglogger.LogLevel { return m.logLevel }

// LogBodyConfigProvider
func (m *MockOpenSearchFlushManagerConfig) LogLineDateFormat() string { return m.logLineDateFormat }
func (m *MockOpenSearchFlushManagerConfig) ErrorTraceDepth() int      { return m.errorTraceDepth }
func (m *MockOpenSearchFlushManagerConfig) ErrorTraceDepthDebugMode() int {
	return m.errorTraceDepthDebugMode
}

// OpenSearchFlushManagerConfigProvider
func (m *MockOpenSearchFlushManagerConfig) Index() string { return m.target }
func (m *MockOpenSearchFlushManagerConfig) UseOpenTelemetryFormat() bool {
	return m.useOpenTelemetryFormat
}
func (m *MockOpenSearchFlushManagerConfig) FailureStoreConfig() durable.FailureStoreConfigProvider {
	return nil
}
func (m *MockOpenSearchFlushManagerConfig) PostWarnThresholdDuration() time.Duration {
	return m.postWarnThresholdDuration
}
func (m *MockOpenSearchFlushManagerConfig) PostErrorThresholdDuration() time.Duration {
	return m.postErrorThresholdDuration
}
func (m *MockOpenSearchFlushManagerConfig) IndexLatencyEnabled() bool { return m.indexLatencyEnabled }
func (m *MockOpenSearchFlushManagerConfig) MaxRetryAttempts() int     { return m.maxRetryAttempts }
func (m *MockOpenSearchFlushManagerConfig) RetryWaitTime() time.Duration {
	return m.retryWaitTime
}

// 古いテストが参照していた設定メソッドも残しておくと、差し替え時に壊れにくいです。
func (m *MockOpenSearchFlushManagerConfig) IsSamplingEnabled() bool { return m.isSamplingEnabled }
func (m *MockOpenSearchFlushManagerConfig) EnableLoggingByRequestID() bool {
	return m.enableLoggingByRequestID
}
func (m *MockOpenSearchFlushManagerConfig) LoggingByRequestIDRate() int {
	return m.loggingByRequestIDRate
}
func (m *MockOpenSearchFlushManagerConfig) SamplingDebugRate() int  { return m.samplingDebugRate }
func (m *MockOpenSearchFlushManagerConfig) SamplingInfoRate() int   { return m.samplingInfoRate }
func (m *MockOpenSearchFlushManagerConfig) SamplingWarnRate() int   { return m.samplingWarnRate }
func (m *MockOpenSearchFlushManagerConfig) SamplingErrorRate() int  { return m.samplingErrorRate }
func (m *MockOpenSearchFlushManagerConfig) OnErrorConfig() []string { return m.onErrorConfig }
func (m *MockOpenSearchFlushManagerConfig) OnWarnConfig() []string  { return m.onWarnConfig }
func (m *MockOpenSearchFlushManagerConfig) OnInfoConfig() []string  { return m.onInfoConfig }
func (m *MockOpenSearchFlushManagerConfig) OnDebugConfig() []string { return m.onDebugConfig }

// MockFlushManagerClient は pkg/opensearch.Client インターフェースのモック実装です。
type MockFlushManagerClient struct {
	IndexDocumentCalled    bool
	IndexDocumentCallCount int
	IndexDocumentArgs      pkgopensearch.DocumentWriteRequest

	IndexDocumentsCalled    bool
	IndexDocumentsCallCount int
	IndexDocumentsArgs      []pkgopensearch.DocumentWriteRequest
	IndexDocumentsCalls     [][]pkgopensearch.DocumentWriteRequest

	// bulk 用
	returnError               bool
	returnErrorOnce           bool
	errorCount                int
	bulkErr                   error
	bulkResponses             []*pkgopensearch.BulkResponse
	retryableBulkOnceReturned bool
	returnRetryableBulkOnce   bool

	// single index 用
	indexReturnError     bool
	indexReturnErrorOnce bool
	indexErrorCount      int
	indexErr             error
	indexErrOnce         error

	indexResponse *pkgopensearch.IndexResponse
	bulkResponse  *pkgopensearch.BulkResponse
	bulkHandler   func(context.Context, []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error)
}

var _ pkgopensearch.Client = (*MockFlushManagerClient)(nil)

func (m *MockFlushManagerClient) IndexDocument(
	ctx context.Context,
	doc pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.IndexResponse, error) {
	m.IndexDocumentCalled = true
	m.IndexDocumentCallCount++
	m.IndexDocumentArgs = doc

	if m.indexErr != nil {
		return nil, m.indexErr
	}
	if m.indexReturnError || m.returnError {
		return nil, errors.New("mock index error")
	}
	if m.indexErrOnce != nil {
		m.indexErrorCount++
		if m.indexErrorCount == 1 {
			return nil, m.indexErrOnce
		}
	}
	if m.indexReturnErrorOnce {
		m.indexErrorCount++
		if m.indexErrorCount == 1 {
			return nil, errors.New("mock index error once")
		}
	}
	if m.returnErrorOnce {
		m.errorCount++
		if m.errorCount == 1 {
			return nil, errors.New("mock index error once")
		}
	}
	if m.indexResponse != nil {
		return m.indexResponse, nil
	}

	return &pkgopensearch.IndexResponse{
		Index:   doc.Index,
		ID:      doc.ID,
		Version: 1,
		Result:  "created",
		Status:  201,
	}, nil
}

func (m *MockFlushManagerClient) IndexDocuments(
	ctx context.Context,
	docs []pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.BulkResponse, error) {
	m.IndexDocumentsCalled = true
	m.IndexDocumentsCallCount++
	m.IndexDocumentsArgs = append([]pkgopensearch.DocumentWriteRequest(nil), docs...)
	m.IndexDocumentsCalls = append(
		m.IndexDocumentsCalls,
		append([]pkgopensearch.DocumentWriteRequest(nil), docs...),
	)

	if m.bulkHandler != nil {
		return m.bulkHandler(ctx, docs)
	}

	if m.bulkErr != nil {
		return nil, m.bulkErr
	}
	if m.returnError {
		return nil, errMockBulkRequest
	}
	if m.returnErrorOnce {
		m.errorCount++
		if m.errorCount == 1 {
			return nil, errors.New("mock bulk error once")
		}
	}
	if m.returnRetryableBulkOnce && !m.retryableBulkOnceReturned {
		m.retryableBulkOnceReturned = true
		return retryableBulkResponse(docs), nil
	}
	if len(m.bulkResponses) > 0 {
		resp := m.bulkResponses[0]
		m.bulkResponses = m.bulkResponses[1:]
		return resp, nil
	}
	if m.bulkResponse != nil {
		return m.bulkResponse, nil
	}

	return successBulkResponse(docs), nil
}

func successBulkResponse(docs []pkgopensearch.DocumentWriteRequest) *pkgopensearch.BulkResponse {
	items := make([]map[string]pkgopensearch.BulkItemResult, 0, len(docs))
	for _, doc := range docs {
		action := "create"
		if doc.Op == pkgopensearch.WriteOpIndex {
			action = "index"
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
	}
}

func retryableBulkResponse(docs []pkgopensearch.DocumentWriteRequest) *pkgopensearch.BulkResponse {
	items := make([]map[string]pkgopensearch.BulkItemResult, 0, len(docs))
	for _, doc := range docs {
		items = append(items, map[string]pkgopensearch.BulkItemResult{
			"create": {
				Index:  doc.Index,
				ID:     doc.ID,
				Status: 429,
				Error: &pkgopensearch.ErrorDetail{
					Type:   "es_rejected_execution_exception",
					Reason: "too many requests",
				},
			},
		})
	}

	return &pkgopensearch.BulkResponse{
		Took:   10,
		Errors: true,
		Items:  items,
	}
}

func NewMockOpenSearchClientWrapper() pkgopensearch.Client {
	return &MockFlushManagerClient{}
}
func newFlushManagerTestTelemetry(t *testing.T) (context.Context, observer.Tracer, observer.Meter, func()) {
	t.Helper()

	ctx, tracer, meter, _, finish := newFlushManagerTestTelemetryWithExporter(t)
	return ctx, tracer, meter, finish
}

func newFlushManagerTestTelemetryWithExporter(
	t *testing.T,
) (context.Context, observer.Tracer, observer.Meter, *tracetest.InMemoryExporter, func()) {
	t.Helper()

	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	spanProcessor := sdktrace.NewSimpleSpanProcessor(exporter)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanProcessor))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	// processのglobal TracerProviderは置き換えず、この試験専用のproviderからtracerを作ります。
	// 置き換えると、後続の試験がshutdown済みproviderへspanを送ることになります。
	otelTracer := tp.Tracer("internal/infrastructure/logger/opensearch/test")
	otelMeter := iootel.Meter("internal/infrastructure/logger/opensearch/test")
	tracer := appotel.NewOtelTracer(otelTracer)
	meter := appotel.NewOtelMeter(otelMeter)

	ctx, span := tracer.StartSpan(ctx, "test")
	finish := func() {
		span.End()
		require.NoError(t, tp.ForceFlush(context.Background()))
		assertExportedSpanNamed(t, exporter, "test")
	}

	return ctx, tracer, meter, exporter, finish
}

func assertExportedSpanNamed(
	t *testing.T,
	exporter *tracetest.InMemoryExporter,
	name string,
) tracetest.SpanStub {
	t.Helper()

	spans := exporter.GetSpans()
	for i, s := range spans {
		t.Logf("=== span[%d] ===", i)
		t.Logf("name=%s", s.Name)
		t.Logf(
			"trace_id=%s span_id=%s",
			s.SpanContext.TraceID().String(),
			s.SpanContext.SpanID().String(),
		)
		t.Logf("start=%s end=%s", s.StartTime, s.EndTime)
		t.Logf("status=%v", s.Status)

		if s.Name == name {
			return s
		}
	}

	t.Fatalf("%q span が exporter に出てこない", name)
	return tracetest.SpanStub{}
}

func assertSendMetaHasTestTelemetry(
	t *testing.T,
	meta loggerfailure.Meta,
	span tracetest.SpanStub,
	inputCount int,
) {
	t.Helper()

	traceID := span.SpanContext.TraceID().String()
	require.NotEmpty(t, traceID)
	require.NotEmpty(t, meta.TraceID)
	assert.Contains(t, string(meta.TraceID), traceID)

	assert.Equal(t, inputCount, meta.InputCount)
	assert.Equal(t, "mock-service", meta.AppName)
	assert.Equal(t, "localhost", meta.HostName)
	assert.Equal(t, "test", meta.Environment)
	assert.Equal(t, "go", meta.RuntimeType)
	assert.Equal(t, "test-runtime-instance-id", meta.ExecutionUnitID)
	assert.Equal(t, deliveryBackendOpenSearch, meta.Backend)
	assert.False(t, meta.AttemptedAt.IsZero())
}

func assertOpenSearchFailurePayload(
	t *testing.T,
	failed loggerfailure.FailedRecord,
	want pkgopensearch.DocumentWriteRequest,
) {
	t.Helper()

	require.NotNil(t, failed.Payload)
	assert.Equal(t, want.Index, failed.Payload["index"])
	assert.Equal(t, want.ID, failed.Payload["id"])
	assert.Equal(t, want.Doc, failed.Payload["doc"])
	assert.Equal(t, string(want.Op), failed.Payload["op"])
}

func newTestLoggerBase(
	clock pkgclock.Clock,
	cfg *MockOpenSearchFlushManagerConfig,
	tracer observer.Tracer,
	meter observer.Meter,
) *LoggerBase {
	return &LoggerBase{
		Clock:              clock,
		TimestampFormatter: timestamp.RFC3339Formatter(),
		config:             cfg,
		Tracer:             tracer,
		Meter:              meter,
	}
}

func newTestFlushManager(
	client pkgopensearch.Client,
	cfg *MockOpenSearchFlushManagerConfig,
	tracer observer.Tracer,
	meter observer.Meter,
) *OpenSearchFlushManager {
	mockClock := pkgclock.NewUTCClock()
	res := newTestResourceProvider()
	loggerBase := newTestLoggerBase(mockClock, cfg, tracer, meter)

	manager, err := NewOpenSearchFlushManager(
		loggerBase, client, cfg,
		NewLogEntryConverter(
			cfg.UseOpenTelemetryFormat(),
			res,
			loggerBase.TimestampFormatter,
		),
		nil, res, tracer, meter,
	)
	if err != nil {
		panic(err)
	}
	return manager
}

// wrapErrorByLogger はテスト用のヘルパー関数です。
func (fm *OpenSearchFlushManager) wrapErrorByLogger(err error, operation string) error {
	return wrapErrorByLogger(err, operation)
}

func TestNewOpenSearchFlushManager(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()
	_ = ctx

	mockClock := pkgclock.NewUTCClock()
	mockClient := NewMockOpenSearchClientWrapper()
	mockConfig := NewMockOpenSearchFlushManagerConfig()
	resourceProvider := newTestResourceProvider()
	loggerBase := newTestLoggerBase(mockClock, mockConfig, tracer, meter)
	converter := NewLogEntryConverter(
		mockConfig.UseOpenTelemetryFormat(),
		resourceProvider,
		loggerBase.TimestampFormatter,
	)
	flushManager, err := NewOpenSearchFlushManager(
		loggerBase,
		mockClient,
		mockConfig,
		converter,
		nil,
		resourceProvider,
		tracer,
		meter,
	)

	require.NoError(t, err)
	require.NotNil(t, flushManager)
	assert.Same(t, loggerBase, flushManager.LoggerBase)
	assert.Same(t, mockClient, flushManager.osClient)
}

func TestNewOpenSearchFlushManagerRejectsInvalidBulkRetryConfig(t *testing.T) {
	_, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockConfig := NewMockOpenSearchFlushManagerConfig()
	resourceProvider := newTestResourceProvider()
	loggerBase := newTestLoggerBase(pkgclock.NewUTCClock(), mockConfig, tracer, meter)
	for _, test := range []struct {
		name     string
		attempts int
		wait     time.Duration
		want     error
	}{
		{"zero attempts", 0, 0, ErrInvalidMaxRetryAttempts},
		{"negative wait", 1, -time.Second, ErrNegativeRetryWaitTime},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := NewMockOpenSearchFlushManagerConfig()
			cfg.maxRetryAttempts, cfg.retryWaitTime = test.attempts, test.wait
			manager, err := NewOpenSearchFlushManager(
				loggerBase, NewMockOpenSearchClientWrapper(), cfg,
				NewLogEntryConverter(false, resourceProvider, loggerBase.TimestampFormatter),
				nil, resourceProvider, tracer, meter,
			)
			require.ErrorIs(t, err, test.want)
			require.Nil(t, manager)
		})
	}
}

// 構築時に受理したbulk再送設定は、送信時のitem送信回数（初回を含む）を決めます。
// 設定値を何回読み出すかという実装詳細ではなく、送信回数と残った失敗recordで検証します。
func TestOpenSearchBulkConfigValidatedAtConstruction(t *testing.T) {
	cfg := NewMockOpenSearchFlushManagerConfig()
	cfg.maxRetryAttempts = 3
	cfg.retryWaitTime = 0
	res := newTestResourceProvider()
	base := newTestLoggerBase(pkgclock.NewUTCClock(), cfg, observer.NopTracer{}, observer.NopMeter{})
	client := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	client.bulkHandler = func(_ context.Context, reqs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
		return retryableBulkResponse(reqs), nil
	}
	manager, err := NewOpenSearchFlushManager(
		base, client, cfg,
		NewLogEntryConverter(false, res, base.TimestampFormatter),
		nil, res, observer.NopTracer{}, observer.NopMeter{},
	)
	require.NoError(t, err)

	result, err := manager.sendDocumentsToOpenSearch(context.Background(), []map[string]any{{"message": "test"}})
	require.NoError(t, err)
	assert.Equal(t, cfg.maxRetryAttempts, client.IndexDocumentsCallCount)
	require.Len(t, result.RetryableRecords, 1)
	assert.Equal(t, http.StatusTooManyRequests, result.RetryableRecords[0].StatusCode)
	assert.Empty(t, result.DeadRecords)
	assert.Empty(t, result.QuarantineRecords)
}

func TestOpenSearchFlushManager_Flush_Success(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockConfig := NewMockOpenSearchFlushManagerConfig()
	flushManager := newTestFlushManager(mockClient, mockConfig, tracer, meter)

	entries := []*logging.LogEntry{{
		OccurredAt: flushManager.Clock.Now(),
		Level:      pkglogger.LogLevelError,
		Message:    "test error",
		Source:     "test-source",
	}}

	require.NoError(t, flushManager.Flush(ctx, entries))
	assert.True(t, mockClient.IndexDocumentCalled)
	assert.False(t, mockClient.IndexDocumentsCalled)
	assert.Equal(t, mockConfig.Index(), mockClient.IndexDocumentArgs.Index)
	assert.Equal(t, pkgopensearch.WriteOpCreate, mockClient.IndexDocumentArgs.Op)
	assert.NotEmpty(t, mockClient.IndexDocumentArgs.ID)
}

func TestOpenSearchFlushManager_FlushWithNoFallbackUsesOneBulkDispatchWithoutItemRetry(t *testing.T) {
	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.returnRetryableBulkOnce = true
	mockConfig := NewMockOpenSearchFlushManagerConfig()
	flushManager := newTestFlushManager(
		mockClient,
		mockConfig,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	flushManager.FlushWithNoFallback(context.Background(), []*logging.LogEntry{
		{
			OccurredAt: flushManager.Clock.Now(),
			Level:      pkglogger.LogLevelError,
			Message:    "first fallback bulk entry",
			Source:     "test-source",
		},
		{
			OccurredAt: flushManager.Clock.Now(),
			Level:      pkglogger.LogLevelError,
			Message:    "second fallback bulk entry",
			Source:     "test-source",
		},
	})

	if mockClient.IndexDocumentsCallCount != 1 {
		t.Fatalf("IndexDocuments calls = %d, want one dispatch", mockClient.IndexDocumentsCallCount)
	}
	if got := len(mockClient.IndexDocumentsArgs); got != 2 {
		t.Fatalf("bulk document count = %d, want 2", got)
	}
	if mockClient.IndexDocumentCallCount != 0 {
		t.Fatalf("IndexDocument calls = %d, want 0", mockClient.IndexDocumentCallCount)
	}
}

func TestOpenSearchFlushManager_Flush_TreatsFixedIDCreateConflictAsSuccess(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.indexErr = &pkgopensearch.HTTPStatusError{Code: http.StatusConflict}
	mockConfig := NewMockOpenSearchFlushManagerConfig()
	flushManager := newTestFlushManager(mockClient, mockConfig, tracer, meter)
	entries := []*logging.LogEntry{{
		OccurredAt: flushManager.Clock.Now(),
		Level:      pkglogger.LogLevelError,
		Message:    "already created",
		Source:     "test-source",
	}}

	require.NoError(t, flushManager.Flush(ctx, entries))
	assert.Equal(t, 1, mockClient.IndexDocumentCallCount)
	assert.Equal(t, pkgopensearch.WriteOpCreate, mockClient.IndexDocumentArgs.Op)
	assert.NotEmpty(t, mockClient.IndexDocumentArgs.ID)
}

func TestOpenSearchFlushManager_Flush_EmptyEntries(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	flushManager := newTestFlushManager(mockClient, NewMockOpenSearchFlushManagerConfig(), tracer, meter)

	require.NoError(t, flushManager.Flush(ctx, []*logging.LogEntry{}))
	assert.False(t, mockClient.IndexDocumentCalled)
	assert.False(t, mockClient.IndexDocumentsCalled)
}

func TestOpenSearchFlushManager_Flush_ClientNotInitialized(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClock := pkgclock.NewUTCClock()
	mockConfig := NewMockOpenSearchFlushManagerConfig()
	errorHandlerCalled := false
	errorHandler := func(ctx context.Context, err error, entry *logging.LogEntry, tracer observer.Tracer, meter observer.Meter) {
		errorHandlerCalled = true
	}

	loggerBase := newTestLoggerBase(mockClock, mockConfig, tracer, meter)
	loggerBase.OnError = errorHandler

	flushManager := &OpenSearchFlushManager{
		LoggerBase: loggerBase,
		osClient:   nil,
		config:     mockConfig,
		resource:   newTestResourceProvider(),
		Tracer:     tracer,
		Meter:      meter,
	}
	entries := []*logging.LogEntry{{Level: pkglogger.LogLevelError, Message: "test error"}}

	err := flushManager.Flush(ctx, entries)
	require.Error(t, err)
	assert.True(t, errorHandlerCalled)
}

func TestOpenSearchFlushManager_SendDocumentToOpenSearch_UnclassifiedIndexError_Quarantine(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	unclassifiedErr := errors.New("unclassified index failure")
	mockClient.indexErr = unclassifiedErr
	flushManager := newTestFlushManager(mockClient, NewMockOpenSearchFlushManagerConfig(), tracer, meter)

	res, err := flushManager.sendDocumentToOpenSearch(ctx, map[string]any{
		"level":   "error",
		"message": "test error",
	})

	require.NoError(t, err)
	assert.True(t, res.HasSendFailure())
	// 送信時のlabel文言ではなく、元の原因と分類結果で検証します。
	require.ErrorIs(t, res.TerminalError, unclassifiedErr)

	require.Empty(t, res.RetryableRecords)
	require.Empty(t, res.DeadRecords)
	require.Len(t, res.QuarantineRecords, 1)

	failed := res.QuarantineRecords[0]
	assert.Equal(t, loggerfailure.FailureCode(retryable.RetryReasonUnknownOutcome), failed.FailureCode)
	assert.Equal(t, pkglogger.DeliveryUnconfirmed, failed.DeliveryStatus)
	assert.Equal(t, "sendDocumentToOpenSearch", failed.Operation)
	// durable recordには運用者が原因を確認できるよう元のerrorを残します。recordの原因を
	// errors.Isで確認し、保存するFailureReasonがその原因を表すことを確かめます。
	require.ErrorIs(t, failed.Cause, unclassifiedErr)
	assert.Equal(t, failed.Cause.Error(), failed.FailureReason)
	assertOpenSearchFailurePayload(t, failed, mockClient.IndexDocumentArgs)

	assert.Equal(t, 1, mockClient.IndexDocumentCallCount)
}

func TestOpenSearchFlushManager_SendDocumentToOpenSearch_TransportError_Retryable(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.indexErr = io.ErrUnexpectedEOF
	flushManager := newTestFlushManager(mockClient, NewMockOpenSearchFlushManagerConfig(), tracer, meter)

	res, err := flushManager.sendDocumentToOpenSearch(ctx, map[string]any{
		"level":   "error",
		"message": "test error",
	})

	require.NoError(t, err)
	assert.True(t, res.HasSendFailure())
	assert.ErrorIs(t, res.TerminalError, io.ErrUnexpectedEOF)
	require.Len(t, res.RetryableRecords, 1)
	require.Empty(t, res.DeadRecords)
	assert.Equal(t, 0, res.RetryableRecords[0].StatusCode)
	assert.Equal(t, loggerfailure.FailureCode(retryable.RetryReasonEOF), res.RetryableRecords[0].FailureCode)
	require.ErrorIs(t, res.RetryableRecords[0].Cause, io.ErrUnexpectedEOF)
	assert.Equal(t, res.RetryableRecords[0].Cause.Error(), res.RetryableRecords[0].FailureReason)
	assertOpenSearchFailurePayload(t, res.RetryableRecords[0], mockClient.IndexDocumentArgs)
	assert.Equal(t, 1, mockClient.IndexDocumentCallCount)
}

func TestOpenSearchFlushManager_SendDocumentToOpenSearch_ResponseDecodeError_Quarantine(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	decodeErr := &pkgopensearch.ResponseDecodeError{
		Operation: "index",
		Err:       io.ErrUnexpectedEOF,
	}
	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.indexErr = decodeErr
	flushManager := newTestFlushManager(mockClient, NewMockOpenSearchFlushManagerConfig(), tracer, meter)

	res, err := flushManager.sendDocumentToOpenSearch(ctx, map[string]any{
		"level":   "error",
		"message": "test error",
	})

	require.NoError(t, err)
	assert.ErrorIs(t, res.TerminalError, io.ErrUnexpectedEOF)
	require.Empty(t, res.RetryableRecords)
	require.Len(t, res.QuarantineRecords, 1)
	assert.Equal(
		t,
		loggerfailure.FailureCode(infraopensearch.FailureReasonResponseDecode),
		res.QuarantineRecords[0].FailureCode,
	)
	assert.Equal(t, 1, mockClient.IndexDocumentCallCount)
}

func TestOpenSearchFlushManager_SendDocumentToOpenSearch_CircuitBreakerOpen_Retryable(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	circuitBreakerErr := retryable.NewCircuitBreakerOpenError(
		"opensearch index",
		time.Now().Add(time.Minute),
	)
	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.indexErr = circuitBreakerErr
	flushManager := newTestFlushManager(mockClient, NewMockOpenSearchFlushManagerConfig(), tracer, meter)

	res, err := flushManager.sendDocumentToOpenSearch(ctx, map[string]any{
		"level":   "error",
		"message": "test error",
	})

	require.NoError(t, err)
	assert.ErrorIs(t, res.TerminalError, circuitBreakerErr)
	require.Len(t, res.RetryableRecords, 1)
	require.Empty(t, res.DeadRecords)
	assert.Equal(
		t,
		loggerfailure.FailureCode(retryable.RetryReasonCircuitBreakerOpen),
		res.RetryableRecords[0].FailureCode,
	)
	assert.Equal(t, 1, mockClient.IndexDocumentCallCount)
}

func TestOpenSearchFlushManager_SendDocumentToOpenSearch_ClassifiesRecordDisposition(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		errorType      pkgopensearch.ErrorType
		wantQuarantine bool
	}{
		{name: "mapper parsing", status: http.StatusBadRequest, errorType: pkgopensearch.ErrorTypeMapperParsing, wantQuarantine: true},
		{name: "document parsing", status: http.StatusBadRequest, errorType: pkgopensearch.ErrorTypeDocumentParsing, wantQuarantine: true},
		{name: "xcontent parsing", status: http.StatusBadRequest, errorType: pkgopensearch.ErrorTypeXContentParse, wantQuarantine: true},
		{name: "parse", status: http.StatusBadRequest, errorType: pkgopensearch.ErrorTypeParse, wantQuarantine: true},
		{name: "strict dynamic mapping", status: http.StatusBadRequest, errorType: pkgopensearch.ErrorTypeStrictDynamicMapping, wantQuarantine: true},
		{name: "generic illegal argument stays dead", status: http.StatusBadRequest, errorType: pkgopensearch.ErrorTypeIllegalArgument},
		{name: "isolated request too large", status: http.StatusRequestEntityTooLarge, wantQuarantine: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
			defer finish()

			statusErr := &pkgopensearch.HTTPStatusError{Code: tt.status}
			if tt.errorType != "" {
				statusErr.Body = []byte(
					`{"error":{"type":"` + string(tt.errorType) + `","reason":"record failure"},"status":400}`,
				)
			}
			mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
			mockClient.indexErr = statusErr
			flushManager := newTestFlushManager(
				mockClient,
				NewMockOpenSearchFlushManagerConfig(),
				tracer,
				meter,
			)

			res, err := flushManager.sendDocumentToOpenSearch(ctx, map[string]any{
				"message": "record failure",
			})

			require.NoError(t, err)
			assert.ErrorIs(t, res.TerminalError, statusErr)
			assert.Empty(t, res.RetryableRecords)
			if tt.wantQuarantine {
				assert.Empty(t, res.DeadRecords)
				require.Len(t, res.QuarantineRecords, 1)
				assert.False(t, res.QuarantineRecords[0].FailedAt.IsZero())
				return
			}

			require.Len(t, res.DeadRecords, 1)
			assert.Empty(t, res.QuarantineRecords)
			assert.Equal(t, loggerfailure.FailureCode(tt.errorType), res.DeadRecords[0].FailureCode)
		})
	}
}

func TestOpenSearchFlushManager_Close(t *testing.T) {
	_, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	flushManager := newTestFlushManager(
		NewMockOpenSearchClientWrapper(),
		NewMockOpenSearchFlushManagerConfig(),
		tracer,
		meter,
	)
	require.NoError(t, flushManager.Close(context.Background()))
}

func TestOpenSearchFlushManager_ConvertLogEntriesToDocuments(t *testing.T) {
	_, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClock := pkgclock.NewUTCClock()
	mockConfig := NewMockOpenSearchFlushManagerConfig()
	flushManager := newTestFlushManager(
		NewMockOpenSearchClientWrapper(),
		mockConfig,
		tracer,
		meter,
	)

	entries := []*logging.LogEntry{
		{
			TraceID:    "test-trace-id01",
			SpanID:     "test-span-id01",
			RequestID:  "req-1",
			OccurredAt: mockClock.Now(),
			Level:      pkglogger.LogLevelError,
			Message:    "test error 1",
			Source:     "test-source",
		},
		{
			TraceID:    "test-trace-id02",
			SpanID:     "test-span-id02",
			RequestID:  "req-2",
			OccurredAt: mockClock.Now(),
			Level:      pkglogger.LogLevelInfo,
			Message:    "test info 2",
			Source:     "test-source",
		},
	}

	docs := flushManager.LogEntryToDocumentConverter().convertLogEntriesToDocuments(entries)
	require.Len(t, docs, len(entries))

	for i, doc := range docs {
		for _, field := range []string{"request_id", "timestamp", "level", "message", "source"} {
			assert.Contains(t, doc, field, "Document %d missing required field: %s", i, field)
		}
		assert.Equal(t, entries[i].RequestID, doc["request_id"])
		assert.Equal(t, entries[i].Level.String(), doc["level"])
		assert.Equal(t, entries[i].Message, doc["message"])
		assert.Equal(t, entries[i].Source, doc["source"])
	}
}

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_Success(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockConfig := NewMockOpenSearchFlushManagerConfig()
	flushManager := newTestFlushManager(mockClient, mockConfig, tracer, meter)

	docs := []map[string]any{{
		"request_id": "req-1",
		"level":      "error",
		"message":    "test error",
	}}

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, docs)
	require.NoError(t, err)
	assert.False(t, res.HasSendFailure())
	assert.True(t, mockClient.IndexDocumentsCalled)
	assert.Equal(t, 1, mockClient.IndexDocumentsCallCount)
	require.Len(t, mockClient.IndexDocumentsArgs, 1)
	assert.Equal(t, mockConfig.Index(), mockClient.IndexDocumentsArgs[0].Index)
	assert.Equal(t, pkgopensearch.WriteOpCreate, mockClient.IndexDocumentsArgs[0].Op)
	assert.NotEmpty(t, mockClient.IndexDocumentsArgs[0].ID)
}

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_ClassifiesDeadAndQuarantineItems(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.bulkHandler = func(
		_ context.Context,
		docs []pkgopensearch.DocumentWriteRequest,
	) (*pkgopensearch.BulkResponse, error) {
		require.Len(t, docs, 3)
		return &pkgopensearch.BulkResponse{
			Errors: true,
			Items: []map[string]pkgopensearch.BulkItemResult{
				{"create": {Index: docs[0].Index, ID: docs[0].ID, Status: http.StatusCreated, Result: "created"}},
				{"create": {
					Index:  docs[1].Index,
					ID:     docs[1].ID,
					Status: http.StatusBadRequest,
					Error: &pkgopensearch.ErrorDetail{
						Type:   pkgopensearch.ErrorTypeMapperParsing,
						Reason: "record mapping failed",
					},
				}},
				{"create": {
					Index:  docs[2].Index,
					ID:     docs[2].ID,
					Status: http.StatusBadRequest,
					Error: &pkgopensearch.ErrorDetail{
						Type:   pkgopensearch.ErrorTypeIllegalArgument,
						Reason: "request configuration failed",
					},
				}},
			},
		}, nil
	}
	flushManager := newTestFlushManager(
		mockClient,
		NewMockOpenSearchFlushManagerConfig(),
		tracer,
		meter,
	)

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, []map[string]any{
		{"message": "success"},
		{"message": "quarantine"},
		{"message": "dead"},
	})

	require.NoError(t, err)
	assert.Empty(t, res.RetryableRecords)
	require.Len(t, res.QuarantineRecords, 1)
	assert.Equal(t, loggerfailure.FailureCode(pkgopensearch.ErrorTypeMapperParsing), res.QuarantineRecords[0].FailureCode)
	require.Len(t, res.DeadRecords, 1)
	assert.Equal(t, loggerfailure.FailureCode(pkgopensearch.ErrorTypeIllegalArgument), res.DeadRecords[0].FailureCode)
	assert.Equal(t, 1, res.SuccessCount())
}

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_QuarantinesLocalPlannerRejection(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	config := NewMockOpenSearchFlushManagerConfig()
	config.maxBulkRequestBytes = 1
	flushManager := newTestFlushManager(mockClient, config, tracer, meter)

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, []map[string]any{{
		"message": "larger than one byte",
	}})

	require.NoError(t, err)
	assert.Empty(t, res.RetryableRecords)
	assert.Empty(t, res.DeadRecords)
	require.Len(t, res.QuarantineRecords, 1)
	assert.Equal(t, FailureCodeRequestTooLarge, res.QuarantineRecords[0].FailureCode)
	assert.True(t, res.QuarantineRecords[0].FailedAt.IsZero())
	assert.Equal(t, 0, mockClient.IndexDocumentsCallCount)
}

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_SplitsRequestEntityTooLarge(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	accepted := make(map[any]int)
	mockClient.bulkHandler = func(
		_ context.Context,
		docs []pkgopensearch.DocumentWriteRequest,
	) (*pkgopensearch.BulkResponse, error) {
		if len(docs) > 1 {
			return nil, &pkgopensearch.HTTPStatusError{Code: http.StatusRequestEntityTooLarge}
		}
		for _, doc := range docs {
			accepted[doc.Doc["message"]]++
		}
		return successBulkResponse(docs), nil
	}
	flushManager := newTestFlushManager(
		mockClient,
		NewMockOpenSearchFlushManagerConfig(),
		tracer,
		meter,
	)
	docs := []map[string]any{
		{"message": "first"},
		{"message": "second"},
		{"message": "third"},
	}

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, docs)

	require.NoError(t, err)
	assert.False(t, res.HasSendFailure())
	assert.Equal(t, len(docs), res.SuccessCount())
	// 分割の位置や順序ではなく、各documentが一度だけ受理され、送信回数が二分割の上限
	// （2*入力件数-1回）以内であることを検証します。
	assert.Equal(t, map[any]int{"first": 1, "second": 1, "third": 1}, accepted)
	assert.LessOrEqual(t, mockClient.IndexDocumentsCallCount, 2*len(docs)-1)
}

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_SingleRequestEntityTooLargeIsQuarantined(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.bulkErr = &pkgopensearch.HTTPStatusError{Code: http.StatusRequestEntityTooLarge}
	flushManager := newTestFlushManager(
		mockClient,
		NewMockOpenSearchFlushManagerConfig(),
		tracer,
		meter,
	)

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, []map[string]any{{"message": "too large"}})

	require.NoError(t, err)
	assert.Nil(t, res.TerminalError)
	require.Len(t, res.QuarantineRecords, 1)
	assert.Empty(t, res.RetryableRecords)
	assert.Empty(t, res.DeadRecords)
	assert.Equal(t, http.StatusRequestEntityTooLarge, res.QuarantineRecords[0].StatusCode)
	assert.Equal(t, 1, mockClient.IndexDocumentsCallCount)
}

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_TransportErrorIsNotRetried(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.bulkErr = io.ErrUnexpectedEOF
	mockConfig := NewMockOpenSearchFlushManagerConfig()
	flushManager := newTestFlushManager(mockClient, mockConfig, tracer, meter)

	docs := []map[string]any{{
		"request_id": "req-1",
		"level":      "error",
		"message":    "test error",
	}}

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, docs)
	require.NoError(t, err)
	assert.True(t, res.HasSendFailure())
	assert.ErrorIs(t, res.TerminalError, io.ErrUnexpectedEOF)
	assert.Equal(t, 1, mockClient.IndexDocumentsCallCount)
	require.Len(t, res.RetryableRecords, 1)
	require.Empty(t, res.DeadRecords)
	assert.Equal(t, loggerfailure.FailureCode(retryable.RetryReasonEOF), res.RetryableRecords[0].FailureCode)
	require.ErrorIs(t, res.RetryableRecords[0].Cause, io.ErrUnexpectedEOF)
	assert.Equal(t, res.RetryableRecords[0].Cause.Error(), res.RetryableRecords[0].FailureReason)
	assertOpenSearchFailurePayload(t, res.RetryableRecords[0], mockClient.IndexDocumentsArgs[0])
}

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_ResponseDecodeErrorQuarantine(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	decodeErr := &pkgopensearch.ResponseDecodeError{
		Operation: "bulk",
		Err:       io.ErrUnexpectedEOF,
	}
	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.bulkErr = decodeErr
	flushManager := newTestFlushManager(
		mockClient,
		NewMockOpenSearchFlushManagerConfig(),
		tracer,
		meter,
	)

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, []map[string]any{{
		"request_id": "req-1",
		"level":      "error",
		"message":    "test error",
	}})

	require.NoError(t, err)
	assert.ErrorIs(t, res.TerminalError, io.ErrUnexpectedEOF)
	require.Empty(t, res.RetryableRecords)
	require.Len(t, res.QuarantineRecords, 1)
	assert.Equal(
		t,
		loggerfailure.FailureCode(infraopensearch.FailureReasonResponseDecode),
		res.QuarantineRecords[0].FailureCode,
	)
	assert.Equal(t, 1, mockClient.IndexDocumentsCallCount)
}

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_UnclassifiedBulkErrorQuarantine(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.returnError = true
	mockConfig := NewMockOpenSearchFlushManagerConfig()
	mockConfig.maxRetryAttempts = 2
	flushManager := newTestFlushManager(mockClient, mockConfig, tracer, meter)

	docs := []map[string]any{{
		"request_id": "req-1",
		"level":      "error",
		"message":    "test error",
	}}

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, docs)
	require.NoError(t, err)
	assert.True(t, res.HasSendFailure())
	assert.Equal(t, 1, mockClient.IndexDocumentsCallCount)
	assert.ErrorIs(t, res.TerminalError, errMockBulkRequest)
	require.Empty(t, res.RetryableRecords)
	require.Len(t, res.QuarantineRecords, 1)
	// 分類できない送信errorでは受理の有無を確認できないため、結果不明として隔離します。
	quarantined := res.QuarantineRecords[0]
	assert.Zero(t, quarantined.StatusCode)
	assert.Equal(t, pkglogger.DeliveryUnconfirmed, quarantined.DeliveryStatus)
	assert.Equal(t, loggerfailure.FailureCode(retryable.RetryReasonUnknownOutcome), quarantined.FailureCode)
	assert.ErrorIs(t, quarantined.Cause, errMockBulkRequest)
}

// errMockBulkRequestは、mock clientがBulk request全体の失敗として返すerrorです。
var errMockBulkRequest = errors.New("mock bulk error")

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_RetriesRetryableBulkItems(t *testing.T) {
	ctx, tracer, meter, finish := newFlushManagerTestTelemetry(t)
	defer finish()

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.returnRetryableBulkOnce = true
	mockConfig := NewMockOpenSearchFlushManagerConfig()
	mockConfig.maxRetryAttempts = 2
	flushManager := newTestFlushManager(mockClient, mockConfig, tracer, meter)

	docs := []map[string]any{{
		"request_id": "req-1",
		"level":      "error",
		"message":    "test error",
	}}

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, docs)
	require.NoError(t, err)
	assert.False(t, res.HasSendFailure())
	assert.Equal(t, 2, mockClient.IndexDocumentsCallCount)
	require.Empty(t, res.RetryableRecords)
	require.Empty(t, res.DeadRecords)
}

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_BulkItemRetryWaitHonorsContext(t *testing.T) {
	// 仮想時刻では全goroutineが待機するまで時間が進まないため、初回送信より前にdeadlineが
	// 切れることはなく、deadlineは必ずitem再送の待機中に到達します。
	synctest.Test(t, func(t *testing.T) {
		_, tracer, meter, finish := newFlushManagerTestTelemetry(t)
		defer finish()

		const deadline = 20 * time.Millisecond
		mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
		mockClient.returnRetryableBulkOnce = true
		mockConfig := NewMockOpenSearchFlushManagerConfig()
		mockConfig.maxRetryAttempts = 2
		mockConfig.retryWaitTime = time.Hour
		flushManager := newTestFlushManager(mockClient, mockConfig, tracer, meter)
		ctx, cancel := context.WithTimeout(context.Background(), deadline)
		defer cancel()

		startedAt := time.Now()
		result, err := flushManager.sendDocumentsToOpenSearch(ctx, []map[string]any{{
			"request_id": "req-1",
			"message":    "throttled",
		}})
		elapsed := time.Since(startedAt)

		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, 1, mockClient.IndexDocumentsCallCount)
		// RetryWaitTime（1時間）を待たず、deadlineで待機を終えます。
		assert.Equal(t, deadline, elapsed)
		require.Len(t, result.RetryableRecords, 1)
		assert.Equal(t, http.StatusTooManyRequests, result.RetryableRecords[0].StatusCode)
		assert.Equal(t, mockClient.IndexDocumentsArgs[0].ID, result.RetryableRecords[0].Payload["id"])
		assert.Equal(t, mockClient.IndexDocumentsArgs[0].Doc, result.RetryableRecords[0].Payload["doc"])
		assert.Empty(t, result.QuarantineRecords)
	})
}

func TestOpenSearchFlushManager_SendDocumentToOpenSearch_TransportError_Retryable_Telemetry(t *testing.T) {
	ctx, tracer, meter, exporter, finish := newFlushManagerTestTelemetryWithExporter(t)

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.indexErr = io.ErrUnexpectedEOF
	flushManager := newTestFlushManager(mockClient, NewMockOpenSearchFlushManagerConfig(), tracer, meter)

	res, err := flushManager.sendDocumentToOpenSearch(ctx, map[string]any{
		"level":   "error",
		"message": "test error",
	})

	require.NoError(t, err)
	assert.True(t, res.HasSendFailure())
	require.Len(t, res.RetryableRecords, 1)
	require.Empty(t, res.DeadRecords)
	assert.ErrorIs(t, res.TerminalError, io.ErrUnexpectedEOF)

	finish()
	testSpan := assertExportedSpanNamed(t, exporter, "test")
	assertSendMetaHasTestTelemetry(t, res.Meta, testSpan, 1)

	failed := res.RetryableRecords[0]
	assertOpenSearchFailurePayload(t, failed, mockClient.IndexDocumentArgs)
}

func TestOpenSearchFlushManager_SendDocumentsToOpenSearch_TransportError_Telemetry(t *testing.T) {
	ctx, tracer, meter, exporter, finish := newFlushManagerTestTelemetryWithExporter(t)

	mockClient := NewMockOpenSearchClientWrapper().(*MockFlushManagerClient)
	mockClient.bulkErr = io.ErrUnexpectedEOF
	mockConfig := NewMockOpenSearchFlushManagerConfig()
	flushManager := newTestFlushManager(mockClient, mockConfig, tracer, meter)

	docs := []map[string]any{{
		"request_id": "req-1",
		"level":      "error",
		"message":    "test error",
	}}

	res, err := flushManager.sendDocumentsToOpenSearch(ctx, docs)
	require.NoError(t, err)
	assert.True(t, res.HasSendFailure())
	assert.ErrorIs(t, res.TerminalError, io.ErrUnexpectedEOF)
	assert.Equal(t, 1, mockClient.IndexDocumentsCallCount)
	require.Len(t, res.RetryableRecords, 1)
	require.Empty(t, res.DeadRecords)

	finish()
	testSpan := assertExportedSpanNamed(t, exporter, "test")
	assertSendMetaHasTestTelemetry(t, res.Meta, testSpan, len(docs))
}
