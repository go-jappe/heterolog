package opensearch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	opensearchwrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	infraspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	spoollifecycle "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	spoollifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	spoolsenderopensearch "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/opensearch"
	spoolstore "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	spoolworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	observermemorypressure "github.com/go-jappe/heterolog/internal/observer/memory/pressure"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openSearchClientForTest(t *testing.T, logger *OpenSearchLogger) pkgopensearch.Client {
	t.Helper()

	flushManager, ok := logger.flushManager.(*OpenSearchFlushManager)
	require.True(t, ok, "flush manager type = %T, want *OpenSearchFlushManager", logger.flushManager)
	return flushManager.osClient
}

func TestOpenSearchLoggerFlushRetriesWithFactoryRetryingRealClient(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	var requestCount atomic.Int32
	var invalidRequestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := requestCount.Add(1)
		if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, "/test-index/_create/") {
			invalidRequestCount.Add(1)
		}

		w.Header().Set("Content-Type", "application/json")
		if count == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"type":"unavailable","reason":"temporary failure"},"status":503}`))
			return
		}

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(pkgopensearch.IndexResponse{
			Index: "test-index", ID: strings.TrimPrefix(r.URL.Path, "/test-index/_create/"),
			Version: 1, Result: "created",
		})
	}))
	t.Cleanup(server.Close)

	clientCfg := infraopensearch.NewOpenSearchClientConfigAdapter(&pkgopensearch.Config{
		Endpoint: server.URL,
		Timeout:  time.Second,
		AuthConfig: pkgopensearch.AuthConfig{
			Type: pkgopensearch.AuthTypeNone,
		},
	})
	retryCfg := NewRetryConfigProvider(
		&pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
			RetryClientMode: pkglogger.RetryClientModeRetry,
			MaxAttempts:     2,
			BaseDelay:       time.Nanosecond,
			MaxDelay:        time.Nanosecond,
		},
	)
	osClient, err := infraopensearch.NewClient(clientCfg, retryCfg, tracer, meter)
	require.NoError(t, err)
	require.IsType(t, &opensearchwrapper.RetryingClient{}, osClient)

	cfg := NewMockOpenSearchConfigProvider()
	cfg.flushInterval = time.Hour
	loggerBase := &LoggerBase{
		Clock:  clock.NewUTCClock(),
		Tracer: tracer,
		Meter:  meter,
	}
	logger, err := NewOpenSearchLogger(
		ctx,
		cfg,
		osClient,
		cfg,
		NewMockResourceProvider(),
		nil,
		tracer,
		meter,
		loggerBase,
	)
	require.NoError(t, err)
	require.IsType(t, &opensearchwrapper.RetryingClient{}, openSearchClientForTest(t, logger))

	require.NoError(t, logger.testInfo(ctx, "retryable client integration test"))
	require.NoError(t, logger.doFlush(ctx))

	assert.Equal(t, int32(2), requestCount.Load())
	assert.Equal(t, int32(0), invalidRequestCount.Load())
	assert.Equal(t, 0, entryCount(logger))
}

func TestOpenSearchLoggerNoBulkFlushModeIndexesToBackend(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	var indexRequestCount atomic.Int32
	var bulkRequestCount atomic.Int32
	var invalidRequestCount atomic.Int32
	var storedMu sync.Mutex
	storedDocs := make(map[string]map[string]any)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/test-index/_create/"):
			indexRequestCount.Add(1)

			docID := strings.TrimPrefix(r.URL.Path, "/test-index/_create/")
			if docID == "" {
				invalidRequestCount.Add(1)
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				invalidRequestCount.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			var doc map[string]any
			if err := json.Unmarshal(body, &doc); err != nil {
				invalidRequestCount.Add(1)
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			storedMu.Lock()
			storedDocs[docID] = doc
			storedMu.Unlock()

			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(pkgopensearch.IndexResponse{
				Index:   "test-index",
				ID:      docID,
				Version: 1,
				Result:  "created",
				Status:  http.StatusCreated,
			})

		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			bulkRequestCount.Add(1)
			w.WriteHeader(http.StatusInternalServerError)

		default:
			invalidRequestCount.Add(1)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	osClient, err := infraopensearch.NewClient(
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

	cfg := NewMockOpenSearchConfigProvider()
	cfg.flushInterval = time.Millisecond
	cfg.maxBulkDocument = 1
	cfg.noBulkFlushModeEnabled = true

	loggerBase := &LoggerBase{
		Clock:  clock.NewUTCClock(),
		Tracer: tracer,
		Meter:  meter,
	}
	logger, err := NewOpenSearchLogger(
		ctx,
		cfg,
		osClient,
		cfg,
		NewMockResourceProvider(),
		nil,
		tracer,
		meter,
		loggerBase,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, logger.Close(ctx))
	})

	require.NoError(t, logger.testInfo(ctx, "nobulk backend registered one"))
	require.NoError(t, logger.testInfo(ctx, "nobulk backend registered two"))

	assert.Equal(t, int32(2), indexRequestCount.Load())
	assert.Equal(t, int32(0), bulkRequestCount.Load())
	assert.Equal(t, int32(0), invalidRequestCount.Load())
	assert.Equal(t, 0, entryCount(logger))

	storedMu.Lock()
	defer storedMu.Unlock()

	require.Len(t, storedDocs, 2)
	messages := make([]string, 0, len(storedDocs))
	for _, doc := range storedDocs {
		messages = append(messages, fmt.Sprint(doc["message"]))
		assert.Equal(t, pkglogger.LogLevelInfo.String(), doc["level"])
	}
	assert.ElementsMatch(t, []string{
		"nobulk backend registered one",
		"nobulk backend registered two",
	}, messages)
}

func TestOpenSearchLoggerNoBulkFlushModeFailureStoresActiveSpool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	var indexRequestCount atomic.Int32
	var bulkRequestCount atomic.Int32
	var invalidRequestCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/test-index/_create/"):
			indexRequestCount.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"type":"unavailable","reason":"temporary failure"},"status":503}`))

		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			bulkRequestCount.Add(1)
			w.WriteHeader(http.StatusInternalServerError)

		default:
			invalidRequestCount.Add(1)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	osClient, err := infraopensearch.NewClient(
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

	spoolCfg := openSearchLoggerSpoolIntegrationConfig{
		spoolPath: t.TempDir(),
		target:    "logger-opensearch-nobulk-failure",
		dir:       "opensearch",
	}
	spoolClock := fixedOpenSearchSpoolClock{
		now: time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
	}
	spoolStore, err := fsstorestore.NewSpoolStore(spoolClock, spoolCfg, tracer, meter)
	require.NoError(t, err)
	overloadHandler, err := spoolstorehandler.NewOverloadHandlerProvider(spoolCfg, meter)
	require.NoError(t, err)
	spoolHandler := spoolstorehandler.NewSpoolStoreHandler(
		ctx,
		spoolClock,
		spoolStore,
		spoolCfg,
		nil,
		overloadHandler,
		tracer,
		meter,
		spoolstorehandler.InternalLogHandlers{},
	)
	failureHandler := mustNewOpenSearchFailureComposite([]flushfailure.FailureHandler{spoolHandler})

	cfg := NewMockOpenSearchConfigProvider()
	cfg.flushInterval = time.Hour
	cfg.maxBulkDocument = 1
	cfg.noBulkFlushModeEnabled = true

	loggerBase := &LoggerBase{
		Clock:  clock.NewUTCClock(),
		Tracer: tracer,
		Meter:  meter,
	}
	logger, err := NewOpenSearchLogger(
		ctx,
		cfg,
		osClient,
		cfg,
		NewMockResourceProvider(),
		failureHandler,
		tracer,
		meter,
		loggerBase,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, logger.Close(ctx))
	})

	require.NoError(t, logger.testInfo(ctx, "nobulk failure spooled active"))

	assert.Equal(t, int32(1), indexRequestCount.Load())
	assert.Equal(t, int32(0), bulkRequestCount.Load())
	assert.Equal(t, int32(0), invalidRequestCount.Load())
	assert.Equal(t, 0, entryCount(logger))

	activeRecords := waitForOpenSearchIntegrationSpoolRecords(
		t,
		openSearchIntegrationSpoolStateRoot(spoolCfg, pkgspool.StateActive),
		1,
	)
	require.Len(t, activeRecords, 1)
	assertOpenSearchSpoolPayload(
		t,
		activeRecords[0],
		"test-index",
		pkgopensearch.WriteOpCreate,
		"nobulk failure spooled active",
	)

	assert.Empty(t, readOpenSearchIntegrationSpoolRecords(
		t,
		openSearchIntegrationSpoolStateRoot(spoolCfg, pkgspool.StateDead),
	))
	assert.Empty(t, readOpenSearchIntegrationSpoolRecords(
		t,
		openSearchIntegrationSpoolStateRoot(spoolCfg, pkgspool.StateQuarantine),
	))
}

// MemoryPressure Spool Runnnerも試験
func TestOpenSearchLoggerFailureSpoolsActiveAndRunnerMovesToDoneWithMemoryPressure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	monitorCtx, cancelMonitor := context.WithCancel(ctx)
	defer cancelMonitor()
	// 実hostのmemory providerは取得可否や値が環境ごとに異なるため使いません。
	// critical閾値未満の信頼できる観測を固定し、monitorを配線したspool経路を検証します。
	memoryMonitor, err := observermemorypressure.New(
		fixedOpenSearchMemoryPressureReader{pressure: memorypressure.MemoryPressure{
			Ratio:          0.5,
			LimitBytes:     1 << 30,
			UsedBytes:      512 << 20,
			AvailableBytes: 512 << 20,
			Source:         memorypressure.SourceUnknown,
			Reliable:       true,
		}},
		memorypressure.CriticalPolicy{
			CriticalRatio:           0.9,
			ConsecutiveCriticalHits: 1,
		},
		time.Hour,
	)
	require.NoError(t, err)
	require.NoError(t, memoryMonitor.Start(monitorCtx))
	currentPressure, err := memoryMonitor.Current(ctx)
	require.NoError(t, err)
	require.NoError(t, currentPressure.Validate())
	isCritical, err := memoryMonitor.IsCritical(ctx)
	require.NoError(t, err)
	require.False(t, isCritical)

	var requestCount atomic.Int32
	var bulkRequestCount atomic.Int32
	var invalidRequestCount atomic.Int32
	var bulkMu sync.Mutex
	var bulkDocumentIDs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/test-index/_create/"):
			requestCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"type":"unavailable","reason":"temporary failure"},"status":503}`))

		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			bulkRequestCount.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				invalidRequestCount.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			items, ok := parseOpenSearchBulkRequestItems(body)
			if !ok || len(items) == 0 {
				invalidRequestCount.Add(1)
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			bulkMu.Lock()
			for _, item := range items {
				bulkDocumentIDs = append(bulkDocumentIDs, item.ID)
			}
			bulkMu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(openSearchBulkSuccessResponse(items))

		default:
			invalidRequestCount.Add(1)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	osClient, err := infraopensearch.NewClient(
		infraopensearch.NewOpenSearchClientConfigAdapter(&pkgopensearch.Config{
			Endpoint: server.URL,
			Timeout:  time.Second,
			AuthConfig: pkgopensearch.AuthConfig{
				Type: pkgopensearch.AuthTypeNone,
			},
		}),
		NewRetryConfigProvider(
			&pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
				RetryClientMode: pkglogger.RetryClientModeRetry,
				MaxAttempts:     2,
				BaseDelay:       time.Nanosecond,
				MaxDelay:        time.Nanosecond,
			},
		),
		tracer,
		meter,
	)
	require.NoError(t, err)
	require.IsType(t, &opensearchwrapper.RetryingClient{}, osClient)

	spoolCfg := openSearchLoggerSpoolIntegrationConfig{
		spoolPath: t.TempDir(),
		target:    "logger-opensearch-integration",
		dir:       "opensearch",
	}
	spoolClock := fixedOpenSearchSpoolClock{
		now: time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC),
	}
	spoolStore, err := fsstorestore.NewSpoolStore(spoolClock, spoolCfg, tracer, meter)
	require.NoError(t, err)
	overloadHandler, err := spoolstorehandler.NewOverloadHandlerProvider(spoolCfg, meter)
	require.NoError(t, err)
	spoolHandler := spoolstorehandler.NewSpoolStoreHandler(
		ctx,
		spoolClock,
		spoolStore,
		spoolCfg,
		memoryMonitor,
		overloadHandler,
		tracer,
		meter,
		spoolstorehandler.InternalLogHandlers{},
	)
	failureHandler := mustNewOpenSearchFailureComposite([]flushfailure.FailureHandler{spoolHandler})

	cfg := NewMockOpenSearchConfigProvider()
	cfg.flushInterval = time.Hour
	loggerBase := &LoggerBase{
		Clock:  clock.NewUTCClock(),
		Tracer: tracer,
		Meter:  meter,
	}
	logger, err := NewOpenSearchLogger(
		ctx,
		cfg,
		osClient,
		cfg,
		NewMockResourceProvider(),
		failureHandler,
		tracer,
		meter,
		loggerBase,
	)
	require.NoError(t, err)

	require.NoError(t, logger.testInfo(ctx, "spool retry after opensearch communication failure"))
	require.NoError(t, logger.doFlush(ctx))
	assert.Equal(t, int32(2), requestCount.Load())
	assert.Equal(t, int32(0), invalidRequestCount.Load())
	assert.Equal(t, 0, entryCount(logger))

	activeRecords := waitForOpenSearchIntegrationSpoolRecords(
		t,
		openSearchIntegrationSpoolStateRoot(spoolCfg, pkgspool.StateActive),
		1,
	)
	require.Len(t, activeRecords, 1)
	activePayload := assertOpenSearchSpoolPayload(
		t,
		activeRecords[0],
		"test-index",
		pkgopensearch.WriteOpCreate,
		"spool retry after opensearch communication failure",
	)
	activeID := activePayload.ID

	stateStore, err := fsworker.NewStateStore(spoolCfg)
	require.NoError(t, err)
	spoolResendClient, err := infraopensearch.NewClient(
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
	spoolSender, err := spoolsenderopensearch.NewOpensearchSpoolSender(
		fixedOpenSearchSpoolClock{now: spoolClock.now.Add(5 * time.Minute)},
		spoolResendClient,
		cfg.MaxBulkRequestBytes(),
	)
	require.NoError(t, err)
	spoolRunnerWorker, err := spoolworker.New(
		stateStore,
		openSearchActiveSpoolWriter{store: spoolStore},
		spoolSender,
	)
	require.NoError(t, err)
	spoolRunnerWorker.TargetBackendService = "opensearch"

	lifecycleManager, err := spoollifecycle.NewManager(openSearchSpoolLifecycleStoreStub{}, tracer, meter)
	require.NoError(t, err)
	runner, err := infraspool.NewRunner(
		spoolRunnerWorker,
		lifecycleManager,
		openSearchRunnerConfigStub{},
		tracer,
		meter,
	)
	require.NoError(t, err)

	report, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, spoolworker.ProcessStatusCompleted, report.ProcessReport.Status)
	assert.Equal(t, 1, report.ProcessReport.RecordCount)
	assert.Equal(t, 1, report.ProcessReport.SuccessRecordCount)
	assert.Equal(t, int32(1), bulkRequestCount.Load())
	bulkMu.Lock()
	assert.Equal(t, []string{activeID}, append([]string(nil), bulkDocumentIDs...))
	bulkMu.Unlock()

	assert.Empty(t, readOpenSearchIntegrationSpoolRecords(t, openSearchIntegrationSpoolStateRoot(spoolCfg, pkgspool.StateActive)))
	assert.Empty(t, readOpenSearchIntegrationSpoolRecords(t, openSearchIntegrationSpoolStateRoot(spoolCfg, pkgspool.StateProcessing)))

	doneRecords := readOpenSearchIntegrationSpoolRecords(
		t,
		openSearchIntegrationSpoolStateRoot(spoolCfg, pkgspool.StateDone),
	)
	require.Len(t, doneRecords, 1)
	donePayload := assertOpenSearchSpoolPayload(
		t,
		doneRecords[0],
		"test-index",
		pkgopensearch.WriteOpCreate,
		"spool retry after opensearch communication failure",
	)
	assert.Equal(t, activeID, donePayload.ID)
	assert.False(t, doneRecords[0].FinalizedAt.IsZero())
	assert.False(t, doneRecords[0].FirstResendAttemptedAt.IsZero())
	assert.Equal(t, 1, doneRecords[0].ResendAttemptCount)
}

// fixedOpenSearchMemoryPressureReaderは実hostに依存しない固定のmemory観測を返します。
type fixedOpenSearchMemoryPressureReader struct {
	pressure memorypressure.MemoryPressure
}

func (r fixedOpenSearchMemoryPressureReader) ReadMemoryPressure(context.Context) (memorypressure.MemoryPressure, error) {
	return r.pressure, nil
}

type openSearchLoggerSpoolIntegrationConfig struct {
	spoolPath string
	target    string
	dir       string
}

func (c openSearchLoggerSpoolIntegrationConfig) SpoolPath() string { return c.spoolPath }
func (c openSearchLoggerSpoolIntegrationConfig) Target() string    { return c.target }
func (c openSearchLoggerSpoolIntegrationConfig) Dir() string       { return c.dir }
func (c openSearchLoggerSpoolIntegrationConfig) FileNameFormat() string {
	return ""
}
func (c openSearchLoggerSpoolIntegrationConfig) FileDateFormat() string {
	return "20060102"
}
func (c openSearchLoggerSpoolIntegrationConfig) MaxWriteAttempts() int {
	return 1
}
func (c openSearchLoggerSpoolIntegrationConfig) FileOperationRetryWaitTime() time.Duration {
	return time.Millisecond
}
func (c openSearchLoggerSpoolIntegrationConfig) OverloadPolicyConfig() spoolstore.SpoolStoreOverloadPolicyConfigProvider {
	return openSearchSpoolOverloadConfig{}
}

type openSearchSpoolOverloadConfig struct{}

func (openSearchSpoolOverloadConfig) OverloadPolicy() pkgdurable.OverloadPolicy {
	return pkgdurable.OverloadPolicy("drop")
}
func (openSearchSpoolOverloadConfig) SpillDestination() pkgdurable.SpillDestination {
	return pkgdurable.SpillDestination("stdout")
}
func (openSearchSpoolOverloadConfig) MaxCallsSinceScan() uint64 { return 0 }
func (openSearchSpoolOverloadConfig) StatsScanInterval() time.Duration {
	return 0
}
func (openSearchSpoolOverloadConfig) MaxDirSize() uint64                 { return 0 }
func (openSearchSpoolOverloadConfig) ActiveBacklogWarnAfterDays() int    { return 0 }
func (openSearchSpoolOverloadConfig) MinAvailableDiskSpace() uint64      { return 0 }
func (openSearchSpoolOverloadConfig) UseMemoryPressureForOverload() bool { return true }
func (openSearchSpoolOverloadConfig) MemoryCriticalRatio() float64       { return 1.1 }
func (openSearchSpoolOverloadConfig) MinAvailableMemoryBytes() uint64    { return 0 }
func (openSearchSpoolOverloadConfig) MemoryCriticalConsecutiveHits() int { return 1 }

type fixedOpenSearchSpoolClock struct {
	now time.Time
}

func (c fixedOpenSearchSpoolClock) Now() time.Time {
	return c.now
}

type openSearchActiveSpoolWriter struct {
	store *fsstorestore.SpoolStore
}

func (w openSearchActiveSpoolWriter) StoreActive(ctx context.Context, records []pkgspool.SpoolRecord) error {
	return w.store.StoreByState(ctx, pkgspool.StateActive, records)
}

type openSearchBulkRequestItem struct {
	Action string
	Index  string
	ID     string
}

func parseOpenSearchBulkRequestItems(body []byte) ([]openSearchBulkRequestItem, bool) {
	scanner := bufio.NewScanner(bytes.NewReader(body))
	items := make([]openSearchBulkRequestItem, 0)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var meta map[string]map[string]string
		if err := json.Unmarshal(line, &meta); err != nil {
			return nil, false
		}

		item, ok := openSearchBulkRequestItemFromMeta(meta)
		if !ok {
			return nil, false
		}
		items = append(items, item)

		if !scanner.Scan() {
			return nil, false
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, false
	}

	return items, true
}

func openSearchBulkRequestItemFromMeta(
	meta map[string]map[string]string,
) (openSearchBulkRequestItem, bool) {
	for _, action := range []string{"create", "index"} {
		fields, ok := meta[action]
		if !ok {
			continue
		}
		index := fields["_index"]
		id := fields["_id"]
		if index == "" || id == "" {
			return openSearchBulkRequestItem{}, false
		}
		return openSearchBulkRequestItem{
			Action: action,
			Index:  index,
			ID:     id,
		}, true
	}

	return openSearchBulkRequestItem{}, false
}

func openSearchBulkSuccessResponse(
	items []openSearchBulkRequestItem,
) pkgopensearch.BulkResponse {
	responseItems := make([]map[string]pkgopensearch.BulkItemResult, 0, len(items))
	for _, item := range items {
		responseItems = append(responseItems, map[string]pkgopensearch.BulkItemResult{
			item.Action: {
				Index:   item.Index,
				ID:      item.ID,
				Status:  http.StatusCreated,
				Result:  "created",
				Version: 1,
			},
		})
	}

	return pkgopensearch.BulkResponse{
		Took:   1,
		Errors: false,
		Items:  responseItems,
	}
}

type openSearchRunnerConfigStub struct{}

func (openSearchRunnerConfigStub) Interval() time.Duration { return time.Millisecond }
func (openSearchRunnerConfigStub) StopOnError() bool       { return true }

type openSearchSpoolLifecycleStoreStub struct{}

func (openSearchSpoolLifecycleStoreStub) PurgeDoneIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func (openSearchSpoolLifecycleStoreStub) PurgeDeadIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func (openSearchSpoolLifecycleStoreStub) PurgeQuarantineIfNeed(
	ctx context.Context,
) (spoollifecyclereport.PurgeReport, error) {
	_ = ctx
	return spoollifecyclereport.PurgeReport{}, nil
}

func waitForOpenSearchIntegrationSpoolRecords(
	t *testing.T,
	root string,
	want int,
) []pkgspool.SpoolRecord {
	t.Helper()

	// 書込みの完了を待ちます。時間の上限はhangの検出だけに使います。
	deadline := time.Now().Add(lifecycleHangTimeout)
	for {
		records := readOpenSearchIntegrationSpoolRecords(t, root)
		if len(records) == want {
			return records
		}
		if time.Now().After(deadline) {
			require.Len(t, records, want)
			return records
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readOpenSearchIntegrationSpoolRecords(t *testing.T, root string) []pkgspool.SpoolRecord {
	t.Helper()

	var records []pkgspool.SpoolRecord
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || filepath.Ext(path) != spooldir.SpoolExt {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var record pkgspool.SpoolRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				return err
			}
			records = append(records, record)
		}
		return scanner.Err()
	})
	require.NoError(t, err)

	return records
}

func assertOpenSearchSpoolPayload(
	t *testing.T,
	record pkgspool.SpoolRecord,
	wantIndex string,
	wantOp pkgopensearch.WriteOp,
	wantMessage string,
) pkgopensearch.DocumentWriteRequest {
	t.Helper()

	var raw map[string]any
	err := json.Unmarshal(record.Payload, &raw)
	require.NoError(t, err)
	assert.NotContains(t, raw, "version")
	assert.NotContains(t, raw, "backend")
	assert.NotContains(t, raw, "data")

	var payload pkgopensearch.DocumentWriteRequest
	require.NoError(t, json.Unmarshal(record.Payload, &payload))
	assert.Equal(t, wantIndex, payload.Index)
	assert.Equal(t, wantOp, payload.Op)
	assert.NotEmpty(t, payload.ID)
	assert.Equal(t, wantMessage, payload.Doc["message"])
	return payload
}

func openSearchIntegrationSpoolStateRoot(
	cfg openSearchLoggerSpoolIntegrationConfig,
	state pkgspool.State,
) string {
	return filepath.Join(cfg.spoolPath, cfg.target, "spool", cfg.dir, string(state))
}
