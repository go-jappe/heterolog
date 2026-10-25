package spoolstorehandler

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload/provider"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/memory/pressure"
	spoolstorecfg "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

const (
	testMapperParsingFailureCode   = "mapper_parsing_exception"
	testBatchTimeSpanFailureCode   = "batch_time_span_exceeded"
	testEmptyMessageFailureCode    = "invalid_event_message_empty"
	testMessageTooLargeFailureCode = "invalid_event_too_large"
	testDeduplicationTokenField    = "insert_deduplication_token"
)

// 保存時刻や警告判定時刻を固定し、テスト結果を安定させる。
type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

// SpoolStoreConfigProvider のテスト用実装。
type testSpoolStoreConfig struct {
	spoolPath string
	target    string
	dir       string
	overload  spoolstorecfg.SpoolStoreOverloadPolicyConfigProvider
}

func (c testSpoolStoreConfig) SpoolPath() string { return c.spoolPath }
func (c testSpoolStoreConfig) Target() string    { return c.target }
func (c testSpoolStoreConfig) Dir() string       { return c.dir }
func (c testSpoolStoreConfig) OverloadPolicyConfig() spoolstorecfg.SpoolStoreOverloadPolicyConfigProvider {
	return c.overload
}

// overload 閾値をテストごとに必要な分だけ差し替えるための設定。
type testOverloadPolicyConfig struct {
	overloadPolicy                durable.OverloadPolicy
	spillDestination              durable.SpillDestination
	maxCallsSinceScan             uint64
	statsScanInterval             time.Duration
	maxDirSize                    uint64
	activeBacklogWarnAfterDays    int
	minAvailableDiskSpace         uint64
	useMemoryPressureForOverload  bool
	memoryCriticalRatio           float64
	minAvailableMemoryBytes       uint64
	memoryCriticalConsecutiveHits int
}

func (c testOverloadPolicyConfig) OverloadPolicy() durable.OverloadPolicy {
	return c.overloadPolicy
}
func (c testOverloadPolicyConfig) SpillDestination() durable.SpillDestination {
	return c.spillDestination
}
func (c testOverloadPolicyConfig) MaxCallsSinceScan() uint64 { return c.maxCallsSinceScan }
func (c testOverloadPolicyConfig) StatsScanInterval() time.Duration {
	return c.statsScanInterval
}
func (c testOverloadPolicyConfig) MaxDirSize() uint64 { return c.maxDirSize }
func (c testOverloadPolicyConfig) ActiveBacklogWarnAfterDays() int {
	return c.activeBacklogWarnAfterDays
}
func (c testOverloadPolicyConfig) MinAvailableDiskSpace() uint64 { return c.minAvailableDiskSpace }
func (c testOverloadPolicyConfig) UseMemoryPressureForOverload() bool {
	return c.useMemoryPressureForOverload
}
func (c testOverloadPolicyConfig) MemoryCriticalRatio() float64 { return c.memoryCriticalRatio }
func (c testOverloadPolicyConfig) MinAvailableMemoryBytes() uint64 {
	return c.minAvailableMemoryBytes
}
func (c testOverloadPolicyConfig) MemoryCriticalConsecutiveHits() int {
	return c.memoryCriticalConsecutiveHits
}

type recordingMemoryMonitor struct {
	isCriticalCalls int
	critical        bool
	err             error
}

type recordingOverloadHandler struct {
	calls int
	err   error
}

func (h *recordingOverloadHandler) Handle(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	_ = ctx
	_ = result
	h.calls++
	return h.err
}

func (m *recordingMemoryMonitor) Current(ctx context.Context) (memorypressure.MemoryPressure, error) {
	_ = ctx
	return memorypressure.MemoryPressure{}, nil
}

func (m *recordingMemoryMonitor) IsCritical(ctx context.Context) (bool, error) {
	_ = ctx
	m.isCriticalCalls++
	return m.critical, m.err
}

func (m *recordingMemoryMonitor) Last() memorypressure.MonitorSnapshot {
	return memorypressure.MonitorSnapshot{}
}

func (m *recordingMemoryMonitor) Reset() {}

func (m *recordingMemoryMonitor) Start(ctx context.Context) error {
	_ = ctx
	return nil
}

// 送信元で確定したretryable/dead/quarantineの分類が、対応するstateに保存されることを確認する。
// FailedAt は record 側の値を優先し、なければ Meta.AttemptedAt を使う。
func TestStoreSendResultByStateStoresRecordsByFailureClass(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 12, 34, 56, 789000000, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{})

	retryFailedAt := now.Add(-2 * time.Hour)
	metaAttemptedAt := now.Add(-1 * time.Hour)
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     "opensearch",
			RequestID:   "meta-request",
			AttemptedAt: metaAttemptedAt,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			failedRecord("retry-id", "es_rejected_execution_exception", retryFailedAt),
		},
		DeadRecords: []loggerfailure.FailedRecord{
			failedRecord("dead-id", "cluster_block_exception", time.Time{}),
		},
		QuarantineRecords: []loggerfailure.FailedRecord{
			failedRecord("quarantine-id", testMapperParsingFailureCode, time.Time{}),
			failedRecord("batch-span-id", testBatchTimeSpanFailureCode, time.Time{}),
			failedRecord("empty-message-id", testEmptyMessageFailureCode, time.Time{}),
			failedRecord("too-large-id", testMessageTooLargeFailureCode, time.Time{}),
		},
	}

	// 実際の SpoolStore を使い、状態別ディレクトリに NDJSON が書かれるところまで検証する。
	if err := handler.StoreSendResultByState(context.Background(), result); err != nil {
		t.Fatalf("StoreSendResultByState returned error: %v", err)
	}

	activeRecords := readSpoolRecords(t, handler.Store.RootDir.ActiveDir().Path())
	assertRecordCount(t, activeRecords, 1, "active")
	assertRecord(t, activeRecords[0], retryFailedAt, "meta-request", "retry-id", "es_rejected_execution_exception")

	deadRecords := readSpoolRecords(t, handler.Store.RootDir.DeadDir().Path())
	assertRecordCount(t, deadRecords, 1, "dead")
	assertRecord(t, deadRecords[0], metaAttemptedAt, "meta-request", "dead-id", "cluster_block_exception")

	quarantineRecords := readSpoolRecords(t, handler.Store.RootDir.QuarantineDir().Path())
	assertRecordCount(t, quarantineRecords, 4, "quarantine")
	assertRecord(t, quarantineRecords[0], metaAttemptedAt, "meta-request", "quarantine-id", testMapperParsingFailureCode)
	assertRecord(t, quarantineRecords[1], metaAttemptedAt, "meta-request", "batch-span-id", testBatchTimeSpanFailureCode)
	assertRecord(t, quarantineRecords[2], metaAttemptedAt, "meta-request", "empty-message-id", testEmptyMessageFailureCode)
	assertRecord(t, quarantineRecords[3], metaAttemptedAt, "meta-request", "too-large-id", testMessageTooLargeFailureCode)
}

func TestStoreSendResultByStateDoesNotReclassifyDeadRecords(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 13, 0, 0, 0, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{})
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{Backend: "opensearch", RequestID: "default-dead", AttemptedAt: now},
		DeadRecords: []loggerfailure.FailedRecord{
			failedRecord("dead-id", testMapperParsingFailureCode, now),
		},
	}

	if err := handler.StoreSendResultByState(context.Background(), result); err != nil {
		t.Fatalf("StoreSendResultByState returned error: %v", err)
	}

	assertRecordCount(t, readSpoolRecords(t, handler.Store.RootDir.DeadDir().Path()), 1, "dead")
	assertRecordCount(t, readSpoolRecords(t, handler.Store.RootDir.QuarantineDir().Path()), 0, "quarantine")
}

// backend固有の再送情報もpayload内にそのまま残ることを確認する。
func TestStoreSendResultByStateKeepsBackendSpecificFieldInPayload(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{})

	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     "clickhouse",
			RequestID:   "backend-request",
			AttemptedAt: now,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			{
				FailedAt: now,
				Payload: map[string]any{
					"message":                   "backend metadata is persisted",
					testDeduplicationTokenField: "backend-token-1",
				},
			},
		},
	}

	if err := handler.StoreSendResultByState(context.Background(), result); err != nil {
		t.Fatalf("StoreSendResultByState returned error: %v", err)
	}

	records := readSpoolRecords(t, handler.Store.RootDir.ActiveDir().Path())
	assertRecordCount(t, records, 1, "active")

	var payload map[string]any
	if err := json.Unmarshal(records[0].Payload, &payload); err != nil {
		t.Fatalf("failed to decode spool payload: %v", err)
	}
	if got, want := payload[testDeduplicationTokenField], "backend-token-1"; got != want {
		t.Fatalf("spool payload %s = %v, want %q", testDeduplicationTokenField, got, want)
	}
}

func TestToSpoolRecordWritesPayloadJSONDirectly(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	record, err := toSpoolRecord(
		fixedClock{now: now},
		loggerfailure.FailedRecord{
			Payload: map[string]any{
				"index": "logs",
				"id":    "durable-id",
				"doc":   map[string]any{"message": "durable"},
				"op":    "index",
			},
		},
		loggerfailure.Meta{Backend: "opensearch", RequestID: "durable-request", AttemptedAt: now},
	)
	if err != nil {
		t.Fatalf("toSpoolRecord returned error: %v", err)
	}

	const want = `{"doc":{"message":"durable"},"id":"durable-id","index":"logs","op":"index"}`
	if got := string(record.Payload); got != want {
		t.Fatalf("Payload = %s, want direct payload %s", got, want)
	}

	var decoded map[string]any
	if err := json.Unmarshal(record.Payload, &decoded); err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}
	if decoded["id"] != "durable-id" || decoded["op"] != "index" {
		t.Fatalf("decoded payload = %+v, want durable id and operation", decoded)
	}
}

// 過負荷時は、構築時に渡した退避先へ処理を委ね、その失敗を呼び出し元へ返します。
// 退避先の種類はfactoryが選ぶため、ここでは退避先の結果だけを切り替えます。
// 退避自体の失敗は内部エラーとしても通知します。
func TestHandleFailureUsesConfiguredOverloadHandler(t *testing.T) {
	t.Parallel()

	overloadErr := errors.New("overload evacuation failed")
	tests := []struct {
		name       string
		handlerErr error
	}{
		{
			name: "evacuation succeeded",
		},
		{
			name:       "evacuation failed",
			handlerErr: overloadErr,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 6, 8, 14, 30, 0, 0, time.UTC)
			overloadHandler := &recordingOverloadHandler{err: tt.handlerErr}
			var notified []error
			handler := newTestHandlerWithOverloadHandler(
				t,
				now,
				testOverloadPolicyConfig{useMemoryPressureForOverload: true},
				&recordingMemoryMonitor{critical: true},
				overloadHandler,
				InternalLogHandlers{OnError: recordSpoolErrors(&notified)},
			)

			result := loggerfailure.SendResult{
				Meta: loggerfailure.Meta{
					Backend:     "opensearch",
					RequestID:   "overload-report-request",
					AttemptedAt: now,
					InputCount:  1,
				},
				RetryableRecords: []loggerfailure.FailedRecord{
					failedRecord("overload-report-id", "retryable", now),
				},
			}

			err := handler.handleFailure(context.Background(), result)
			if tt.handlerErr == nil && err != nil {
				t.Fatalf("handleFailure error = %v, want nil", err)
			}
			if tt.handlerErr != nil && !errors.Is(err, tt.handlerErr) {
				t.Fatalf("handleFailure error = %v, want %v", err, tt.handlerErr)
			}
			wantNotifications := 0
			if tt.handlerErr != nil {
				wantNotifications = 1
			}
			if len(notified) != wantNotifications {
				t.Fatalf("OnError notifications = %v, want %d", notified, wantNotifications)
			}
			if overloadHandler.calls != 1 {
				t.Fatalf("overload handler calls = %d, want 1", overloadHandler.calls)
			}
			if records := readSpoolRecords(t, handler.Store.RootDir.ActiveDir().Path()); len(records) != 0 {
				t.Fatalf("active records count = %d, want 0 after overload", len(records))
			}
		})
	}
}

// drop policyから構築した退避先は、過負荷時のrecordをspoolへ保存せずdrop件数として記録します。
// 破棄したことは同期APIの呼び出し元へErrLogDroppedで返し、内部エラーとして重ねて通知しません。
func TestHandleFailureDropPolicyDropsOverloadedRecords(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 14, 45, 0, 0, time.UTC)
	overloadCfg := testOverloadPolicyConfig{
		overloadPolicy:               durable.OverloadPolicyDrop,
		useMemoryPressureForOverload: true,
	}
	meter := &countingMeter{}
	dropHandler, err := NewOverloadHandlerProvider(testSpoolStoreConfig{overload: overloadCfg}, meter)
	if err != nil {
		t.Fatalf("NewOverloadHandlerProvider error = %v", err)
	}
	var notified []error
	handler := newTestHandlerWithOverloadHandler(
		t,
		now,
		overloadCfg,
		&recordingMemoryMonitor{critical: true},
		dropHandler,
		InternalLogHandlers{OnError: recordSpoolErrors(&notified)},
	)
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     "opensearch",
			RequestID:   "overload-drop-request",
			AttemptedAt: now,
			InputCount:  1,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			failedRecord("overload-drop-id", "retryable", now),
		},
	}

	err = handler.handleFailure(context.Background(), result)
	if !errors.Is(err, pkglogger.ErrLogDropped) {
		t.Fatalf("handleFailure error = %v, want ErrLogDropped", err)
	}
	if len(notified) != 0 {
		t.Fatalf("OnError notifications = %v, want none for the configured drop", notified)
	}
	if got := meter.total(provider.FailureOverloadDropTotal); got != 1 {
		t.Fatalf("%s = %d, want 1", provider.FailureOverloadDropTotal, got)
	}
	if records := readSpoolRecords(t, handler.Store.RootDir.ActiveDir().Path()); len(records) != 0 {
		t.Fatalf("active records count = %d, want 0 after drop", len(records))
	}
}

func TestInternalErrorUsesFailureSpoolSource(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 16, 0, 0, 0, time.UTC)
	handler := newTestHandler(
		t,
		now,
		testOverloadPolicyConfig{},
	)
	var source string
	handler.OnError = func(
		ctx context.Context,
		err error,
		entry *logging.LogEntry,
		tracer observer.Tracer,
		meter observer.Meter,
	) {
		_ = ctx
		_ = err
		_ = tracer
		_ = meter
		if entry != nil {
			source = entry.Source
		}
	}

	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{Backend: "opensearch", RequestID: "invalid-request", AttemptedAt: now},
		RetryableRecords: []loggerfailure.FailedRecord{
			{},
		},
	}
	if err := handler.handleFailure(context.Background(), result); err == nil {
		t.Fatal("handleFailure returned nil error, want validation error")
	}
	if source != failureSpoolInternalLogSource {
		t.Fatalf("internal error source = %q, want %q", source, failureSpoolInternalLogSource)
	}
}

// spoolへ保存するpayloadがないrecordは、保存せずerrorとして返す。
func TestStoreSendResultByStateReturnsValidationErrorAndSkipsInvalidRecords(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{})

	result := loggerfailure.SendResult{
		Meta:             loggerfailure.Meta{Backend: "opensearch", RequestID: "request", AttemptedAt: now},
		RetryableRecords: []loggerfailure.FailedRecord{{}},
	}

	err := handler.StoreSendResultByState(context.Background(), result)
	if err == nil {
		t.Fatal("StoreSendResultByState returned nil error, want validation error")
	}
	if !errors.Is(err, ErrNilDurablePayload) {
		t.Fatalf("error = %v, want ErrNilDurablePayload", err)
	}
	if records := readSpoolRecords(t, handler.Store.RootDir.ActiveDir().Path()); len(records) != 0 {
		t.Fatalf("active records count = %d, want 0", len(records))
	}
}

// active 配下を走査し、通常ファイルだけが件数・容量・最古更新時刻の集計対象になることを確認する。
func TestScanDirStatsCountsRegularFilesOnly(t *testing.T) {
	t.Parallel()

	rootDir, err := spooldir.NewRootDir(t.TempDir(), "test-app", "test-backend")
	if err != nil {
		t.Fatalf("failed to create root dir: %v", err)
	}
	root := rootDir.ActiveDir()
	rootPath := root.Path()
	oldest := time.Date(2026, 6, 7, 10, 0, 0, 0, time.UTC)
	newest := oldest.Add(2 * time.Hour)

	// nested file は集計対象、directory 自体は集計対象外。
	writeFileWithModTime(t, filepath.Join(rootPath, "old.log"), []byte("12345"), oldest)
	writeFileWithModTime(t, filepath.Join(rootPath, "nested", "new.log"), []byte("123"), newest)
	if err := os.Mkdir(filepath.Join(rootPath, "empty-dir"), 0o755); err != nil {
		t.Fatalf("failed to create empty dir: %v", err)
	}

	stats, err := scanDirStats(root)
	if err != nil {
		t.Fatalf("scanDirStats returned error: %v", err)
	}

	if stats.FileCount != 2 {
		t.Fatalf("FileCount = %d, want 2", stats.FileCount)
	}
	if stats.TotalSize != 8 {
		t.Fatalf("TotalSize = %d, want 8", stats.TotalSize)
	}
	if !stats.OldestModTime.Equal(oldest) {
		t.Fatalf("OldestModTime = %s, want %s", stats.OldestModTime, oldest)
	}
}

// active dir の総サイズが MaxDirSize を超えると overload と判定される。
func TestIsActiveSpoolStorageOverloadedByMaxDirSize(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{maxDirSize: 3})
	writeFileWithModTime(t, filepath.Join(handler.Store.RootDir.ActiveDir().Path(), "large.log"), []byte("1234"), now)

	overloaded, err := handler.isActiveSpoolStorageOverloaded(context.Background(), handler.Store.RootDir.ActiveDir())
	if err != nil {
		t.Fatalf("isActiveSpoolStorageOverloaded returned error: %v", err)
	}
	if !overloaded {
		t.Fatal("isActiveSpoolStorageOverloaded = false, want true")
	}
}

func TestIsMemoryOverloadedSkipsMonitorWhenConfigDisabled(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{useMemoryPressureForOverload: false})
	monitor := &recordingMemoryMonitor{critical: true}
	handler.MemoryMonitor = monitor

	overloaded, err := handler.isMemoryOverloaded(context.Background())
	if err != nil {
		t.Fatalf("isMemoryOverloaded returned error: %v", err)
	}
	if overloaded {
		t.Fatal("isMemoryOverloaded = true, want false when memory overload config is disabled")
	}
	if monitor.isCriticalCalls != 0 {
		t.Fatalf("IsCritical calls = %d, want 0", monitor.isCriticalCalls)
	}
}

func TestIsMemoryOverloadedUsesMonitorWhenConfigEnabled(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{useMemoryPressureForOverload: true})
	monitor := &recordingMemoryMonitor{critical: true}
	handler.MemoryMonitor = monitor

	overloaded, err := handler.isMemoryOverloaded(context.Background())
	if err != nil {
		t.Fatalf("isMemoryOverloaded returned error: %v", err)
	}
	if !overloaded {
		t.Fatal("isMemoryOverloaded = false, want true when monitor reports critical")
	}
	if monitor.isCriticalCalls != 1 {
		t.Fatalf("IsCritical calls = %d, want 1", monitor.isCriticalCalls)
	}
}

// 古い active spool は警告対象だが、保存停止の overload としては扱わない。
// 同一日中に同じ警告が連続通知されず、retryable failureの保存が
// 実際に継続することまで確認する。
func TestStaleActiveBacklogWarnsOncePerDayAndStillStoresFailures(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	warnCount := 0
	handler := newTestHandler(t, now, testOverloadPolicyConfig{activeBacklogWarnAfterDays: 1})
	defer func() {
		// 2回目の保存はcacheを返して非同期のdirectory scanを開始します。
		// 一時directoryの削除前に、そのscanの完了まで待ちます。
		if err := shutdownSpoolHandlerForTest(handler); err != nil {
			t.Error(err)
		}
	}()
	overloadHandler := &recordingOverloadHandler{}
	handler.OverloadHandler = overloadHandler
	handler.OnWarn = func(ctx context.Context, entry *logging.LogEntry, tracer observer.Tracer, meter observer.Meter) {
		warnCount++
		if entry == nil {
			t.Fatal("warn entry is nil")
		}
		if !strings.Contains(entry.Message, "active spool backlog is stale") {
			t.Fatalf("warn message = %q, want stale backlog message", entry.Message)
		}
		if !strings.Contains(entry.Message, "active_backlog_warn_after_days=1") {
			t.Fatalf("warn message = %q, want warning threshold field", entry.Message)
		}
	}

	writeFileWithModTime(
		t,
		filepath.Join(handler.Store.RootDir.ActiveDir().Path(), "stale.log"),
		[]byte("1234"),
		now.Add(-48*time.Hour),
	)

	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     "opensearch",
			RequestID:   "stale-backlog-request",
			AttemptedAt: now,
			InputCount:  1,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			failedRecord("stale-backlog-id", "retryable", now),
		},
	}

	// 2 回保存しても warn は 1 回だけ。stats cache は初回 scan 結果を再利用する。
	for i := 0; i < 2; i++ {
		if err := handler.handleFailure(context.Background(), result); err != nil {
			t.Fatalf("handleFailure returned error: %v", err)
		}
	}

	if warnCount != 1 {
		t.Fatalf("warn count = %d, want 1", warnCount)
	}
	if overloadHandler.calls != 0 {
		t.Fatalf("overload handler calls = %d, want 0 for stale backlog warning", overloadHandler.calls)
	}
	if records := readSpoolRecords(t, handler.Store.RootDir.ActiveDir().Path()); len(records) != 2 {
		t.Fatalf("active records count = %d, want 2 after stale backlog warnings", len(records))
	}
}

// dailyWarnStateは明示されcalendar locationの日付単位で通知可否を管理する。
func TestDailyWarnStateShouldNotifyOncePerCalendarDay(t *testing.T) {
	t.Parallel()

	loc := time.FixedZone("JST", 9*60*60)
	state := dailyWarnState{}
	first := time.Date(2026, 6, 8, 10, 0, 0, 0, loc)
	second := time.Date(2026, 6, 8, 23, 59, 0, 0, loc)
	nextDay := time.Date(2026, 6, 9, 0, 1, 0, 0, loc)

	if !state.ShouldNotify(first) {
		t.Fatal("first ShouldNotify = false, want true")
	}
	if state.ShouldNotify(second) {
		t.Fatal("same-day ShouldNotify = true, want false")
	}
	if !state.ShouldNotify(nextDay) {
		t.Fatal("next-day ShouldNotify = false, want true")
	}
}

func TestDailyWarnStateUsesClockTimezone(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("JST", 9*60*60)
	state := dailyWarnState{}
	first := time.Date(2026, 6, 10, 23, 30, 0, 0, location)
	next := time.Date(2026, 6, 11, 0, 30, 0, 0, location)

	if !state.ShouldNotify(first) {
		t.Fatal("first ShouldNotify = false, want true")
	}
	if !state.ShouldNotify(next) {
		t.Fatal("next-calendar-day ShouldNotify = false, want true")
	}
}

// temp dir 上に実 SpoolStore を作り、ファイル保存を含めたテストを行える handler を返す。
func newTestHandler(
	t *testing.T,
	now time.Time,
	overloadCfg testOverloadPolicyConfig,
) *SpoolStoreHandler {
	t.Helper()
	return newTestHandlerWithOverloadHandler(t, now, overloadCfg, nil, nil, InternalLogHandlers{})
}

// newTestHandlerWithOverloadHandlerは、過負荷判定に使うmemory monitor、過負荷時の退避先、
// 内部ログの通知先を構築時に渡したhandlerを返します。
func newTestHandlerWithOverloadHandler(
	t *testing.T,
	now time.Time,
	overloadCfg testOverloadPolicyConfig,
	memoryMonitor pressure.MemoryPressureMonitor,
	overloadHandler overload.Handler,
	internalLogHandlers InternalLogHandlers,
) *SpoolStoreHandler {
	t.Helper()

	cfg := testSpoolStoreConfig{
		spoolPath: t.TempDir(),
		target:    "test-app",
		dir:       "test-backend",
		overload:  overloadCfg,
	}
	clk := fixedClock{now: now}

	store, err := fsstorestore.NewSpoolStore(clk, cfg, nil, nil)
	if err != nil {
		t.Fatalf("failed to create spool store: %v", err)
	}

	return NewSpoolStoreHandler(
		context.Background(),
		clk,
		store,
		cfg,
		memoryMonitor,
		overloadHandler,
		nil,
		nil,
		internalLogHandlers,
	)
}

// recordSpoolErrorsは、内部エラーの通知をnotifiedへ記録するhandlerを返します。
func recordSpoolErrors(notified *[]error) logging.LoggerErrorHandler {
	return func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		*notified = append(*notified, err)
	}
}

// countingMeterは、counterへの加算をmetric名ごとに集計します。
type countingMeter struct {
	mu     sync.Mutex
	totals map[string]int64
}

func (m *countingMeter) Counter(name string) observer.Counter {
	return countingCounter{meter: m, name: name}
}

func (m *countingMeter) Gauge(string) observer.Gauge {
	return observer.NopGauge{}
}

func (m *countingMeter) total(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totals[name]
}

type countingCounter struct {
	meter *countingMeter
	name  string
}

func (c countingCounter) Add(_ context.Context, value int64, _ ...observer.Attr) {
	c.meter.mu.Lock()
	defer c.meter.mu.Unlock()
	if c.meter.totals == nil {
		c.meter.totals = make(map[string]int64)
	}
	c.meter.totals[c.name] += value
}

// 失敗recordの分類テストで使う最小限のpayloadを作る。
func failedRecord(id, failureCode string, failedAt time.Time) loggerfailure.FailedRecord {
	return loggerfailure.FailedRecord{
		FailedAt:      failedAt,
		Operation:     "index",
		StatusCode:    599,
		FailureCode:   loggerfailure.FailureCode(failureCode),
		FailureReason: failureCode + " reason",
		Payload: map[string]any{
			"index": "logs",
			"id":    id,
			"doc":   map[string]any{"message": id},
			"op":    "index",
		},
	}
}

// state dir 配下に保存された spool NDJSON をすべて読み戻す。
func readSpoolRecords(t *testing.T, root string) []pkgspool.SpoolRecord {
	t.Helper()

	var records []pkgspool.SpoolRecord
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() || filepath.Ext(path) != spooldir.SpoolExt {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var rec pkgspool.SpoolRecord
			if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
				return err
			}
			records = append(records, rec)
		}
		return scanner.Err()
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed to read spool records: %v", err)
	}

	return records
}

// 状態名を failure message に含め、どの spool state の件数不一致か分かるようにする。
func assertRecordCount(t *testing.T, records []pkgspool.SpoolRecord, want int, state string) {
	t.Helper()

	if len(records) != want {
		t.Fatalf("%s record count = %d, want %d", state, len(records), want)
	}
}

// spool record のメタ情報と payload 内の document ID をまとめて検証する。
func assertRecord(
	t *testing.T,
	rec pkgspool.SpoolRecord,
	wantOccurredAt time.Time,
	wantRequestID, wantDocID, wantFailureCode string,
) {
	t.Helper()

	if !rec.OccurredAt.Equal(wantOccurredAt) {
		t.Fatalf("OccurredAt = %s, want %s", rec.OccurredAt, wantOccurredAt)
	}
	if rec.RequestID != wantRequestID {
		t.Fatalf("RequestID = %q, want %q", rec.RequestID, wantRequestID)
	}
	if rec.InitialFailureOperation != "index" {
		t.Fatalf("InitialFailureOperation = %q, want %q", rec.InitialFailureOperation, "index")
	}
	if rec.InitialFailureStatusCode != 599 {
		t.Fatalf("InitialFailureStatusCode = %d, want %d", rec.InitialFailureStatusCode, 599)
	}
	if rec.InitialFailureCode != wantFailureCode {
		t.Fatalf("InitialFailureCode = %q, want %q", rec.InitialFailureCode, wantFailureCode)
	}
	if want := wantFailureCode + " reason"; rec.InitialFailureReason != want {
		t.Fatalf("InitialFailureReason = %q, want %q", rec.InitialFailureReason, want)
	}
	if want := wantFailureCode + " reason"; rec.LastFailureReason != want {
		t.Fatalf("LastFailureReason = %q, want %q", rec.LastFailureReason, want)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}
	if payload["id"] != wantDocID {
		t.Fatalf("payload ID = %v, want %q", payload["id"], wantDocID)
	}
}

// dir stats のテスト用に、任意の mtime を持つ通常ファイルを作成する。
func writeFileWithModTime(t *testing.T, path string, body []byte, modTime time.Time) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("failed to create parent dir: %v", err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("failed to set file mod time: %v", err)
	}
}
