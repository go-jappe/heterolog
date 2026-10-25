package clickhouse

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestClickHouseFallbackOwnsSingleBatchWorker(t *testing.T) {
	cfg := newTestClickHouseLoggerBaseConfig()
	cfg.FlushInterval = time.Hour
	fallback := newTimerTestClickHouseFallback(t, &recordingClickHouseClient{}, cfg)

	fallback.mu.Lock()
	workerStarted := fallback.flushWorkerStarted
	ticker := fallback.ticker
	stopCh := fallback.stopCh
	wakeCh := fallback.wakeCh
	fallback.mu.Unlock()
	if !workerStarted || ticker == nil || stopCh == nil || wakeCh == nil {
		t.Fatalf(
			"fallback worker state = started:%t ticker:%t stop:%t wake:%t",
			workerStarted,
			ticker != nil,
			stopCh != nil,
			wakeCh != nil,
		)
	}

	require.NoError(t, fallback.Close(context.Background()))
}

func TestClickHouseFallbackFlushesByInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &recordingClickHouseClient{}
		cfg := newTestClickHouseLoggerBaseConfig()
		cfg.FlushInterval = 2 * time.Millisecond
		cfg.MaxEntries = 100
		fallback := newTimerTestClickHouseFallback(t, client, cfg)

		fallback.enqueueProcess(
			context.Background(),
			newTestLogEntry(pkglogger.LogLevelInfo, "interval entry", nil),
		)
		// 件数・byteの閾値に届かないentryは、FlushIntervalが経過するまで送信しません。
		synctest.Wait()
		require.Equal(t, 0, client.batchCount())
		time.Sleep(cfg.FlushInterval)
		synctest.Wait()
		require.Equal(t, 1, client.batchCount())

		require.NoError(t, fallback.Close(context.Background()))
		require.Equal(t, 1, clickHouseRecordCount(client))
	})
}

func TestClickHouseFallbackThresholdWakeIsCoalescedAndQueueDropsOldestWhileBlocked(t *testing.T) {
	cfg := newTestClickHouseLoggerBaseConfig()
	cfg.FlushInterval = time.Hour
	cfg.MaxEntries = 1
	cfg.FallbackQueueMaxEntries = 2
	fallback := newTimerTestClickHouseFallback(t, &recordingClickHouseClient{}, cfg)
	manager := newBlockingFallbackFlushManager()
	// fallback構築時に登録したCloseより先に実行し、失敗時もblockしたflushを解放します。
	t.Cleanup(manager.releaseFlush)
	fallback.mu.Lock()
	fallback.flushManager = manager
	fallback.mu.Unlock()

	fallback.enqueueProcess(
		context.Background(),
		newTestLogEntry(pkglogger.LogLevelInfo, "first", nil),
	)
	select {
	case <-manager.started:
	case <-time.After(fallbackHangTimeout):
		t.Fatal("fallback worker did not start the first flush")
	}

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		for _, message := range []string{"second", "third", "fourth"} {
			fallback.enqueueProcess(
				context.Background(),
				newTestLogEntry(pkglogger.LogLevelInfo, message, nil),
			)
		}
	}()
	// backendがblockしていてもenqueueは戻ります。backendの解放を待つ実装では
	// この完了通知が届かないため、時間の上限はhangの検出だけに使います。
	select {
	case <-returned:
	case <-time.After(fallbackHangTimeout):
		t.Fatal("fallback enqueue waited for the blocked backend")
	}

	fallback.mu.Lock()
	queuedMessages := make([]string, 0, len(fallback.entries))
	for _, entry := range fallback.entries {
		queuedMessages = append(queuedMessages, entry.Message)
	}
	queuedBytes := fallback.bufferedBytes
	wantBytes := 0
	for _, entryBytes := range fallback.entryBytes {
		wantBytes += entryBytes
	}
	fallback.mu.Unlock()
	require.Equal(t, []string{"third", "fourth"}, queuedMessages)
	require.Equal(t, wantBytes, queuedBytes)

	manager.releaseFlush()
	// 合流したwakeによる二回目のflushを、完了の通知で待ちます。
	waitBlockingFallbackFlushes(t, manager, 2)
	require.NoError(t, fallback.Close(context.Background()))
	require.Equal(t, [][]string{{"first"}, {"third", "fourth"}}, manager.batchMessages())
}

func TestClickHouseFallbackFailedSnapshotIsAttemptedOnlyOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestClickHouseLoggerBaseConfig()
		cfg.FlushInterval = time.Hour
		cfg.MaxEntries = 1
		fallback := newTimerTestClickHouseFallback(t, &recordingClickHouseClient{}, cfg)
		manager := &failingFallbackFlushManager{err: errors.New("insert failed")}
		fallback.mu.Lock()
		fallback.flushManager = manager
		fallback.mu.Unlock()

		fallback.enqueueProcess(
			context.Background(),
			newTestLogEntry(pkglogger.LogLevelInfo, "send once", nil),
		)
		// workerが閾値のwakeで一度だけ送信し、失敗したsnapshotをqueueへ戻しません。
		synctest.Wait()
		require.Equal(t, 1, manager.flushCount())
		fallback.mu.Lock()
		queued := len(fallback.entries)
		fallback.mu.Unlock()
		require.Zero(t, queued)

		require.NoError(t, fallback.Close(context.Background()))
		require.Equal(t, 1, manager.flushCount())
	})
}

func TestClickHouseFallbackConcurrentEnqueueFlushesEachAcceptedEntryOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &recordingClickHouseClient{}
		cfg := newTestClickHouseLoggerBaseConfig()
		cfg.FlushInterval = 2 * time.Millisecond
		cfg.MaxEntries = 7
		cfg.FallbackQueueMaxEntries = 1000
		fallback := newTimerTestClickHouseFallback(t, client, cfg)

		const entryCount = 200
		var writers sync.WaitGroup
		writers.Add(entryCount)
		for range entryCount {
			go func() {
				defer writers.Done()
				fallback.enqueueProcess(
					context.Background(),
					newTestLogEntry(pkglogger.LogLevelInfo, "concurrent fallback entry", nil),
				)
			}()
		}
		writers.Wait()

		// 閾値のflushが進んだ後、残りのentryはFlushIntervalの経過時に送信します。
		synctest.Wait()
		time.Sleep(cfg.FlushInterval)
		synctest.Wait()
		require.Equal(t, entryCount, clickHouseRecordCount(client))
		require.NoError(t, fallback.Close(context.Background()))
		require.Equal(t, entryCount, clickHouseRecordCount(client))
	})
}

func newTimerTestClickHouseFallback(
	t *testing.T,
	client Client,
	baseCfg pkglogger.ClickHouseLoggerBaseConfig,
) *FallbackLogger {
	t.Helper()

	tracer := observer.EnsureTracer(nil)
	meter := observer.EnsureMeter(nil)
	cfg := NewClickHouseLoggerConfigAdapter(baseCfg)
	loggerBase := newTestClickHouseLoggerBase()
	loggerBase.config = cfg
	loggerBase.Tracer = tracer
	loggerBase.Meter = meter
	timestampFormatter, err := NewConfiguredTimestampFormatter(cfg)
	require.NoError(t, err)
	loggerBase.LogFormatter = formatter.NewPlainTextFormatter(timestampFormatter)

	logger := &ClickHouseLogger{
		LoggerBase:         loggerBase,
		entries:            make([]*logging.LogEntry, 0, cfg.FallbackQueueMaxEntries()),
		entryBytes:         make([]int, 0, cfg.FallbackQueueMaxEntries()),
		entryRequiresFlush: make([]bool, 0, cfg.FallbackQueueMaxEntries()),
		timeProvider:       time.Now,
		flushIntervalGen:   time.NewTicker,
		resourceProvider:   newTestClickHouseResourceProvider(),
		Tracer:             tracer,
		Meter:              meter,
	}
	logger.lastFlushTime = logger.timeProvider()
	logger.flushManager = NewClickHouseFlushManager(
		loggerBase,
		client,
		newTestClickHouseFlushManagerConfig(),
		nil,
		newTestClickHouseResourceProvider(),
		tracer,
		meter,
	)
	fallback := &FallbackLogger{ClickHouseLogger: logger}
	fallback.startFallbackWorker()
	closeClickHouseTestLoggerOnCleanup(t, func() error { return fallback.Close(context.Background()) })
	return fallback
}

func clickHouseRecordCount(client *recordingClickHouseClient) int {
	client.mu.Lock()
	defer client.mu.Unlock()

	count := 0
	for _, batch := range client.batches {
		count += len(batch)
	}
	return count
}

type blockingFallbackFlushManager struct {
	mu          sync.Mutex
	started     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	batches     [][]*logging.LogEntry
	// flushedはFlushの完了ごとに通知します。
	flushed chan struct{}
}

// fallbackHangTimeoutは、完了するはずの処理がhangしたことを検出するための時間の上限です。
// 処理の完了は通知で待ち、この上限は失敗の検出だけに使います。
const fallbackHangTimeout = 10 * time.Second

// waitBlockingFallbackFlushesは、Flushの完了通知を合計count件受け取るまで待ちます。
func waitBlockingFallbackFlushes(t *testing.T, manager *blockingFallbackFlushManager, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-manager.flushed:
		case <-time.After(fallbackHangTimeout):
			t.Fatalf("fallback flush %d did not complete; flushes = %d", i+1, manager.flushCount())
		}
	}
}

// releaseFlushはblockした最初のflushを一度だけ解放します。試験の途中で失敗した場合も
// cleanupから呼び、fallbackのCloseが解放待ちのまま止まらないようにします。
func (m *blockingFallbackFlushManager) releaseFlush() {
	m.releaseOnce.Do(func() { close(m.release) })
}

func newBlockingFallbackFlushManager() *blockingFallbackFlushManager {
	return &blockingFallbackFlushManager{
		started: make(chan struct{}),
		release: make(chan struct{}),
		flushed: make(chan struct{}, 16),
	}
}

func (m *blockingFallbackFlushManager) Flush(_ context.Context, entries []*logging.LogEntry) error {
	m.mu.Lock()
	batch := append([]*logging.LogEntry(nil), entries...)
	m.batches = append(m.batches, batch)
	call := len(m.batches)
	m.mu.Unlock()
	if call == 1 {
		m.once.Do(func() { close(m.started) })
		<-m.release
	}
	select {
	case m.flushed <- struct{}{}:
	default:
	}
	return nil
}

func (m *blockingFallbackFlushManager) FlushWithNoFallback(
	ctx context.Context,
	entries []*logging.LogEntry,
) {
	_ = m.Flush(ctx, entries)
}

func (m *blockingFallbackFlushManager) Close(context.Context) error { return nil }

func (m *blockingFallbackFlushManager) flushCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.batches)
}

func (m *blockingFallbackFlushManager) batchMessages() [][]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]string, 0, len(m.batches))
	for _, batch := range m.batches {
		messages := make([]string, 0, len(batch))
		for _, entry := range batch {
			messages = append(messages, entry.Message)
		}
		out = append(out, messages)
	}
	return out
}

type failingFallbackFlushManager struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (m *failingFallbackFlushManager) Flush(context.Context, []*logging.LogEntry) error {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	return m.err
}

func (m *failingFallbackFlushManager) FlushWithNoFallback(
	ctx context.Context,
	entries []*logging.LogEntry,
) {
	_ = m.Flush(ctx, entries)
}

func (m *failingFallbackFlushManager) Close(context.Context) error { return nil }

func (m *failingFallbackFlushManager) flushCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

var _ FlushManager = (*blockingFallbackFlushManager)(nil)
var _ FlushManager = (*failingFallbackFlushManager)(nil)
