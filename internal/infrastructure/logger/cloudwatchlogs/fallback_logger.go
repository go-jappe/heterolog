package cloudwatchlogs

import (
	"context"
	"fmt"
	"log"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

// FallbackLogger は他 logger の handler から呼ばれる CloudWatch Logs fallback logger です。
// handlerは保持件数に上限のあるqueueへの追加だけを行い、このinstance専用workerが
// 件数・byte数・intervalのいずれかを契機にsnapshotを一度だけ送ります。失敗したsnapshotは
// failure handlerへ渡さず、queueへも再投入しません。
type FallbackLogger struct {
	*CloudWatchLogsLogger

	// wakeChは件数・時間閾値到達を、このFallback instance専用workerへ通知します。
	// 初期化と参照はCloudWatchLogsLogger.muで保護します。
	wakeCh chan struct{}
}

// startFallbackWorkerはfallback固有のwake channelとflush callbackを、通常Loggerと共通の
// worker loopへ渡します。request contextはenqueue後に終了し得るため、owned workerは
// loggerのlifecycle contextでflushします。
func (f *FallbackLogger) startFallbackWorker() {
	if f == nil || f.CloudWatchLogsLogger == nil {
		return
	}

	f.mu.Lock()
	if f.flushWorkerStarted {
		f.mu.Unlock()
		return
	}
	if f.wakeCh == nil {
		f.wakeCh = make(chan struct{}, 1)
	}
	wakeCh := f.wakeCh
	f.mu.Unlock()

	workerCtx := context.Background()
	f.startBatchFlushWorker(wakeCh, func() {
		f.flushFallback(workerCtx)
	})
}

// enqueueProcessはfallback entryを保持件数に上限のあるqueueへ追加して直ちに戻ります。
// queueが満杯の場合は最古entryを除去し、件数または時間閾値に達した場合はworkerをwakeします。
// BufferFlushMaxEvents/BufferFlushBytesはwake条件、FallbackQueueMaxEventsは
// 保持のhard capとして独立して扱います。
func (f *FallbackLogger) enqueueProcess(entry *logging.LogEntry) {
	if f == nil || f.CloudWatchLogsLogger == nil {
		return
	}

	f.mu.Lock()
	if f.closeState != common.LoggerCloseStateOpen {
		f.mu.Unlock()
		return
	}
	maxEntries := f.config.FallbackQueueMaxEvents()
	if maxEntries <= 0 {
		f.mu.Unlock()
		log.Printf("failed to enqueue CloudWatch Logs fallback log: invalid queue capacity %d", maxEntries)
		return
	}
	if len(f.entries) > 0 && f.bufferBytes == 0 {
		f.bufferBytes = cloudWatchLogsBufferedEntriesBytes(f.entries, f.LogFormatter)
	}
	if len(f.entries) >= maxEntries {
		evictedBytes := cloudWatchLogsBufferedEntryBytes(f.entries[0], f.LogFormatter)
		copy(f.entries, f.entries[1:])
		f.entries[len(f.entries)-1] = nil
		f.entries = f.entries[:len(f.entries)-1]
		f.bufferBytes -= evictedBytes
		if f.bufferBytes < 0 {
			f.bufferBytes = cloudWatchLogsBufferedEntriesBytes(f.entries, f.LogFormatter)
		}
		// overflow件数は将来metricsで観測します。
	}
	f.entries = append(f.entries, entry)
	f.bufferBytes += cloudWatchLogsBufferedEntryBytes(entry, f.LogFormatter)
	shouldWake := len(f.entries) >= f.config.BufferFlushMaxEvents() ||
		f.bufferBytes >= f.config.BufferFlushBytes() ||
		f.flushIntervalElapsedLocked()
	wakeCh := f.wakeCh
	f.mu.Unlock()

	if !shouldWake || wakeCh == nil {
		return
	}
	select {
	case wakeCh <- struct{}{}:
	default:
	}
}

func (f *FallbackLogger) flushFallback(ctx context.Context) {
	if err := f.doFallbackFlush(ctx); err != nil {
		log.Printf("failed to flush CloudWatch Logs fallback logger: %v", err)
	}
}

// doFallbackFlushはworkerからのfallback flushを直列化し、Close開始後の新規flushを止めます。
func (f *FallbackLogger) doFallbackFlush(ctx context.Context) error {
	f.flushMu.Lock()
	defer f.flushMu.Unlock()

	f.mu.Lock()
	closeStarted := f.closeState != common.LoggerCloseStateOpen
	f.mu.Unlock()
	if closeStarted {
		return nil
	}

	return f.flushFallbackBatch(ctx)
}

// flushFallbackBatchは現在のsnapshotを一度だけno-fallback経路へ渡します。
// 送信結果にかかわらずsnapshotを再投入せず、OnErrorやfailure handlerも呼びません。
func (f *FallbackLogger) flushFallbackBatch(ctx context.Context) error {
	snapshot := f.detachBufferedBatch(f.config.FallbackQueueMaxEvents())
	if len(snapshot) == 0 {
		return nil
	}
	flushManager := f.flushManager
	if flushManager == nil {
		// manager欠落もfallbackの一回の試行として扱い、snapshotは復元しません。
		f.updateLastFlushTime()
		return fmt.Errorf("CloudWatch Logs flush manager is not initialized")
	}

	ctx, span := f.Tracer.StartSpan(ctx, SpanCloudWatchLogsLoggerFlush)
	defer span.End()

	flushManager.FlushWithNoFallback(ctx, snapshot)
	// no-fallback送信は結果を返さないため、送信成否にかかわらず今回の試行時刻を記録します。
	f.updateLastFlushTime()
	return nil
}

// Closeはintakeを停止し、in-flight workerとClose前に受理した残りのbatchを待ちます。
// 送信失敗と終了済みの結果errorは標準loggerへ記録し、batchは再投入しません。
// managerの終了が未完了ならerrorを返し、再呼び出しで終了処理だけを再試行します。
func (f *FallbackLogger) Close(ctx context.Context) error {
	if f == nil || f.CloudWatchLogsLogger == nil {
		return nil
	}

	f.closeMu.Lock()
	defer f.closeMu.Unlock()

	if !f.beginClose() {
		return nil
	}
	f.stopBatchFlushWorker()

	// closeStateをStartedにした後は新しい処理を受け付けないため、受付済みの処理の
	// 完了だけを待ちます。
	f.inFlight.Wait()

	f.flushMu.Lock()
	defer f.flushMu.Unlock()

	if err := f.flushFallbackBatch(ctx); err != nil {
		log.Printf("failed to flush CloudWatch Logs fallback logger during Close: %v", err)
	}
	if err := f.closeFlushManager(ctx); err != nil {
		log.Printf("failed to close CloudWatch Logs fallback flush manager: %v", err)
		if !common.ShutdownComplete(f.flushManager) {
			return err
		}
	}

	f.completeClose()
	return nil
}
