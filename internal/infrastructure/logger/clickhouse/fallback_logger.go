package clickhouse

import (
	"context"
	"fmt"
	"log"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

// FallbackLoggerはinternal log handlerから呼ばれるClickHouse loggerです。
// handlerは有界queueへの追加だけを行い、このinstanceが所有する単一workerが件数、
// JSONEachRow byte数、またはintervalを契機にsnapshotを一度だけ送信します。
// failure handlerは持たず、失敗したsnapshotはqueueへ戻しません。
type FallbackLogger struct {
	*ClickHouseLogger

	// wakeChは件数・byte数・時間閾値到達を、このFallback instance専用workerへ通知します。
	// 初期化と参照はClickHouseLogger.muで保護します。
	wakeCh chan struct{}
}

// startFallbackWorkerはFallback固有のwake channelとflush callbackを共通worker loopへ
// 渡し、このinstance専用workerを開始します。
func (f *FallbackLogger) startFallbackWorker() {
	if f == nil || f.ClickHouseLogger == nil {
		return
	}

	f.mu.Lock()
	if f.wakeCh == nil {
		f.wakeCh = make(chan struct{}, 1)
	}
	wakeCh := f.wakeCh
	f.mu.Unlock()

	f.startBatchFlushWorker(context.Background(), wakeCh, f.flushFallback)
}

// enqueueProcessはfallback entryを有界queueへ追加して直ちに戻ります。
// queueが満杯の場合は最古entryを一件除去し、byte accountingも同じentry分だけ減らします。
func (f *FallbackLogger) enqueueProcess(_ context.Context, entry *logging.LogEntry) {
	if f == nil || f.ClickHouseLogger == nil {
		return
	}
	entryBytes, forceFlush := f.measureEntryForBuffer(entry)

	f.mu.Lock()
	if f.closeState != loggercommon.LoggerCloseStateOpen {
		f.mu.Unlock()
		return
	}
	queueMaxEntries := f.config.FallbackQueueMaxEntries()
	if queueMaxEntries <= 0 {
		f.mu.Unlock()
		log.Printf("failed to enqueue ClickHouse fallback log: invalid queue capacity %d", queueMaxEntries)
		return
	}
	if len(f.entries) >= queueMaxEntries {
		f.dropOldestFallbackEntryLocked()
		// overflow件数は将来metricsで観測します。
	}
	f.appendBufferedEntryLocked(entry, entryBytes, forceFlush)
	shouldWake := f.shouldFlushLocked()
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

// dropOldestFallbackEntryLockedはentriesと測定metadataの先頭要素を同時に除去します。
// 呼び出し元はClickHouseLogger.muを保持します。
func (f *FallbackLogger) dropOldestFallbackEntryLocked() {
	if len(f.entries) == 0 {
		return
	}
	if len(f.entryBytes) != len(f.entries) || len(f.entryRequiresFlush) != len(f.entries) {
		f.rebuildFallbackBufferMeasurementsLocked()
	}
	evictedBytes := f.entryBytes[0]
	copy(f.entryBytes, f.entryBytes[1:])
	f.entryBytes[len(f.entryBytes)-1] = 0
	f.entryBytes = f.entryBytes[:len(f.entryBytes)-1]
	copy(f.entries, f.entries[1:])
	f.entries[len(f.entries)-1] = nil
	f.entries = f.entries[:len(f.entries)-1]
	f.bufferedBytes -= evictedBytes
	if f.bufferedBytes < 0 {
		f.bufferedBytes = 0
	}

	copy(f.entryRequiresFlush, f.entryRequiresFlush[1:])
	f.entryRequiresFlush[len(f.entryRequiresFlush)-1] = false
	f.entryRequiresFlush = f.entryRequiresFlush[:len(f.entryRequiresFlush)-1]
	f.bufferRequiresFlush = false
	for _, required := range f.entryRequiresFlush {
		f.bufferRequiresFlush = f.bufferRequiresFlush || required
	}
}

// rebuildFallbackBufferMeasurementsLockedは試験用の直接buffer組立てなどでmetadataが
// 欠けていても、drop-oldest前に実送信と同じ測定値へ復元します。
func (f *FallbackLogger) rebuildFallbackBufferMeasurementsLocked() {
	f.entryBytes = make([]int, 0, len(f.entries))
	f.entryRequiresFlush = make([]bool, 0, len(f.entries))
	f.bufferedBytes = 0
	f.bufferRequiresFlush = false
	for _, entry := range f.entries {
		entryBytes, forceFlush := f.measureEntryForBuffer(entry)
		f.entryBytes = append(f.entryBytes, entryBytes)
		f.entryRequiresFlush = append(f.entryRequiresFlush, forceFlush)
		f.bufferedBytes += entryBytes
		f.bufferRequiresFlush = f.bufferRequiresFlush || forceFlush
	}
}
func (f *FallbackLogger) flushFallback(ctx context.Context) {
	if err := f.doFallbackFlush(ctx); err != nil {
		log.Printf("failed to flush ClickHouse fallback logger: %v", err)
	}
}

// doFallbackFlushはworkerからのfallback flushを直列化し、Close開始後の新規flushを止めます。
func (f *FallbackLogger) doFallbackFlush(ctx context.Context) error {
	f.flushMu.Lock()
	defer f.flushMu.Unlock()

	f.mu.Lock()
	closeStarted := f.closeState != loggercommon.LoggerCloseStateOpen
	f.mu.Unlock()
	if closeStarted {
		return nil
	}
	return f.flushFallbackBatch(ctx)
}

// flushFallbackBatchは現在のsnapshotを一度だけ送信します。送信失敗やFlushManager欠落でも
// snapshotをbufferへ戻さず、fallback failureを再帰的なfailure handlerへ渡しません。
func (f *FallbackLogger) flushFallbackBatch(ctx context.Context) error {
	f.mu.Lock()
	if len(f.entries) == 0 {
		f.mu.Unlock()
		return nil
	}
	batch := f.entries
	f.entries = make([]*logging.LogEntry, 0, f.config.FallbackQueueMaxEntries())
	f.entryBytes = make([]int, 0, f.config.FallbackQueueMaxEntries())
	f.entryRequiresFlush = make([]bool, 0, f.config.FallbackQueueMaxEntries())
	f.bufferedBytes = 0
	f.bufferRequiresFlush = false
	flushManager := f.flushManager
	f.mu.Unlock()

	// nonempty snapshotは送信結果にかかわらず今回の一度きりの試行で終了します。
	defer f.updateLastFlushTime()
	if flushManager == nil {
		return fmt.Errorf("ClickHouse flush manager is not initialized")
	}

	ctx, span := f.Tracer.StartSpan(ctx, SpanClickHouseLoggerFlush)
	defer span.End()
	flushManager.FlushWithNoFallback(ctx, batch)
	return nil
}

// Closeは受付とworkerを停止し、Close前に受理した残りを一度だけ送信してから
// FlushManagerを閉じます。送信失敗と終了済みの結果errorは標準loggerへ記録します。
// managerの終了が未完了ならerrorを返し、再呼び出しでは送信せず終了処理だけを再試行します。
func (f *FallbackLogger) Close(ctx context.Context) error {
	if f == nil || f.ClickHouseLogger == nil {
		return nil
	}

	f.closeMu.Lock()
	defer f.closeMu.Unlock()

	f.mu.Lock()
	if f.closeState == loggercommon.LoggerCloseStateCompleted {
		f.mu.Unlock()
		return nil
	}
	f.closeState = loggercommon.LoggerCloseStateStarted
	f.mu.Unlock()

	f.stopBatchFlushWorker()

	f.flushMu.Lock()
	defer f.flushMu.Unlock()
	if err := f.flushFallbackBatch(ctx); err != nil {
		log.Printf("failed to flush ClickHouse fallback logger during Close: %v", err)
	}

	f.mu.Lock()
	flushManager := f.flushManager
	f.mu.Unlock()
	if flushManager != nil {
		if err := flushManager.Close(ctx); err != nil {
			log.Printf("failed to close ClickHouse fallback flush manager: %v", err)
			if !loggercommon.ShutdownComplete(flushManager) {
				return err
			}
		}
	}

	f.mu.Lock()
	f.closeState = loggercommon.LoggerCloseStateCompleted
	f.mu.Unlock()
	return nil
}
