package opensearch

import (
	"context"
	"errors"
	"fmt"
	"log"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// FallbackLogger は内部エラーハンドラから呼ばれる OpenSearch logger です。
// handlerは保持件数に上限のあるqueueへの追加だけを行い、通常Loggerと同じworker loop実装で
// 動く、このFallback instance専用のworkerが、件数またはintervalを契機に
// snapshotをBulk APIへ一度だけ送ります。
// 失敗したsnapshotは再投入しません。
type FallbackLogger struct {
	*OpenSearchLogger
	// wakeChは件数または時間の閾値到達を、このFallback instance専用の
	// workerへ通知します。OpenSearchLogger.muで保護します。
	wakeCh chan struct{}
}

// startFallbackWorkerは共通のworker loopへFallback専用のwake channelと
// no-fallback flushを渡し、このFallback instance専用のworkerを開始します。
// handlerはwake通知までで戻り、workerは一度の送信完了を待って次のbatchへ進みます。
func (f *FallbackLogger) startFallbackWorker() {
	if f == nil || f.OpenSearchLogger == nil {
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

	f.startBatchFlushWorker(context.Background(), wakeCh, f.flushFallback)
}

// enqueueProcessはFallback専用の有界queueへentryを追加し、件数または時間の
// 閾値に達した場合はworkerへ通知して直ちに戻ります。
func (f *FallbackLogger) enqueueProcess(entry *logging.LogEntry) {
	if f == nil || f.OpenSearchLogger == nil {
		return
	}

	shouldWake, err := f.enqueueFallbackEntry(entry)
	if err != nil {
		if !errors.Is(err, pkglogger.ErrLoggerClosed) {
			log.Printf("failed to enqueue OpenSearch fallback log: %v", err)
		}
		return
	}
	if !shouldWake {
		return
	}

	f.mu.Lock()
	wakeCh := f.wakeCh
	f.mu.Unlock()
	select {
	case wakeCh <- struct{}{}:
	default:
	}
}

// enqueueFallbackEntryは呼び出し元へbackpressureを掛けない有界queueへ
// entryを受け入れます。満杯時は最古entryを除去し、新しいentryを残します。
func (f *FallbackLogger) enqueueFallbackEntry(entry *logging.LogEntry) (bool, error) {
	entryBytes, err := f.measureBufferedEntryBytes(entry)
	if err != nil {
		return false, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closeState != loggercommon.LoggerCloseStateOpen {
		return false, pkglogger.ErrLoggerClosed
	}
	maxEntries := f.config.MaxBulkDocument()
	if maxEntries <= 0 {
		return false, fmt.Errorf("OpenSearch fallback queue capacity must be greater than zero: %d", maxEntries)
	}
	if len(f.entries) >= maxEntries {
		evictedBytes, measureErr := f.measureBufferedEntryBytes(f.entries[0])
		if measureErr != nil {
			return false, fmt.Errorf("measure evicted OpenSearch fallback entry: %w", measureErr)
		}
		copy(f.entries, f.entries[1:])
		f.entries[len(f.entries)-1] = nil
		f.entries = f.entries[:len(f.entries)-1]
		f.bufferBytes -= evictedBytes
		if f.bufferBytes < 0 {
			f.bufferBytes = 0
		}
		// overflow件数は将来metricsで観測します。
	}

	f.entries = append(f.entries, entry)
	f.bufferBytes += entryBytes
	return f.shouldFlushLocked(), nil
}

// flushFallbackはworkerからFallback batchを一度だけ送信し、初期化不備を
// Go標準loggerへ記録します。送信したsnapshotはqueueへ戻しません。
func (f *FallbackLogger) flushFallback(ctx context.Context) {
	if err := f.doFallbackFlush(ctx); err != nil {
		log.Printf("failed to flush OpenSearch fallback logger: %v", err)
	}
}

func (f *FallbackLogger) doFallbackFlush(ctx context.Context) error {
	f.flushMu.Lock()
	defer f.flushMu.Unlock()

	f.mu.Lock()
	closeStarted := f.closeState != loggercommon.LoggerCloseStateOpen
	f.mu.Unlock()
	if closeStarted {
		return nil
	}

	return f.flushFallbackBuffered(ctx)
}

// flushFallbackBufferedは現在のsnapshotをfailure handlerの再帰を起こさない
// FlushWithNoFallbackへ一度だけ渡します。送信結果は呼び出し元のlog処理へ
// 返さず、snapshotもqueueへ戻しません。
func (f *FallbackLogger) flushFallbackBuffered(ctx context.Context) error {
	batch := f.detachBufferedBatch()
	if len(batch) == 0 {
		return nil
	}
	flushManager := f.flushManager

	// Fallbackは送信結果にかかわらずsnapshotを破棄するため、今回の試行時刻を記録します。
	defer f.updateLastFlushTime()
	if flushManager == nil {
		return fmt.Errorf("OpenSearch fallback flush manager is not initialized")
	}

	ctx, span := f.Tracer.StartSpan(ctx, SpanOpenSearchLoggerFlush)
	defer span.End()
	flushManager.FlushWithNoFallback(ctx, batch)
	return nil
}

// CloseはFallbackの新規受付とworkerを停止し、受理済みbatchを一度だけ
// no-fallback送信してFlushManagerを閉じます。送信失敗と終了済みの結果errorは
// Go標準loggerへ記録します。managerの終了が未完了ならerrorを返し、
// 再呼び出しでは送信せず終了処理だけを再試行します。
func (f *FallbackLogger) Close(ctx context.Context) error {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()

	if !f.beginClose() {
		return nil
	}
	flushErr, closeErr := f.closeBufferedRuntime(ctx, f.flushFallbackBuffered)
	if err := errors.Join(flushErr, closeErr); err != nil {
		log.Printf("failed to close OpenSearch fallback logger: %v", err)
	}
	if closeErr != nil && !loggercommon.ShutdownComplete(f.flushManager) {
		return closeErr
	}
	f.completeClose()
	return nil
}
