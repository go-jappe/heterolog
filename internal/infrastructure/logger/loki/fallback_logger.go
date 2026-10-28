package loki

import (
	"context"
	"fmt"
	"log"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

// FallbackLogger は内部エラーハンドラから呼ばれる Loki logger です。
// handlerは保持件数に上限のあるqueueへの追加だけを行い、通常Loggerと同じworker loop実装で
// 動く、このFallback instance専用のworkerが、件数、formatted-line byte数、またはintervalを契機にsnapshotを
// 一度だけ送ります。失敗したsnapshotは再投入しません。
type FallbackLogger struct {
	*LokiLogger

	// wakeChはFallback instance専用workerの件数・byte数・時間閾値通知です。LokiLogger.muで保護します。
	wakeCh chan struct{}
}

// startFallbackWorkerは共通worker loopへFallback専用のwake channelとflush callbackを渡し、
// このinstance専用workerを開始します。primaryの一件送信設定には依存しません。
func (f *FallbackLogger) startFallbackWorker() {
	if f == nil || f.LokiLogger == nil {
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

// enqueueProcessはfallback entryを保持件数に上限のあるqueueへ追加して直ちに戻ります。
// 送信中にqueueが満杯になった場合は古いentryを一件破棄し、件数、byte数、または時間閾値でwakeします。
func (f *FallbackLogger) enqueueProcess(entry *logging.LogEntry) {
	if f == nil || f.LokiLogger == nil {
		return
	}
	entryBytes, err := f.measureBufferedEntryBytes(entry)
	if err != nil {
		log.Printf("failed to enqueue Loki fallback log: %v", err)
		return
	}

	f.mu.Lock()
	if f.closeState != common.LoggerCloseStateOpen {
		f.mu.Unlock()
		return
	}
	queueMaxEntries := f.config.FallbackQueueMaxEntries()
	if queueMaxEntries <= 0 {
		f.mu.Unlock()
		log.Printf("failed to enqueue Loki fallback log: invalid queue capacity %d", queueMaxEntries)
		return
	}
	if len(f.entries) >= queueMaxEntries {
		evictedBytes := f.entryBytes[0]
		copy(f.entries, f.entries[1:])
		f.entries[len(f.entries)-1] = nil
		f.entries = f.entries[:len(f.entries)-1]
		copy(f.entryBytes, f.entryBytes[1:])
		f.entryBytes[len(f.entryBytes)-1] = 0
		f.entryBytes = f.entryBytes[:len(f.entryBytes)-1]
		f.bufferBytes -= evictedBytes
		if f.bufferBytes < 0 {
			// constructorとenqueueが維持する不変条件上は到達しませんが、
			// fallback経路で誤った負値を次のbatchへ引き継がないよう防御します。
			f.bufferBytes = 0
		}
		// overflow件数は将来metricsで観測します。
	}
	f.entries = append(f.entries, entry)
	f.entryBytes = append(f.entryBytes, entryBytes)
	f.bufferBytes += entryBytes
	shouldWake := len(f.entries) >= f.config.MaxEntries() ||
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
		log.Printf("failed to flush Loki fallback logger: %v", err)
	}
}

func (f *FallbackLogger) doFallbackFlush(ctx context.Context) error {
	return f.runBatchFlush(ctx, f.flushFallbackBatch)
}

// flushFallbackBatchは現在のfallback snapshotをFlushWithNoFallbackで一度だけ送信します。
// 送信失敗やFlushManager未初期化でもsnapshotをbufferへ戻しません。
func (f *FallbackLogger) flushFallbackBatch(ctx context.Context) error {
	batch, _ := f.detachBufferedBatch(
		f.config.FallbackQueueMaxEntries(),
	)
	if len(batch) == 0 {
		return nil
	}
	flushManager := f.flushManager
	// nonempty snapshotはFlushManager欠落を含めて今回の一度きりの試行で破棄します。
	// 試行時刻も必ず進め、以後の各enqueueが時間閾値で即wakeし続けることを防ぎます。
	defer f.updateLastFlushTime()
	if flushManager == nil {
		return fmt.Errorf("Loki flush manager is not initialized")
	}

	ctx, span := f.Tracer.StartSpan(ctx, SpanLokiLoggerFlush)
	defer span.End()

	flushManager.FlushWithNoFallback(ctx, batch)
	return nil
}

// Closeは受付とworkerを停止し、Close前に受理した残りを一度だけ送信してから
// FlushManagerを閉じます。送信失敗と終了済みの結果errorは標準loggerへ記録します。
// managerの終了が未完了ならerrorを返し、再呼び出しでは送信せず終了処理だけを再試行します。
func (f *FallbackLogger) Close(ctx context.Context) error {
	if f == nil || f.LokiLogger == nil {
		return nil
	}

	f.closeMu.Lock()
	defer f.closeMu.Unlock()

	if !f.beginClose() {
		return nil
	}

	f.stopBatchFlushWorker()
	f.lockBatchFlushForClose()
	defer f.unlockBatchFlushForClose()

	if err := f.flushFallbackBatch(ctx); err != nil {
		log.Printf("failed to flush Loki fallback logger during Close: %v", err)
	}
	if err := f.closeFlushManager(ctx); err != nil {
		log.Printf("failed to close Loki fallback flush manager: %v", err)
		if !common.ShutdownComplete(f.flushManager) {
			return err
		}
	}

	f.completeClose()
	return nil
}
