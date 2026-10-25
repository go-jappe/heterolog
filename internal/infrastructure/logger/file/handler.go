package file

import (
	"context"
	"fmt"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/duration"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"time"
)

// compressDelayNotificationは、圧縮処理中に通知内容を確定し、通知先を実行する関数を返します。
// 呼び出し元は通知の実行を処理完了後まで保留できます。
// rotation処理では終了待ちを解除してから通知しますが、時刻やメッセージは発生時の値を保ちます。
func (l *FileLogger) compressDelayNotification(
	ctx context.Context,
	elapsed time.Duration,
	meta any,
) func() {
	durationConfig := l.config.Fsstore().Duration()
	errorThreshold := durationConfig.CompressErrorThresholdDuration()
	warnThreshold := durationConfig.CompressWarnThresholdDuration()
	level := duration.LogLevelForElapsed(elapsed, errorThreshold, warnThreshold)
	if level == pkglogger.LogLevelDebug && !durationConfig.WriteFileLatencyEnabled() {
		return func() {}
	}

	filePath, ok := meta.(string)
	if !ok {
		filePath = "(unknown filepath)"
	}
	var err error
	if level == pkglogger.LogLevelError {
		err = l.wrapErrorByLogger(nil, OperationCompressDelay)
	}

	switch level {
	case pkglogger.LogLevelError:
		message := fmt.Sprintf(ErrorMsgFileCompressDelayed, elapsed, errorThreshold, filePath)
		entry := l.InternalErrorEntryWithContext(ctx, message, err)
		return func() { l.wrapAndDispatchError(ctx, err, entry) }
	case pkglogger.LogLevelWarn:
		message := fmt.Sprintf(MsgFileCompressDelayed, elapsed, warnThreshold, filePath)
		entry := l.InternalWarnEntryWithContext(ctx, message)
		return func() { l.dispatchWarn(ctx, entry) }
	default:
		message := fmt.Sprintf(MsgFileCompressNoDelayed, elapsed, filePath)
		entry := l.InternalDebugEntryWithContext(ctx, message)
		return func() { l.dispatchDebug(ctx, entry) }
	}
}

// ファイルの書き込みにかかった時間をログ。書き込みの同期的なinternal logなので、
// このFileLoggerの通知の中から再入した書き込みの遅延は通知せず同じfileへ直接記録します。
func (l *FileLogger) handleWriteLogDelay(ctx context.Context, elapsed time.Duration, meta any) {
	durationConfig := l.config.Fsstore().Duration()
	errorThreshold := durationConfig.WriteFileErrorThresholdDuration()
	warnThreshold := durationConfig.WriteFileWarnThresholdDuration()
	level := duration.LogLevelForElapsed(elapsed, errorThreshold, warnThreshold)
	if level == pkglogger.LogLevelDebug && !durationConfig.WriteFileLatencyEnabled() {
		return
	}

	requestId, ok := meta.(string)
	if !ok {
		requestId = "(unknown filepath)"
	}
	var err error
	if level == pkglogger.LogLevelError {
		err = l.wrapErrorByLogger(nil, OperationWriteDelay)
	}

	switch level {
	case pkglogger.LogLevelError:
		message := fmt.Sprintf(ErrorMsgWriteLogDelayed, elapsed, errorThreshold, requestId)
		l.wrapAndReportWriteError(ctx, err, l.InternalErrorEntryWithContext(ctx, message, err))
	case pkglogger.LogLevelWarn:
		message := fmt.Sprintf(MsgWriteLogDelayed, elapsed, warnThreshold, requestId)
		entry := l.InternalWarnEntryWithContext(ctx, message)
		l.reportWriteInternalLog(ctx, entry, l.OnWarn != nil, func() { l.dispatchWarn(ctx, entry) })
	default:
		message := fmt.Sprintf(MsgWriteLogNoDelayed, elapsed, requestId)
		entry := l.InternalDebugEntryWithContext(ctx, message)
		l.reportWriteInternalLog(ctx, entry, l.OnDebug != nil, func() { l.dispatchDebug(ctx, entry) })
	}
}
