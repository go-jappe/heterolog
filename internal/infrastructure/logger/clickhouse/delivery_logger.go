package clickhouse

import (
	"context"
	"fmt"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

type DeliveryClickHouseLogger struct {
	*ClickHouseLogger
	// wakeChは件数・byte数・時間閾値到達を、このDelivery instance専用workerへ通知します。
	wakeCh chan struct{}
}

var _ logging.DeliveryBackendLogEntryHandler = (*DeliveryClickHouseLogger)(nil)

// DeliveryClickHouseLogger returns after accepting an entry into the local queue.
// It does not report ClickHouse persistence from HandleLogEntry.
// ClickHouse recommends batched inserts, so actual backend delivery happens later
// through the flush timer or Close; DeliveryEnqueued must not be treated as
// "saved to ClickHouse", "delivered", or backend delivery success.
func NewDeliveryClickHouseLogger(
	ctx context.Context,
	cfg ClickHouseConfigProvider,
	client Client,
	fmCfg DeliveryClickHouseFlushManagerConfigProvider,
	resourceProvider resource.ResourceProvider,
	failureHandler *flushfailure.ReportingCompositeFailureHandler,
	tracer observer.Tracer,
	meter observer.Meter,
	loggerBase *LoggerBase,
	opts ...ClickHouseLoggerOption,
) (*DeliveryClickHouseLogger, error) {
	var baseFailureHandler *flushfailure.CompositeFailureHandler
	if failureHandler != nil {
		baseFailureHandler = failureHandler.CompositeFailureHandler
	}
	base, err := newClickHouseLogger(
		ctx,
		cfg,
		client,
		fmCfg,
		resourceProvider,
		baseFailureHandler,
		tracer,
		meter,
		loggerBase,
		opts...,
	)
	if err != nil {
		return nil, err
	}
	logger := &DeliveryClickHouseLogger{
		ClickHouseLogger: base,
		wakeCh:           make(chan struct{}, 1),
	}
	logger.startDeliveryBatchFlushWorker(ctx)
	return logger, nil
}

// startDeliveryBatchFlushWorkerはDelivery固有のcoalesced wake channelを共通worker loopへ
// 渡します。HandleLogEntryはenqueue結果だけを返し、backend送信完了を待ちません。
func (l *DeliveryClickHouseLogger) startDeliveryBatchFlushWorker(ctx context.Context) {
	if l == nil || l.ClickHouseLogger == nil {
		return
	}
	l.startBatchFlushWorker(ctx, l.wakeCh, l.flush)
}

// HandleLogEntryは、infraのtop-level delivery loggerが生成したLogEntryへ
// 未設定のOccurredAtをClockから補完し、ClickHouse固有のstack trace設定を適用して、
// queueへの受理結果を返します。
func (l *DeliveryClickHouseLogger) HandleLogEntry(
	ctx context.Context,
	entry *logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	if closedErr := l.loggerClosedError(); closedErr != nil {
		return pkglogger.NewTargetDeliveryReport(clickHouseDeliveryRejectedResult(closedErr)), closedErr
	}
	if entry == nil {
		err := fmt.Errorf("clickhouse delivery logger entry is nil")
		return pkglogger.NewTargetDeliveryReport(clickHouseDeliveryRejectedResult(err)), err
	}
	logging.EnsureEntryOccurredAt(entry, l.Clock)
	if entry.StackTraceRequested {
		// ErrorWithStackにerrorが渡されなかった場合も拒否せず、要求されたstackを持つ
		// ERRORとして受理します。rowのerrorは空のままです。
		if len(entry.FuncName) == 0 && len(entry.File) == 0 && len(entry.Line) == 0 {
			depth := logging.GetCallerDepth(NewStackTraceConfigAdapter(l.config))
			logging.EnsureEntryStackTrace(entry, depth)
		}
	}
	return l.enqueueWithReport(ctx, entry)
}

func (l *DeliveryClickHouseLogger) Close() error {
	return l.ClickHouseLogger.Close(context.Background())
}

func (l *DeliveryClickHouseLogger) enqueueWithReport(
	ctx context.Context,
	entry *logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	entryBytes, forceFlush := l.measureEntryForBuffer(entry)

	l.mu.Lock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		l.mu.Unlock()
		err := applogging.ErrLoggerClosed
		return pkglogger.NewTargetDeliveryReport(clickHouseDeliveryRejectedResult(err)), err
	}

	flushManager := l.flushManager
	if flushManager == nil {
		l.mu.Unlock()
		// entryをqueueへ受理せずbackendへの送信も試みないため、backendの明示的な拒否を表す
		// Failedではなく、送信前の拒否としてRejectedを返します。
		err := fmt.Errorf("ClickHouse flush manager is not initialized")
		l.notifyError(
			ctx,
			l.wrapErrorByLogger(err, "clickhouse delivery enqueue"),
			l.InternalErrorEntry("failed to enqueue by ClickHouse delivery logger; flush manager is not initialized; error:", "", err),
			l.Tracer,
			l.Meter,
		)
		return pkglogger.NewTargetDeliveryReport(clickHouseDeliveryRejectedResult(err)), err
	}

	l.appendBufferedEntryLocked(entry, entryBytes, forceFlush)
	shouldFlush := l.shouldFlushLocked()
	wakeCh := l.wakeCh
	l.mu.Unlock()

	if shouldFlush && wakeCh != nil {
		select {
		case wakeCh <- struct{}{}:
		default:
		}
	}

	return pkglogger.NewTargetDeliveryReport(clickHouseDeliveryEnqueuedResult()), nil
}
