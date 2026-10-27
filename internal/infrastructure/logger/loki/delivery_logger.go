package loki

import (
	"context"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type DeliveryLokiLogger struct {
	*LokiLogger
}

var _ logging.DeliveryBackendLogEntryHandler = (*DeliveryLokiLogger)(nil)

func NewDeliveryLokiLogger(
	ctx context.Context,
	cfg LokiConfigProvider,
	client Client,
	fmCfg DeliveryLokiFlushManagerConfigProvider,
	resourceProvider resource.ResourceProvider,
	failureHandler *flushfailure.ReportingCompositeFailureHandler,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	loggerBase *LoggerBase,
	opts ...LokiLoggerOption,
) (*DeliveryLokiLogger, error) {
	var baseFailureHandler *flushfailure.CompositeFailureHandler
	if failureHandler != nil {
		baseFailureHandler = failureHandler.CompositeFailureHandler
	}
	base, err := newLokiLogger(
		ctx,
		cfg,
		client,
		fmCfg,
		resourceProvider,
		baseFailureHandler,
		failureHandler,
		tracer,
		meter,
		loggerBase,
		opts...,
	)
	if err != nil {
		return nil, err
	}
	return &DeliveryLokiLogger{LokiLogger: base}, nil
}

// HandleLogEntryはtop-level loggerで生成済みのentryをLoki Delivery用に加工し、
// 一件Pushの送達結果を返します。発生時刻・ID・Fieldsは作り直さず、LogFormatterと
// 要求されたstack traceだけをこの保存先の設定から適用します。
func (l *DeliveryLokiLogger) HandleLogEntry(
	ctx context.Context,
	entry *logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	if err := l.loggerClosedError(); err != nil {
		return pkglogger.NewTargetDeliveryReport(lokiDeliveryRejectedResult(err)), err
	}
	if entry == nil {
		err := fmt.Errorf("Loki delivery log entry is nil")
		return pkglogger.NewTargetDeliveryReport(lokiDeliveryRejectedResult(err)), err
	}

	logging.EnsureEntryOccurredAt(entry, l.Clock)
	if entry.StackTraceRequested {
		// ErrorWithStackにerrorが渡されなかった場合も拒否せず、要求されたstackを持つ
		// ERRORとして記録します。Formatterはerrorの欄を出力しません。
		if len(entry.FuncName) == 0 && len(entry.File) == 0 && len(entry.Line) == 0 {
			depth := logging.GetCallerDepth(NewStackTraceConfigAdapter(l.config))
			logging.EnsureEntryStackTrace(entry, depth)
		}
	}
	return l.pushImmediatelyWithReport(ctx, entry)
}

func (l *DeliveryLokiLogger) Close() error {
	return l.LokiLogger.Close(context.Background())
}

func (l *DeliveryLokiLogger) pushImmediatelyWithReport(
	ctx context.Context,
	entry *logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	// Close開始後の呼び出しは拒否し、受付済みの送信だけをCloseの待機対象にします。
	l.mu.Lock()
	if l.closeState != common.LoggerCloseStateOpen {
		l.mu.Unlock()
		err := pkglogger.ErrLoggerClosed
		return pkglogger.NewTargetDeliveryReport(lokiDeliveryRejectedResult(err)), err
	}
	flushManager := l.flushManager
	l.inFlight.Add(1)
	l.mu.Unlock()
	defer l.inFlight.Done()

	if flushManager == nil {
		// backendへの送信を試みないため、backendの明示的な拒否を表すFailedではなく、
		// 送信前の拒否としてRejectedを返します。
		err := fmt.Errorf("Loki flush manager is not initialized")
		l.notifyError(
			ctx,
			l.wrapErrorByLogger(err, "loki delivery push"),
			l.InternalErrorEntry("failed to push by Loki delivery logger; flush manager is not initialized; error:", "", err),
			l.Tracer,
			l.Meter,
		)
		return pkglogger.NewTargetDeliveryReport(lokiDeliveryRejectedResult(err)), err
	}

	return flushManager.PushWithReport(ctx, entry)
}
