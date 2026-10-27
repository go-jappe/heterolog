package cloudwatchlogs

import (
	"context"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// DeliveryCloudWatchLogsLogger は delivery pipeline 向けに DeliveryReport を返す CloudWatch Logs logger です。
type DeliveryCloudWatchLogsLogger struct {
	*CloudWatchLogsLogger
}

var _ logging.DeliveryBackendLogEntryHandler = (*DeliveryCloudWatchLogsLogger)(nil)

// NewDeliveryCloudWatchLogsLogger は delivery logger 用の wrapper を生成します。
func NewDeliveryCloudWatchLogsLogger(
	ctx context.Context,
	cfg CloudWatchLogsConfigProvider,
	client pkgcloudwatchlogs.Client,
	fmCfg DeliveryCloudWatchLogsFlushManagerConfigProvider,
	resourceProvider resource.ResourceProvider,
	failureHandler *flushfailure.ReportingCompositeFailureHandler,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	loggerBase *LoggerBase,
	opts ...CloudWatchLogsLoggerOption,
) (*DeliveryCloudWatchLogsLogger, error) {
	var baseFailureHandler *flushfailure.CompositeFailureHandler
	if failureHandler != nil {
		baseFailureHandler = failureHandler.CompositeFailureHandler
	}
	base, err := newCloudWatchLogsLogger(
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
	return &DeliveryCloudWatchLogsLogger{CloudWatchLogsLogger: base}, nil
}

// HandleLogEntryは、infraのtop-level delivery loggerが生成したLogEntryへ
// 未設定のOccurredAtをClockから補完し、CloudWatch Logs固有のstack trace設定を適用して、
// 一件送信の結果を返します。
func (l *DeliveryCloudWatchLogsLogger) HandleLogEntry(
	ctx context.Context,
	entry *logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	if closedErr := l.loggerClosedError(); closedErr != nil {
		return pkglogger.NewTargetDeliveryReport(cloudWatchLogsDeliveryRejectedResult(closedErr)), closedErr
	}
	if entry == nil {
		err := fmt.Errorf("cloudwatch logs delivery logger entry is nil")
		return pkglogger.NewTargetDeliveryReport(cloudWatchLogsDeliveryRejectedResult(err)), err
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
	return l.putImmediatelyWithReport(ctx, entry)
}

// Close は delivery logger を background context で閉じます。
func (l *DeliveryCloudWatchLogsLogger) Close() error {
	return l.CloudWatchLogsLogger.Close(context.Background())
}

// putImmediatelyWithReport はdelivery logger用に1件送信し、targetとdurableの結果を返します。
// Closeの待機対象として受付を数えるだけなので、複数のdelivery送信は互いに直列化されません。
func (l *DeliveryCloudWatchLogsLogger) putImmediatelyWithReport(
	ctx context.Context,
	entry *logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	l.mu.Lock()
	if l.closeState != common.LoggerCloseStateOpen {
		l.mu.Unlock()
		err := pkglogger.ErrLoggerClosed
		return pkglogger.NewTargetDeliveryReport(cloudWatchLogsDeliveryRejectedResult(err)), err
	}
	flushManager := l.flushManager
	l.inFlight.Add(1)
	l.mu.Unlock()
	defer l.inFlight.Done()

	if flushManager == nil {
		// backendへの送信を試みないため、backendの明示的な拒否を表すFailedではなく、
		// 送信前の拒否としてRejectedを返します。
		err := fmt.Errorf("CloudWatch Logs flush manager is not initialized")
		l.notifyError(
			ctx,
			l.wrapErrorByLogger(err, "cloudwatch logs delivery put"),
			l.InternalErrorEntry("failed to put by CloudWatch Logs delivery logger; flush manager is not initialized; error:", "", err),
			l.Tracer,
			l.Meter,
		)
		return pkglogger.NewTargetDeliveryReport(cloudWatchLogsDeliveryRejectedResult(err)), err
	}

	return flushManager.PutWithReport(ctx, entry)
}
