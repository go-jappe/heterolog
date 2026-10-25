package opensearch

import (
	"context"
	"fmt"
	"sync"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

type DeliveryOpenSearchLogger struct {
	*LoggerBase
	stackTraceConfig logging.StackTraceConfigProvider
	indexer          deliveryOpenSearchIndexer
	stateMu          sync.Mutex
	// closeMuはCloseの呼び出しを直列化します。待機をchannelで行うため、testing/synctestの
	// bubble内でも待機を観測できます。
	closeMu loggercommon.WaitableMutex
	// inFlightは、受付を確定したIndexの完了を数えます。CloseはcloseStateをStartedにして
	// 新規受付を止めた後、受付済みのIndexの完了を待ってからindexerを閉じます。
	inFlight   sync.WaitGroup
	closeState loggercommon.LoggerCloseState
}

var _ logging.DeliveryBackendLogEntryHandler = (*DeliveryOpenSearchLogger)(nil)

// deliveryOpenSearchIndexer はDelivery OpenSearchが使用する1件Indexと終了処理の境界です。
// bulk flushやspool再送をDelivery loggerから呼べないよう、必要なメソッドだけを公開します。
type deliveryOpenSearchIndexer interface {
	IndexWithReport(context.Context, *logging.LogEntry) (pkglogger.DeliveryReport, error)
	Close(context.Context) error
}

func NewDeliveryOpenSearchLogger(
	ctx context.Context,
	loggerConfig OpenSearchLoggerConfigProvider,
	osClient opensearch.Client,
	fmCfg DeliveryOpenSearchFlushManagerConfigProvider,
	resourceProvider resource.ResourceProvider,
	failureHandler *flushfailure.ReportingCompositeFailureHandler,
	tracer observer.Tracer,
	meter observer.Meter,
	loggerBase *LoggerBase,
) *DeliveryOpenSearchLogger {
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)
	_, span := tracer.StartSpan(ctx, SpanOpenSearchLoggerCreate)
	defer span.End()

	loggerBase.Tracer = tracer
	loggerBase.Meter = meter
	loggerBase.config = loggerConfig

	indexer := newDeliveryOpenSearchFlushManager(
		loggerBase,
		osClient,
		fmCfg,
		NewLogEntryConverter(
			fmCfg.UseOpenTelemetryFormat(),
			resourceProvider,
			loggerBase.TimestampFormatter,
		),
		failureHandler,
		resourceProvider,
		tracer,
		meter,
	)
	// On*の中から同じloggerへ再入した送信の内部ログは、通知せず同じindexへ直接書き込みます。
	loggerBase.directInternalLog = newDirectInternalLogWriter(loggerConfig, indexer.indexInternalLogDirectly)

	return &DeliveryOpenSearchLogger{
		LoggerBase:       loggerBase,
		stackTraceConfig: NewStackTraceConfigAdapter(loggerConfig),
		indexer:          indexer,
	}
}

// HandleLogEntryはtop-level loggerで生成済みのentryをOpenSearch Delivery用に加工し、
// 一件Indexの送達結果を返します。発生時刻・ID・Fieldsは作り直さず、TimestampFormatterと
// 要求されたstack traceだけをこの保存先の設定から適用します。
func (l *DeliveryOpenSearchLogger) HandleLogEntry(
	ctx context.Context,
	entry *logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	if err := l.loggerClosedError(); err != nil {
		return pkglogger.NewTargetDeliveryReport(openSearchDeliveryRejectedResult(err)), err
	}
	if entry == nil {
		err := fmt.Errorf("OpenSearch delivery log entry is nil")
		return pkglogger.NewTargetDeliveryReport(openSearchDeliveryRejectedResult(err)), err
	}

	logging.EnsureEntryOccurredAt(entry, l.Clock)
	if entry.StackTraceRequested {
		// ErrorWithStackにerrorが渡されなかった場合も拒否せず、要求されたstackを持つ
		// ERRORとして記録します。documentのerrorは空のままです。
		if len(entry.FuncName) == 0 && len(entry.File) == 0 && len(entry.Line) == 0 {
			depth := logging.GetCallerDepth(l.stackTraceConfig)
			logging.EnsureEntryStackTrace(entry, depth)
		}
	}
	return l.indexImmediatelyWithReport(ctx, entry)
}

func (l *DeliveryOpenSearchLogger) Close() error {
	l.stateMu.Lock()
	if l.closeState == loggercommon.LoggerCloseStateCompleted {
		l.stateMu.Unlock()
		return nil
	}
	l.closeState = loggercommon.LoggerCloseStateStarted
	l.stateMu.Unlock()

	l.closeMu.Lock()
	defer l.closeMu.Unlock()

	// Started以降は新しいIndexを受け付けないため、受付済みのIndexの完了だけを待ちます。
	l.inFlight.Wait()

	l.stateMu.Lock()
	if l.closeState == loggercommon.LoggerCloseStateCompleted {
		l.stateMu.Unlock()
		return nil
	}
	indexer := l.indexer
	l.stateMu.Unlock()

	if indexer == nil {
		l.stateMu.Lock()
		l.closeState = loggercommon.LoggerCloseStateCompleted
		l.stateMu.Unlock()
		return nil
	}
	closeErr := indexer.Close(context.Background())
	if closeErr != nil && !loggercommon.ShutdownComplete(indexer) {
		// close失敗後もStartedを維持して新規送信を拒否し、Closeの再試行だけを許可します。
		return closeErr
	}
	l.stateMu.Lock()
	l.closeState = loggercommon.LoggerCloseStateCompleted
	l.stateMu.Unlock()
	return closeErr
}

func (l *DeliveryOpenSearchLogger) indexImmediatelyWithReport(
	ctx context.Context,
	entry *logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	l.stateMu.Lock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		l.stateMu.Unlock()
		err := applogging.ErrLoggerClosed
		return pkglogger.NewTargetDeliveryReport(openSearchDeliveryRejectedResult(err)), err
	}
	indexer := l.indexer
	// closeStateの確認と同じlockの下で受付を数えるため、Close開始後に受付が増えることはなく、
	// Closeは受付済みのIndexだけを待ちます。
	l.inFlight.Add(1)
	l.stateMu.Unlock()
	defer l.inFlight.Done()

	if indexer == nil {
		// backendへの送信を試みないため、backendの明示的な拒否を表すFailedではなく、
		// 送信前の拒否としてRejectedを返します。
		err := fmt.Errorf("OpenSearch indexer is not initialized")
		l.notifyError(
			ctx,
			l.wrapErrorByLogger(err, OperationOpenSearchError),
			l.InternalErrorEntry("failed to index by OpenSearch delivery logger; indexer is not initialized; error:", "", err),
			l.Tracer,
			l.Meter,
		)
		return pkglogger.NewTargetDeliveryReport(openSearchDeliveryRejectedResult(err)), err
	}

	return indexer.IndexWithReport(ctx, entry)
}

func (l *DeliveryOpenSearchLogger) loggerClosedError() error {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		return applogging.ErrLoggerClosed
	}
	return nil
}
