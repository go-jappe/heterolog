package stdout

import (
	"context"
	"fmt"
	"os"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout/errors"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/logger"
)

const BackendServices string = "stdout"

var _ infralogging.DeliveryBackendLogEntryHandler = (*DeliveryStdoutLogger)(nil)

type DeliveryStdoutLogger struct {
	Clock     clock.Clock
	config    DeliveryStdoutLoggerConfigProvider
	OnError   infralogging.LoggerErrorHandler
	OnWarn    infralogging.LoggerHandlerFunc
	OnInfo    infralogging.LoggerHandlerFunc
	OnDebug   infralogging.LoggerHandlerFunc
	formatter formatter.Formatter
	Tracer    observer.Tracer // 現在createのみ計装する方針
	Meter     observer.Meter

	// internalLogReentryは、OnErrorへ渡すcontextへこのloggerの印を付け、通知を実行するgoroutineを記録します。
	// 通知の中から同じloggerへの書き込みが失敗した場合、その内部ログはOnErrorを再び呼ばず、
	// stdout・stderrへ直接書き込みます（handleWriteError）。
	internalLogReentry handler.ReentryGuard
}

// NewDeliveryStdoutLogger は新しい DeliveryStdoutLogger を作成します。
func NewDeliveryStdoutLogger(
	ctx context.Context,
	config DeliveryStdoutLoggerConfigProvider,
	otelResource resource.OtelLogResourceProvider,
	tracer observer.Tracer,
	meter observer.Meter,
	opts ...LoggerOption,
) (*DeliveryStdoutLogger, error) {
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)

	_, span := tracer.StartSpan(ctx, SpanStdoutLoggerCreate)
	defer span.End()

	timestampFormatter, err := NewConfiguredTimestampFormatter(
		config.UseOpenTelemetryFormat(),
		config.Time().EnableLocalTime(),
		config.Time().TimeZone(),
		config.LogBody().LogLineDateFormat(),
	)
	if err != nil {
		return nil, err
	}
	logFormatter, err := NewConfiguredFormatter(
		config.UseOpenTelemetryFormat(),
		otelResource,
		timestampFormatter,
	)
	if err != nil {
		return nil, err
	}

	var handlers internalLogHandlers
	for _, option := range opts {
		if option != nil {
			option(&handlers)
		}
	}
	return &DeliveryStdoutLogger{
		Clock:     clock.NewUTCClock(),
		config:    config,
		OnError:   handlers.onError,
		OnWarn:    handlers.onWarn,
		OnInfo:    handlers.onInfo,
		OnDebug:   handlers.onDebug,
		formatter: logFormatter,
		Tracer:    tracer,
		Meter:     meter,
	}, nil
}

// HandleLogEntryは生成済みentryをstdout delivery targetへ記録します。application由来の
// 値は再解決せず、stdout固有のFormatter、stack trace、出力streamの選択だけを適用します。
func (s *DeliveryStdoutLogger) HandleLogEntry(
	ctx context.Context,
	entry *infralogging.LogEntry,
) (logger.DeliveryReport, error) {
	if entry == nil {
		err := fmt.Errorf("delivery stdout logger log entry is nil")
		return logger.NewTargetDeliveryReport(deliveryStdoutRejectedResult(err)), err
	}
	infralogging.EnsureEntryOccurredAt(entry, s.Clock)
	if entry.StackTraceRequested {
		// ErrorWithStackにerrorが渡されなかった場合も拒否せず、要求されたstackを持つ
		// ERRORとして記録します。Formatterはerrorの欄を出力しません。
		if len(entry.FuncName) == 0 && len(entry.File) == 0 && len(entry.Line) == 0 {
			infralogging.EnsureEntryStackTrace(entry,
				infralogging.GetCallerDepth(newDeliveryStdoutStackTraceConfig(s.config)),
			)
		}
	}
	result, err := s.logToAutoSink(ctx, entry)
	return logger.NewTargetDeliveryReport(result), err
}

// Warn以上のレベルの場合はstderrに出力
func (s *DeliveryStdoutLogger) logToAutoSink(ctx context.Context, meta *infralogging.LogEntry) (logger.DeliveryResult, error) {
	if meta.Level < logger.LogLevelWarn {
		// stdoutに出力
		return s.logWithMeta(ctx, meta)
	}
	// stderrに出力
	return s.logStderrWithMeta(ctx, meta)
}

// 明示的にstdoutに出力
func (s *DeliveryStdoutLogger) logWithMeta(ctx context.Context, meta *infralogging.LogEntry) (logger.DeliveryResult, error) {
	_, err := fmt.Fprintln(
		os.Stdout,
		s.formatter.Format(meta),
	)
	if err != nil {
		internalErr := &errors.StdoutLoggerInternalError{
			Op:  MsgFailedWriteStdout,
			Err: err,
		}
		s.handleWriteError(ctx, internalErr, meta)
		return deliveryStdoutFailedResult(internalErr), internalErr
	}
	return deliveryStdoutSucceededResult(), nil
}

type deliveryStdoutStackTraceConfig struct {
	isDebug                  bool
	errorTraceDepthDebugMode int
	errorTraceDepth          int
}

func newDeliveryStdoutStackTraceConfig(cfg DeliveryStdoutLoggerConfigProvider) deliveryStdoutStackTraceConfig {
	return deliveryStdoutStackTraceConfig{
		isDebug:                  cfg.Runtime().IsDebug(),
		errorTraceDepthDebugMode: cfg.LogBody().ErrorTraceDepthDebugMode(),
		errorTraceDepth:          cfg.LogBody().ErrorTraceDepth(),
	}
}

func (a deliveryStdoutStackTraceConfig) IsDebug() bool {
	return a.isDebug
}

func (a deliveryStdoutStackTraceConfig) ErrorTraceDepthDebugMode() int {
	return a.errorTraceDepthDebugMode
}

func (a deliveryStdoutStackTraceConfig) ErrorTraceDepth() int {
	return a.errorTraceDepth
}

// stderrに出力
func (s *DeliveryStdoutLogger) logStderrWithMeta(ctx context.Context, meta *infralogging.LogEntry) (logger.DeliveryResult, error) {
	_, err := fmt.Fprintln(
		os.Stderr,
		s.formatter.Format(meta),
	)
	if err != nil {
		internalErr := &errors.StdoutLoggerInternalError{
			Op:  OperationWriteError,
			Err: err,
		}
		s.handleWriteError(ctx, internalErr, meta)
		return deliveryStdoutFailedResult(internalErr), internalErr
	}
	return deliveryStdoutSucceededResult(), nil
}

func (s *DeliveryStdoutLogger) handleWriteError(
	ctx context.Context,
	err error,
	entry *infralogging.LogEntry,
) {
	if entry == nil {
		return
	}
	internalEntry := *entry
	internalEntry.Internal = true
	internalEntry.Source = common.SourceNameStdoutLogger
	internalEntry.Err = err
	internalEntry.OccurredAt = infralogging.ResolveOccurredAt(s.Clock)
	// OnErrorの中から同じloggerへの書き込みが失敗した場合（再入）は、OnErrorを再び呼ばず、
	// 内部ログをlevelに応じたstdout・stderrへ直接書き込みます。直接の書き込みは失敗しても
	// 通知しません。
	s.internalLogReentry.CallErrorHandler(
		s.OnError,
		ctx,
		err,
		&internalEntry,
		s.Tracer,
		s.Meter,
		func(_ context.Context, entry *infralogging.LogEntry) { _ = writeToAutoSink(s.formatter, entry) },
	)
}

func deliveryStdoutFailedResult(err error) logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: BackendServices,
		Status:  logger.DeliveryFailed,
		Err:     err,
	}
}

func deliveryStdoutSucceededResult() logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: BackendServices,
		Status:  logger.DeliverySucceeded,
	}
}

func deliveryStdoutRejectedResult(err error) logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: BackendServices,
		Status:  logger.DeliveryRejected,
		Err:     err,
	}
}
