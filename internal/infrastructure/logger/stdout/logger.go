package stdout

import (
	"context"
	stderrors "errors"
	"fmt"
	"log"
	"os"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	resource "github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout/errors"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

var _ infralogging.BackendLogEntryHandler = (*StdoutLogger)(nil)

type StdoutLogger struct {
	Clock     clock.Clock
	config    StdoutLoggerConfigProvider
	OnError   infralogging.LoggerErrorHandler // 通知用の関数　内部エラー発生時にハンドル
	OnWarn    infralogging.LoggerHandlerFunc
	OnInfo    infralogging.LoggerHandlerFunc
	OnDebug   infralogging.LoggerHandlerFunc
	formatter formatter.Formatter
	Tracer    observer.Tracer // 現在createのみ計装する方針
	Meter     observer.Meter

	// internalLogReentryは、OnErrorへ渡すcontextへこのloggerの印を付け、通知を実行するgoroutineを記録します。
	// 通知の中から同じloggerへの書き込みが失敗した場合、その内部ログはOnErrorを再び呼ばず、
	// stdout・stderrへ直接書き込みます（reportWriteError）。
	internalLogReentry handler.ReentryGuard

	// notificationOnlyは、内部ログの通知先として作った専用instance（NotificationTarget）で
	// あることを表します。通知専用instanceは通知先を持たないため、書き込みの失敗を通知せず、
	// 失敗ごとに標準loggerへ一行残します。
	notificationOnly bool
}

// HandleLogEntryはtop-level loggerが生成済みのentryをstdoutまたはstderrへ記録します。
// ID、Fields、Message、Err、OccurredAtは再解決せず、構築済みFormatter、level filter、sampling、
// stack traceと出力streamの選択だけを適用します。
func (s *StdoutLogger) HandleLogEntry(ctx context.Context, entry *infralogging.LogEntry) error {
	if entry == nil {
		return fmt.Errorf("stdout logger log entry is nil")
	}
	if !entry.StackTraceRequested && entry.Level < s.config.Runtime().LogLevel() {
		return nil
	}

	infralogging.EnsureEntryOccurredAt(entry, s.Clock)
	if entry.StackTraceRequested {
		// ErrorWithStackにerrorが渡されなかった場合も拒否せず、要求されたstackを持つ
		// ERRORとして記録します。Formatterはerrorの欄を出力しません。
		if len(entry.FuncName) == 0 && len(entry.File) == 0 && len(entry.Line) == 0 {
			infralogging.EnsureEntryStackTrace(entry,
				infralogging.GetCallerDepth(NewStackTraceConfigAdapter(s.config)),
			)
		}
		return s.logToAutoSink(ctx, entry)
	}

	// 共通サンプリングロジックを適用
	if !sampling.ApplySamplingLogic(entry, s.config.Sampling()) {
		return nil
	}

	return s.logToAutoSink(ctx, entry)
}

// stderrに出力
func (s *StdoutLogger) logStderrWithMeta(ctx context.Context, meta *infralogging.LogEntry) error {
	_, err := fmt.Fprintln(
		os.Stderr,
		s.formatter.Format(meta),
	)
	if err != nil {
		message := fmt.Sprintf(MsgFailedWriteStderr, err)
		internalErr := &errors.StdoutLoggerInternalError{Op: OperationWriteError, Err: err}
		s.reportWriteError(ctx, internalErr, s.InternalErrorEntry(message, "", err))
		return internalErr
	}
	return nil
}

// 明示的にstdoutに出力
func (s *StdoutLogger) logWithMeta(ctx context.Context, meta *infralogging.LogEntry) error {
	_, err := fmt.Fprintln(
		os.Stdout,
		s.formatter.Format(meta),
	)
	if err != nil {
		message := fmt.Sprintf(MsgFailedWriteStdout, err)
		internalErr := &errors.StdoutLoggerInternalError{Op: OperationWriteError, Err: err}
		s.reportWriteError(ctx, internalErr, s.InternalErrorEntry(message, "", err))
		return internalErr
	}
	return nil
}

// reportWriteErrorは、stdout・stderrへの書き込みの失敗をOnErrorへ通知します。このloggerの
// 通知の中から同じloggerへの書き込みが失敗した場合（再入）は、OnErrorを再び呼ばず、内部ログを
// levelに応じたstdout・stderrへ直接書き込みます。直接の書き込みは失敗しても通知しないため、
// 再入はここで終わります。内部ログの通知専用instanceの失敗は、通知せず標準loggerへ残します。
func (s *StdoutLogger) reportWriteError(ctx context.Context, err error, entry *infralogging.LogEntry) {
	if s.notificationOnly {
		reportNotificationWriteFailure(err, entry)
		return
	}
	s.internalLogReentry.CallErrorHandler(s.OnError, ctx, err, entry, s.Tracer, s.Meter, s.writeReentrantInternalLog)
}

// reportNotificationWriteFailureは、通知専用instanceの書き込みの失敗を、通知せずに失敗ごとに
// 一行、標準loggerへ残します。entryのmessageは失敗したstreamと原因を含みます。
func reportNotificationWriteFailure(err error, entry *infralogging.LogEntry) {
	detail := err.Error()
	if entry != nil && entry.Message != "" {
		detail = entry.Message
	}
	log.Printf(MsgFailedWriteNotification, detail)
}

// writeReentrantInternalLogは、再入した書き込みの内部ログをlog level以上の場合だけ、通知せず
// levelに応じたstdout・stderrへ書き込みます。
func (s *StdoutLogger) writeReentrantInternalLog(_ context.Context, entry *infralogging.LogEntry) {
	if entry.Level >= s.config.Runtime().LogLevel() {
		_ = writeToAutoSink(s.formatter, entry)
	}
}

// writeToAutoSinkはlogToAutoSinkと同じ規則（Warn以上はstderr）でentryを書き込み、
// 失敗を通知せずに返します。
func writeToAutoSink(logFormatter formatter.Formatter, entry *infralogging.LogEntry) error {
	output := os.Stdout
	if entry.Level >= pkglogger.LogLevelWarn {
		output = os.Stderr
	}
	_, err := fmt.Fprintln(output, logFormatter.Format(entry))
	return err
}

// Warn以上のレベルの場合はstderrに出力
func (s *StdoutLogger) logToAutoSink(ctx context.Context, meta *infralogging.LogEntry) error {
	if meta.Level < pkglogger.LogLevelWarn {
		// stdoutに出力
		return s.logWithMeta(ctx, meta)
	}
	// stderrに出力
	return s.logStderrWithMeta(ctx, meta)
}

// NewStdoutLogger は新しい StdoutLogger を作成します
func NewStdoutLogger(
	ctx context.Context,
	config StdoutLoggerConfigProvider,
	otelResource resource.OtelLogResourceProvider,
	tracer observer.Tracer,
	meter observer.Meter,
	opts ...LoggerOption,
) (*StdoutLogger, error) {
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)

	_, span := tracer.StartSpan(ctx, SpanStdoutLoggerCreate)
	defer span.End()

	if config == nil {
		return nil, fmt.Errorf("stdout logger config is nil")
	}
	if err := sampling.ValidateSamplingConfigProviderFor(pkglogger.LogOutputStdout, config.Sampling()); err != nil {
		return nil, fmt.Errorf("stdout logger sampling config: %w", err)
	}

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
	return &StdoutLogger{
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

var (
	// ErrOpenTelemetryResourceRequiredは、OpenTelemetry形式の出力にresourceが渡されて
	// いないことを表します。
	ErrOpenTelemetryResourceRequired = stderrors.New("OpenTelemetry resource is not initialized")
	// ErrTimestampFormatterRequiredは、plain text形式の出力に初期化済みの
	// timestamp formatterが渡されていないことを表します。
	ErrTimestampFormatterRequired = stderrors.New("timestamp formatter is not initialized")
)

// NewConfiguredFormatterはstdoutの出力形式に対応する、構築済みformatterを返します。
// OpenTelemetry形式にはresource、plain text形式には初期化済みtimestampFormatterが必須です。
func NewConfiguredFormatter(
	isOtelFormatEnabled bool,
	otelResource resource.OtelLogResourceProvider,
	timestampFormatter timestamp.Formatter,
) (formatter.Formatter, error) {
	if isOtelFormatEnabled {
		if otelResource == nil {
			return nil, fmt.Errorf("stdout %w", ErrOpenTelemetryResourceRequired)
		}
		return &formatter.OtelFormatter{
			Resource: otelResource,
		}, nil
	}
	if timestampFormatter.IsZero() {
		return nil, fmt.Errorf("stdout %w", ErrTimestampFormatterRequired)
	}
	return formatter.NewPlainTextFormatter(timestampFormatter), nil
}

// NewConfiguredTimestampFormatterはstdoutの出力形式に対応する表示設定を作ります。
// OpenTelemetry形式ではapplicationのplain-text設定を使用せずUTC RFC3339に固定します。
func NewConfiguredTimestampFormatter(
	isOtelFormatEnabled bool,
	useLocal bool,
	timezone string,
	format string,
) (timestamp.Formatter, error) {
	if isOtelFormatEnabled {
		return timestamp.RFC3339Formatter(), nil
	}
	return infralogging.NewTimestampFormatter(useLocal, timezone, format)
}
