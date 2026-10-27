package slack

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-jappe/heterolog/internal/application/logging/logtemplate"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/slack/errors"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/slack-go/slack"
)

const BackendServices string = "slack"

var _ infralogging.DeliveryBackendLogEntryHandler = (*DeliverySlackLogger)(nil)

type DeliverySlackLoggerConfigProvider interface {
	Time() loggertime.TimeConfigProvider
	Runtime() RuntimeConfigProvider
	LogBody() LogBodyConfigProvider
	Handler() loggerhandler.ConfigProvider

	Timeout() time.Duration
	// HTTPClientの契約はSlackLoggerConfigProvider.HTTPClientと同じです。
	HTTPClient() *http.Client
	Token() string
	Channel() string
	ErrorChannel() string
	WarnChannel() string
	InfoChannel() string
	DebugChannel() string

	MaxMessageLength() int

	SlackPostWarnThresholdDuration() time.Duration
	SlackPostErrorThresholdDuration() time.Duration
}

// DeliverySlackLoggerは、各application logについてSlack APIの応答を待ち、
// その結果をDeliveryReportとして返します。1件ごとのHTTP往復時間が呼び出し元の
// latencyへ直接加算されるため、低頻度の通知用途を想定しています。高頻度または
// latencyを重視するDelivery出力先には推奨しません。
type DeliverySlackLogger struct {
	Clock clock.Clock
	// LogFormatterはSlack message本文に記録する時刻表示規則を保持します。
	LogFormatter formatter.PlainTextFormatter
	slackAPI     *slack.Client
	config       DeliverySlackLoggerConfigProvider
	OnError      infralogging.LoggerErrorHandler
	OnWarn       infralogging.LoggerHandlerFunc
	OnInfo       infralogging.LoggerHandlerFunc
	OnDebug      infralogging.LoggerHandlerFunc
	Tracer       pkgobserver.Tracer
	Meter        pkgobserver.Meter

	// internalLogReentryは、On*へ渡すcontextへこのloggerの印を付け、通知を実行するgoroutineを記録します。
	// On*の中から同じloggerへの送信で発生した失敗・遅延の内部ログは、On*を再び呼ばず、
	// sendInternalLogDirectlyで同じSlackへ直接送信します。
	internalLogReentry loggerhandler.ReentryGuard
}

// NewDeliverySlackLoggerはSlackのdelivery loggerを作成します。
func NewDeliverySlackLogger(
	ctx context.Context,
	cfg DeliverySlackLoggerConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	opts ...LoggerOption,
) (*DeliverySlackLogger, error) {
	_, span := tracer.StartSpan(ctx, SpanSlackLoggerCreate)
	defer span.End()

	if err := validateSlackDestinationConfig(cfg); err != nil {
		return nil, err
	}

	timestampFormatter, err := infralogging.NewTimestampFormatter(
		cfg.Time().EnableLocalTime(),
		cfg.Time().TimeZone(),
		cfg.LogBody().LogLineDateFormat(),
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

	logger := &DeliverySlackLogger{
		Clock:        clock.NewUTCClock(),
		LogFormatter: formatter.NewPlainTextFormatter(timestampFormatter),
		slackAPI:     newSlackAPI(cfg.Token(), cfg.Timeout(), cfg.HTTPClient(), cfg.Runtime().IsDebug()),
		config:       cfg,
		OnError:      handlers.onError,
		OnWarn:       handlers.onWarn,
		OnInfo:       handlers.onInfo,
		OnDebug:      handlers.onDebug,
		Tracer:       tracer,
		Meter:        meter,
	}

	return logger, nil
}

// スラックに送信
func (s *DeliverySlackLogger) send(ctx context.Context, level logger.LogLevel, msg string) (channel string, timestamp string, err error) {
	channel, timestamp, err = postSlackMessage(ctx, s.slackAPI, channelForLogLevel(s.config, level), msg)
	return
}

// HandleLogEntryは生成済みentryをSlack delivery targetへ同期送信します。application由来の
// 値は再解決せず、Slack固有のLogFormatter、level filter、stack traceと送信処理だけを適用します。
func (s *DeliverySlackLogger) HandleLogEntry(
	ctx context.Context,
	entry *infralogging.LogEntry,
) (logger.DeliveryReport, error) {
	if entry == nil {
		err := fmt.Errorf("delivery slack logger log entry is nil")
		return logger.NewTargetDeliveryReport(s.deliveryRejectedResult(err)), err
	}
	if !entry.StackTraceRequested && entry.Level < s.config.Runtime().LogLevel() {
		return logger.NewTargetDeliveryReport(s.deliverySkippedResult()), nil
	}

	infralogging.EnsureEntryOccurredAt(entry, s.Clock)
	if entry.StackTraceRequested {
		// ErrorWithStackにerrorが渡されなかった場合も拒否せず、要求されたstackを持つ
		// ERRORとして記録します。Formatterはerrorの欄を出力しません。
		if len(entry.FuncName) == 0 && len(entry.File) == 0 && len(entry.Line) == 0 {
			infralogging.EnsureEntryStackTrace(entry, s.stackTraceDepth())
		}
	}
	result, err := s.sendEntry(ctx, entry)
	return logger.NewTargetDeliveryReport(result), err
}

// formatMessageはentryをSlackへ送信する本文へ変換し、設定の最大長で切り詰めます。
func (s *DeliverySlackLogger) formatMessage(entry *infralogging.LogEntry) string {
	message := fmt.Sprintf(
		logtemplate.ErrMessageSlack,
		s.LogFormatter.Format(entry),
		s.config.Runtime().AppName(),
	)
	if maxMessageLength := s.config.MaxMessageLength(); maxMessageLength > 10 {
		message = truncateMessage(message, maxMessageLength)
	}
	return message
}

func (s *DeliverySlackLogger) sendEntry(ctx context.Context, entry *infralogging.LogEntry) (logger.DeliveryResult, error) {
	message := s.formatMessage(entry)

	start := time.Now()
	_, _, err := s.send(ctx, entry.Level, message)
	elapsed := time.Since(start)
	if err != nil {
		internalErr := &errors.SlackLoggerInternalError{
			Op:  OperationSlackPostError,
			Err: err,
		}
		s.handleDeliveryInternalError(ctx, internalErr, entry)
		return logger.DeliveryResult{Backend: BackendServices, Status: slackFailureDeliveryStatus(err), Err: internalErr}, internalErr
	}
	s.handleDeliveryLatency(ctx, elapsed, entry)

	return s.deliverySucceededResult(), nil
}

func (s *DeliverySlackLogger) handleDeliveryInternalError(
	ctx context.Context,
	err error,
	entry *infralogging.LogEntry,
) {
	if entry == nil {
		return
	}
	internalEntry := *entry
	internalEntry.Internal = true
	internalEntry.Source = common.SourceNameSlackLogger
	internalEntry.Err = err
	internalEntry.OccurredAt = infralogging.ResolveOccurredAt(s.Clock)
	s.internalLogReentry.CallErrorHandler(
		s.OnError,
		ctx,
		err,
		&internalEntry,
		s.Tracer,
		s.Meter,
		s.sendInternalLogDirectly,
	)
}

func (s *DeliverySlackLogger) handleDeliveryLatency(
	ctx context.Context,
	elapsed time.Duration,
	entry *infralogging.LogEntry,
) {
	if entry == nil {
		return
	}
	internalEntry := *entry
	internalEntry.Internal = true
	internalEntry.Source = common.SourceNameSlackLogger
	internalEntry.OccurredAt = infralogging.ResolveOccurredAt(s.Clock)
	errorThreshold := s.config.SlackPostErrorThresholdDuration()
	warnThreshold := s.config.SlackPostWarnThresholdDuration()
	switch {
	case errorThreshold > 0 && elapsed > errorThreshold:
		err := &errors.SlackLoggerInternalError{Op: OperationSlackPostDelay}
		internalEntry.Level = logger.LogLevelError
		internalEntry.Err = err
		s.internalLogReentry.CallErrorHandler(
			s.OnError,
			ctx,
			err,
			&internalEntry,
			s.Tracer,
			s.Meter,
			s.sendInternalLogDirectly,
		)
	case warnThreshold > 0 && elapsed > warnThreshold:
		internalEntry.Level = logger.LogLevelWarn
		s.internalLogReentry.CallWarnHandler(
			s.OnWarn,
			ctx,
			&internalEntry,
			s.Tracer,
			s.Meter,
			s.sendInternalLogDirectly,
		)
	}
}

// sendInternalLogDirectlyは、On*の中から再入した送信の失敗・遅延の内部ログを、設定した
// log level以上の場合だけ同じSlackへ直接送信します。送信の失敗・遅延は通知しないため、
// 再入はここで終わります。失敗はfallback loggerと同じく標準loggerへ残します。
func (s *DeliverySlackLogger) sendInternalLogDirectly(ctx context.Context, entry *infralogging.LogEntry) {
	if entry.Level < s.config.Runtime().LogLevel() {
		return
	}
	if _, _, err := s.send(ctx, entry.Level, s.formatMessage(entry)); err != nil {
		log.Printf("failed to send Slack internal log directly: %v", err)
	}
}

func (s *DeliverySlackLogger) deliveryFailedResult(err error) logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: BackendServices,
		Status:  logger.DeliveryFailed,
		Err:     err,
	}
}

func (s *DeliverySlackLogger) deliverySucceededResult() logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: BackendServices,
		Status:  logger.DeliverySucceeded,
	}
}

func (s *DeliverySlackLogger) deliveryRejectedResult(err error) logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: BackendServices,
		Status:  logger.DeliveryRejected,
		Err:     err,
	}
}

func (s *DeliverySlackLogger) deliverySkippedResult() logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: BackendServices,
		Status:  logger.DeliverySkipped,
	}
}

func (s *DeliverySlackLogger) stackTraceDepth() int {
	if s.config.Runtime().IsDebug() {
		return s.config.LogBody().ErrorTraceDepthDebugMode()
	}
	return s.config.LogBody().ErrorTraceDepth()
}
