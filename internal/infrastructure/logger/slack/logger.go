package slack

import (
	"context"
	"fmt"
	"sync"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/slack/errors"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/slack-go/slack"
)

var _ logging.BackendLogEntryHandler = (*SlackLogger)(nil)

type SlackLogger struct {
	Clock clock.Clock
	// LogFormatterはSlack message本文に記録する時刻表示規則を保持します。
	LogFormatter formatter.PlainTextFormatter
	// muはbufferと送信予約、Closeの状態を保護します。待機をchannelで行うため、
	// testing/synctestのbubble内でも待機を観測できます（network送信中は保持しません）。
	mu loggercommon.WaitableMutex
	// sendMuは、bufferの取り出しからSlack送信完了までを直列化します。
	sendMu loggercommon.WaitableMutex
	// closeMuはCloseの呼び出しを直列化します。sendMu・muと同じく待機をchannelで行います。
	closeMu       loggercommon.WaitableMutex
	timerWG       sync.WaitGroup
	logs          []*logging.LogEntry
	intervalIndex int
	// nextNotificationImmediateは次のbatchだけをIntervalsと無関係に即時送信する状態です。
	nextNotificationImmediate bool
	slackAPI                  *slack.Client
	timer                     *time.Timer
	lastLogTime               time.Time
	// closeStateはmuの保護下でbuffer受付とCloseの状態遷移を管理します。
	closeState loggercommon.LoggerCloseState
	config     SlackLoggerConfigProvider
	OnError    logging.LoggerErrorHandler // 通知用の関数　内部エラー発生時にハンドル
	OnWarn     logging.LoggerHandlerFunc
	OnInfo     logging.LoggerHandlerFunc
	OnDebug    logging.LoggerHandlerFunc
	Tracer     observer.Tracer
	Meter      observer.Meter

	// internalLogReentryは、書き込み（HandleLogEntry・enqueue）が同期的に通知する内部ログの
	// On*へ渡すcontextへこのloggerの印を付け、通知を実行するgoroutineを記録します。
	// 通知の中から同じloggerへの書き込みが再び内部ログを発生させた場合は、通知せず直接記録します
	// （bufferのdebug logはbufferへ追加し、log level未満のwarningは記録しません）。
	internalLogReentry handler.ReentryGuard
}

// NewSlackLogger は新しい SlackLogger を作成します
func NewSlackLogger(
	ctx context.Context,
	cfg SlackLoggerConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
	opts ...LoggerOption,
) (*SlackLogger, error) {
	_, span := tracer.StartSpan(ctx, SpanSlackLoggerCreate)
	defer span.End()

	if err := validateSlackBufferedConfig(cfg); err != nil {
		return nil, err
	}

	timestampFormatter, err := logging.NewTimestampFormatter(
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

	// デバッグ環境ではデバッグオプションを有効にする
	logger := &SlackLogger{
		Clock:                     clock.NewUTCClock(),
		LogFormatter:              formatter.NewPlainTextFormatter(timestampFormatter),
		logs:                      make([]*logging.LogEntry, 0, cfg.MaxLogsPerNotification()),
		slackAPI:                  newSlackAPI(cfg.Token(), cfg.Timeout(), cfg.HTTPClient(), cfg.Runtime().IsDebug()),
		lastLogTime:               time.Now(),
		intervalIndex:             0,
		nextNotificationImmediate: true,
		config:                    cfg,
		OnError:                   handlers.onError,
		OnWarn:                    handlers.onWarn,
		OnInfo:                    handlers.onInfo,
		OnDebug:                   handlers.onDebug,
		Tracer:                    tracer,
		Meter:                     meter,
	}

	return logger, nil
}

// HandleLogEntryはtop-level loggerが生成済みのentryをSlackのbufferへ受理します。
// ID、Fields、Message、Err、OccurredAtは再解決せず、LogFormatter、level filter、sampling、
// stack traceと通知scheduleだけを適用します。
func (s *SlackLogger) HandleLogEntry(ctx context.Context, entry *logging.LogEntry) error {
	if err := s.rejectIfClosed(); err != nil {
		return err
	}
	if entry == nil {
		return fmt.Errorf("slack logger log entry is nil")
	}
	if !entry.StackTraceRequested && entry.Level < s.config.Runtime().LogLevel() {
		if entry.Level == pkglogger.LogLevelError {
			// このwarningはlog level未満なので、再入時も直接の記録は行いません。
			s.internalLogReentry.CallWarnHandler(s.OnWarn, ctx, s.InternalWarnEntry(MsgLogLevelTooHighWarn, ""), s.Tracer, s.Meter, nil)
		}
		return nil
	}

	logging.EnsureEntryOccurredAt(entry, s.Clock)
	if entry.StackTraceRequested {
		// ErrorWithStackにerrorが渡されなかった場合も拒否せず、要求されたstackを持つ
		// ERRORとして記録します。Formatterはerrorの欄を出力しません。
		if len(entry.FuncName) == 0 && len(entry.File) == 0 && len(entry.Line) == 0 {
			logging.EnsureEntryStackTrace(entry,
				logging.GetCallerDepth(NewStackTraceConfigAdapter(s.config)),
			)
		}
		return s.enqueue(ctx, entry)
	}

	// 共通サンプリングロジックを適用
	if !sampling.ApplySamplingLogic(entry, s.config.Sampling()) {
		return nil
	}

	return s.enqueue(ctx, entry)
}

// enqueue はログをバッファに追加し、送信スケジュールを更新します。buffer管理のdebug logは
// s.muを解放してからOnDebugへ通知します。s.muを保持したまま通知すると、通知の中から同じloggerへの
// 書き込みがs.muを待って停止するためです。通知の順序はbuffer操作の順序のままです。
func (s *SlackLogger) enqueue(ctx context.Context, entry *logging.LogEntry) error {
	debugEntries, err := s.enqueueLocked(ctx, entry)
	for _, debugEntry := range debugEntries {
		s.internalLogReentry.CallDebugHandler(
			s.OnDebug, ctx, debugEntry, s.Tracer, s.Meter, s.config.Runtime().IsDebug(), s.enqueueInternalLogDirectly,
		)
	}
	return err
}

// enqueueInternalLogDirectlyは、OnDebugの中から再入した書き込みのbuffer管理のdebug logを、
// 通知せずlog level以上の場合だけbufferへ追加します。この追加のdebug logは通知しないため、
// 再入はここで終わります。
func (s *SlackLogger) enqueueInternalLogDirectly(ctx context.Context, entry *logging.LogEntry) {
	if entry.Level < s.config.Runtime().LogLevel() {
		return
	}
	_, _ = s.enqueueLocked(ctx, entry)
}

// enqueueLockedはs.muの下でentryをbufferへ追加して送信を予約し、通知するbuffer管理の
// debug logを発生順に返します。呼び出し元はs.muを解放してから通知します。
func (s *SlackLogger) enqueueLocked(ctx context.Context, entry *logging.LogEntry) ([]*logging.LogEntry, error) {
	if s == nil || entry == nil {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closeState != loggercommon.LoggerCloseStateOpen {
		return nil, applogging.ErrLoggerClosed
	}

	var debugEntries []*logging.LogEntry
	now := time.Now()
	resetThreshold := s.config.MaxRestThreshold()
	if !s.lastLogTime.IsZero() &&
		now.Sub(s.lastLogTime) > resetThreshold {
		s.stopBatchTimerLocked()
		s.intervalIndex = 0
		s.nextNotificationImmediate = true
		mes := fmt.Sprintf("Slack通知の連続のための遅延をリセット: 次のログは即時送信 最後のログは%v 閾値は%vです", s.lastLogTime, resetThreshold)
		debugEntries = append(debugEntries, s.InternalDebugEntry(mes, entry.RequestID))
	}

	// メモリ節約のためにログが maxLogs を超えたら古いものを削除
	maxLogs := s.config.MaxLogsPerNotification()
	if len(s.logs) >= maxLogs {
		s.logs = s.logs[1:] // 先頭の古いログを削除
		mes := fmt.Sprintf("先頭の古いログを削除しました。現在のlogsは%vです", len(s.logs))
		debugEntries = append(debugEntries, s.InternalDebugEntry(mes, entry.RequestID))
	}

	s.logs = append(s.logs, entry)
	s.lastLogTime = now // 最終ログ時間を更新

	// タイマーの再設定（遅延を適用）
	debugEntries = append(debugEntries, s.scheduleNextBatchLocked(ctx))
	return debugEntries, nil
}

// rejectIfClosedはlevel filterやsamplingより前にClose開始後の要求を拒否します。
// enqueueでも再確認し、事前確認とbuffer lock取得の間にCloseが始まるraceを閉じます。
func (s *SlackLogger) rejectIfClosed() error {
	if s == nil {
		return applogging.ErrLoggerClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeState != loggercommon.LoggerCloseStateOpen {
		return applogging.ErrLoggerClosed
	}
	return nil
}

// sendBatchはbufferの取り出しからSlack送信完了までを直列化します。
// network送信中はmuを保持しないため、通常のenqueueは送信完了を待ちません。
func (s *SlackLogger) sendBatch(ctx context.Context) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	return s.sendBuffered(ctx)
}

// sendBufferedはsendMuを保持した呼び出し元が現在のbufferを一度送信します。
// 送信に失敗したchannelと未送信channelのentryはbuffer先頭へ戻し、
// Closeを再実行した際に再送できるようにします。
func (s *SlackLogger) sendBuffered(ctx context.Context) error {
	s.mu.Lock()
	logs := s.takeBufferedLogsLocked()
	s.mu.Unlock()
	return s.sendLogs(ctx, logs)
}

// sendLogsはsendMuを保持した呼び出し元が取り出し済みのbatchを送信します。
func (s *SlackLogger) sendLogs(ctx context.Context, logs []*logging.LogEntry) error {
	ctx, span := s.Tracer.StartSpan(ctx, SpanSlackLoggerSendBatch)
	defer span.End()

	mesSlackStart := "slack送信バッチ処理の開始"
	handler.CallInfoHandlerIfPresent(s.OnInfo, ctx, s.InternalInfoEntry(mesSlackStart, ""), s.Tracer, s.Meter)

	if len(logs) == 0 {
		mes := "sendBatch()が実行されましたが、ログがないためバッチ送信をスキップします"
		handler.CallDebugHandlerIfPresent(s.OnDebug, ctx, s.InternalDebugEntry(mes, ""), s.Tracer, s.Meter, s.config.Runtime().IsDebug())
		return nil
	}
	mesTrySlackSend := "Slackへ送信を試みています..."
	handler.CallDebugHandlerIfPresent(s.OnDebug, ctx, s.InternalDebugEntry(mesTrySlackSend, ""), s.Tracer, s.Meter, s.config.Runtime().IsDebug())

	succeededChannels := make(map[string]struct{})
	for _, batch := range slackChannelBatches(s.config, logs, s.LogFormatter) {
		message := fmt.Sprintf("ログ一覧:%s", batch.messages)
		msgToSend := message

		maxMessageLength := s.config.MaxMessageLength()
		if maxMessageLength > 10 && len([]rune(message)) > maxMessageLength {
			msgToSend = truncateMessage(message, maxMessageLength)
		}

		// slackを送信　遅延が発生したらログを出力
		channel, timestamp, err := s.sendSlackWithLatencyCheck(ctx, batch.channel, msgToSend)

		if err != nil {
			s.restoreUnsentLogs(logs, succeededChannels)
			// 送信失敗時のインターナルログを出力
			s.handleSlackError(ctx, err)
			return err
		}
		succeededChannels[batch.channel] = struct{}{}
		mesTriedSlackSend := "Slackへ送信成功"
		handler.CallDebugHandlerIfPresent(s.OnDebug, ctx, s.InternalDebugEntry(mesTriedSlackSend, ""), s.Tracer, s.Meter, s.config.Runtime().IsDebug())

		// 送信成功時のインターナルログを出力
		s.handleSlackSuccess(ctx, channel, timestamp)
	}

	return nil
}

// restoreUnsentLogsは、既に送信成功したchannelを除くentryだけを、
// 送信中に追加された新しいentryより前へ元の順序で戻します。
func (s *SlackLogger) restoreUnsentLogs(
	logs []*logging.LogEntry,
	succeededChannels map[string]struct{},
) {
	unsent := make([]*logging.LogEntry, 0, len(logs))
	for _, entry := range logs {
		if entry == nil {
			continue
		}
		if _, succeeded := succeededChannels[channelForLogLevel(s.config, entry.Level)]; succeeded {
			continue
		}
		unsent = append(unsent, entry)
	}

	s.mu.Lock()
	restored := make([]*logging.LogEntry, 0, len(unsent)+len(s.logs))
	restored = append(restored, unsent...)
	restored = append(restored, s.logs...)
	maxLogs := s.config.MaxLogsPerNotification()
	if len(restored) > maxLogs {
		dropCount := len(restored) - maxLogs
		for i := 0; i < dropCount; i++ {
			restored[i] = nil
		}
		restored = restored[dropCount:]
	}
	s.logs = restored
	s.mu.Unlock()
}

// スラックの送信と遅延が発生した場合には警告ログ
func (s *SlackLogger) sendSlackWithLatencyCheck(ctx context.Context, targetChannel, msg string) (channel string, timestamp string, err error) {
	start := time.Now()
	channel, timestamp, err = postSlackMessage(ctx, s.slackAPI, targetChannel, msg)
	elapsed := time.Since(start)

	switch {
	case elapsed > s.config.SlackPostErrorThresholdDuration():
		s.handleSlackDelayLog(ctx, pkglogger.LogLevelError, elapsed)
	case elapsed > s.config.SlackPostWarnThresholdDuration():
		s.handleSlackDelayLog(ctx, pkglogger.LogLevelWarn, elapsed)
	}

	return
}

// スラックの送信に時間がかかったらログ
func (s *SlackLogger) handleSlackDelayLog(ctx context.Context, level pkglogger.LogLevel, elapsed time.Duration) {

	switch level {
	case pkglogger.LogLevelError:
		message := fmt.Sprintf(MsgSlackPostDelay, elapsed, s.config.SlackPostErrorThresholdDuration())
		handler.CallErrorHandlerIfPresent(s.OnError, ctx, &errors.SlackLoggerInternalError{Op: OperationSlackPostDelay, Err: nil}, s.InternalErrorEntry(message, "", nil), s.Tracer, s.Meter)
	case pkglogger.LogLevelWarn:
		message := fmt.Sprintf(MsgSlackPostDelay, elapsed, s.config.SlackPostWarnThresholdDuration())
		handler.CallWarnHandlerIfPresent(s.OnWarn, ctx, s.InternalWarnEntry(message, ""), s.Tracer, s.Meter)
	}
}

// メッセージを最大長でカットし、省略記号を追加する
func truncateMessage(message string, maxLength int) string {
	runes := []rune(message) // マルチバイト対応
	if len(runes) > maxLength {
		return string(runes[:maxLength-10]) + " (省略...)"
	}
	return message
}

// scheduleNextBatchLockedは、呼び出し元がmuを保持した状態で次回送信を予約し、通知する
// debug logを返します。呼び出し元はmuを解放してから通知します。
// startup/reset後の次の通知は即時送信し、以降はIntervalsを先頭から使用します。
func (s *SlackLogger) scheduleNextBatchLocked(ctx context.Context) *logging.LogEntry {
	if s.timer != nil {
		delay, _ := slackNotificationDelay(
			s.config.Intervals(),
			s.intervalIndex,
			s.nextNotificationImmediate,
		)
		mes := fmt.Sprintf("scheduleNextBatchを実行したが、すでにタイマーが存在する delay:%v", delay)
		return s.InternalDebugEntry(mes, "")
	}

	delay, normalizedIndex := slackNotificationDelay(
		s.config.Intervals(),
		s.intervalIndex,
		s.nextNotificationImmediate,
	)
	s.intervalIndex = normalizedIndex
	mes := fmt.Sprintf("タイマーをセット delay:%v", delay)
	debugEntry := s.InternalDebugEntry(mes, "")

	s.scheduleBatchTimerLocked(delay, func(logs []*logging.LogEntry) {
		// timer送信はapplication呼び出し元のcancel/deadlineから独立させます。
		// Slack HTTP client自身のtimeoutがnetwork処理の停止境界です。
		_ = s.sendLogs(context.WithoutCancel(ctx), logs)
	})
	return debugEntry
}

// Closeは予約済みtimerを止め、発火済みcallbackの完了を待ってから、
// 残っているbatchを一度送信します。送信成功後の再呼び出しは何もしません。
// 最終送信に失敗した場合はentryを保持し、次のCloseで再試行できます。
func (s *SlackLogger) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	s.mu.Lock()
	if s.closeState == loggercommon.LoggerCloseStateCompleted {
		s.mu.Unlock()
		return nil
	}
	s.closeState = loggercommon.LoggerCloseStateStarted
	s.stopBatchTimerLocked()
	s.mu.Unlock()

	s.timerWG.Wait()
	if err := s.sendBatch(context.Background()); err != nil {
		return err
	}

	s.mu.Lock()
	s.closeState = loggercommon.LoggerCloseStateCompleted
	s.mu.Unlock()
	return nil
}
