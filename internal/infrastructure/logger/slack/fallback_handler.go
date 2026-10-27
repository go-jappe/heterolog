package slack

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// SlackのFallBackHandlerはError以外の使用は非推奨 (エラー通知間隔で使用するものを想定)
// Error以外の通知に使ったら、通知の頻度が鬱陶しいはず

func NewNamedErrorFallbackHandler(adapter SlackLoggerConfigProvider) logging.NamedLoggerErrorHandler {
	controller := newFallbackHandlerController(adapter)
	return logging.NamedLoggerErrorHandler{
		Name:    common.HandlerNameSlackLogger,
		Handler: controller.HandleError,
		Closer:  controller,
	}
}

func NewNamedWarnFallbackHandler(adapter SlackLoggerConfigProvider) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(adapter)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameSlackLogger,
		Handler: controller.Handle,
		Level:   logger.LogLevelWarn,
		Closer:  controller,
	}
}

func NewNamedInfoFallbackHandler(adapter SlackLoggerConfigProvider) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(adapter)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameSlackLogger,
		Handler: controller.Handle,
		Level:   logger.LogLevelInfo,
		Closer:  controller,
	}
}

func NewNamedDebugFallbackHandler(adapter SlackLoggerConfigProvider) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(adapter)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameSlackLogger,
		Handler: controller.Handle,
		Level:   logger.LogLevelDebug,
		Closer:  controller,
	}
}

// fallbackHandlerControllerはhandler closureと遅延生成されるFallbackLoggerを
// 同じ所有単位にまとめ、top-level loggerのCloseへbuffer/timerを公開します。
type fallbackHandlerController struct {
	adapter SlackLoggerConfigProvider

	mu      sync.Mutex
	closeMu sync.Mutex
	active  sync.WaitGroup
	closing bool
	// initializationAttemptedはFallbackLoggerの遅延初期化を試行済みかを表します。
	// 初期化に成功したかはloggerの有無で判断し、失敗後の再試行は行いません。
	initializationAttempted bool
	// initializationErrは遅延初期化に失敗した理由です。loggerを作れなかった場合は、
	// 通知を黙って捨てないよう、通知ごとにこの理由を標準loggerへ一行残します。
	initializationErr error
	logger            *FallbackLogger
}

var _ io.Closer = (*fallbackHandlerController)(nil)

func newFallbackHandlerController(adapter SlackLoggerConfigProvider) *fallbackHandlerController {
	return &fallbackHandlerController{adapter: adapter}
}

func (h *fallbackHandlerController) acquire(
	ctx context.Context,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*FallbackLogger, bool) {
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		return nil, false
	}

	// acquireはclosingの確認とactive.Add(1)を同じmuの下で行います。
	// Closeも同じmuの下でclosingをtrueにしてからactive.Wait()へ進むため、
	// Wait開始後に新しいhandlerが実行中として登録されることはありません。
	h.active.Add(1)
	if !h.initializationAttempted {
		h.initializationAttempted = true
		fallbackLogger, err := newHandleSlackLogger(ctx, h.adapter, tracer, meter)
		if err != nil {
			h.initializationErr = err
		} else {
			h.logger = fallbackLogger
		}
	}
	fallbackLogger, initializationErr := h.logger, h.initializationErr
	h.mu.Unlock()

	if fallbackLogger == nil {
		h.active.Done()
		if initializationErr != nil {
			// fallback logger自身の初期化失敗へ別のfallbackを重ねると再帰し得るため、
			// Go標準logger（通常はstderr）だけへ記録します。初期化は再試行しませんが、通知を
			// 黙って捨てないよう、作れなかった通知先への通知ごとに一行残します。
			log.Printf(MsgFailedCreateFallbackLogger, initializationErr)
		}
		return nil, false
	}
	return fallbackLogger, true
}

func (h *fallbackHandlerController) release() {
	h.active.Done()
}

func (h *fallbackHandlerController) Handle(
	ctx context.Context,
	entry *logging.LogEntry,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) {
	fallbackLogger, ok := h.acquire(ctx, tracer, meter)
	if !ok {
		return
	}
	defer h.release()

	logging.EnsureEntryOccurredAt(entry, fallbackLogger.Clock)

	fallbackLogger.enqueue(entry)
}

func (h *fallbackHandlerController) HandleError(
	ctx context.Context,
	_ error,
	entry *logging.LogEntry,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) {
	h.Handle(ctx, entry, tracer, meter)
}

func (h *fallbackHandlerController) Close() error {
	if h == nil {
		return nil
	}

	h.closeMu.Lock()
	defer h.closeMu.Unlock()

	h.mu.Lock()
	h.closing = true
	fallbackLogger := h.logger
	h.mu.Unlock()

	h.active.Wait()
	if fallbackLogger == nil {
		return nil
	}
	return fallbackLogger.Close()
}

type FallbackLogger struct {
	*SlackLogger
}

// 一つのsource・level専用handlerが所有するinternal log用loggerです。
// fallback専用のenqueue/sendBatchを使い、Slack投稿の失敗はhandlerへ通知せず標準loggerへ残します。
func newHandleSlackLogger(
	ctx context.Context,
	cfg SlackLoggerConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*FallbackLogger, error) {
	_, span := tracer.StartSpan(ctx, SpanFallbackSlackLoggerCreate)
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

	fl := &SlackLogger{
		Clock:                     clock.NewUTCClock(),
		LogFormatter:              formatter.NewPlainTextFormatter(timestampFormatter),
		logs:                      make([]*logging.LogEntry, 0, cfg.MaxLogsPerNotification()),
		slackAPI:                  newSlackAPI(cfg.Token(), cfg.Timeout(), cfg.HTTPClient(), cfg.Runtime().IsDebug()),
		lastLogTime:               time.Now(),
		intervalIndex:             0,
		nextNotificationImmediate: true,
		config:                    cfg,
		Tracer:                    tracer,
		Meter:                     meter,
	}

	// FallbackLogger 側の batch 送信は通常 logger と異なり、送信失敗を再 fallback しない。
	exfl := &FallbackLogger{SlackLogger: fl}

	return exfl, nil
}

// enqueue はログをバッファに追加し、送信スケジュールを更新
func (s *FallbackLogger) enqueue(entry *logging.LogEntry) {
	if s == nil || entry == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeState != common.LoggerCloseStateOpen {
		return
	}

	now := time.Now()
	resetThreshold := s.config.MaxRestThreshold()
	if !s.lastLogTime.IsZero() &&
		now.Sub(s.lastLogTime) > resetThreshold {
		s.stopBatchTimerLocked()
		s.intervalIndex = 0
		s.nextNotificationImmediate = true
	}

	// メモリ節約のためにログが maxLogs を超えたら古いものを削除
	maxLogs := s.config.MaxLogsPerNotification()
	if len(s.logs) >= maxLogs {
		s.logs = s.logs[1:] // 先頭の古いログを削除
	}

	s.logs = append(s.logs, entry)
	s.lastLogTime = now // 最終ログ時間を更新

	s.scheduleNextBatchLocked()
}

// scheduleNextBatchLocked は、呼び出し元がs.muを保持した状態で次回送信を予約します。
// startup/reset後の次の通知は即時送信し、以降はIntervalsを先頭から使用します。
func (s *FallbackLogger) scheduleNextBatchLocked() {
	if s.closeState != common.LoggerCloseStateOpen || s.timer != nil {
		return
	}

	delay, normalizedIndex := slackNotificationDelay(
		s.config.Intervals(),
		s.intervalIndex,
		s.nextNotificationImmediate,
	)
	s.intervalIndex = normalizedIndex
	s.scheduleBatchTimerLocked(delay, s.sendLogs)
}

// sendBatch はtimer状態と送信対象entryをlock内で確定し、Slack送信はlock外で行います。
// timerを先にnilへ戻すため、送信中に追加されたentryは必ず次のtimerを予約できます。
// fallback処理で発生した送信errorは、再帰的なhandler呼び出しを避けるためhandlerへ通知せず、
// sendLogsが標準loggerへ残します。
func (s *FallbackLogger) sendBatch() {
	if s == nil {
		return
	}

	// timer発火間隔よりSlack送信が長い場合でも、同じfallback loggerから
	// PostMessageを並行実行しないようbatch送信全体を直列化します。
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	s.mu.Lock()
	s.stopBatchTimerLocked()
	logs := s.takeBufferedLogsLocked()
	s.mu.Unlock()
	s.sendLogs(logs)
}

// sendLogsはsendMuを保持した呼び出し元が取り出し済みのbatchを送信します。
// timer callbackは自身の所有権を確認済みで、後から予約されたtimerには触れません。
func (s *FallbackLogger) sendLogs(logs []*logging.LogEntry) {
	if s.slackAPI == nil {
		return
	}

	for _, batch := range slackChannelBatches(s.config, logs, s.LogFormatter) {
		message := fmt.Sprintf("ログ一覧:%s", batch.messages)
		msgToSend := message
		maxMessageLength := s.config.MaxMessageLength()
		if maxMessageLength > 10 && len([]rune(message)) > maxMessageLength {
			msgToSend = truncateMessage(message, maxMessageLength)
		}

		// fallback送信の結果はhandlerへ通知せず（通知の再帰を避けます）、他の通知先のfallback
		// loggerと同じく、失敗した送信ごとに標準loggerへ一行残します。
		if _, _, err := postSlackMessage(context.Background(), s.slackAPI, batch.channel, msgToSend); err != nil {
			log.Printf("failed to flush Slack fallback logger: %v", err)
		}
	}
}

// Closeはhandler controllerがtop-level loggerのCloseから呼び出します。
// 予約済みtimerを止め、発火済みcallbackを待ってから残りのbatchを
// fallback専用の非再帰sendBatchで送信します。
func (s *FallbackLogger) Close() error {
	if s == nil || s.SlackLogger == nil {
		return nil
	}

	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	s.mu.Lock()
	if s.closeState == common.LoggerCloseStateCompleted {
		s.mu.Unlock()
		return nil
	}
	s.closeState = common.LoggerCloseStateStarted
	s.stopBatchTimerLocked()
	s.mu.Unlock()

	s.timerWG.Wait()
	s.sendBatch()

	s.mu.Lock()
	s.closeState = common.LoggerCloseStateCompleted
	s.mu.Unlock()
	return nil
}
