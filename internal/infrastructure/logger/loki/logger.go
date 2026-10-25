package loki

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	lokierrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki/errors"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/utils/jsonutil"
)

const (
	SpanLokiLoggerCreate = "internal.infrastructure.logger.loki.create"
	SpanLokiLoggerFlush  = "internal.infrastructure.logger.loki.flush"
	SpanLokiPush         = "internal.infrastructure.logger.loki.push"

	deliveryBackendLoki = "loki"
)

type Client interface {
	Push(ctx context.Context, streams []pkgloki.Stream) error
}

type LokiLogger struct {
	*LoggerBase
	mu sync.Mutex
	// flushMuはbatch Flush同士と、Closeの最終flush・FlushManager.Closeを直列化します。
	// 待機をchannelで行うため、testing/synctestのbubble内でも待機を観測できます。
	flushMu common.WaitableMutex
	// inFlightは、受付を確定したbatch Flush（flushで発生した内部ログの通知を含みます）、
	// 通常Push、Delivery PushWithReportの完了を数え、送信中のFlushManagerをCloseから
	// 保護します。受付はcloseStateの確認と同じmuの下で数えるため、CloseがcloseStateを
	// Startedにした後に増えることはありません。送信同士は互いに並行でき、Closeだけが
	// この完了を待ちます。
	inFlight sync.WaitGroup
	// closeMuはCloseの呼び出しを直列化します。待機をchannelで行うため、testing/synctestの
	// bubble内でも待機を観測できます。
	closeMu common.WaitableMutex
	timerWG sync.WaitGroup
	entries []*logging.LogEntry
	// entryBytesはentriesと同じindexに、enqueue時のformatter済みline byte数を
	// 保持します。fallbackのdrop-oldestでも同じ測定値を差し引きます。
	entryBytes  []int
	bufferBytes int

	lastFlushTime time.Time
	ticker        *time.Ticker
	stopCh        chan struct{}
	// flushWorkerStartedはbatch workerの二重起動を防ぎます。muで保護します。
	flushWorkerStarted bool
	// closeStateは新規要求の受付可否とCloseの進行状態を一つの値で表します。
	// Close失敗後はStartedを維持し、新規要求を拒否しながらCloseの再試行を許可します。
	closeState       common.LoggerCloseState
	timeProvider     func() time.Time
	flushIntervalGen func(time.Duration) *time.Ticker
	// flushManagerは構築完了前に設定し、運用中は差し替えないLogger所有の送信依存です。
	// FlushとCloseの実行順序はinFlightとflushMuで保護します。
	flushManager FlushManager
	Tracer       observer.Tracer
	Meter        observer.Meter
}

var _ logging.BackendLogEntryHandler = (*LokiLogger)(nil)

type LokiLoggerOption func(*LokiLogger)

// WithClock は、試験でログエントリの時刻を固定するための差し替え用オプションです。
func WithClock(clock clock.Clock) LokiLoggerOption {
	return func(l *LokiLogger) {
		l.Clock = clock
	}
}

// WithTimeProvider は、試験で flush 判定や lastFlushTime 更新時刻を固定するための差し替え用オプションです。
func WithTimeProvider(provider func() time.Time) LokiLoggerOption {
	return func(l *LokiLogger) {
		l.timeProvider = provider
	}
}

// WithFlushIntervalGenerator は、試験で ticker 生成を差し替えて定期 flush を制御するためのオプションです。
func WithFlushIntervalGenerator(gen func(time.Duration) *time.Ticker) LokiLoggerOption {
	return func(l *LokiLogger) {
		l.flushIntervalGen = gen
	}
}

func NewLokiLogger(
	ctx context.Context,
	cfg LokiConfigProvider,
	client Client,
	fmCfg LokiFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	tracer observer.Tracer,
	meter observer.Meter,
	loggerBase *LoggerBase,
	opts ...LokiLoggerOption,
) (*LokiLogger, error) {
	if cfg == nil {
		return nil, fmt.Errorf("loki logger config is nil")
	}
	if err := pkglogger.ValidateBufferedFlushInterval(cfg.FlushInterval()); err != nil {
		return nil, fmt.Errorf("loki logger config is invalid: %w", err)
	}
	if err := sampling.ValidateSamplingConfigProviderFor(pkglogger.LogOutputLoki, cfg.Sampling()); err != nil {
		return nil, fmt.Errorf("loki logger sampling config: %w", err)
	}

	return newLokiLogger(
		ctx,
		cfg,
		client,
		fmCfg,
		resourceProvider,
		failureHandler,
		nil,
		tracer,
		meter,
		loggerBase,
		opts...,
	)
}

func newLokiLogger(
	ctx context.Context,
	cfg LokiConfigProvider,
	client Client,
	fmCfg LokiFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	failureReportHandler *flushfailure.ReportingCompositeFailureHandler,
	tracer observer.Tracer,
	meter observer.Meter,
	loggerBase *LoggerBase,
	opts ...LokiLoggerOption,
) (*LokiLogger, error) {
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)

	ctx, span := tracer.StartSpan(ctx, SpanLokiLoggerCreate)
	defer span.End()

	if err := validateLokiBufferedConfig(cfg); err != nil {
		return nil, err
	}
	if err := pkglogger.ValidateLokiPushLimits(
		cfg.MaxLineBytes(),
		cfg.MaxPushRequestBytes(),
	); err != nil {
		return nil, fmt.Errorf("loki logger push limits are invalid: %w", err)
	}
	if loggerBase == nil {
		loggerBase = &LoggerBase{}
	}
	if loggerBase.Clock == nil {
		loggerBase.Clock = clock.NewUTCClock()
	}
	if loggerBase.LogFormatter.IsZero() {
		timestampFormatter, err := NewConfiguredTimestampFormatter(cfg)
		if err != nil {
			return nil, err
		}
		loggerBase.LogFormatter = formatter.NewPlainTextFormatter(timestampFormatter)
	}
	loggerBase.config = cfg
	loggerBase.Tracer = tracer
	loggerBase.Meter = meter

	logger := &LokiLogger{
		LoggerBase:       loggerBase,
		entries:          make([]*logging.LogEntry, 0, cfg.MaxEntries()),
		entryBytes:       make([]int, 0, cfg.MaxEntries()),
		timeProvider:     time.Now,
		flushIntervalGen: time.NewTicker,
		Tracer:           tracer,
		Meter:            meter,
	}
	logger.lastFlushTime = logger.timeProvider()

	for _, opt := range opts {
		opt(logger)
	}

	flushManager := newLokiFlushManager(
		loggerBase,
		client,
		fmCfg,
		failureHandler,
		failureReportHandler,
		resourceProvider,
		tracer,
		meter,
	)
	logger.flushManager = flushManager
	// On*の中から同じloggerへ再入した送信の内部ログは、通知せず同じ送信先へ直接書き込みます。
	loggerBase.directInternalLog = newDirectInternalLogWriter(cfg, flushManager.FlushWithNoFallback)
	if !logger.noBulkFlushModeEnabled() {
		logger.startBatchFlushWorker(ctx, nil, logger.flush)
	}

	return logger, nil
}

// validateLokiMaxEntriesは、buffer件数上限がconstructor以降で補正を必要としない
// 正の値であることを検証します。
func validateLokiMaxEntries(cfg LokiConfigProvider) error {
	if cfg == nil {
		return fmt.Errorf("loki logger config is nil")
	}
	if err := pkglogger.ValidateLokiMaxEntries(cfg.MaxEntries()); err != nil {
		return fmt.Errorf("loki logger config is invalid: %w", err)
	}
	return nil
}

// validateLokiBufferedConfigは件数とformatter済みline byteの早期flush閾値を
// constructor境界で検証します。BufferFlushBytesはsoft thresholdであり、
// JSON escapeやPush envelopeを含むcanonical requestのhard上限は既存plannerの
// MaxPushRequestBytesが独立して担当するため、両者の大小関係は強制しません。
func validateLokiBufferedConfig(cfg LokiConfigProvider) error {
	if err := validateLokiMaxEntries(cfg); err != nil {
		return err
	}
	if bufferFlushBytes := cfg.BufferFlushBytes(); bufferFlushBytes <= 0 {
		// pkg/loggerの設定検証と同じ不正の種類と項目で返し、errors.Is/Asで判別できるようにします。
		return fmt.Errorf("loki logger config is invalid: %w", &pkglogger.ConfigFieldError{
			Field:  pkglogger.ConfigFieldBufferFlushBytes,
			Detail: fmt.Sprintf("buffer flush bytes must be positive: %d", bufferFlushBytes),
			Value:  bufferFlushBytes,
			Err:    pkglogger.ErrNonPositiveSetting,
		})
	}
	return nil
}

func validateLokiFallbackQueueMaxEntries(cfg LokiConfigProvider) error {
	if cfg == nil {
		return fmt.Errorf("loki logger config is nil")
	}
	if queueMaxEntries := cfg.FallbackQueueMaxEntries(); queueMaxEntries <= 0 {
		// pkg/loggerの設定検証と同じ不正の種類と項目で返し、errors.Is/Asで判別できるようにします。
		return fmt.Errorf("loki fallback logger config is invalid: %w", &pkglogger.ConfigFieldError{
			Field:  pkglogger.ConfigFieldFallbackQueueMaxEntries,
			Detail: fmt.Sprintf("fallback queue max entries must be positive: %d", queueMaxEntries),
			Value:  queueMaxEntries,
			Err:    pkglogger.ErrNonPositiveSetting,
		})
	}
	return nil
}

// startBatchFlushWorkerは、各LokiLogger instanceが個別に所有するbatch workerを
// 開始します。worker loopだけを共用し、wake契機とflush処理は起動時に呼び出し元が固定します。
// buffer、FlushManager、client、送信先はinstanceごとに独立します。
// workerは構築時のctxの値だけを引き継ぎ、cancelとdeadlineからは切り離します。
// 構築後にctxが終了してもCloseまでflushを続け、停止境界はCloseとclientのtimeoutです。
func (l *LokiLogger) startBatchFlushWorker(
	ctx context.Context,
	wakeCh <-chan struct{},
	flush func(context.Context),
) {
	if flush == nil {
		return
	}

	l.mu.Lock()
	if l.flushWorkerStarted || l.closeState != common.LoggerCloseStateOpen {
		l.mu.Unlock()
		return
	}
	if l.flushIntervalGen == nil {
		l.flushIntervalGen = time.NewTicker
	}
	l.ticker = l.flushIntervalGen(l.config.FlushInterval())
	l.stopCh = make(chan struct{})
	l.flushWorkerStarted = true
	ticker := l.ticker
	stopCh := l.stopCh
	workerCtx := context.WithoutCancel(ctx)

	l.timerWG.Add(1)
	l.mu.Unlock()
	go func() {
		defer l.timerWG.Done()
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				flush(workerCtx)
			case <-wakeCh:
				flush(workerCtx)
			case <-stopCh:
				return
			}
		}
	}()
}

// stopBatchFlushWorkerは新しいinterval・閾値flushを停止し、実行中のcallbackが
// 完了するまで待ちます。Close再試行時はstopChのclosed判定によって二重closeを防ぎます。
func (l *LokiLogger) stopBatchFlushWorker() {
	l.mu.Lock()
	ticker := l.ticker
	stopCh := l.stopCh
	l.mu.Unlock()

	if ticker != nil {
		ticker.Stop()
	}
	if stopCh != nil {
		select {
		case <-stopCh:
		default:
			close(stopCh)
		}
	}
	l.timerWG.Wait()
}

func (l *LokiLogger) Close(ctx context.Context) error {
	l.closeMu.Lock()
	defer l.closeMu.Unlock()

	if !l.beginClose() {
		return nil
	}

	l.stopBatchFlushWorker()

	l.lockBatchFlushForClose()
	defer l.unlockBatchFlushForClose()

	flushErr := l.flushBuffered(ctx)
	closeErr := l.closeFlushManager(ctx)
	if closeErr == nil || common.ShutdownComplete(l.flushManager) {
		l.completeClose()
	}
	return errors.Join(flushErr, closeErr)
}

// beginCloseは新規entryの受理を停止します。Completedならfalseを返し、Startedなら
// Close再試行としてtrueを返します。呼び出し元はcloseMuを保持します。
func (l *LokiLogger) beginClose() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closeState == common.LoggerCloseStateCompleted {
		return false
	}
	l.closeState = common.LoggerCloseStateStarted
	return true
}

// lockBatchFlushForCloseは進行中の即時送信・batch送信を待ち、最終flushと
// FlushManager.Closeを同じ排他境界へ置きます。呼び出し元がcloseStateをStartedにした後は
// 新しい送信を受け付けないため、受付済みの送信の完了だけを待ちます。
func (l *LokiLogger) lockBatchFlushForClose() {
	l.inFlight.Wait()
	l.flushMu.Lock()
}

func (l *LokiLogger) unlockBatchFlushForClose() {
	l.flushMu.Unlock()
}

// beginInFlightは、loggerが受付中ならClose待機の対象として送信の受付を数えてtrueを返します。
// trueを返した場合、呼び出し元は送信の完了時にl.inFlight.Doneを呼びます。
func (l *LokiLogger) beginInFlight() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closeState != common.LoggerCloseStateOpen {
		return false
	}
	l.inFlight.Add(1)
	return true
}

func (l *LokiLogger) closeFlushManager(ctx context.Context) error {
	l.mu.Lock()
	flushManager := l.flushManager
	l.mu.Unlock()
	if flushManager == nil {
		return nil
	}
	return flushManager.Close(ctx)
}

func (l *LokiLogger) completeClose() {
	l.mu.Lock()
	l.closeState = common.LoggerCloseStateCompleted
	l.mu.Unlock()
}

// HandleLogEntryはtop-level loggerで生成済みのentryをLoki用に加工し、
// 即時Pushまたはbatch bufferへ渡します。発生時刻・ID・Fieldsは作り直さず、
// LogFormatter、stack trace、samplingだけをこの保存先の設定から適用します。
func (l *LokiLogger) HandleLogEntry(
	ctx context.Context,
	entry *logging.LogEntry,
) error {
	if err := l.loggerClosedError(); err != nil {
		return err
	}
	if entry == nil {
		return fmt.Errorf("Loki log entry is nil")
	}
	if entry.Level < l.config.Runtime().LogLevel() {
		if entry.StackTraceRequested {
			l.notifyWarn(
				ctx,
				l.InternalWarnEntry("loki logger log level is higher than error", ""),
				l.Tracer,
				l.Meter,
			)
		}
		return nil
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
	if !sampling.ApplySamplingLogic(entry, l.config.Sampling()) {
		return nil
	}

	if l.noBulkFlushModeEnabled() {
		return l.pushImmediately(ctx, entry)
	}

	return l.enqueue(ctx, entry)
}

func (l *LokiLogger) enqueue(ctx context.Context, entry *logging.LogEntry) error {
	entryBytes, err := l.measureBufferedEntryBytes(entry)
	if err != nil {
		return err
	}

	l.mu.Lock()
	if l.closeState != common.LoggerCloseStateOpen {
		l.mu.Unlock()
		return applogging.ErrLoggerClosed
	}
	l.entries = append(l.entries, entry)
	l.entryBytes = append(l.entryBytes, entryBytes)
	l.bufferBytes += entryBytes
	shouldFlush := l.shouldFlushLocked()
	l.mu.Unlock()

	if !shouldFlush {
		return nil
	}

	// 通常Loggerは閾値到達時の送信完了を待ち、失敗を呼び出し元へ返します。
	return l.doFlush(ctx)
}

func (l *LokiLogger) shouldFlushLocked() bool {
	if l.noBulkFlushModeEnabled() {
		return false
	}
	if len(l.entries) >= l.config.MaxEntries() {
		return true
	}
	if l.bufferBytes >= l.config.BufferFlushBytes() {
		return true
	}
	return l.flushIntervalElapsedLocked()
}

// flushIntervalElapsedLockedは前回のflush時刻から設定間隔以上が経過したかを返します。
// 呼び出し元はl.muを保持していなければなりません。
func (l *LokiLogger) flushIntervalElapsedLocked() bool {
	now := time.Now()
	if l.timeProvider != nil {
		now = l.timeProvider()
	}
	return now.Sub(l.lastFlushTime) >= l.config.FlushInterval()
}

func (l *LokiLogger) flush(ctx context.Context) {
	_ = l.doFlush(ctx)
}

// doFlushは現在のbufferを一度送信します。flush中に発生したこのloggerの内部ログは、
// flushMuを解放してから発生順に通知します（Close待機の対象として数えている間に通知します）。
// flushMuを保持したまま通知すると、通知の中からの同じloggerへの書き込みが閾値でflushし、
// flushMuを待ち続けるためです。
func (l *LokiLogger) doFlush(ctx context.Context) error {
	return l.runBatchFlush(ctx, l.flushBuffered)
}

// runBatchFlushは通常LoggerとFallbackに共通するlifecycle／flush lockと、Close開始後の
// callback抑止だけを担当します。実際の送信契約は起動時に選ばれたcallbackが実装します。
func (l *LokiLogger) runBatchFlush(
	ctx context.Context,
	flush func(context.Context) error,
) error {
	if !l.beginInFlight() {
		return nil
	}
	defer l.inFlight.Done()
	ctx, notifyDeferred := l.internalLogReentry.DeferNotifications(ctx)
	err := l.runBatchFlushLocked(ctx, flush)
	notifyDeferred()
	return err
}

func (l *LokiLogger) runBatchFlushLocked(
	ctx context.Context,
	flush func(context.Context) error,
) error {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	// flush lockを待つ間にCloseが開始した場合は、Closeの最終flushへ任せます。
	l.mu.Lock()
	closeStarted := l.closeState != common.LoggerCloseStateOpen
	l.mu.Unlock()
	if closeStarted {
		return nil
	}

	return flush(ctx)
}

// flushBuffered はflushMuを保持し、Closeの待機対象として数えられた呼び出し元が、
// 現在のbufferを一度送信します。CloseはcloseStateをStartedへ遷移させた後の最終flushに限って
// このメソッドを直接呼びます。空でないsnapshotを切り離した場合は、成否にかかわらず
// lastFlushTimeを今回の試行時刻へ更新します。
func (l *LokiLogger) flushBuffered(ctx context.Context) error {
	if l.noBulkFlushModeEnabled() {
		return nil
	}

	batch := l.detachBufferedBatch(
		l.config.MaxEntries(),
	)
	if len(batch) == 0 {
		return nil
	}
	// 切り離したsnapshotは結果にかかわらず再送しないため、送信やdurable処理の失敗
	// （overload policyのdropを含む）も一回の試行としてinterval判定の基準時刻を更新します。
	// 失敗が続いても、件数・byte閾値に達しない限りFlushInterval内に追加のflushを始めません。
	defer l.updateLastFlushTime()
	flushManager := l.flushManager
	if flushManager == nil {
		err := fmt.Errorf("Loki flush manager is not initialized")
		l.notifyError(
			ctx,
			l.wrapErrorByLogger(err, "loki flush"),
			l.InternalErrorEntry("failed to flush by Loki logger; flush manager is not initialized; error:", "", err),
			l.Tracer,
			l.Meter,
		)
		return err
	}

	ctx, span := l.Tracer.StartSpan(ctx, SpanLokiLoggerFlush)
	defer span.End()

	return flushManager.Flush(ctx, batch)
}

// detachBufferedBatchは現在のbufferを一回限りの送信用snapshotとして切り離します。
// 切り離したsnapshotは送信結果にかかわらず通常bufferへ戻しません。
// nextBufferCapacityには、呼び出し側のbuffer契約に対応する設定件数を指定します。
func (l *LokiLogger) detachBufferedBatch(nextBufferCapacity int) []*logging.LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.entries) == 0 {
		return nil
	}

	batch := l.entries
	// 以後の追加は別bufferへ流し、backend送信中はmuを保持しません。一時的に
	// 拡張されたsnapshotのcapacityは引き継がず、設定上の容量へ戻します。
	l.entries = make([]*logging.LogEntry, 0, nextBufferCapacity)
	l.entryBytes = make([]int, 0, nextBufferCapacity)
	l.bufferBytes = 0
	return batch
}

// measureBufferedEntryBytesはbufferのsoft thresholdだけに使う測定です。
// formatter済みlineのUTF-8 byte数を数え、JSON escape、timestamp配列、labels、
// Push envelopeは含めません。それらを含むhard request上限は
// logbackend/loki.PlanPushRequestsがcanonical JSONで正確に適用します。
func (l *LokiLogger) measureBufferedEntryBytes(entry *logging.LogEntry) (size int, err error) {
	if l == nil {
		return 0, fmt.Errorf("Loki logger is nil")
	}
	if entry == nil {
		return 0, fmt.Errorf("Loki log entry is nil")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			size = 0
			err = fmt.Errorf("format Loki log entry for buffer measurement: %v", recovered)
		}
	}()
	return jsonutil.UTF8StringBytes(l.LogFormatter.Format(entry)), nil
}

func (l *LokiLogger) pushImmediately(ctx context.Context, entry *logging.LogEntry) error {
	// Close開始後の呼び出しは拒否し、受付済みの送信だけをCloseの待機対象にします。
	l.mu.Lock()
	if l.closeState != common.LoggerCloseStateOpen {
		l.mu.Unlock()
		return applogging.ErrLoggerClosed
	}
	flushManager := l.flushManager
	l.inFlight.Add(1)
	l.mu.Unlock()
	defer l.inFlight.Done()

	if flushManager == nil {
		err := fmt.Errorf("Loki flush manager is not initialized")
		l.notifyError(
			ctx,
			l.wrapErrorByLogger(err, "loki push"),
			l.InternalErrorEntry("failed to push by Loki logger; flush manager is not initialized; error:", "", err),
			l.Tracer,
			l.Meter,
		)
		return err
	}

	return flushManager.Push(ctx, entry)
}

func (l *LokiLogger) updateLastFlushTime() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.timeProvider != nil {
		l.lastFlushTime = l.timeProvider()
		return
	}
	l.lastFlushTime = time.Now()
}

func (l *LokiLogger) shouldLog(level pkglogger.LogLevel) bool {
	return level >= l.config.Runtime().LogLevel()
}

// noBulkFlushModeEnabledは、bufferへ蓄積せずlog呼び出しごとに1件をPushする設定を返します。
func (l *LokiLogger) noBulkFlushModeEnabled() bool {
	return l.config.NoBulkFlushModeEnabled()
}

func (l *LokiLogger) loggerClosedError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closeState != common.LoggerCloseStateOpen {
		return applogging.ErrLoggerClosed
	}
	return nil
}

func (l *LokiLogger) wrapErrorByLogger(err error, operation string) error {
	return &lokierrors.LokiLoggerInternalError{Op: operation, Err: err}
}
