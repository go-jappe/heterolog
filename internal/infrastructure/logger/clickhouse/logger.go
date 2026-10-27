package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	clickhouseerrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse/errors"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

const (
	SpanClickHouseLoggerCreate = "internal.infrastructure.logger.clickhouse.create"
	SpanClickHouseLoggerFlush  = "internal.infrastructure.logger.clickhouse.flush"
	SpanClickHouseInsert       = "internal.infrastructure.logger.clickhouse.insert"

	deliveryBackendClickHouse = "clickhouse"
)

type Client interface {
	InsertLogsWithOptions(ctx context.Context, records []pkgclickhouse.LogRecord, opts pkgclickhouse.InsertOptions) (*pkgclickhouse.InsertResponse, error)
}

type ClickHouseLogger struct {
	*LoggerBase
	mu sync.Mutex
	// flushMuはbatch Flush同士と、Closeの最終flush・FlushManager.Closeを直列化します。
	// 待機をchannelで行うため、testing/synctestのbubble内でも待機を観測できます。
	flushMu loggercommon.WaitableMutex
	// closeMuはCloseの呼び出しを直列化します。待機をchannelで行うため、testing/synctestの
	// bubble内でも待機を観測できます。
	closeMu loggercommon.WaitableMutex
	// inFlightは、受付を確定したbatch flush（flushで発生した内部ログの通知を含みます）の
	// 完了を数えます。CloseはcloseStateをStartedにして新規受付を止めた後、この完了を待ちます。
	// 受付はcloseStateの確認と同じmuの下で数えるため、Close開始後に増えることはありません。
	inFlight sync.WaitGroup
	timerWG  sync.WaitGroup
	stopOnce sync.Once
	entries  []*logging.LogEntry
	// entryBytesはentriesと同じindexに、enqueue時に測定したJSONEachRow byte数を保持します。
	// fallback queueのdrop-oldestでも同じ値を差し引きます。
	entryBytes []int
	// entryRequiresFlushはentriesと同じindexに、encoding失敗または単一row hard limit超過を
	// 送信前分類へ直ちに渡す必要があるかを保持します。
	entryRequiresFlush []bool
	// bufferedBytesはentriesを実送信と同じJSONEachRowへ変換した未圧縮byte数です。
	bufferedBytes int
	// bufferRequiresFlushはencoding不能または単一row hard limit超過を送信前分類へ
	// 直ちに渡す必要があることを表します。
	bufferRequiresFlush bool
	lastFlushTime       time.Time
	ticker              *time.Ticker
	stopCh              chan struct{}
	// flushWorkerStartedは各logger instanceがworkerを一つだけ所有することを保証します。
	flushWorkerStarted bool
	closeState         loggercommon.LoggerCloseState
	timeProvider       func() time.Time
	flushIntervalGen   func(time.Duration) *time.Ticker
	flushManager       FlushManager
	resourceProvider   resource.ResourceProvider
	Tracer             pkgobserver.Tracer
	Meter              pkgobserver.Meter
}

var _ logging.BackendLogEntryHandler = (*ClickHouseLogger)(nil)

type ClickHouseLoggerOption func(*ClickHouseLogger)

// WithClock は、試験でログエントリの時刻を固定するための差し替え用オプションです。
func WithClock(clock clock.Clock) ClickHouseLoggerOption {
	return func(l *ClickHouseLogger) {
		l.Clock = clock
	}
}

// WithTimeProvider は、試験で flush 判定や lastFlushTime 更新時刻を固定するための差し替え用オプションです。
func WithTimeProvider(provider func() time.Time) ClickHouseLoggerOption {
	return func(l *ClickHouseLogger) {
		l.timeProvider = provider
	}
}

// WithFlushIntervalGenerator は、試験で ticker 生成を差し替えて定期 flush を制御するためのオプションです。
func WithFlushIntervalGenerator(gen func(time.Duration) *time.Ticker) ClickHouseLoggerOption {
	return func(l *ClickHouseLogger) {
		l.flushIntervalGen = gen
	}
}

func NewClickHouseLogger(
	ctx context.Context,
	cfg ClickHouseConfigProvider,
	client Client,
	fmCfg ClickHouseFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	loggerBase *LoggerBase,
	opts ...ClickHouseLoggerOption,
) (*ClickHouseLogger, error) {
	if cfg == nil {
		return nil, fmt.Errorf("clickhouse logger config is nil")
	}
	if err := sampling.ValidateSamplingConfigProviderFor(pkglogger.LogOutputClickHouse, cfg.Sampling()); err != nil {
		return nil, fmt.Errorf("clickhouse logger sampling config: %w", err)
	}

	logger, err := newClickHouseLogger(
		ctx,
		cfg,
		client,
		fmCfg,
		resourceProvider,
		failureHandler,
		tracer,
		meter,
		loggerBase,
		opts...,
	)
	if err != nil {
		return nil, err
	}
	logger.startNormalBatchFlushWorker(ctx)
	return logger, nil
}

func newClickHouseLogger(
	ctx context.Context,
	cfg ClickHouseConfigProvider,
	client Client,
	fmCfg ClickHouseFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	loggerBase *LoggerBase,
	opts ...ClickHouseLoggerOption,
) (*ClickHouseLogger, error) {
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)

	ctx, span := tracer.StartSpan(ctx, SpanClickHouseLoggerCreate)
	defer span.End()

	if err := validateClickHouseBatchConfig(cfg); err != nil {
		return nil, err
	}
	if err := pkglogger.ValidateBufferedFlushInterval(cfg.FlushInterval()); err != nil {
		return nil, fmt.Errorf("clickhouse logger config is invalid: %w", err)
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

	logger := &ClickHouseLogger{
		LoggerBase:         loggerBase,
		entries:            make([]*logging.LogEntry, 0, cfg.MaxEntries()),
		entryBytes:         make([]int, 0, cfg.MaxEntries()),
		entryRequiresFlush: make([]bool, 0, cfg.MaxEntries()),
		timeProvider:       time.Now,
		flushIntervalGen:   time.NewTicker,
		resourceProvider:   resourceProvider,
		Tracer:             tracer,
		Meter:              meter,
	}
	logger.lastFlushTime = logger.timeProvider()

	for _, opt := range opts {
		opt(logger)
	}

	flushManager := NewClickHouseFlushManager(
		loggerBase,
		client,
		fmCfg,
		failureHandler,
		resourceProvider,
		tracer,
		meter,
	)
	logger.flushManager = flushManager
	// On*の中から同じloggerへ再入した書き込みの内部ログは、通知せず同じtableへ直接書き込みます。
	loggerBase.directInternalLog = newDirectInternalLogWriter(cfg, flushManager.insertInternalLogDirectly)
	return logger, nil
}

// validateClickHouseBatchConfigは通常bufferのsoft threshold、fallback queueのhard上限、
// 物理INSERT requestのhard limitをconstructor境界で検証します。
func validateClickHouseBatchConfig(cfg ClickHouseConfigProvider) error {
	if cfg == nil {
		return fmt.Errorf("clickhouse logger config is nil")
	}
	if err := pkglogger.ValidateClickHouseBatchLimits(
		cfg.MaxEntries(),
		cfg.BufferFlushBytes(),
		cfg.FallbackQueueMaxEntries(),
		cfg.MaxInsertRequestBytes(),
	); err != nil {
		return fmt.Errorf("clickhouse logger config is invalid: %w", err)
	}
	return nil
}

// startNormalBatchFlushWorkerは通常Logger固有のinterval flushを開始します。
// 件数・byte閾値では公開logging method自身が同期flushし、そのerrorを返します。
func (l *ClickHouseLogger) startNormalBatchFlushWorker(ctx context.Context) {
	l.startBatchFlushWorker(ctx, nil, l.flush)
}

// startBatchFlushWorkerは各ClickHouse logger instanceが所有する単一workerを開始します。
// loopだけを共有し、通常・Delivery・fallback固有のwake channelとflush callbackは
// 各型の専用start methodが固定します。
// workerは構築時のctxの値だけを引き継ぎ、cancelとdeadlineからは切り離します。
// 構築後にctxが終了してもCloseまでflushを続け、停止境界はCloseとclientのtimeoutです。
func (l *ClickHouseLogger) startBatchFlushWorker(
	ctx context.Context,
	wakeCh <-chan struct{},
	flush func(context.Context),
) {
	if flush == nil {
		return
	}

	l.mu.Lock()
	if l.flushWorkerStarted || l.closeState != loggercommon.LoggerCloseStateOpen {
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

// stopBatchFlushWorkerは新しいinterval・threshold flushを停止し、実行中のcallbackが
// 完了するまで待ちます。
func (l *ClickHouseLogger) stopBatchFlushWorker() {
	l.mu.Lock()
	ticker := l.ticker
	stopCh := l.stopCh
	l.mu.Unlock()

	l.stopOnce.Do(func() {
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
	})
	l.timerWG.Wait()
}

func (l *ClickHouseLogger) Close(ctx context.Context) error {
	l.closeMu.Lock()
	defer l.closeMu.Unlock()

	l.mu.Lock()
	if l.closeState == loggercommon.LoggerCloseStateCompleted {
		l.mu.Unlock()
		return nil
	}
	l.closeState = loggercommon.LoggerCloseStateStarted
	l.mu.Unlock()

	l.stopBatchFlushWorker()

	// 呼び出し元のgoroutineで実行中のbatch flush（flushで発生した内部ログの通知を含みます）の
	// 完了を待ちます。closeStateをStartedにした後は新しいflushを受け付けません。
	l.inFlight.Wait()

	// FallbackLoggerにも外側のflushMuがありますが、このlockは埋め込まれた
	// normal ClickHouseLoggerのFlushManager操作だけを直列化します。
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	flushErr := l.flushBuffered(ctx)

	l.mu.Lock()
	flushManager := l.flushManager
	l.mu.Unlock()
	var closeErr error
	if flushManager != nil {
		closeErr = flushManager.Close(ctx)
	}

	if closeErr == nil || loggercommon.ShutdownComplete(flushManager) {
		l.mu.Lock()
		l.closeState = loggercommon.LoggerCloseStateCompleted
		l.mu.Unlock()
	}
	return errors.Join(flushErr, closeErr)
}

// HandleLogEntryは、infraのtop-level loggerが生成した一件のLogEntryを
// 未設定のOccurredAtをClockから補完し、ClickHouse固有のlog level、sampling設定を
// 適用してbufferへ渡します。
// 呼び出し元はfan-out先ごとに独立したentryを渡すため、このmethodはentryを直接加工します。
func (l *ClickHouseLogger) HandleLogEntry(
	ctx context.Context,
	entry *logging.LogEntry,
) error {
	if closedErr := l.loggerClosedError(); closedErr != nil {
		return closedErr
	}
	if entry == nil {
		return fmt.Errorf("clickhouse logger entry is nil")
	}
	if entry.Level < l.config.Runtime().LogLevel() {
		if !entry.StackTraceRequested {
			return nil
		}
		l.notifyWarn(
			ctx,
			l.InternalWarnEntry("clickhouse logger log level is higher than error", ""),
			l.Tracer,
			l.Meter,
		)
		return nil
	}
	logging.EnsureEntryOccurredAt(entry, l.Clock)
	if entry.StackTraceRequested {
		// ErrorWithStackにerrorが渡されなかった場合も拒否せず、要求されたstackを持つ
		// ERRORとして記録します。rowのerrorは空のままです。
		if len(entry.FuncName) == 0 && len(entry.File) == 0 && len(entry.Line) == 0 {
			depth := logging.GetCallerDepth(NewStackTraceConfigAdapter(l.config))
			logging.EnsureEntryStackTrace(entry, depth)
		}
	}
	if !sampling.ApplySamplingLogic(entry, l.config.Sampling()) {
		return nil
	}

	return l.enqueue(ctx, entry)
}

func (l *ClickHouseLogger) enqueue(ctx context.Context, entry *logging.LogEntry) error {
	entryBytes, forceFlush := l.measureEntryForBuffer(entry)

	l.mu.Lock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		l.mu.Unlock()
		return pkglogger.ErrLoggerClosed
	}
	l.appendBufferedEntryLocked(entry, entryBytes, forceFlush)
	shouldFlush := l.shouldFlushLocked()
	l.mu.Unlock()

	if !shouldFlush {
		return nil
	}
	return l.doFlush(ctx)
}

func (l *ClickHouseLogger) shouldFlushLocked() bool {
	if l.shouldFlushByBufferLimitsLocked() {
		return true
	}
	now := time.Now()
	if l.timeProvider != nil {
		now = l.timeProvider()
	}
	return now.Sub(l.lastFlushTime) >= l.config.FlushInterval()
}

func (l *ClickHouseLogger) shouldFlushByBufferLimitsLocked() bool {
	return l.bufferRequiresFlush ||
		l.bufferedBytes >= l.config.BufferFlushBytes() ||
		len(l.entries) >= l.config.MaxEntries()
}

// appendBufferedEntryLockedはentriesと測定metadataを同じindexで追加します。
// 呼び出し元はmuを保持します。
func (l *ClickHouseLogger) appendBufferedEntryLocked(
	entry *logging.LogEntry,
	entryBytes int,
	forceFlush bool,
) {
	l.entries = append(l.entries, entry)
	l.entryBytes = append(l.entryBytes, entryBytes)
	l.entryRequiresFlush = append(l.entryRequiresFlush, forceFlush)
	l.bufferedBytes += entryBytes
	l.bufferRequiresFlush = l.bufferRequiresFlush || forceFlush
}

func (l *ClickHouseLogger) flush(ctx context.Context) {
	_ = l.doFlush(ctx)
}

// doFlushは現在のbufferを一度送信します。flush中に発生したこのloggerの内部ログは、
// flushMuを解放してから発生順に通知します（Close待機の対象として数えている間に通知します）。
// flushMuを保持したまま通知すると、通知の中からの同じloggerへの書き込みが閾値でflushし、
// flushMuを待ち続けるためです。Close開始後は、bufferに残ったentryをCloseの最終flushへ任せます。
func (l *ClickHouseLogger) doFlush(ctx context.Context) error {
	if !l.beginInFlight() {
		return nil
	}
	defer l.inFlight.Done()
	ctx, notifyDeferred := l.internalLogReentry.DeferNotifications(ctx)
	err := l.doFlushLocked(ctx)
	notifyDeferred()
	return err
}

// beginInFlightは、loggerが受付中ならClose待機の対象として処理の受付を数えてtrueを返します。
// trueを返した場合、呼び出し元は処理の完了時にl.inFlight.Doneを呼びます。
func (l *ClickHouseLogger) beginInFlight() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		return false
	}
	l.inFlight.Add(1)
	return true
}

func (l *ClickHouseLogger) doFlushLocked(ctx context.Context) error {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	l.mu.Lock()
	closeStarted := l.closeState != loggercommon.LoggerCloseStateOpen
	l.mu.Unlock()
	if closeStarted {
		return nil
	}

	return l.flushBuffered(ctx)
}

// flushBufferedはflushMuを保持した呼び出し元が、現在のbufferを一回限りの送信用
// snapshotとして切り離して送信します。送信結果にかかわらずsnapshotはbufferへ戻しません。
// 空でないsnapshotを切り離した場合は、成否にかかわらずlastFlushTimeを今回の試行時刻へ更新します。
// CloseはcloseStateをStartedにした後の最終flushに限ってこのメソッドを直接呼びます。
func (l *ClickHouseLogger) flushBuffered(ctx context.Context) error {
	l.mu.Lock()
	if len(l.entries) == 0 {
		l.mu.Unlock()
		return nil
	}
	batch := l.entries
	// 一時的に拡張されたbatchのcapacityは引き継がず、設定上のflush件数を
	// entriesと対応metadataの初期容量として使用します。
	nextBufferCapacity := l.config.MaxEntries()
	l.entries = make([]*logging.LogEntry, 0, nextBufferCapacity)
	l.entryBytes = make([]int, 0, nextBufferCapacity)
	l.entryRequiresFlush = make([]bool, 0, nextBufferCapacity)
	l.bufferedBytes = 0
	l.bufferRequiresFlush = false
	flushManager := l.flushManager
	l.mu.Unlock()
	// 切り離したsnapshotは結果にかかわらず再送しないため、送信やdurable処理の失敗
	// （overload policyのdropを含む）も一回の試行としてinterval判定の基準時刻を更新します。
	// 失敗が続いても、件数・byte閾値に達しない限りFlushInterval内に追加のflushを始めません。
	defer l.updateLastFlushTime()
	if flushManager == nil {
		err := fmt.Errorf("ClickHouse flush manager is not initialized")
		l.notifyError(
			ctx,
			l.wrapErrorByLogger(err, "clickhouse flush"),
			l.InternalErrorEntry("failed to flush by ClickHouse logger; flush manager is not initialized; error:", "", err),
			l.Tracer,
			l.Meter,
		)
		return err
	}

	ctx, span := l.Tracer.StartSpan(ctx, SpanClickHouseLoggerFlush)
	defer span.End()

	return flushManager.Flush(ctx, batch)
}

func (l *ClickHouseLogger) measureEntryForBuffer(entry *logging.LogEntry) (int, bool) {
	entryBytes, err := measureClickHouseLogEntryJSONEachRowBytes(
		l.LoggerBase,
		l.resourceProvider,
		entry,
	)
	if err != nil {
		return 0, true
	}
	return entryBytes, entryBytes > l.config.MaxInsertRequestBytes()
}

func (l *ClickHouseLogger) updateLastFlushTime() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.timeProvider != nil {
		l.lastFlushTime = l.timeProvider()
		return
	}
	l.lastFlushTime = time.Now()
}

func (l *ClickHouseLogger) shouldLog(level pkglogger.LogLevel) bool {
	return level >= l.config.Runtime().LogLevel()
}

func (l *ClickHouseLogger) loggerClosedError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		return pkglogger.ErrLoggerClosed
	}
	return nil
}

func (l *ClickHouseLogger) wrapErrorByLogger(err error, operation string) error {
	return &clickhouseerrors.ClickHouseLoggerInternalError{Op: operation, Err: err}
}
