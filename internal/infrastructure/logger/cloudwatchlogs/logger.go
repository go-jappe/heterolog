// Package cloudwatchlogs は、アプリケーション内で生成された LogEntry を
// CloudWatch Logs の PutLogEvents へ送信するロガー実装を提供します。
//
// 通常のバッファ送信、即時送信、デリバリーロガー、フォールバックロガーは、
// 同じFlushManager契約と実装を利用します。通常・delivery送信に失敗したレコードは
// backend共通のfailure handlerへ渡し、fallback送信は再帰を避けるため一度で破棄します。
package cloudwatchlogs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	cloudwatchlogserrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs/errors"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

const (
	// SpanCloudWatchLogsLoggerCreate は、CloudWatch Logs ロガーの生成処理に使用するスパン名です。
	SpanCloudWatchLogsLoggerCreate = "internal.infrastructure.logger.cloudwatchlogs.create"

	// SpanCloudWatchLogsLoggerFlush は、バッファをフラッシュする処理に使用するスパン名です。
	SpanCloudWatchLogsLoggerFlush = "internal.infrastructure.logger.cloudwatchlogs.flush"

	// SpanCloudWatchLogsPut は、PutLogEvents の呼び出しに使用するスパン名です。
	SpanCloudWatchLogsPut = "internal.infrastructure.logger.cloudwatchlogs.put"

	deliveryBackendCloudWatchLogs = "cloudwatchlogs"
)

// CloudWatchLogsLogger は、通常ログを一時的にバッファへ蓄積し、
// 設定に応じて CloudWatch Logs へ送信します。
//
// NoBulkFlushModeEnabledが有効な場合は、バッファを使用せずに1件ずつ送信します。
// それ以外の場合は、BufferFlushMaxEvents、BufferFlushBytesのいずれかへの
// 到達時、または FlushInterval の経過時に、バッチを FlushManager へ渡します。
type CloudWatchLogsLogger struct {
	*LoggerBase

	// mu は、バッファとロガーの終了状態を保護します。
	mu sync.Mutex
	// inFlight は、受付を確定した処理（即時送信、batch flush とその flush で発生した内部ログの
	// 通知）の完了を数えます。即時送信同士の並行実行を許可しながら、Close が実行中の処理を
	// 待ちます。受付は closeState の確認と同じ mu の下で数えるため、Close 開始後に FlushManager の
	// 送信処理が始まることはありません。
	inFlight sync.WaitGroup
	// flushMu は、タイマー、件数上限、Close 時の最終フラッシュから
	// バッチフラッシュが同時に呼び出されないよう直列化します。待機を channel で行うため、
	// testing/synctest の bubble 内でも待機を観測できます。
	flushMu common.WaitableMutex
	// closeMu は、複数の Close 呼び出しが重ならないよう直列化します。待機を channel で行うため、
	// testing/synctest の bubble 内でも待機を観測できます。
	closeMu common.WaitableMutex
	// timerWG は、Close が最終フラッシュを行う前に、定期フラッシュ用ゴルーチンの終了を待つために使用します。
	timerWG sync.WaitGroup
	// stopOnce は、Close を再試行した際に stopCh を二重に閉じてパニックになることを防ぎます。
	stopOnce sync.Once

	entries []*logging.LogEntry
	// bufferBytesはentriesをCloudWatch Logs eventへformatしたmessageの
	// UTF-8 byte数に26 bytes/eventを加えた概算合計です。muで保護します。
	bufferBytes   int
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
	Tracer       pkgobserver.Tracer
	Meter        pkgobserver.Meter
}

var _ logging.BackendLogEntryHandler = (*CloudWatchLogsLogger)(nil)

// CloudWatchLogsLoggerOption は、テストや特殊な実行環境で時計やティッカーを差し替えるためのオプションです。
type CloudWatchLogsLoggerOption func(*CloudWatchLogsLogger)

// WithClock は、LogEntry の時刻生成に使用する時計を差し替えます。
// 主にテストでタイムスタンプを固定するために使用します。
func WithClock(clock clock.Clock) CloudWatchLogsLoggerOption {
	return func(l *CloudWatchLogsLogger) {
		l.Clock = clock
	}
}

// WithTimeProvider は、フラッシュ間隔の判定と lastFlushTime の更新に使用する、
// 現在時刻の取得関数を差し替えます。主にテストで判定時刻を固定したり、進めたりするために使用します。
func WithTimeProvider(provider func() time.Time) CloudWatchLogsLoggerOption {
	return func(l *CloudWatchLogsLogger) {
		l.timeProvider = provider
	}
}

// WithFlushIntervalGenerator は、定期フラッシュ用ティッカーの生成関数を差し替えます。
// 主にテストで実時間のティッカーに依存せず、フラッシュのタイミングを制御するために使用します。
func WithFlushIntervalGenerator(gen func(time.Duration) *time.Ticker) CloudWatchLogsLoggerOption {
	return func(l *CloudWatchLogsLogger) {
		l.flushIntervalGen = gen
	}
}

// NewCloudWatchLogsLogger は、CloudWatch Logs ロガーと FlushManager を初期化します。
// バルクモードでは生成時に定期フラッシュを開始し、即時送信モードではティッカーを使用しません。
// 定期フラッシュは構築時のctxが終了した後もCloseまで続きます。
func NewCloudWatchLogsLogger(
	ctx context.Context,
	cfg CloudWatchLogsConfigProvider,
	client pkgcloudwatchlogs.Client,
	fmCfg CloudWatchLogsFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	loggerBase *LoggerBase,
	opts ...CloudWatchLogsLoggerOption,
) (*CloudWatchLogsLogger, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cloudwatch logs logger config is nil")
	}
	if err := pkglogger.ValidateBufferedFlushInterval(cfg.FlushInterval()); err != nil {
		return nil, fmt.Errorf("cloudwatch logs logger config is invalid: %w", err)
	}
	if err := sampling.ValidateSamplingConfigProviderFor(pkglogger.LogOutputCloudWatchLogs, cfg.Sampling()); err != nil {
		return nil, fmt.Errorf("cloudwatch logs logger sampling config: %w", err)
	}

	return newCloudWatchLogsLogger(
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

func newCloudWatchLogsLogger(
	ctx context.Context,
	cfg CloudWatchLogsConfigProvider,
	client pkgcloudwatchlogs.Client,
	fmCfg CloudWatchLogsFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	failureReportHandler *flushfailure.ReportingCompositeFailureHandler,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	loggerBase *LoggerBase,
	opts ...CloudWatchLogsLoggerOption,
) (*CloudWatchLogsLogger, error) {
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)

	ctx, span := tracer.StartSpan(ctx, SpanCloudWatchLogsLoggerCreate)
	defer span.End()

	if cfg == nil {
		return nil, fmt.Errorf("cloudwatch logs logger config is nil")
	}
	if err := validateCloudWatchLogsBufferConfig(cfg); err != nil {
		return nil, fmt.Errorf("cloudwatch logs logger config is invalid: %w", err)
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

	logger := &CloudWatchLogsLogger{
		LoggerBase:       loggerBase,
		entries:          make([]*logging.LogEntry, 0, cfg.BufferFlushMaxEvents()),
		timeProvider:     time.Now,
		flushIntervalGen: time.NewTicker,
		Tracer:           tracer,
		Meter:            meter,
	}
	logger.lastFlushTime = logger.timeProvider()

	for _, opt := range opts {
		opt(logger)
	}
	if fmCfg == nil {
		return nil, fmt.Errorf("cloudwatch logs flush manager config is nil")
	}
	streamNameResolver, err := infracloudwatchlogs.NewLogStreamNameResolver(
		fmCfg.LogStreamName(),
		fmCfg.LogStreamRoutingGranularity(),
	)
	if err != nil {
		return nil, fmt.Errorf("create cloudwatch logs stream name resolver: %w", err)
	}

	flushManager := newCloudWatchLogsFlushManager(
		loggerBase,
		client,
		streamNameResolver,
		fmCfg,
		failureHandler,
		failureReportHandler,
		resourceProvider,
		tracer,
		meter,
	)
	logger.flushManager = flushManager
	// On*の中から同じloggerへ再入した送信の内部ログは、通知せず同じlog streamへ直接書き込みます。
	loggerBase.directInternalLog = newDirectInternalLogWriter(cfg, flushManager.FlushWithNoFallback)
	if !logger.noBulkFlushModeEnabled() {
		// workerは構築時のctxの値だけを引き継ぎ、cancelとdeadlineからは切り離します。
		// 構築後にctxが終了してもCloseまでflushを続け、停止境界はCloseとclientのtimeoutです。
		workerCtx := context.WithoutCancel(ctx)
		logger.startBatchFlushWorker(nil, func() {
			logger.flush(workerCtx)
		})
	}

	return logger, nil
}

// startBatchFlushWorkerは各logger instanceが個別に所有するbatch workerを開始します。
// worker loop、ticker、停止待機だけを通常LoggerとFallbackで共用し、wake channelとflush処理は
// 呼び出し元が明示的に渡します。buffer、FlushManager、client、送信先はinstanceごとに独立します。
func (l *CloudWatchLogsLogger) startBatchFlushWorker(
	wakeCh <-chan struct{},
	flushBatch func(),
) {
	if flushBatch == nil {
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

	l.timerWG.Add(1)
	l.mu.Unlock()
	go func() {
		defer l.timerWG.Done()
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				flushBatch()
			case <-wakeCh:
				flushBatch()
			case <-stopCh:
				return
			}
		}
	}()
}

// stopBatchFlushWorkerは新しいinterval・閾値flushを停止し、実行中のcallbackが
// 完了するまで待ちます。Close再試行時のstopCh二重closeはstopOnceで防ぎます。
func (l *CloudWatchLogsLogger) stopBatchFlushWorker() {
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

// beginCloseは新規entryの受付を停止します。falseはClose完了済みを表し、Startedは
// normal Close失敗後の再試行として引き続き処理します。
func (l *CloudWatchLogsLogger) beginClose() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closeState == common.LoggerCloseStateCompleted {
		return false
	}
	l.closeState = common.LoggerCloseStateStarted
	return true
}

func (l *CloudWatchLogsLogger) completeClose() {
	l.mu.Lock()
	l.closeState = common.LoggerCloseStateCompleted
	l.mu.Unlock()
}

// closeFlushManagerはlogger instanceが所有するFlushManagerの終了処理を呼びます。
func (l *CloudWatchLogsLogger) closeFlushManager(ctx context.Context) error {
	l.mu.Lock()
	flushManager := l.flushManager
	l.mu.Unlock()
	if flushManager == nil {
		return nil
	}
	return flushManager.Close(ctx)
}

// Closeは受付とtimerを停止し、残ったsnapshotを一度だけ送信してからFlushManagerを閉じます。
// 最終flushが失敗してもsnapshotはbufferへ戻さず、managerのCloseまで続行します。
// managerのCloseだけが失敗した場合はStartedを維持し、次のCloseで終了処理を再試行します。
func (l *CloudWatchLogsLogger) Close(ctx context.Context) error {
	l.closeMu.Lock()
	defer l.closeMu.Unlock()

	if !l.beginClose() {
		return nil
	}

	l.stopBatchFlushWorker()

	// closeStateをStartedにした後は新しい処理を受け付けないため、受付済みの即時送信と、
	// 呼び出し元のgoroutineで実行中のbatch flush（flushで発生した内部ログの通知を含みます）の
	// 完了だけを待ちます。
	l.inFlight.Wait()

	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	flushErr := l.flushBuffered(ctx)
	closeErr := l.closeFlushManager(ctx)
	if closeErr == nil || common.ShutdownComplete(l.flushManager) {
		l.completeClose()
	}
	return errors.Join(flushErr, closeErr)
}

// HandleLogEntryは、infraのtop-level loggerが生成した一件のLogEntryを
// 未設定のOccurredAtをClockから補完し、CloudWatch Logs固有のlog level、sampling設定を
// 適用して送信経路へ渡します。
// 呼び出し元はfan-out先ごとに独立したentryを渡すため、このmethodはentryを直接加工します。
func (l *CloudWatchLogsLogger) HandleLogEntry(
	ctx context.Context,
	entry *logging.LogEntry,
) error {
	if closedErr := l.loggerClosedError(); closedErr != nil {
		return closedErr
	}
	if entry == nil {
		return fmt.Errorf("cloudwatch logs logger entry is nil")
	}
	if entry.Level < l.config.Runtime().LogLevel() {
		if !entry.StackTraceRequested {
			return nil
		}
		l.notifyWarn(
			ctx,
			l.InternalWarnEntry("cloudwatch logs logger log level is higher than error", ""),
			l.Tracer,
			l.Meter,
		)
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
		return l.putImmediately(ctx, entry)
	}

	return l.enqueue(ctx, entry)
}

// enqueue は、バルクモードのバッファにログを 1 件追加し、フラッシュ条件を満たした場合にのみフラッシュします。
func (l *CloudWatchLogsLogger) enqueue(ctx context.Context, entry *logging.LogEntry) error {
	entryBytes := cloudWatchLogsBufferedEntryBytes(entry, l.LogFormatter)

	l.mu.Lock()
	if l.closeState != common.LoggerCloseStateOpen {
		l.mu.Unlock()
		return pkglogger.ErrLoggerClosed
	}
	l.entries = append(l.entries, entry)
	l.bufferBytes += entryBytes
	shouldFlush := l.shouldFlushLocked()
	l.mu.Unlock()

	if !shouldFlush {
		return nil
	}
	return l.doFlush(ctx)
}

// shouldFlushLocked はl.muを保持した状態で呼び出し、件数、AWSの
// message bytes + 26 bytes/eventで概算したsoft byte threshold、または
// 経過時間に基づいてflushの要否を判定します。物理PutLogEvents requestの
// 上限による分割はFlushManager以下のplannerが独立して行います。
func (l *CloudWatchLogsLogger) shouldFlushLocked() bool {
	if l.noBulkFlushModeEnabled() {
		return false
	}
	if len(l.entries) >= l.config.BufferFlushMaxEvents() {
		return true
	}
	if l.bufferBytes >= l.config.BufferFlushBytes() {
		return true
	}
	return l.flushIntervalElapsedLocked()
}

// flushIntervalElapsedLockedは前回のflush時刻から設定間隔以上が経過したかを返します。
// 呼び出し元はl.muを保持していなければなりません。
func (l *CloudWatchLogsLogger) flushIntervalElapsedLocked() bool {
	now := time.Now()
	if l.timeProvider != nil {
		now = l.timeProvider()
	}
	return now.Sub(l.lastFlushTime) >= l.config.FlushInterval()
}

// flush は、ティッカーから呼び出され、発生したエラーを呼び出し元へ返さないフラッシュ処理です。
func (l *CloudWatchLogsLogger) flush(ctx context.Context) {
	_ = l.doFlush(ctx)
}

// doFlushは現在のbufferを切り離してFlushManagerへ一度だけ渡します。
// 送信に失敗したsnapshotは通常bufferへ戻しません。flush中に発生したこのloggerの内部ログは、
// flushMuを解放してから発生順に通知します（Close待機の対象として数えている間に通知します）。
// flushMuを保持したまま通知すると、通知の中からの同じloggerへの書き込みが閾値でflushし、
// flushMuを待ち続けるためです。Close開始後は、bufferに残ったentryをCloseの最終flushへ任せます。
func (l *CloudWatchLogsLogger) doFlush(ctx context.Context) error {
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
func (l *CloudWatchLogsLogger) beginInFlight() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closeState != common.LoggerCloseStateOpen {
		return false
	}
	l.inFlight.Add(1)
	return true
}

func (l *CloudWatchLogsLogger) doFlushLocked(ctx context.Context) error {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	l.mu.Lock()
	closeStarted := l.closeState != common.LoggerCloseStateOpen
	l.mu.Unlock()
	if closeStarted {
		return nil
	}

	return l.flushBuffered(ctx)
}

// flushBufferedは、flushMuを保持している呼び出し元から使用し、現在のbuffer snapshotを
// 一回だけFlushManagerへ渡します。実log streamの解決とphysical PutLogEvents requestへの
// 分割はCloudWatch Logs固有の送信調整を担うFlushManagerより下で行います。
//
// 切り離したsnapshotはbackend送信またはdurable処理の成否にかかわらずbufferへ戻しません。
// 空でないsnapshotを切り離した場合は、成否にかかわらずlastFlushTimeを今回の試行時刻へ更新します。
// CloseはcloseStateをStartedへ遷移させた後の最終flushに限り、このメソッドを直接呼び出します。
func (l *CloudWatchLogsLogger) flushBuffered(ctx context.Context) error {
	if l.noBulkFlushModeEnabled() {
		return nil
	}

	snapshot := l.detachBufferedBatch(l.config.BufferFlushMaxEvents())
	if len(snapshot) == 0 {
		return nil
	}
	// 切り離したsnapshotは結果にかかわらず再送しないため、送信やdurable処理の失敗
	// （overload policyのdropを含む）も一回の試行としてinterval判定の基準時刻を更新します。
	// 失敗が続いても、件数・byte閾値に達しない限りFlushInterval内に追加のflushを始めません。
	defer l.updateLastFlushTime()
	flushManager := l.flushManager
	if flushManager == nil {
		err := fmt.Errorf("CloudWatch Logs flush manager is not initialized")
		l.notifyError(
			ctx,
			l.wrapErrorByLogger(err, "cloudwatch logs flush"),
			l.InternalErrorEntry("failed to flush by CloudWatch Logs logger; flush manager is not initialized; error:", "", err),
			l.Tracer,
			l.Meter,
		)
		return err
	}

	ctx, span := l.Tracer.StartSpan(ctx, SpanCloudWatchLogsLoggerFlush)
	defer span.End()

	return flushManager.Flush(ctx, snapshot)
}

// detachBufferedBatchは現在のbufferを一回限りの送信用snapshotとして切り離します。
// 通常LoggerとFallbackのどちらも切り離したsnapshotをbufferへ戻しません。
// nextBufferCapacityには、呼び出し側のbuffer契約に対応する
// 設定件数を指定します。entryがない場合は空のsnapshotを返します。
func (l *CloudWatchLogsLogger) detachBufferedBatch(nextBufferCapacity int) []*logging.LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.entries) == 0 {
		l.bufferBytes = 0
		return nil
	}

	snapshot := l.entries
	// 以後の追加は別bufferへ流し、backend送信中はmuを保持しません。一時的に
	// 拡張されたsnapshotのcapacityは引き継がず、設定上の容量へ戻します。
	l.entries = make([]*logging.LogEntry, 0, nextBufferCapacity)
	l.bufferBytes = 0
	return snapshot
}

// putImmediately は、即時送信モードでログを 1 件だけ FlushManager へ渡します。
// Close の待機対象として受付を数えるだけなので、複数の即時送信は互いに直列化されません。
func (l *CloudWatchLogsLogger) putImmediately(ctx context.Context, entry *logging.LogEntry) error {
	l.mu.Lock()
	if l.closeState != common.LoggerCloseStateOpen {
		l.mu.Unlock()
		return pkglogger.ErrLoggerClosed
	}
	flushManager := l.flushManager
	l.inFlight.Add(1)
	l.mu.Unlock()
	defer l.inFlight.Done()

	if flushManager == nil {
		err := fmt.Errorf("CloudWatch Logs flush manager is not initialized")
		l.notifyError(
			ctx,
			l.wrapErrorByLogger(err, "cloudwatch logs put"),
			l.InternalErrorEntry("failed to put by CloudWatch Logs logger; flush manager is not initialized; error:", "", err),
			l.Tracer,
			l.Meter,
		)
		return err
	}

	return flushManager.Put(ctx, entry)
}

// updateLastFlushTime は、フラッシュ成功後に次回の間隔判定の基準時刻を更新します。
func (l *CloudWatchLogsLogger) updateLastFlushTime() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.timeProvider != nil {
		l.lastFlushTime = l.timeProvider()
		return
	}
	l.lastFlushTime = time.Now()
}

// shouldLog は、フォールバックハンドラーから受け取った LogEntry を、このロガーのログレベル設定に基づいて送信するか判定します。
func (l *CloudWatchLogsLogger) shouldLog(level pkglogger.LogLevel) bool {
	return level >= l.config.Runtime().LogLevel()
}

// noBulkFlushModeEnabledは、bufferへ蓄積せずlog呼び出しごとに1件をPutLogEventsへ渡す設定を返します。
func (l *CloudWatchLogsLogger) noBulkFlushModeEnabled() bool {
	return l.config.NoBulkFlushModeEnabled()
}

func (l *CloudWatchLogsLogger) loggerClosedError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closeState != common.LoggerCloseStateOpen {
		return pkglogger.ErrLoggerClosed
	}
	return nil
}

// wrapErrorByLogger は、ハンドラーへ渡す内部エラーに CloudWatch Logs ロガーの操作名を付与します。
func (l *CloudWatchLogsLogger) wrapErrorByLogger(err error, operation string) error {
	return &cloudwatchlogserrors.CloudWatchLogsLoggerInternalError{Op: operation, Err: err}
}
