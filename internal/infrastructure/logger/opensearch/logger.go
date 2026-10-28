package opensearch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

var _ logging.BackendLogEntryHandler = (*OpenSearchLogger)(nil)

type OpenSearchLogger struct {
	*LoggerBase
	mu sync.Mutex
	// flushMuはbatch Flush同士と、Closeの最終flush・FlushManager.Closeを直列化します。
	// 待機をchannelで行うため、testing/synctestのbubble内でも待機を観測できます。
	flushMu loggercommon.WaitableMutex
	// closeMuはCloseの呼び出しを直列化します。待機をchannelで行うため、testing/synctestの
	// bubble内でも待機を観測できます。
	closeMu loggercommon.WaitableMutex
	// inFlightは、受付を確定した処理（NoBulk modeの即時Index、batch flushとそのflushで
	// 発生した内部ログの通知）の完了を数えます。CloseはcloseStateをStartedにして新規受付を
	// 止めた後、この完了を待ちます。受付はcloseStateの確認と同じmuの下で数えるため、
	// Close開始後に増えることはありません。
	inFlight sync.WaitGroup
	timerWG  sync.WaitGroup
	stopOnce sync.Once
	entries  []*logging.LogEntry // bulk送信用
	// bufferBytesはentriesをOpenSearch document JSONへencodeしたときの概算byte数です。
	// l.muで保護し、detach時にentriesと同時にresetします。
	bufferBytes   int
	lastFlushTime time.Time // 最後にflushした時間
	ticker        *time.Ticker
	stopCh        chan struct{} // タイマー停止用チャネル
	// flushWorkerStartedはbatch workerの二重起動を防ぎます。muで保護します。
	flushWorkerStarted bool
	closeState         loggercommon.LoggerCloseState
	timeProvider       func() time.Time                 // 時間取得用の関数（テスト容易性向上のため）
	flushIntervalGen   func(time.Duration) *time.Ticker // タイマー生成用の関数（テスト容易性向上のため）
	// entryConverterは、buffer追加時にOpenSearch document bodyのbyte数を
	// 見積もるため、FlushManagerと同じ値を保持します。
	entryConverter LogEntryToDocumentConverter
	// flushManagerは構築完了前に設定し、運用中は差し替えないLogger所有の送信依存です。
	// FlushとCloseの実行順序はflushMuで保護します。
	flushManager FlushManager
	Tracer       pkgobserver.Tracer
	Meter        pkgobserver.Meter
}

// OpenSearchLoggerOption はOpenSearchLoggerのオプション設定用の関数型
type OpenSearchLoggerOption func(*OpenSearchLogger)

// NewOpenSearchLogger は新しいOpenSearchLoggerを作成します。
// bulk modeのinterval flushは構築時のctxが終了した後もCloseまで続きます。
func NewOpenSearchLogger(
	ctx context.Context,
	cfg OpenSearchLoggerConfigProvider,
	osClient opensearch.Client,
	fmCfg OpenSearchFlushManagerConfigProvider,
	resource resource.ResourceProvider,
	failureHandler *flushfailure.CompositeFailureHandler,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	loggerBase *LoggerBase,
	opts ...OpenSearchLoggerOption,
) (*OpenSearchLogger, error) {
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)
	ctx, span := tracer.StartSpan(ctx, SpanOpenSearchLoggerCreate)
	defer span.End()

	if cfg == nil {
		return nil, fmt.Errorf("opensearch logger config is nil")
	}
	if err := pkglogger.ValidateBufferedFlushInterval(cfg.FlushInterval()); err != nil {
		return nil, fmt.Errorf("opensearch logger config is invalid: %w", err)
	}
	if err := pkglogger.ValidateOpenSearchMaxBulkDocument(cfg.MaxBulkDocument()); err != nil {
		return nil, fmt.Errorf("opensearch logger config is invalid: %w", err)
	}
	if err := validateBufferFlushBytes(cfg.BufferFlushBytes()); err != nil {
		return nil, fmt.Errorf("opensearch logger config is invalid: %w", err)
	}
	if err := infraopensearch.ValidateMaxBulkRequestBytes(cfg.MaxBulkRequestBytes()); err != nil {
		return nil, fmt.Errorf("opensearch logger config is invalid: %w", err)
	}
	if err := sampling.ValidateSamplingConfigProviderFor(pkglogger.LogOutputOpenSearch, cfg.Sampling()); err != nil {
		return nil, fmt.Errorf("opensearch logger sampling config: %w", err)
	}

	if loggerBase == nil {
		loggerBase = &LoggerBase{}
	}
	if loggerBase.Clock == nil {
		loggerBase.Clock = clock.NewUTCClock()
	}
	if loggerBase.TimestampFormatter.IsZero() {
		configuredFormatter, err := NewConfiguredTimestampFormatter(cfg)
		if err != nil {
			return nil, err
		}
		loggerBase.TimestampFormatter = configuredFormatter
	}
	loggerBase.config = cfg

	converter := NewLogEntryConverter(
		fmCfg.UseOpenTelemetryFormat(),
		resource,
		loggerBase.TimestampFormatter,
	)
	logger := &OpenSearchLogger{
		LoggerBase:       loggerBase,
		entries:          make([]*logging.LogEntry, 0, cfg.MaxBulkDocument()),
		timeProvider:     time.Now,
		flushIntervalGen: time.NewTicker,
		entryConverter:   converter,
		Tracer:           tracer,
		Meter:            meter,
	}

	logger.lastFlushTime = logger.timeProvider()

	for _, opt := range opts {
		opt(logger)
	}

	flushManager, err := NewOpenSearchFlushManager(
		loggerBase,
		osClient,
		fmCfg,
		converter,
		failureHandler,
		resource,
		tracer,
		meter,
	)
	if err != nil {
		return nil, err
	}
	logger.flushManager = flushManager
	// On*の中から同じloggerへ再入した送信の内部ログは、通知せず同じindexへ直接書き込みます。
	loggerBase.directInternalLog = newDirectInternalLogWriter(cfg, flushManager.indexInternalLogDirectly)
	if !cfg.NoBulkFlushModeEnabled() {
		logger.startBatchFlushWorker(ctx, nil, logger.flush)
	}

	return logger, nil
}

// WithClock は、試験でログエントリの時刻を固定するための差し替え用オプションです。
func WithClock(clock clock.Clock) OpenSearchLoggerOption {
	return func(l *OpenSearchLogger) {
		l.Clock = clock
	}
}

// WithTimeProvider は、試験で flush 判定や lastFlushTime 更新時刻を固定するための差し替え用オプションです。
func WithTimeProvider(provider func() time.Time) OpenSearchLoggerOption {
	return func(l *OpenSearchLogger) {
		l.timeProvider = provider
	}
}

// WithFlushIntervalGenerator は、試験で ticker 生成を差し替えて定期 flush を制御するためのオプションです。
func WithFlushIntervalGenerator(gen func(time.Duration) *time.Ticker) OpenSearchLoggerOption {
	return func(l *OpenSearchLogger) {
		l.flushIntervalGen = gen
	}
}

// startBatchFlushWorkerは、このLogger instanceが所有するbatch workerを開始します。
// intervalとwake通知の受信、停止、callbackの終了待ちだけを担当し、
// 通常送信かno-fallback送信かは構築時に渡すflushで確定します。
// wakeChがnilの場合はintervalだけを契機にflushします。
// workerは構築時のctxの値だけを引き継ぎ、cancelとdeadlineからは切り離します。
// 構築後にctxが終了してもCloseまでflushを続け、停止境界はCloseとclientのtimeoutです。
func (l *OpenSearchLogger) startBatchFlushWorker(
	ctx context.Context,
	wakeCh <-chan struct{},
	flush func(context.Context),
) {
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

// stopBatchFlushWorkerは新しいinterval・閾値flushを停止し、実行中のcallbackが
// 完了するまで待ちます。Close再試行時のstopCh二重closeはstopOnceで防ぎます。
func (l *OpenSearchLogger) stopBatchFlushWorker() {
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

// Closeは通常Loggerの受付を停止し、残ったbatchを一度だけ送信してからFlushManagerを
// 終了します。最終flushが失敗しても切り離したbatchは再投入せず、managerのCloseまで
// 続行します。managerのCloseだけが失敗した場合はStartedを維持し、次のCloseで再試行します。
func (l *OpenSearchLogger) Close(ctx context.Context) error {
	l.closeMu.Lock()
	defer l.closeMu.Unlock()

	if !l.beginClose() {
		return nil
	}
	flushErr, closeErr := l.closeBufferedRuntime(ctx, l.flushBuffered)
	if closeErr == nil || loggercommon.ShutdownComplete(l.flushManager) {
		l.completeClose()
	}
	return errors.Join(flushErr, closeErr)
}

// beginCloseは新規logの受付を停止します。過去のCloseが失敗して
// Startedの場合は再試行を許可し、Completedの場合だけfalseを返します。
func (l *OpenSearchLogger) beginClose() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closeState == loggercommon.LoggerCloseStateCompleted {
		return false
	}
	l.closeState = loggercommon.LoggerCloseStateStarted
	return true
}

// closeBufferedRuntimeはworkerを止め、受理済みsnapshotの一回限りの送信と
// FlushManagerの終了を同じlock境界で順に実行します。flushが失敗してもmanager Closeを
// 試みられるよう、二つのerrorを分けて返します。
func (l *OpenSearchLogger) closeBufferedRuntime(
	ctx context.Context,
	flush func(context.Context) error,
) (flushErr error, closeErr error) {
	l.stopBatchFlushWorker()

	// Close開始前に受理したNoBulk modeのIndexと、呼び出し元のgoroutineで実行中のbatch flush
	// （flushで発生した内部ログの通知を含みます）の完了を待ちます。呼び出し元がcloseStateを
	// Startedにした後は新しい処理を受け付けないため、以後の処理はFlushManager.Closeと
	// 重なりません。Close開始前の複数のIndex同士は並行実行できます。
	l.inFlight.Wait()

	// batch Flushは従来どおり直列化し、最終flushとFlushManager.Closeも
	// 同じ排他制御の単位に置きます。
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	flushErr = flush(ctx)
	closeErr = l.closeFlushManager(ctx)
	return flushErr, closeErr
}

func (l *OpenSearchLogger) closeFlushManager(ctx context.Context) error {
	l.mu.Lock()
	flushManager := l.flushManager
	l.mu.Unlock()
	if flushManager == nil {
		return nil
	}
	return flushManager.Close(ctx)
}

func (l *OpenSearchLogger) completeClose() {
	l.mu.Lock()
	l.closeState = loggercommon.LoggerCloseStateCompleted
	l.mu.Unlock()
}

// HandleLogEntryはtop-level loggerで生成済みのentryをOpenSearch用に加工し、
// 即時Indexまたはbatch bufferへ渡します。発生時刻・ID・Fieldsは作り直さず、
// TimestampFormatter、stack trace、samplingだけをこの保存先の設定から適用します。
func (l *OpenSearchLogger) HandleLogEntry(
	ctx context.Context,
	entry *logging.LogEntry,
) error {
	if err := l.loggerClosedError(); err != nil {
		return err
	}
	if entry == nil {
		return fmt.Errorf("OpenSearch log entry is nil")
	}
	if entry.Level < l.config.Runtime().LogLevel() {
		if entry.StackTraceRequested {
			l.notifyWarn(
				ctx,
				l.InternalWarnEntry(MsgLogLevelTooHighWarn, ""),
				l.Tracer,
				l.Meter,
			)
		}
		return nil
	}

	logging.EnsureEntryOccurredAt(entry, l.Clock)
	if entry.StackTraceRequested {
		// ErrorWithStackにerrorが渡されなかった場合も拒否せず、要求されたstackを持つ
		// ERRORとして記録します。documentのerrorは空のままです。
		if len(entry.FuncName) == 0 && len(entry.File) == 0 && len(entry.Line) == 0 {
			depth := logging.GetCallerDepth(NewStackTraceConfigAdapter(l.config))
			logging.EnsureEntryStackTrace(entry, depth)
		}
	}

	// 共通サンプリングロジックを適用
	if !sampling.ApplySamplingLogic(entry, l.config.Sampling()) {
		return nil
	}

	if l.config.NoBulkFlushModeEnabled() {
		return l.indexImmediately(ctx, entry)
	}

	return l.enqueue(ctx, entry)
}

// enqueue はログをバッファに追加し、必要であれば非ロック状態で flush します。
func (l *OpenSearchLogger) enqueue(ctx context.Context, entry *logging.LogEntry) error {
	shouldFlush, err := l.enqueueNormalEntry(entry)
	if err != nil {
		return err
	}
	if !shouldFlush {
		return nil
	}

	// doFlush は内部で lock するので、enqueue 側の lock を解放してから呼ぶ。
	return l.doFlush(ctx)
}

// enqueueNormalEntryは通常Loggerのentryをbufferへ受け入れ、件数、byte数、
// intervalのいずれかがflush閾値に達したかを返します。
// 受理済みentryを廃棄するFallback固有の上限処理はここでは行いません。
func (l *OpenSearchLogger) enqueueNormalEntry(entry *logging.LogEntry) (bool, error) {
	entryBytes, err := l.measureBufferedEntryBytes(entry)
	if err != nil {
		return false, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closeState != loggercommon.LoggerCloseStateOpen {
		return false, pkglogger.ErrLoggerClosed
	}

	l.entries = append(l.entries, entry)
	l.bufferBytes += entryBytes
	return l.shouldFlushLocked(), nil
}

// shouldFlushLockedはl.muを保持した状態で呼び出します。件数、bufferの
// soft byte threshold、intervalのいずれかに達したらflushします。
// MaxBulkRequestBytesによるrequest分割はFlushManagerの独立した責務です。
func (l *OpenSearchLogger) shouldFlushLocked() bool {
	if len(l.entries) >= l.config.MaxBulkDocument() {
		return true
	}
	if l.bufferBytes >= l.config.BufferFlushBytes() {
		return true
	}
	now := time.Now()
	if l.timeProvider != nil {
		now = l.timeProvider()
	}
	return now.Sub(l.lastFlushTime) >= l.config.FlushInterval()
}

// flush はタイマーから呼ばれる 1 回分の flush 実行メソッドです。
func (l *OpenSearchLogger) flush(ctx context.Context) {
	// doFlush は内部で lock するので、ここでは lock しない。
	_ = l.doFlush(ctx)
}

// doFlush は実際にログエントリをOpenSearchに送信する処理です。
// このメソッド自身が lock を取得するため、呼び出し元は lock してはいけません。
// flush中に発生したこのloggerの内部ログは、flushMuを解放してから発生順に通知します
// （Close待機の対象として数えている間に通知します）。flushMuを保持したまま通知すると、
// 通知の中からの同じloggerへの書き込みが閾値でflushし、flushMuを待ち続けるためです。
// Close開始後は、bufferに残ったentryをCloseの最終flushへ任せます。
func (l *OpenSearchLogger) doFlush(ctx context.Context) error {
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
func (l *OpenSearchLogger) beginInFlight() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		return false
	}
	l.inFlight.Add(1)
	return true
}

func (l *OpenSearchLogger) doFlushLocked(ctx context.Context) error {
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

// flushBuffered はflushMuを保持した呼び出し元が、現在のbufferを一度送信します。
// CloseだけはcloseStateをStartedにした後もこのメソッドを直接呼び、残っているentryを最終flushします。
// 空でないsnapshotを切り離した場合は、成否にかかわらずlastFlushTimeを今回の試行時刻へ更新します。
func (l *OpenSearchLogger) flushBuffered(ctx context.Context) error {
	if l.config.NoBulkFlushModeEnabled() {
		return nil
	}

	batch, batchBytes := l.detachBufferedBatch()
	if len(batch) == 0 {
		return nil
	}
	// 切り離したsnapshotは結果にかかわらず再送しないため、送信やdurable処理の失敗
	// （overload policyのdropを含む）も一回の試行としてinterval判定の基準時刻を更新します。
	// 失敗が続いても、件数・byte閾値に達しない限りFlushInterval内に追加のflushを始めません。
	defer l.updateLastFlushTime()
	flushManager := l.flushManager
	if flushManager == nil {
		err := fmt.Errorf("OpenSearch flush manager is not initialized")
		l.notifyError(
			ctx,
			l.wrapErrorByLogger(err, OperationOpenSearchError),
			l.InternalErrorEntry("failed to flush by OpenSearch logger; flush manager is not initialized; error:", "", err),
			l.Tracer,
			l.Meter,
		)
		return err
	}

	ctx, span := telemetry.StartFlush(ctx, l.Tracer, l.Meter, SpanOpenSearchLoggerFlush, deliveryBackendOpenSearch, len(batch), batchBytes)
	err := flushManager.Flush(ctx, batch)
	telemetry.EndSpan(span, err)
	return err
}

// detachBufferedBatchは現在のbufferを一回限りの送信用snapshotとして切り離します。
// 切り離したsnapshotは送信結果にかかわらず通常bufferへ戻しません。
// bytesは、切り離したentryについてbufferが数えていたbyte数です。
func (l *OpenSearchLogger) detachBufferedBatch() (batch []*logging.LogEntry, bytes int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.entries) == 0 {
		return nil, 0
	}

	batch, bytes = l.entries, l.bufferBytes
	// 以後の追加は別bufferへ流し、backend送信中はmuを保持しません。一時的に
	// 拡張されたbatchのcapacityは引き継がず、設定上のflush件数を初期容量に戻します。
	l.entries = make([]*logging.LogEntry, 0, l.config.MaxBulkDocument())
	l.bufferBytes = 0
	return batch, bytes
}

func (l *OpenSearchLogger) indexImmediately(ctx context.Context, entry *logging.LogEntry) error {
	// 複数の即時Indexは並行実行を許可しつつ、Closeだけは開始済みIndexの完了を待ちます。
	// closeStateの確認と同じlockの下で受付を数えるため、Close開始後に受付は増えません。
	l.mu.Lock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		l.mu.Unlock()
		return pkglogger.ErrLoggerClosed
	}
	flushManager := l.flushManager
	l.inFlight.Add(1)
	l.mu.Unlock()
	defer l.inFlight.Done()

	if flushManager == nil {
		err := fmt.Errorf("OpenSearch flush manager is not initialized")
		l.notifyError(
			ctx,
			l.wrapErrorByLogger(err, OperationOpenSearchError),
			l.InternalErrorEntry("failed to index by OpenSearch logger; flush manager is not initialized; error:", "", err),
			l.Tracer,
			l.Meter,
		)
		return err
	}
	return flushManager.Index(ctx, entry)
}

func (l *OpenSearchLogger) measureBufferedEntryBytes(entry *logging.LogEntry) (int, error) {
	return measureEncodedLogEntryDocumentBodySize(entry, l.entryConverter)
}

func (l *OpenSearchLogger) updateLastFlushTime() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.timeProvider != nil {
		l.lastFlushTime = l.timeProvider()
		return
	}
	l.lastFlushTime = time.Now()
}

// 記録を行うべき条件でなければnilを返してロギング処理を終了
func (l *OpenSearchLogger) shouldLog(level pkglogger.LogLevel) bool {
	// ロガーは ログレベルが設定値より低ければ記録しない
	if level < l.config.Runtime().LogLevel() {
		return false
	}
	return true
}

func (l *OpenSearchLogger) loggerClosedError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		return pkglogger.ErrLoggerClosed
	}
	return nil
}
