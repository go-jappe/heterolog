package file

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgtime "github.com/go-jappe/heterolog/pkg/time"
)

const LogFileExtension = "log"

var ErrFileAlreadyOpen = errors.New("log file is already open")

var _ logging.BackendLogEntryHandler = (*FileLogger)(nil)

type loggerType string

const (
	DefaultLogger loggerType = "default"
	ErrorLogger   loggerType = "error"
	DebugLogger   loggerType = "debug"
)

// ロガー構造体
type FileLogger struct {
	// Clockの時刻とタイムゾーンを、ファイル名・rotationの日付基準に使います。
	// 構築時にUTCClockを設定します。差し替える場合もnilにはせず、処理中に別の時計へ補完しません。
	Clock clock.Clock
	// LogFormatterは本文に記録する発生時刻のtimezoneとlayoutを保持します。
	LogFormatter   formatter.PlainTextFormatter
	logger         loggerType
	config         FileLoggerConfigProvider
	directoryPaths loggerDirectoryPaths
	overload       *fileOverload

	Fsstore appendlog.FileStore // ファイル永続化操作
	// ArchiveUploaderは、単一宛先の場合も宛先IDごとに送信・完了を管理します。
	ArchiveUploader uploader.DestinationUploader
	// archiveUploadCheckpointsは、inline modeの宛先別完了cursorと、送信・退避の途中経過を
	// 永続化します。checkpoint設定を持たないprogrammaticな構成ではnilとします。
	archiveUploadCheckpoints checkpointstore.Store
	// archiveGenerationPendingは共有directoryのpeer roleについて、lazy時は初期cursor、
	// 生成後はそのroleのStoreから現在の完了状態を読みます。
	archiveGenerationPending func(loggerType, checkpointstore.CheckpointKey) bool
	// archiveLogFileProcessingRecoveryはcheckpoint更新の受理前やfile削除で中断した処理を、
	// purgeのUsageCacheTTLや次のログ出力を待たずに再開します。
	archiveLogFileProcessingRecovery logfile.LogFileProcessingRecovery[archiveLogFileProcessingRecoveryKey]
	// forceWorkは明示的な強制終了だけを受理済み処理へ通知します。
	forceWork       fileForceContext
	closeController shutdown.Controller
	// inlinePurgeはinline modeでだけ構築し、purgeの日次状態、概算Tracker、filesystem観測、
	// cleanup多重起動防止をまとめて所有します。runner・disable modeではnilです。
	inlinePurge *inlinePurgeRuntime
	// lifecycleGuardは同じ正規化済みdirectory pathを扱うFileLoggerと
	// lifecycle処理の排他・active path集合を共有します。
	lifecycleGuard *appendlog.DirectoryGuard
	// lifecycleTokenのownerは、このphysical FileLogger writer instanceです。
	// active path（rotationの新open待ちでは旧path）を識別します。rotation archiveとcleanup retryは
	// 並行owner同士の解除が干渉しないよう、一回のtaskごとに別tokenを作成します。
	lifecycleToken *appendlog.DirectoryGuardToken

	// managedByLoggerSetの場合、logical loggerのCloseでは共有fileを閉じません。
	// underlying file resourceは、このloggerを作成したLoggerSetが最後に一度だけ閉じます。
	managedByLoggerSet   bool
	ownerCloseController atomic.Pointer[shutdown.Controller]

	// backgroundMuとbackgroundWGは、rotation後のarchiveおよびinline cleanupが
	// 完了するまでunderlying file resourceをcloseしないためのlifecycle境界です。
	// backgroundWGは、Closeとの競合中に受理済みwrite由来のrotation taskを安全に
	// 登録するための予約も数えます。通常はrotation判定まで、容量不足時は退避完了まで
	// 予約を保持し、いずれも利用側への通知より前に解放します。
	backgroundMu sync.Mutex
	backgroundWG sync.WaitGroup
	// closeStateはbackgroundMuの保護下で新規write受付とCloseの状態を管理します。
	closeState loggercommon.LoggerCloseState
	// closeAttemptは同時に呼ばれたCloseが共有する一回の終了処理です。
	// backgroundMuで保護し、失敗した試行の完了後は新しいCloseから再試行できます。
	closeAttempt *fileLoggerCloseAttempt

	OnError logging.LoggerErrorHandler // 通知用の関数　内部エラー発生時にハンドル
	OnWarn  logging.LoggerHandlerFunc
	OnInfo  logging.LoggerHandlerFunc
	OnDebug logging.LoggerHandlerFunc
	Tracer  pkgobserver.Tracer // observebility計装用
	Meter   pkgobserver.Meter  // observebility計装用

	// internalLogReentryは、On*へ渡すcontextへこのFileLoggerの印を付け、通知を実行するgoroutineを
	// 記録します。通知の中から同じFileLoggerへ書き込んだ場合、その書き込みが同期的に
	// 発生させたinternal logはOn*を再び呼ばず、同じfileへ直接記録します（reportWriteInternalLog）。
	internalLogReentry handler.ReentryGuard
}

// fileLoggerCloseAttemptは完了した一回の結果を保持します。次の試行が始まっても、
// この試行を待っていたcallerには同じerrorを返します。errはdoneを閉じる前に確定します。
type fileLoggerCloseAttempt struct {
	done chan struct{}
	err  error
}

// HandleLogEntryはtop-level loggerが生成済みのentryをfile backendへ記録します。
// ID、Fields、Message、Err、OccurredAtは再解決せず、role filter、sampling、
// stack traceなどfile固有の処理だけを適用します。
func (l *FileLogger) HandleLogEntry(ctx context.Context, entry *logging.LogEntry) error {
	if err := l.rejectIfClosed(); err != nil {
		return err
	}
	if entry == nil {
		return fmt.Errorf("file logger log entry is nil")
	}
	if !l.shouldLog(entry.Level) {
		return nil
	}

	logging.EnsureEntryOccurredAt(entry, l.Clock)
	if entry.StackTraceRequested {
		// ErrorWithStackにerrorが渡されなかった場合も拒否せず、要求されたstackを持つ
		// ERRORとして記録します。Formatterはerrorの欄を出力しません。
		if len(entry.FuncName) == 0 && len(entry.File) == 0 && len(entry.Line) == 0 {
			depth := logging.GetCallerDepth(NewStackTraceConfigAdapter(l.config.Runtime().IsDebug(), l.config.LogBody()))
			logging.EnsureEntryStackTrace(entry, depth)
		}
	}

	// 共通サンプリングロジックを適用
	if !sampling.ApplySamplingLogic(entry, l.config.Sampling()) {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	err := l.logWithMeta(ctx, entry)
	recordWrite(ctx, l.Meter, err)
	if err != nil {
		// Close開始後の拒否はfile operation失敗ではなく、同期Logger契約上の
		// pre-attempt refusalなので内部errorへ再委譲せず共通causeを維持します。
		if errors.Is(err, pkglogger.ErrLoggerClosed) {
			return err
		}
		// overload policyによるdropは、書込み処理の中でOnErrorへ通知済み（再入した
		// 書き込みでは直接記録済み）です。内部errorとして重ねて通知せず、破棄を判別できる
		// errorをそのまま返します。
		if errors.Is(err, pkglogger.ErrLogDropped) {
			return err
		}
		// ファイルロガーの内部エラーはここで処理を行う。通知の中から同じFileLoggerへの
		// 書き込みが失敗した場合は、通知を繰り返さず同じfileへ直接記録します。
		errorMes := fmt.Sprintf(
			ErrorMsgLog,
			l.LogFormatter.Format(entry),
			err,
		)
		l.wrapAndReportWriteError(ctx, err, l.InternalErrorEntry(errorMes, err, *entry))
		// 呼び出し元へ一応返す
		return fmt.Errorf(ErrLog, err)
	}
	return nil
}

// 記録を行うべき条件でなければnilを返してロギング処理を終了
func (l *FileLogger) shouldLog(level pkglogger.LogLevel) bool {
	// ロガーは ログレベルが設定値より低ければ記録しない
	if level < l.config.Runtime().LogLevel() {
		return false
	}

	if l.logger == DefaultLogger {
		// 通常のロガーは エラーログ設定が有効の場合は エラーログ以上は記録しない
		if l.config.Fsstore().Layout().IsSeparateErrorLogsEnabled() && level >= pkglogger.LogLevelError {
			return false
		}
		// 通常のロガーは デバッグログ設定が有効の場合は デバッグログは記録しない
		if l.config.Fsstore().Layout().IsSeparateDebugLogsEnabled() && level == pkglogger.LogLevelDebug {
			return false
		}
	}
	// デバッグロガーは デバッグ以外は記録しない
	if l.logger == DebugLogger && level != pkglogger.LogLevelDebug {
		return false
	}
	// エラーロガーは エラー未満は記録しない
	if l.logger == ErrorLogger && level < pkglogger.LogLevelError {
		return false
	}
	return true
}

func (l *FileLogger) logWithMeta(ctx context.Context, entry *logging.LogEntry) error {
	return l.writeEntry(ctx, entry, true)
}

// writeFallbackEntryはinternal logを同じ物理streamへ記録します。
// fallback自体の失敗を再び同じhandlerへ渡すと再帰するため、retry/latencyのinternal logは
// dispatchしません。一方で通常writeと同じrotation・cleanup境界は使用し、その後の
// background lifecycle errorは通常のhandler通知対象です。
func (l *FileLogger) writeFallbackEntry(ctx context.Context, entry *logging.LogEntry) error {
	return l.writeEntry(ctx, entry, false)
}

func (l *FileLogger) writeEntry(
	ctx context.Context,
	entry *logging.LogEntry,
	dispatchInternalLogs bool,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// 全てのオプションが on の場合、高並列性能より単純さ・整合性優先
	// 重い環境では各オプションを　off にして調整してください
	if err := l.lockForWrite(); err != nil {
		return err
	}
	// lockForWriteで受け付けた書き込みは、付随する内部ログ通知まで終了待ちに含めます。
	// 通知時はwriter lockを解放し、通知内からの終了要求はRequestCloseで受け付けます。
	fileLockHeld := true
	unlockFile := func() {
		if !fileLockHeld {
			return
		}
		fileLockHeld = false
		l.Fsstore.Mu.Unlock()
	}
	// rotation判定・write・将来追加されるcallbackのpanicを含む全経路で、
	// file lockと実行中件数の両方を一度だけ解放します。
	defer func() {
		unlockFile()
		l.endBackgroundOperation()
	}()

	// 同期的に発生したinternal logは、writer lockの解放後にreportWriteInternalLogの規則で
	// 通知します。このFileLoggerの通知の中から再入した書き込みでは、通知せず同じfileへ
	// 直接記録します。dispatchInternalLogs=falseの書き込み（fallback・直接記録）は
	// internal logを記録も通知もしません。
	var deferredEvents []func()
	var retryLog logging.RetryLogFunc
	var entryFactory logging.EntryFactory
	var latencyHook appendlog.LatencyHook
	if dispatchInternalLogs {
		retryLog = func(eventCtx context.Context, err error, eventEntry *logging.LogEntry) {
			deferredEvents = append(deferredEvents, func() {
				l.wrapAndReportWriteError(eventCtx, err, eventEntry)
			})
		}
		entryFactory = l.InternalErrorEntryWithContext
		latencyHook = func(
			eventCtx context.Context,
			elapsed time.Duration,
			meta any,
		) {
			deferredEvents = append(deferredEvents, func() {
				l.handleWriteLogDelay(eventCtx, elapsed, meta)
			})
		}
	}

	// ファイルハンドルがない場合はエラー
	if err := l.ensureFileWritable(); err != nil {
		unlockFile()
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if capacityErr := l.overload.check(l.Fsstore.FileName.Dir); capacityErr != nil {
		fileName := l.Fsstore.FileName
		overloaded := errors.Is(capacityErr, ErrFileStorageOverloaded)
		// drop/退避ではrotationもfile書込みも行いません。stdoutへの退避と通知は
		// writer lock外で行い、同じFileLoggerへの通知の再入を妨げません。
		unlockFile()
		var handlingErr error
		if overloaded {
			handlingErr = l.overload.handle(ctx, l.LogFormatter.Format(entry)+"\n")
		}
		// 受理済みの退避と、その結果通知が終わるまでCloseを待たせます。
		if l.inlinePurge != nil {
			// 書込み抑止中も期限の来たcleanupは止めません。退避・破棄したbyte数は
			// directory使用量へ加算しません。
			l.triggerCleanup(ctx, fileName)
		}
		// dropは設定した処置が完了した結果なので、退避の成功と同じく容量不足を通知します。
		dropped := errors.Is(handlingErr, pkglogger.ErrLogDropped)
		if !overloaded || (handlingErr != nil && !dropped) {
			return errors.Join(capacityErr, handlingErr)
		}
		if dispatchInternalLogs {
			l.wrapAndReportWriteError(ctx, capacityErr, l.InternalErrorEntry(
				"file write skipped by overload policy", capacityErr, *entry,
			))
		}
		// 退避に成功した場合はnilです。dropした場合は、記録しなかったことを
		// 同期APIの呼び出し元が判別できるよう、ErrLogDroppedを包んだerrorを返します。
		return handlingErr
	}

	// 日付の変更またはファイルサイズの上限に達しているかを確認し、
	// 必要であれば新しい末尾番号のファイルを作成（ローテーション）する。
	// 圧縮を行う設定であれば、非アクティブなログを圧縮する
	rotationErr := l.rotateIfEnableWithHandlers(
		ctx,
		retryLog,
		entryFactory,
	)
	// rotationで開始した後続処理は別に登録済みです。このwrite自身の登録も、
	// 実際の書き込みと通知が戻るまで維持します。
	if rotationErr != nil {
		unlockFile()
		for _, dispatch := range deferredEvents {
			dispatch()
		}
		return rotationErr
	}

	// 「書き込み＋リトライ」
	// retryはsleepを伴うため
	// ベストエフォートで高並列性を求めるのであれば
	// MaxWriteRetriesを0にすること
	writtenBytes, err := l.Fsstore.WriteStringWithRetry(
		ctx,
		l.LogFormatter.Format(entry),
		entry.RequestID,
		retryLog,
		entryFactory,
		ErrorMesFailedWriteFile,
		latencyHook,
		l.config.Fsstore().Retry(),
	)
	inlinePurge := l.inlinePurge
	shouldCleanup := false
	if inlinePurge != nil {
		// mode非依存のwrite処理からinline lifecycleへ入る判定はここで一度だけ行います。
		// 以降のinline purge処理はruntimeが存在することを前提とします。
		// 最終attemptがerrorでも部分writeはdirectoryを実際に増やします。
		// 0byteの場合はdirectory size Tracker側で無視します。
		inlinePurge.directorySizeTracker.RecordGrowth(writtenBytes)
		shouldCleanup = err == nil || writtenBytes > 0
	}

	var cleanupFileNameSnapshot logpattern.FileNamePattern
	if shouldCleanup {
		cleanupFileNameSnapshot = l.Fsstore.FileName
	}
	unlockFile()

	// retry/latency handlerは同じFileLoggerへ戻る場合があります。writer lockを解放してから
	// dispatchし、共有streamの自己deadlockを防ぎます。
	for _, dispatch := range deferredEvents {
		dispatch()
	}

	if shouldCleanup {
		// 通常writeでも日次ageとdirectory sizeの観測要否を確認します。
		// directoryを自然観測する回は、同じsnapshotでfile保持世代数も再評価します。
		l.triggerCleanup(ctx, cleanupFileNameSnapshot)
	}
	return err
}

func (l *FileLogger) inlineLifecycleEnabled() bool {
	if l == nil || l.config == nil {
		return false
	}
	return usesInlineLifecycle(l.config.LifecycleMode())
}

// usesInlineLifecycleは、rotation後の圧縮・archive・purgeをFileLogger自身が
// 所有するmodeかを判定します。空値は従来互換のinline modeとして扱います。
func usesInlineLifecycle(mode pkgfsstore.FileLifecycleMode) bool {
	return mode == "" || mode == pkgfsstore.FileLifecycleModeInline
}

// triggerCleanupは、同じFileLoggerのcleanupが未実行の場合だけ、日次ageの
// purge要否を判定してdirectory size実測用の全体走査権を取得します。directoryを
// 観測する回は、同じsnapshotでgeneration保持数も再評価します。実行中の
// cleanup要求は、次のwriteで新しい判定機会が得られるため待機・pending化せず破棄します。
// 処理理由がない通常writeでは
// background処理もgoroutineも作成しません。default、error、debugの別roleは各自のgateを
// 持ちますが、同じ正規化済みdirectoryの実処理はDirectoryGuard上で直列化されます。
// 呼び出し元はinlinePurgeが構築済みであることを確認していなければなりません。
func (l *FileLogger) triggerCleanup(
	ctx context.Context,
	fileNameSnapshot logpattern.FileNamePattern,
) {
	l.triggerCleanupUsing(
		ctx,
		fileNameSnapshot,
		l.cleanupOldLogsWithNotifications,
		true,
		"",
	)
}

// triggerPostRotationCleanupはrotation taskが保護解除と同じlifecycle境界で
// directory estimateを無効化した後に、一回分の全体観測を要求します。この強制観測は
// pending compressed archiveをpurgeから保護しますが、uploadは再試行しません。
// 別cleanupが実行中なら要求は破棄され、無効化されたcacheを次の自然なcleanupが
// 観測します。複数のrotation taskが近接完了しても、どのpending pathもその時点まで待ちます。
func (l *FileLogger) triggerPostRotationCleanup(
	ctx context.Context,
	fileNameSnapshot logpattern.FileNamePattern,
	rotatedSourcePath string,
) {
	deferredRawRecoveryPath := ""
	if l.config.Fsstore().Lifecycle().EnableFileCompress() {
		deferredRawRecoveryPath = rotatedSourcePath
	}
	l.triggerCleanupUsing(
		ctx,
		fileNameSnapshot,
		l.cleanupOldLogsWithoutPendingUploadRetryWithNotifications,
		false,
		deferredRawRecoveryPath,
	)
}

// triggerCleanupUsingはcleanup gateと観測planを共通化し、通常観測と
// post-rotation強制観測の後処理だけを明示的なcleanup methodで切り替えます。
func (l *FileLogger) triggerCleanupUsing(
	ctx context.Context,
	fileNameSnapshot logpattern.FileNamePattern,
	cleanup func(
		context.Context,
		logpattern.FileNamePattern,
		inlinePurgePlan,
		taskNotificationDelivery,
	) error,
	includeSeriesUploadRetry bool,
	deferredRawRecoveryPath string,
) {
	inlinePurge := l.inlinePurge
	if !inlinePurge.cleanupRunning.CompareAndSwap(false, true) {
		return
	}

	var plan inlinePurgePlan
	cleanupOwnershipTransferred := false
	defer func() {
		if cleanupOwnershipTransferred {
			return
		}
		if plan.directorySizeScanAcquired {
			inlinePurge.directorySizeTracker.AbortObservation()
		}
		inlinePurge.cleanupRunning.Store(false)
	}()

	plan = l.planInlineCleanupIncludingSeriesUploadRetry(includeSeriesUploadRetry)
	plan.deferredRawRecoveryPath = deferredRawRecoveryPath
	if !plan.cleanupRequired() {
		return
	}
	if !l.beginBackgroundOperation() {
		return
	}
	cleanupOwnershipTransferred = true

	go func(plan inlinePurgePlan) {
		// 受理済みcleanupには内部ログ通知の出力も含めます。通常Closeはこの完了まで待ちます。
		defer l.endBackgroundOperation()
		notifications := &cleanupTaskNotificationQueue{}
		cleanupCtx := context.Background()
		if ctx != nil {
			cleanupCtx = context.WithoutCancel(ctx)
		}
		cleanupCtx, cancelCleanup := l.forceWork.context(cleanupCtx)
		defer cancelCleanup()
		// filesystem処理とcleanup gateを解放してから通知します。
		// 通知中も終了待ちの件数は維持し、出力先を先に閉じないようにします。
		cleanupErr := func() error {
			defer inlinePurge.cleanupRunning.Store(false)
			return cleanup(
				cleanupCtx,
				fileNameSnapshot,
				plan,
				notifications.enqueue,
			)
		}()
		if cleanupErr != nil {
			wrappedCleanupErr := fmt.Errorf("failed to cleanup old logs: %w", cleanupErr)
			entry := l.InternalCallerErrorEntry("failed to cleanup old logs", cleanupErr)
			notifications.enqueue(func() {
				l.wrapAndDispatchError(cleanupCtx, wrappedCleanupErr, entry)
			})
		}
		// 通知先から終了を求める場合は、自身の完了を待たないRequestCloseを使います。
		notifications.runDeferredNotifications()
	}(plan)
}

// planInlineCleanupはcleanup gateを所有するcallerについて、現在時刻とdirectory使用量
// Trackerの最新値から一回分のpurge planを構築します。日次状態はownerだけが読み書きします。
func (l *FileLogger) planInlineCleanup() inlinePurgePlan {
	return l.planInlineCleanupIncludingSeriesUploadRetry(true)
}

// planInlineCleanupIncludingSeriesUploadRetryは、post-rotation強制観測が
// series retry scheduleを消費したりtarget-only起動したりしないよう、
// 通常cleanupのみ期限済みscheduleをplanに含めます。
func (l *FileLogger) planInlineCleanupIncludingSeriesUploadRetry(
	includeSeriesUploadRetry bool,
) inlinePurgePlan {
	inlinePurge := l.inlinePurge
	// cleanupTriggeredAtはTTL判定用の正確な時刻、agePurgeReferenceDateは日次age purgeの
	// 判定・削除対象期間・最終実行日に使う基準日です。
	cleanupTriggeredAt := l.Clock.Now()
	agePurgeReferenceDate := pkgtime.StartOfDay(cleanupTriggeredAt)
	plan := inlinePurgePlan{
		cleanupTriggeredAt:    cleanupTriggeredAt,
		agePurgeReferenceDate: agePurgeReferenceDate,
		dailyAgePurgeDue: fsstore.ShouldPurgeByAgeToday(
			agePurgeReferenceDate,
			inlinePurge.lastAgePurgeDate,
		),
	}
	if includeSeriesUploadRetry {
		plan.seriesUploadRetryDue = inlinePurge.seriesUploadRetry.isRetryDue(
			cleanupTriggeredAt,
			inlinePurge.config.UsageCacheTTL(),
		)
	}
	// 通常の期限済みupload scheduleはtarget-onlyを優先し、directory TTLは次の
	// writeへ残します。ただし、このphysical seriesの起動後初回だけは、別roleが
	// 共有Trackerのbaselineを確定済みでもraw archiveを再発見できるよう、upload
	// bootstrapと同じcleanupで全体観測を強制します。
	bootstrapRecoveryPending := inlinePurge.rawArchiveRecoveryBootstrapPending.Load()
	if !plan.seriesUploadRetryDue || bootstrapRecoveryPending {
		plan.directorySizeScanAcquired = inlinePurge.directorySizeTracker.TryStartObservation(
			cleanupTriggeredAt.UTC(),
			inlinePurge.config,
			bootstrapRecoveryPending,
		)
	}
	return plan
}

// rotationの新open待ちは後段のrotation処理で再開します。それ以外のnilはエラーです。
func (l *FileLogger) ensureFileWritable() error {
	if l.Fsstore.File == nil && !l.Fsstore.RotationPending() {
		return &FileLoggerError{
			Op:  "failed to write log",
			Err: errors.New("file is already closed or not opened"),
		}
	}
	return nil
}

func (l *FileLogger) rotateIfEnableWithHandlers(
	ctx context.Context,
	retryLog logging.RetryLogFunc,
	entryFactory logging.EntryFactory,
) error {
	if l.Fsstore.RotationPending() {
		// nil handleを日付・size判定へ渡さず、旧pathを保持したrotationを再開します。
		return l.rotateWithHandlers(ctx, l.Clock.Now(), retryLog, entryFactory)
	}
	if l.config.Fsstore().Lifecycle().EnableRotate() {
		rotationTime := l.Clock.Now()
		return logpattern.CheckAndRotateAt(
			ctx,
			l.Fsstore.File,
			appendlog.NewNameLogPatternConfigAdapter(l.Fsstore.Config),
			&l.Fsstore.FileName,
			rotationTime,
			func(ctx context.Context) error {
				return l.rotateWithHandlers(ctx, rotationTime, retryLog, entryFactory)
			},
		)
	}
	return nil
}

// cleanupOldLogsは、inlinePurge構築済みのFileLoggerについてdirectory共有のlifecycle
// lock内で一回の観測・purgeとpending compressed pathのretainを完了します。remote uploadは
// lock解放後に実行し、全宛先成功時のlocal削除だけを短いlock区間へ戻します。
func (l *FileLogger) cleanupOldLogs(
	ctx context.Context,
	fileNameSnapshot logpattern.FileNamePattern,
	plan inlinePurgePlan,
) error {
	return l.cleanupOldLogsWithNotifications(ctx, fileNameSnapshot, plan, runNotificationImmediately)
}

// cleanupOldLogsWithNotificationsはcleanup中の内部ログ通知をnotifyへ渡します。
// background cleanupの呼び出し元はqueue.enqueueを渡し、処理完了後に通知します。
// 同期入口のcleanupOldLogsはrunNotificationImmediatelyを渡し、その場で通知します。
func (l *FileLogger) cleanupOldLogsWithNotifications(
	ctx context.Context,
	fileNameSnapshot logpattern.FileNamePattern,
	plan inlinePurgePlan,
	notify taskNotificationDelivery,
) error {
	return l.cleanupOldLogsWithPendingUploadCompletionAndNotifications(
		ctx,
		fileNameSnapshot,
		plan,
		l.processRemainingLogFiles,
		notify,
	)
}

// cleanupOldLogsWithoutPendingUploadRetryはpost-rotation強制観測専用です。
// 観測中に再発見したpending compressed archiveはretainしてpurgeから保護し、
// 観測後に解除しますが、upload再試行は次の自然な観測へ回します。
func (l *FileLogger) cleanupOldLogsWithoutPendingUploadRetry(
	ctx context.Context,
	fileNameSnapshot logpattern.FileNamePattern,
	plan inlinePurgePlan,
) error {
	return l.cleanupOldLogsWithoutPendingUploadRetryWithNotifications(
		ctx,
		fileNameSnapshot,
		plan,
		runNotificationImmediately,
	)
}

func (l *FileLogger) cleanupOldLogsWithoutPendingUploadRetryWithNotifications(
	ctx context.Context,
	fileNameSnapshot logpattern.FileNamePattern,
	plan inlinePurgePlan,
	notify taskNotificationDelivery,
) error {
	return l.cleanupOldLogsWithPendingUploadCompletionAndNotifications(
		ctx,
		fileNameSnapshot,
		plan,
		nil,
		notify,
	)
}

// cleanupOldLogsWithPendingUploadCompletionは観測・purge・pending pathのretainを
// 共通のpanic-safeなownership範囲で実行し、通常観測のみlock外のupload再試行を行います。
func (l *FileLogger) cleanupOldLogsWithPendingUploadCompletion(
	ctx context.Context,
	fileNameSnapshot logpattern.FileNamePattern,
	plan inlinePurgePlan,
	completePendingUploads func(context.Context, []archiveUploadGroup) bool,
) error {
	var completionWithNotifications func(
		context.Context,
		[]archiveUploadGroup,
		taskNotificationDelivery,
	) bool
	if completePendingUploads != nil {
		completionWithNotifications = func(
			ctx context.Context,
			groups []archiveUploadGroup,
			_ taskNotificationDelivery,
		) bool {
			return completePendingUploads(ctx, groups)
		}
	}
	return l.cleanupOldLogsWithPendingUploadCompletionAndNotifications(
		ctx,
		fileNameSnapshot,
		plan,
		completionWithNotifications,
		runNotificationImmediately,
	)
}

func (l *FileLogger) cleanupOldLogsWithPendingUploadCompletionAndNotifications(
	ctx context.Context,
	fileNameSnapshot logpattern.FileNamePattern,
	plan inlinePurgePlan,
	completePendingUploads func(
		context.Context,
		[]archiveUploadGroup,
		taskNotificationDelivery,
	) bool,
	notify taskNotificationDelivery,
) error {
	inlinePurge := l.inlinePurge
	cleanupToken := appendlog.NewDirectoryGuardToken()
	var (
		lifecycleProtectedPaths []string
		pendingUploads          []archiveUploadGroup
	)
	defer func() {
		// compression回復、upload失敗、purge error、またはpanicのどの場合も
		// 今回取得したretainを解放します。残存圧縮fileは設定から導く
		// semantic protectionで保護されます。
		l.lifecycleGuard.UnprotectPaths(cleanupToken, lifecycleProtectedPaths...)
	}()

	var uploadRetryAllowed bool
	purgeErr := l.lifecycleGuard.RunWithLifecycleLock(func() error {
		if err := l.purgeLogsWithinGuard(
			ctx,
			fileNameSnapshot,
			plan,
			cleanupToken,
			&lifecycleProtectedPaths,
			&pendingUploads,
			notify,
		); err != nil {
			if plan.seriesUploadRetryDue {
				inlinePurge.seriesUploadRetry.backoffDue(
					l.Clock.Now(),
					inlinePurge.config.UsageCacheTTL(),
				)
			}
			return err
		}
		if completePendingUploads != nil {
			// 走査と保護対象の確定後、送信前に今回の予約を使用済みとして解除します。
			// 成功後には解除しないため、送信中に別のrotationが登録した
			// 新しい予約を、先行する再試行の成功で消しません。
			uploadRetryAllowed = inlinePurge.seriesUploadRetry.claimForAttempt(
				plan.cleanupTriggeredAt,
				inlinePurge.config.UsageCacheTTL(),
			)
		}
		return nil
	})

	if purgeErr == nil && completePendingUploads != nil && uploadRetryAllowed {
		// UploaderやOnError handlerは同じdirectoryへ再入し得るため、remote I/Oは
		// lifecycle lockを保持せずに行います。
		uploadCompletionReturned := false
		defer func() {
			if uploadCompletionReturned {
				return
			}
			// 今回の予約は送信前に解除済みです。Uploaderがpanicしても、残存ログを
			// 次のログ書き込みで再発見できるよう再予約します。
			inlinePurge.seriesUploadRetry.scheduleRetry(
				l.Clock.Now(),
				inlinePurge.config.UsageCacheTTL(),
			)
		}()
		retryRequired := completePendingUploads(
			ctx,
			pendingUploads,
			notify,
		)
		uploadCompletionReturned = true
		if retryRequired {
			inlinePurge.seriesUploadRetry.scheduleRetry(
				l.Clock.Now(),
				inlinePurge.config.UsageCacheTTL(),
			)
		}
	}
	return purgeErr
}

// openLogFileはfileOperationTimeが属するrotation期間のactive fileを開きます。
// rotation判定と呼び出す場合は、判定時に確定した同じ時刻を渡します。
func (l *FileLogger) openLogFile(ctx context.Context, fileOperationTime time.Time) error {
	if l.Fsstore.File != nil {
		return &FileLoggerError{Op: "failed to open log file", Err: ErrFileAlreadyOpen}
	}
	previousPath := l.Fsstore.ActiveFilePath
	previousFileName := l.Fsstore.FileName
	transition := func() (string, error) {
		// file pathの解決からopen完了までをdirectory guardのactive transition境界に含めます。
		if err := updateActiveFilePath(
			&l.Fsstore,
			appendlog.NewNameLogPatternConfigAdapter(l.Fsstore.Config),
			fileOperationTime,
		); err != nil {
			return "", err
		}
		newPath := l.Fsstore.ActiveFilePath
		file, err := appendlog.OpenFile(newPath)
		if err != nil {
			l.Fsstore.ActiveFilePath = previousPath
			l.Fsstore.FileName = previousFileName
			return "", err
		}
		l.Fsstore.File = file
		return newPath, nil
	}

	return l.lifecycleGuard.TransitionActive(l.lifecycleToken, transition)
}

// カスタムエラー型
type FileLoggerError struct {
	Op  string // 操作の種類
	Err error  // 内部エラー
}

func (e *FileLoggerError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *FileLoggerError) Unwrap() error {
	return e.Err
}

// RequestCloseは新規受付を止め、通常終了を別goroutineで開始します。
// 通知内から呼び、通知が戻った後の終了完了はCloseまたはShutdownDoneで確認します。
func (l *FileLogger) RequestClose() {
	if l == nil || l.managedByLoggerSet {
		return
	}
	l.backgroundMu.Lock()
	if l.closeState == loggercommon.LoggerCloseStateOpen {
		l.closeState = loggercommon.LoggerCloseStateStarted
	}
	l.backgroundMu.Unlock()
	go l.Close()
}

// Closeは受理済み処理・checkpoint保存・非同期通知の完了を待って資源を解放します。
// 自身の通知内から同期呼び出しせず、RequestCloseを使ってください。
func (l *FileLogger) Close() error {
	if l == nil || l.managedByLoggerSet {
		return nil
	}
	if !l.closeController.IsForced() && l.closeController.IsComplete() && l.ShutdownComplete() {
		return nil
	}
	if !l.closeController.IsForced() {
		l.backgroundMu.Lock()
		if l.closeState == loggercommon.LoggerCloseStateOpen {
			l.closeState = loggercommon.LoggerCloseStateStarted
		}
		l.backgroundMu.Unlock()
	}
	return l.closeController.Close("file logger", func() (bool, error) {
		err := l.closeOwnedFileResource()
		l.backgroundMu.Lock()
		complete := l.closeState == loggercommon.LoggerCloseStateCompleted
		l.backgroundMu.Unlock()
		return complete, err
	})
}

// closeOwnedFileResourceはbackground処理の完了を待ち、このFileLoggerが所有する
// open fileとdirectory guard tokenを閉じます。
func (l *FileLogger) closeOwnedFileResource() (resultErr error) {
	l.backgroundMu.Lock()
	if attempt := l.closeAttempt; attempt != nil {
		l.backgroundMu.Unlock()
		shutdown.Wait("file logger close already in progress", attempt.done)
		return attempt.err
	}
	if l.closeState == loggercommon.LoggerCloseStateCompleted {
		l.backgroundMu.Unlock()
		return nil
	}
	l.closeState = loggercommon.LoggerCloseStateStarted
	attempt := &fileLoggerCloseAttempt{done: make(chan struct{})}
	l.closeAttempt = attempt
	l.backgroundMu.Unlock()
	defer func() {
		l.backgroundMu.Lock()
		attempt.err = resultErr
		close(attempt.done)
		l.closeAttempt = nil
		l.backgroundMu.Unlock()
	}()
	// 受理済みwrite・rotationが復旧処理を登録し終えるまで、その受付を残します。
	// 新規writeは上で停止済みです。実行中の処理と再試行の両方を完了まで待ちます。
	shutdown.WaitGroup(fmt.Sprintf("file logger accepted tasks: %s", l.directoryPaths[l.logger]), &l.backgroundWG)
	recoveryCloseErr := l.archiveLogFileProcessingRecovery.Close()
	// checkpointへ新しい更新が入らなくなってから、受理済みstateの保存完了を待ちます。
	// その間、通知先・file・uploaderは利用可能なまま維持します。
	var checkpointCloseErr error
	if l.archiveUploadCheckpoints != nil {
		checkpointCloseErr = l.archiveUploadCheckpoints.Close()
	}

	// 受理済みtaskが完了してから退避先の登録を解放し、最後の通知を排出します。
	// 通知が戻るまで、fileとdirectory guardの所有権も維持します。
	uploaderCloseErr := closeArchiveUploader(l.ArchiveUploader)
	loggercommon.WaitForForcedShutdown(l.ArchiveUploader, uploaderCloseErr)
	if uploaderCloseErr != nil && !loggercommon.ShutdownComplete(l.ArchiveUploader) {
		return errors.Join(recoveryCloseErr, checkpointCloseErr, uploaderCloseErr)
	}
	// 復旧・保存が最後に登録した内部ログ通知も、通知先の所有者へ終了を返す前に届けます。
	shutdown.Wait("file processing recovery notifications", l.archiveLogFileProcessingRecovery.NotificationsDone())
	if notifications, ok := l.archiveUploadCheckpoints.(interface{ NotificationsDone() <-chan struct{} }); ok {
		shutdown.Wait("file checkpoint notifications", notifications.NotificationsDone())
	}
	l.Fsstore.Mu.Lock()
	fileCloseErr := l.closeLocked()
	l.Fsstore.Mu.Unlock()
	l.backgroundMu.Lock()
	l.ArchiveUploader = nil
	l.closeState = loggercommon.LoggerCloseStateCompleted
	l.backgroundMu.Unlock()
	return errors.Join(recoveryCloseErr, checkpointCloseErr, fileCloseErr, uploaderCloseErr)
}

// rejectIfClosedはClose開始後の新しい同期記録要求を、level filterやsamplingより
// 前に拒否します。これにより、記録対象外のlevelでもClose後に成功扱いしません。
func (l *FileLogger) rejectIfClosed() error {
	if l == nil || l.forceRequested() {
		return pkglogger.ErrLoggerClosed
	}
	l.backgroundMu.Lock()
	defer l.backgroundMu.Unlock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		return pkglogger.ErrLoggerClosed
	}
	return nil
}

// lockForWriteはClose開始と新規write受付の順序を確定し、backgroundWG予約を
// 追加してからfile lockを取得します。予約は書き込み・退避と、その内部ログ通知が
// 終わるまで維持します。Close開始後のwriteはErrLoggerClosedを返します。
// 成功したcallerはerror・panicを含む全経路で予約を一度だけDoneしてください。
func (l *FileLogger) lockForWrite() error {
	if l == nil || l.forceRequested() {
		return pkglogger.ErrLoggerClosed
	}
	l.backgroundMu.Lock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		l.backgroundMu.Unlock()
		return pkglogger.ErrLoggerClosed
	}
	// Add(1)をCloseのstate遷移およびWait開始と同じmutexで順序付けます。
	// この予約が残る間、受理済みwriteはClose開始後でもrotation archive childを
	// 同じWaitGroupへ追加できます。
	l.backgroundWG.Add(1)
	l.Fsstore.Mu.Lock()
	l.backgroundMu.Unlock()
	return nil
}

func (l *FileLogger) beginBackgroundOperation() bool {
	if l.forceRequested() {
		return false
	}
	l.backgroundMu.Lock()
	defer l.backgroundMu.Unlock()
	if l.closeState != loggercommon.LoggerCloseStateOpen {
		return false
	}
	l.backgroundWG.Add(1)
	return true
}

// beginRotationArchiveOperationFromAcceptedWriteは、Close開始前に受理されたwriteの
// rotation archive childをbackgroundWGへ登録します。callerはlockForWriteが作成した
// registration予約をまだ保持していなければなりません。そのためCloseがすでにWaitを
// 開始していてもcounterは0ではなく、childのAddと予約のDoneを安全に順序付けられます。
func (l *FileLogger) beginRotationArchiveOperationFromAcceptedWrite() {
	l.backgroundWG.Add(1)
}

func (l *FileLogger) endBackgroundOperation() {
	l.backgroundWG.Done()
}

func (l *FileLogger) closeLocked() error {
	err := l.Fsstore.Close()
	// os.FileはCloseがエラーでも再利用できません。エラーは呼び出し元へ返し、
	// 使用終了したfileの参照とactive保護は解除します。受理済み処理は終了待ち済みです。
	l.Fsstore.File = nil
	l.lifecycleGuard.RemoveToken(l.lifecycleToken)
	return err
}

func newFileLoggerBase(
	ctx context.Context,
	cfg FileLoggerConfigProvider,
	loggerType loggerType,
	handlers InternalHandlers,
	managedByLoggerSet bool,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	directories loggerDirectoryPaths,
	providedSpoolRuntimes ...compressedlog.RootRuntimes,
) (*FileLogger, error) {
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)

	ctx, span := tracer.StartSpan(ctx, SpanFileLoggerCreate)
	defer span.End()

	// LoggerSetはlease予約前に設定と実体directoryを検証済みです。lazy生成時に
	// configのaliasを再検証せず、予約時のsnapshotをそのまま使います。
	if directories == nil {
		if err := validateFileLoggerConfigProvider(cfg); err != nil {
			return nil, fmt.Errorf(ErrorMsgInvalidConfigStatus, err)
		}
		var err error
		directories, err = resolveLoggerDirectoryPaths(cfg.Fsstore().Layout())
		if err != nil {
			return nil, err
		}
	}

	timestampFormatter, err := logging.NewTimestampFormatter(
		cfg.Time().EnableLocalTime(),
		cfg.Time().TimeZone(),
		cfg.LogBody().LogLineDateFormat(),
	)
	if err != nil {
		return nil, fmt.Errorf(ErrorMsgCreateLogger, err)
	}

	overload, err := newFileOverload(cfg.Overload(), meter)
	if err != nil {
		return nil, err
	}
	fl := &FileLogger{
		Clock:          clock.NewUTCClock(),
		LogFormatter:   formatter.NewPlainTextFormatter(timestampFormatter),
		logger:         loggerType,
		config:         cfg,
		directoryPaths: directories,
		overload:       overload,
		Fsstore: appendlog.FileStore{
			Config: cfg.Fsstore(),
			Tracer: tracer,
			Meter:  meter,
		},
		// managed uploaderは構築中から通知workerを起動します。公開後にこれらの
		// 初期値を書き換えず、最初のmaintenance errorから設定済みhandlerへ渡します。
		managedByLoggerSet: managedByLoggerSet,
		OnError:            handlers.OnError,
		OnWarn:             handlers.OnWarn,
		OnInfo:             handlers.OnInfo,
		OnDebug:            handlers.OnDebug,
		Tracer:             tracer,
		Meter:              meter,
	}

	startupTime := fl.Clock.Now()
	if err := fl.initFileName(startupTime); err != nil {
		return nil, err
	}
	if usesInlineLifecycle(cfg.LifecycleMode()) {
		var spoolRuntimes compressedlog.RootRuntimes
		if len(providedSpoolRuntimes) > 0 {
			spoolRuntimes = providedSpoolRuntimes[0]
		} else if upload := cfg.ObjectStorageUpload(); upload.UsesCompressedLogFileUploadSpool(
			cfg.Fsstore().Layout().IsSeparateErrorLogsEnabled(), cfg.Fsstore().Layout().IsSeparateDebugLogsEnabled(),
		) {
			if upload.CompressedLogFileUploadSpool() == nil {
				return nil, fmt.Errorf("%w: spool config provider is nil", compressedlog.ErrInvalidCompressedLogFileUploadSpoolConfig)
			}
			services := upload.CompressedLogFileUploadSpoolServices(
				cfg.Fsstore().Layout().IsSeparateErrorLogsEnabled(), cfg.Fsstore().Layout().IsSeparateDebugLogsEnabled(),
			)
			configs := compressedlog.NewConfigAdapters(upload.CompressedLogFileUploadSpool(), services)
			spoolRuntimes, err = compressedlog.NewRootRuntimes(configs...)
			if err != nil {
				return nil, err
			}
			// 単体loggerのroute登録は構築時の一度だけです。以後はuploaderが
			// 閉じるまでrouteがrootを保持するため、初期化ownerを解放します。
			defer spoolRuntimes.Close()
		}
		fl.ArchiveUploader, err = newLogFileUploader(
			cfg.ObjectStorageUpload(),
			loggerType,
		)
		if err != nil {
			return nil, fmt.Errorf(ErrorMsgCreateLogger, err)
		}
		var checkpointOnError func(error)
		if fl.OnError != nil {
			// 保存再試行中に変わるFileNameではなく、構築時の保存先を通知へ使います。
			directory := fl.Fsstore.FileName.Dir
			checkpointOnError = func(err error) {
				ctx, cancel := fl.forceWork.context(context.Background())
				defer cancel()
				fl.notifyArchiveUploadCheckpointError(ctx, directory, err, runNotificationImmediately)
			}
		}
		destinations := fl.archiveUploadDestinationsSnapshot()
		fl.archiveUploadCheckpoints, err = newArchiveCompressedLogCheckpointStore(
			cfg.ArchiveCheckpoint(),
			loggerType,
			fl.Fsstore.FileName,
			destinations.uploader != nil,
			destinations.destinationIDs,
			checkpointOnError,
		)
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("failed to create logger upload checkpoint: %w", err),
				closeArchiveUploader(fl.ArchiveUploader),
			)
		}
		// 送信の完了cursorと途中経過は同じStoreで管理します。Storeを先に構築し、
		// uploaderの初期化時に渡すことで、実行中の依存の差し替えを避けます。
		if fl.ArchiveUploader != nil {
			managed, manageErr := newLogFileUploaderWithFailureStorage(
				uploadTargetConfigForLoggerType(cfg.ObjectStorageUpload(), loggerType),
				fl.Fsstore.FileName, fl.ArchiveUploader, fl, spoolRuntimes,
			)
			if manageErr != nil {
				var checkpointCloseErr error
				if fl.archiveUploadCheckpoints != nil {
					checkpointCloseErr = fl.archiveUploadCheckpoints.Close()
				}
				return nil, errors.Join(manageErr, checkpointCloseErr, closeArchiveUploader(fl.ArchiveUploader))
			}
			fl.ArchiveUploader = managed
		}
	}
	fl.initLifecycleGuard()
	fl.initInlinePurgeRuntime()

	var openErr error
	if fl.inlinePurge != nil {
		// lazy roleの初回openは、同じdirectoryの別roleによるraw回復の完了を待ちます。
		// 回復はretain後にstates lockを解放して圧縮するため、active transitionだけでは
		// 読み取り済みrawの再openと追記を防げません。path選択からactive登録までを
		// lifecycle内に置き、回復済みgenerationを新しいwriterが再利用しないようにします。
		// 通常rotationにはこの待機を追加せず、writer lockとの取得順序を維持します。
		openErr = fl.lifecycleGuard.RunWithLifecycleLock(func() error {
			return fl.openLogFile(ctx, startupTime)
		})
	} else {
		openErr = fl.openLogFile(ctx, startupTime)
	}
	if openErr != nil {
		openErr = fmt.Errorf(ErrorMsgOpenFile, openErr)
		return nil, errors.Join(openErr, fl.closeOwnedFileResource())
	}

	return fl, nil
}

func (l *FileLogger) initFileName(fileOperationTime time.Time) error {
	logDir, err := l.GetFileSystemStoreDirectoryPath()
	if err != nil {
		return fmt.Errorf(MsgInvalidDirectoryPath, err)
	}
	logDir, err = fsstore.GetFileSystemStoreDirectoryPath(logDir, "", "")
	if err != nil {
		return fmt.Errorf(MsgInvalidDirectoryPath, err)
	}
	dateString, err := logpattern.ResolveDateStringForFileNameAt(
		appendlog.NewNameLogPatternConfigAdapter(l.config.Fsstore()),
		fileOperationTime,
	)
	if err != nil {
		return err
	}

	l.Fsstore.FileName = logpattern.FileNamePattern{
		Dir:       logDir,
		Prefix:    l.getPrefixIfEnabled(),
		Target:    l.config.Fsstore().Layout().Target(),
		Date:      dateString,
		Extension: LogFileExtension,
	}

	if err := logpattern.ResolveMaxIndex(&l.Fsstore.FileName); err != nil {
		return fmt.Errorf(MsgInvalidFileIndex, err)
	}
	// directoryのfileだけでなくcheckpointに記録済みの世代も使用済みとし、その後から始めます。
	// inline・runnerのどちらも同じ系列の記録を読みます。
	if err := advancePastRecordedGeneration(
		&l.Fsstore.FileName,
		l.config.ArchiveCheckpoint(),
		l.logger,
		roleUploadEnabled(l.config.ObjectStorageUpload(), l.logger),
	); err != nil {
		return err
	}

	return nil
}
