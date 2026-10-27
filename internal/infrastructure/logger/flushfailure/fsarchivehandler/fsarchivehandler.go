package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorysize"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgtime "github.com/go-jappe/heterolog/pkg/time"
)

// archiveDirectoryRuntimeは、一つのfailure archive directoryについて、writerの
// DirectoryGuard登録とinline purgeの実行状態・概算Trackerをまとめます。
// runner・disable modeではGuardとtokenだけを使用します。
type archiveDirectoryRuntime struct {
	guard *appendlog.DirectoryGuard
	// tokenのownerは、このruntimeが表すretryableまたはpermanentの一系列です。
	// active archive pathと、その系列が処理中のretained pathを識別します。
	token *appendlog.DirectoryGuardToken
	// cleanupRunningは同じfailure categoryのinline purgeを同時に一つだけ実行するための状態です。
	// 実行中に到着した要求は待機・pending化せず破棄します。
	cleanupRunning atomic.Bool
	// lastAgePurgeAtは、このdirectoryでage purgeを最後に開始したcalendar時刻です。
	// 個別fileの削除失敗を含め、purgeを開始した日は再実行しません。
	// 読み書きはcleanupRunningの取得中だけ行います。
	lastAgePurgeAt time.Time

	directorySizeTracker *directorysize.Tracker
	// appendRollbackは、失敗したappendが次のrecordを汚さないようにします。
	// Fsstore.Muが保護し、rotationは元の境界が復元されるまで待ちます。
	appendRollback *archiveAppendRollback
	// uploadCheckpointsは、このruntimeが表す一つのfailure archive系列について、
	// 圧縮log fileごとにupload成功済みの宛先を永続化します。
	// lifecycle modeがinlineでない場合は使用しません。
	uploadCheckpoints checkpointstore.Store
	// uploadRetryはこのruntimeが表す一つのfile系列のtarget-only再試行期限です。
	// directorySizeTrackerとは独立し、upload失敗で全体走査を起動しません。
	uploadRetry archiveUploadRetrySchedule
}

// archiveCleanupNotificationSentErrorは、cleanup内の詳細reporterがOnErrorを
// 所有したerrorを識別します。trigger境界は同じerrorを再通知せず、
// callerへのcontext付与とerror返却だけを行います。
type archiveCleanupNotificationSentError struct {
	err error
}

func (e *archiveCleanupNotificationSentError) Error() string {
	return e.err.Error()
}

func (e *archiveCleanupNotificationSentError) Unwrap() error {
	return e.err
}

// FsArchiveHandlerは受付済みfailureのoverload退避、retryable/permanent archiveへの
// 書き込み・rotation、inline modeの圧縮・upload・purgeを実行します。周期保守runner、
// archive root Lease、handler全体の受付・終了状態は所有しません。
type FsArchiveHandler struct {
	Fsstore *failurearchive.FileStore
	Clock   clock.Clock
	Config  loggerdurable.FileArchiveConfigProvider
	// archiveRootは構築時に解決した保存先です。空き容量の確認もwriterと同じ
	// 実体directoryを使い、設定上のaliasを再び辿りません。
	archiveRoot string

	OnError         logging.LoggerErrorHandler // 通知用の関数　内部エラー発生時にハンドル
	OnWarn          logging.LoggerHandlerFunc
	OnInfo          logging.LoggerHandlerFunc
	OnDebug         logging.LoggerHandlerFunc
	Tracer          pkgobserver.Tracer // observebility計装用
	Meter           pkgobserver.Meter  // observebility計装用
	OverloadHandler overload.Handler

	// uploaderとその宛先は構築時に確定し、稼働開始後は変更しません。
	retryableArchiveUploader uploader.Uploader
	permanentArchiveUploader uploader.Uploader
	archiveUploaderMu        sync.RWMutex
	// appendJSONLWriterはactive archiveへ実際に書けたbyte数を返します。
	// nilの場合はproduction実装を使用し、testではpartial writeを注入できます。
	appendJSONLWriter appendJSONLWriterFunc
	// truncateArchiveAppendは、失敗したappendを回復するためのfilesystem境界です。
	// nilの場合はtruncateFailedArchiveAppendを使い、testでは一時的なI/O失敗を注入できます。
	truncateArchiveAppend func(*os.File, int64) error
	// globArchiveFiles/inspectArchiveFilesはfull/target-only観測のfilesystem境界です。
	// 一回の観測で名前、通常fileの情報、個別pathの検査errorを確定します。
	globArchiveFiles    func(logpattern.FileNamePattern, logpattern.GlobMode) ([]string, error)
	inspectArchiveFiles func([]string) fsstore.FileInspection
	// availableDiskSpaceは過負荷判定でarchive directoryの空き容量を取得するfilesystem境界です。
	// nilの場合はOSの空き容量を使用し、testでは実行環境の空き容量に依存しない値を注入できます。
	availableDiskSpace func(directory string) (uint64, error)
	// waitArchiveWriterLockは、走査が命名情報を読むためのwriter lockをrotationなどが保持しているとき、
	// lockを待つ直前に呼ぶ試験用の境界です。nilなら何もせずにlockを待ちます。
	waitArchiveWriterLock func()

	retryableDirectory    archiveDirectoryRuntime
	permanentDirectory    archiveDirectoryRuntime
	archiveTaskMu         sync.Mutex    // background圧縮・upload taskの世代と件数を保護する
	archiveTaskClosing    bool          // Shutdown開始後の新規圧縮・upload task登録を拒否する
	archiveTaskCount      int           // 現在の世代で実行中の圧縮・upload task数
	archiveTaskGeneration chan struct{} // 現在の世代の全task完了時に閉じるchannel
	// logFileProcessingRecoveryは更新の受理前やfile削除で中断した処理を、writeやpurgeのTTLを待たず再開します。
	// Pipelineの構築時に既定の再開間隔で作成し、稼働開始後は差し替えません。
	// 直接構築したhandlerではnilのため、終了処理はnilを空の再開処理として扱います。
	logFileProcessingRecovery *logfile.LogFileProcessingRecovery[archiveLogFileProcessingRecoveryKey]
	// generationOrderWaitsは、古い世代を追い越さないために待機している世代の処理です。
	// 古い世代の完了記録が進んだ時点で受理済みの再開処理へ戻します。
	generationOrderWaits archiveGenerationOrderWaits
	forceWork            archiveForceContext
}

// handleFailureは外側のPipelineが受理済み処理として登録した要求を同期処理します。
// 過負荷のためdrop policyでrecordを破棄した場合は、後処理を行ったうえで
// pkglogger.ErrLogDroppedを包んだerrorを返します。

func (f *FsArchiveHandler) handleFailure(
	ctx context.Context,
	r loggerfailure.SendResult,
) error {
	ctx = contextOrBackground(ctx)

	// 失敗データをファイルに保存処理という大きめな粒度に対して span を切る
	ctx, span := f.Tracer.StartSpan(ctx, SpanFileWrite)
	defer span.End()

	write := f.rotateAndWriteFailureArchive(ctx, r)
	// OnErrorはfile保存とlock解放の後で呼び、通知先の遅延や再入がdurable保存を
	// 妨げないようにする。
	f.reportDeferredRotations(ctx, write.deferredRotations)
	f.reportPreviousFileCloseFailures(
		ctx,
		write.previousFileCloseFailures,
	)
	f.reportArchiveRotationPreparationFailures(ctx, write.preparationFailures)

	if write.rotationErr != nil {
		span.RecordError(write.rotationErr)
		wrapped := fmt.Errorf("failed to rotate failure archive: %w", write.rotationErr)
		f.delegateError(
			ctx,
			wrapped,
			f.internalCallerErrorEntry("failed to rotate failure archive", write.rotationErr),
		)
		return wrapped
	}

	var dropErr error
	if write.overloaded {
		if f.OverloadHandler == nil {
			err := errNilOverloadHandler
			span.RecordError(err)
			f.delegateError(
				ctx,
				err,
				f.internalCallerErrorEntry("failed to handle failure archive overload", err),
			)
			return err
		}
		if err := f.OverloadHandler.Handle(ctx, r); err != nil {
			if !errors.Is(err, pkglogger.ErrLogDropped) {
				span.RecordError(err)
				f.delegateError(
					ctx,
					err,
					f.internalCallerErrorEntry("failed to evacuate overloaded failure archive", err),
				)
				return err
			}
			// dropは設定した処置の結果であり、metricとspan eventで記録済みです。内部エラー
			// として重ねて通知せず、容量を回復できるよう通常のcleanupを続けてから返します。
			dropErr = err
		}
	} else {
		if write.archiveErr != nil {
			span.RecordError(write.archiveErr)
			wrapped := fmt.Errorf("%w: %w", ErrFailureArchiveWrite, write.archiveErr)
			f.delegateError(
				ctx,
				wrapped,
				f.internalCallerErrorEntry("failed to write failure archive", write.archiveErr),
			)
			if !f.inlineLifecycleEnabled() {
				return wrapped
			}
			return errors.Join(wrapped, f.runInlineCleanup(ctx, r))
		}
	}

	if !f.inlineLifecycleEnabled() {
		// runner modeでは専用runner、disable modeでは誰もpost-rotation lifecycleを
		// 所有しません。writerはdurable appendとrotationだけを同期的に完了します。
		return dropErr
	}

	// inline modeだけが書き込み経路でage、generation、directory-size purgeを行います。
	return errors.Join(dropErr, f.runInlineCleanup(ctx, r))
}

func (f *FsArchiveHandler) runInlineCleanup(
	ctx context.Context,
	_ loggerfailure.SendResult,
) error {
	// retryable/permanentの両trackerへ毎回安価に問い合わせます。各trackerが初回観測、
	// 概算上限、日次age判定、定期補正を判断するため、ここで状態を二重管理しません。
	return errors.Join(
		f.triggerArchiveCleanup(ctx, failurearchive.FailureCategoryRetryable),
		f.triggerArchiveCleanup(ctx, failurearchive.FailureCategoryPermanent),
	)
}

// inlineLifecycleEnabledはwriter自身がcompress、upload、purgeを所有するmodeかを返します。
// 空値はmode導入前の直接構築との互換性のためinlineとして扱います。
func (f *FsArchiveHandler) inlineLifecycleEnabled() bool {
	if f == nil || f.Config == nil {
		return false
	}
	mode := f.Config.LifecycleMode()
	return mode == "" || mode == pkgfsstore.FileLifecycleModeInline
}

func (f *FsArchiveHandler) runnerLifecycleEnabled() bool {
	return f != nil &&
		f.Config != nil &&
		f.Config.LifecycleMode() == pkgfsstore.FileLifecycleModeRunner
}

func contextWithoutCancel(ctx context.Context) context.Context {
	return context.WithoutCancel(contextOrBackground(ctx))
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// rotateIfEnableは日付変更またはfile size上限を確認し、必要なarchiveだけを
// 新しいactive fileへ切り替えます。rotation後のlifecycleはmodeごとのownerへ渡します。
type deferredArchiveRotation struct {
	failureCategory failurearchive.FailureCategory
	err             error
}

type previousArchiveFileCloseFailure struct {
	failureCategory failurearchive.FailureCategory
	path            string
	err             error
}

// archiveRotationResultは書き込み先の切替結果と、切替後の処理を開始できなかったエラーです。
// preparationErrがあっても切替済みのactive fileは維持し、今回のレコードを書き込みます。
type archiveRotationResult struct {
	failurearchive.RotationResult
	preparationErr error
}

type archiveRotationPreparationFailure struct {
	path string
	err  error
}

type failureArchiveRotationAttempt struct {
	deferredRotations         []deferredArchiveRotation
	previousFileCloseFailures []previousArchiveFileCloseFailure
	preparationFailures       []archiveRotationPreparationFailure
	err                       error
}

type failureArchiveWriteAttempt struct {
	deferredRotations         []deferredArchiveRotation
	previousFileCloseFailures []previousArchiveFileCloseFailure
	preparationFailures       []archiveRotationPreparationFailure
	overloaded                bool
	rotationErr               error
	archiveErr                error
}

// rotateAndWriteFailureArchiveはrotation判定、overload判定、active fileへの追記を
// 一つのFsstore.Mu区間で実行します。rotation先をopenできず延期した場合も、別の
// failure処理がactive fileを切り替える前に今回のrecordをold handleへ保存します。
// OnError通知とoverload handlerの実行は、このmethodがlockを解放した後に行います。
func (f *FsArchiveHandler) rotateAndWriteFailureArchive(
	ctx context.Context,
	result loggerfailure.SendResult,
) failureArchiveWriteAttempt {
	f.Fsstore.Mu.Lock()
	defer f.Fsstore.Mu.Unlock()
	if err := f.recoverArchiveAppendsLocked(); err != nil {
		return failureArchiveWriteAttempt{archiveErr: err}
	}

	rotation := f.rotateIfEnable(ctx)
	attempt := failureArchiveWriteAttempt{
		deferredRotations:         rotation.deferredRotations,
		previousFileCloseFailures: rotation.previousFileCloseFailures,
		preparationFailures:       rotation.preparationFailures,
		rotationErr:               rotation.err,
	}
	if rotation.err != nil {
		return attempt
	}

	attempt.overloaded, attempt.archiveErr = f.isOverloaded(ctx)
	if attempt.archiveErr != nil {
		return attempt
	}
	if !attempt.overloaded {
		attempt.archiveErr = f.archiveLocked(ctx, result)
	}
	return attempt
}

func (f *FsArchiveHandler) rotateIfEnable(
	ctx context.Context,
) failureArchiveRotationAttempt {
	if !f.Config.EnableRotate() {
		return failureArchiveRotationAttempt{}
	}

	var attempt failureArchiveRotationAttempt
	// retryable/permanentの判定と切替先pathは、同じClockの時刻から解決します。
	// 二つのfailure categoryの処理中に日付境界を跨いでも、同じfailure処理では同じ期間へ切り替えます。
	rotationAt := f.Clock.Now()
	check := func(
		failureCategory failurearchive.FailureCategory,
		file *os.File,
		fileName *logpattern.FileNamePattern,
		rotate func(context.Context) (archiveRotationResult, error),
	) {
		originalFileName := *fileName
		var rotationResult archiveRotationResult
		err := logpattern.CheckAndRotateAt(
			ctx,
			file,
			f.Config,
			fileName,
			rotationAt,
			func(ctx context.Context) error {
				var err error
				rotationResult, err = rotate(ctx)
				return err
			},
		)
		if err != nil {
			*fileName = originalFileName
			if errors.Is(err, failurearchive.ErrRotationTargetUnavailable) {
				attempt.deferredRotations = append(
					attempt.deferredRotations,
					deferredArchiveRotation{failureCategory: failureCategory, err: err},
				)
				return
			}
			attempt.err = errors.Join(attempt.err, err)
			return
		}
		if rotationResult.PreviousFileCloseErr != nil {
			attempt.previousFileCloseFailures = append(
				attempt.previousFileCloseFailures,
				previousArchiveFileCloseFailure{
					failureCategory: failureCategory,
					path:            rotationResult.OldFilePath,
					err:             rotationResult.PreviousFileCloseErr,
				},
			)
		}
		if rotationResult.preparationErr != nil {
			attempt.preparationFailures = append(attempt.preparationFailures, archiveRotationPreparationFailure{
				path: rotationResult.OldFilePath, err: rotationResult.preparationErr,
			})
		}
	}

	check(
		failurearchive.FailureCategoryPermanent,
		f.Fsstore.FilePermanent,
		&f.Fsstore.FileNamePermanent,
		func(ctx context.Context) (archiveRotationResult, error) {
			return f.rotatePermanent(ctx, rotationAt)
		},
	)
	check(
		failurearchive.FailureCategoryRetryable,
		f.Fsstore.FileRetryable,
		&f.Fsstore.FileNameRetryable,
		func(ctx context.Context) (archiveRotationResult, error) {
			return f.rotateRetryable(ctx, rotationAt)
		},
	)
	return attempt
}

// reportDeferredRotationsは新しいarchive fileを準備できなかった異常をOnErrorへ通知します。
// 現在のactive fileはopenされたままなので、呼び出し元は今回のfailure recordをそこへ
// 保存し、次回の書き込み時にrotationを再試行します。

func (f *FsArchiveHandler) reportDeferredRotations(
	ctx context.Context,
	rotations []deferredArchiveRotation,
) {
	for _, rotation := range rotations {
		wrapped := fmt.Errorf(
			"%s %w; continuing with current active file: %w",
			rotation.failureCategory,
			ErrFailureArchiveRotationDeferred,
			rotation.err,
		)
		f.delegateError(
			ctx,
			wrapped,
			f.internalCallerErrorEntry(
				fmt.Sprintf(
					"%s failure archive rotation was deferred; continuing with current active file",
					rotation.failureCategory,
				),
				rotation.err,
			),
		)
	}
}

// reportPreviousFileCloseFailuresは新しいactive fileへの切替と今回のrecord保存が
// 完了した後、使用済みfileのclose異常をOnErrorへ通知します。

func (f *FsArchiveHandler) reportPreviousFileCloseFailures(
	ctx context.Context,
	failures []previousArchiveFileCloseFailure,
) {
	for _, failure := range failures {
		wrapped := fmt.Errorf(
			"%s failure archive switched to a new active file, but closing previous file %q failed: %w",
			failure.failureCategory,
			failure.path,
			failure.err,
		)
		f.delegateError(
			ctx,
			wrapped,
			f.internalCallerErrorEntry(
				fmt.Sprintf(
					"%s failure archive switched to a new active file, but closing the previous file failed",
					failure.failureCategory,
				),
				failure.err,
			),
		)
	}
}

// reportArchiveRotationPreparationFailuresは、ログの書き込み先を切り替えた後、
// 元ログの圧縮・送信を開始する準備で発生したエラーを、一件ずつOnErrorへ通知します。
// 既存の圧縮ファイルの探索や、圧縮先パスの決定などの失敗が対象です。
// 呼び出し元は、今回のレコードの保存を試みてFsstore.Muを解放した後、
// ディレクトリ走査・purgeなどの後処理を始める前に呼んでください。
// 通知中にwriterのlockを保持せず、後処理の完了まで通知を遅らせないためです。
func (f *FsArchiveHandler) reportArchiveRotationPreparationFailures(ctx context.Context, failures []archiveRotationPreparationFailure) {
	for _, failure := range failures {
		f.notifyCompressedArchiveUploadCheckpointError(ctx, failure.path, failure.err)
	}
}

// rotateRetryableはretryable archiveをrotationし、old fileをmodeごとのownerへ渡します。
func (f *FsArchiveHandler) rotateRetryable(
	ctx context.Context,
	at time.Time,
) (archiveRotationResult, error) {
	oldActivePath := f.Fsstore.ActiveFilePathRetryable
	protectedPaths := f.protectRotatedFileBeforeTransition(
		failurearchive.FailureCategoryRetryable,
		oldActivePath,
	)
	var result failurearchive.RotationResult
	err := f.retryableDirectory.guard.TransitionActive(
		f.retryableDirectory.token,
		func() (string, error) {
			var err error
			result, err = f.Fsstore.Rotate(ctx, failurearchive.FailureCategoryRetryable, at)
			if err != nil {
				return "", err
			}
			return f.Fsstore.ActiveFilePathRetryable, nil
		},
	)
	preparationErr := f.finishRotatedFileTransition(
		ctx,
		f.Fsstore.ActiveFilePathRetryable,
		result.OldFilePath,
		failurearchive.FailureCategoryRetryable,
		protectedPaths,
		err,
	)
	return archiveRotationResult{RotationResult: result, preparationErr: preparationErr}, err
}

// rotatePermanentはpermanent archiveをrotationし、old fileをmodeごとのownerへ渡します。
func (f *FsArchiveHandler) rotatePermanent(
	ctx context.Context,
	at time.Time,
) (archiveRotationResult, error) {
	oldActivePath := f.Fsstore.ActiveFilePathPermanent
	protectedPaths := f.protectRotatedFileBeforeTransition(
		failurearchive.FailureCategoryPermanent,
		oldActivePath,
	)
	var result failurearchive.RotationResult
	err := f.permanentDirectory.guard.TransitionActive(
		f.permanentDirectory.token,
		func() (string, error) {
			var err error
			result, err = f.Fsstore.Rotate(ctx, failurearchive.FailureCategoryPermanent, at)
			if err != nil {
				return "", err
			}
			return f.Fsstore.ActiveFilePathPermanent, nil
		},
	)
	preparationErr := f.finishRotatedFileTransition(
		ctx,
		f.Fsstore.ActiveFilePathPermanent,
		result.OldFilePath,
		failurearchive.FailureCategoryPermanent,
		protectedPaths,
		err,
	)
	return archiveRotationResult{RotationResult: result, preparationErr: preparationErr}, err
}

// triggerArchiveCleanupは、指定した失敗分類のディレクトリについて、未完了の送信の
// 再試行を含む通常のcleanupを要求します。同じ分類のcleanupが実行中なら、
// 今回の要求は待機や再実行の予約をせずに終了します。
func (l *FsArchiveHandler) triggerArchiveCleanup(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
) error {
	return l.triggerArchiveCleanupWithOptions(ctx, failureCategory, true, "")
}

// triggerPostRotationArchiveCleanupは、rotation後にディレクトリ全体の再確認と
// purgeを要求します。送信待ちのファイルをpurgeから保護し、送信の再試行は
// 次の通常のcleanupに任せます。同じ分類のcleanupが実行中なら、今回の要求は終了します。
// deferredRawRecoveryPathには、今回のcleanupで圧縮を再試行しない元ログのパスを渡します。
// 空文字の場合は、この理由で除外する元ログはありません。
func (l *FsArchiveHandler) triggerPostRotationArchiveCleanup(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
	deferredRawRecoveryPath string,
) error {
	return l.triggerArchiveCleanupWithOptions(
		ctx,
		failureCategory,
		false,
		deferredRawRecoveryPath,
	)
}

func (l *FsArchiveHandler) triggerArchiveCleanupWithOptions(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
	retryPendingUploads bool,
	deferredRawRecoveryPath string,
) error {
	state := l.directoryForFailureCategory(failureCategory)
	if state == nil {
		return nil
	}
	cleanupCtx := contextWithoutCancel(ctx)
	cleanupCtx, cancelCleanup := l.forceWork.context(cleanupCtx)
	defer cancelCleanup()
	if !state.cleanupRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer state.cleanupRunning.Store(false)

	if err := l.runInlineArchivePurgeWithOptions(
		cleanupCtx,
		failureCategory,
		retryPendingUploads,
		deferredRawRecoveryPath,
	); err != nil {
		wrapped := fmt.Errorf("failed to cleanup %s failure archives: %w", failureCategory, err)
		var notificationSent *archiveCleanupNotificationSentError
		if !errors.As(err, &notificationSent) {
			l.delegateError(
				cleanupCtx,
				wrapped,
				l.internalCallerErrorEntry(
					fmt.Sprintf("failed to cleanup old %s failure archives", failureCategory),
					err,
				),
			)
		}
		return wrapped
	}
	return nil
}

func (f *FsArchiveHandler) runInlineArchivePurgeWithOptions(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
	retryPendingUploads bool,
	deferredRawRecoveryPath string,
) error {
	state := f.directoryForFailureCategory(failureCategory)
	if state == nil {
		return fmt.Errorf("failure archive directory runtime is unavailable for failureCategory %q", failureCategory)
	}
	purgeAt := f.Clock.Now()
	retryInterval := f.Config.UsageCacheTTL()
	// 送信の再試行期限に達していれば、その系列だけを走査します。
	// ディレクトリ全体の走査・purgeは、次のcleanupで別に要否を判定します。
	// rotation直後のcleanupは再試行予約を消費せず、ディレクトリ全体の処理へ進みます。
	if retryPendingUploads && state.uploadRetry.due(
		purgeAt.UTC(),
		retryInterval,
	) {
		return f.retryScheduledArchiveUploads(ctx, failureCategory, state, purgeAt.UTC(), retryInterval)
	}
	return f.cleanupArchiveDirectory(ctx, failureCategory, state, purgeAt, deferredRawRecoveryPath)
}

// archivePurgeResultは、一回の走査から行った各purgeの結果と削除後の概算使用量です。
// ファイル操作の結果だけを保持し、通知はlifecycle lockの解放後に行います。
type archivePurgeResult struct {
	ageResult              fsstore.PurgeResult
	generationResult       fsstore.PurgeResult
	dirSizeResult          fsstore.PurgeResult
	dirSizeDiagnostic      archiveDirectorySizePurgeDiagnostic
	ageExecuted            bool
	generationExecuted     bool
	dirSizeExecuted        bool
	observedDirectoryBytes uint64
}

// purgeObservedArchiveFilesは、一度の走査で取得したファイル一覧から、保持期限・
// 世代数・ディレクトリ容量の順にpurgeを行います。削除が失敗した時点で後続のpurgeを止めます。
// 保持日数・保持世代数・ディレクトリ最大サイズが0の条件は上限なしとして、そのpurgeを行いません。
// 戻り値の使用量は削除成功分だけを除いた概算値で、削除失敗時も利用できます。
// 通常構築では呼び出し元が対象ディレクトリのlifecycle lockを保持してください。
// Guardのない直接構築では、この関数が共有Guardを取得します。
// 通知は行わず、呼び出し元がlockを解放してからreportArchivePurgeへ結果を渡します。
func (f *FsArchiveHandler) purgeObservedArchiveFiles(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
	lastPurgedDate time.Time,
	purgeAt time.Time,
	ageDue bool,
	inspection archiveDirectoryInspection,
	retainedUploadPaths []string,
) archivePurgeResult {
	var (
		ageResult          fsstore.PurgeResult
		generationResult   fsstore.PurgeResult
		dirSizeResult      fsstore.PurgeResult
		dirSizeDiagnostic  archiveDirectorySizePurgeDiagnostic
		ageExecuted        bool
		generationExecuted bool
		dirSizeExecuted    bool
		remainingAllFiles  = inspection.allFileInfos()
	)
	prepareProtection := func(protectedPaths map[string]struct{}) map[string]struct{} {
		purgeProtectedPaths := includeProtectedArchivePaths(
			protectedPaths,
			retainedUploadPaths,
		)
		return includeProtectedArchiveGenerationPaths(
			purgeProtectedPaths,
			inspection.seriesSources.Files.RegularFiles,
			inspection.seriesPattern,
		)
	}
	purgeWithRetainedCandidates := func(purge *appendlog.Purge) {
		remainingTarget := inspection.seriesSources.Files.RegularFiles
		updateRemainingAllFiles := func() {
			remainingAllFiles = replaceArchiveFileInfos(
				inspection.allFileInfos(),
				inspection.seriesSources.Files.RegularFiles,
				remainingTarget,
			)
		}

		// 保持日数0は期限なしのため、期限では削除しません。
		if ageDue && f.Config.MaxFileAge() > 0 {
			ageExecuted = true
			ageResult = f.executeArchiveAgePurge(
				ctx,
				purge,
				remainingTarget,
				lastPurgedDate,
				purgeAt,
			)
			remainingTarget = ageResult.Remaining
			updateRemainingAllFiles()
			if fsstore.HasFileRemovalFailures(ageResult.RemovalResults) {
				return
			}
		}

		// 保持世代数0は上限なしのため、世代数では削除しません。
		if f.Config.MaxFileBackups() > 0 {
			generationExecuted = true
			generationResult = f.executeArchiveGenerationPurge(
				ctx,
				purge,
				remainingTarget,
				inspection.seriesPattern.Extension,
			)
			remainingTarget = generationResult.Remaining
			updateRemainingAllFiles()
			if fsstore.HasFileRemovalFailures(generationResult.RemovalResults) {
				return
			}
		}

		// ディレクトリ最大サイズ0は上限なしのため、容量では削除せず超過の警告も行いません。
		if f.Config.MaxDirSize() == 0 {
			return
		}
		// 開始時のactive/retainedも容量合計には残し、同じ保護snapshotを削除候補から
		// だけ除外する。並行rotationでactiveが変わっても診断はこの観測に揃える。
		dirSizeExecuted = true
		dirSizeResult = f.executeArchiveDirectorySizePurge(
			ctx,
			purge,
			remainingAllFiles,
		)
		remainingAllFiles = dirSizeResult.Remaining
		dirSizeDiagnostic = newArchiveDirectorySizePurgeDiagnostic(
			failureCategory,
			inspection.seriesPattern.Dir,
			remainingAllFiles,
			purge.DeletionCandidates(remainingAllFiles),
			purge.ActivePaths(),
		)
	}
	state := f.directoryForFailureCategory(failureCategory)
	if state != nil && state.guard != nil {
		// 両failure categoryの初回openは構築時に完了済みです。既存writerは新しい世代へだけ
		// 切り替わるため、削除候補を再openしません。lifecycle lockはcallerが保持します。
		state.guard.RunPurgeWithProtection(inspection.allFileInfos(), prepareProtection, purgeWithRetainedCandidates)
	} else {
		// 直接構築されたhandlerでも、同じdirectoryのwriterが登録した保護を利用する。
		// 通常branchと異なりcallerのlifecycle lockがないため、ここで取得する。
		guard := appendlog.DirectoryGuardForPath(inspection.seriesPattern.Dir)
		_ = guard.RunWithLifecycleLock(func() error {
			guard.RunPurgeWithProtection(inspection.allFileInfos(), prepareProtection, purgeWithRetainedCandidates)
			return nil
		})
	}

	return archivePurgeResult{
		ageResult: ageResult, generationResult: generationResult, dirSizeResult: dirSizeResult,
		dirSizeDiagnostic: dirSizeDiagnostic, ageExecuted: ageExecuted,
		generationExecuted: generationExecuted, dirSizeExecuted: dirSizeExecuted,
		observedDirectoryBytes: archiveDirectoryBytes(remainingAllFiles),
	}
}

// reportArchivePurgeは実行したpurgeの結果を内部ログへ通知し、削除エラーを返します。
// 呼び出し元は検査・削除に使ったlifecycle lockを解放してから呼んでください。
func (f *FsArchiveHandler) reportArchivePurge(ctx context.Context, result archivePurgeResult, purgeAt time.Time) error {
	if result.ageExecuted {
		if err := f.reportArchiveAgePurge(ctx, result.ageResult, purgeAt); err != nil {
			return err
		}
	}
	if result.generationExecuted {
		if err := f.reportArchiveGenerationPurge(ctx, result.generationResult); err != nil {
			return err
		}
	}
	if result.dirSizeExecuted {
		return f.reportArchiveDirectorySizePurge(ctx, result.dirSizeResult, result.dirSizeDiagnostic)
	}
	return nil
}

// includeProtectedArchivePathsは、観測中にupload待ちとしてretainしたcompressed pathを
// purge用の保護集合へ加えます。DirectoryGuardがないdirect構築でも同じ削除契約を守り、
// Guardがある通常経路ではretain直後のpathを明示的に同じ観測へ引き継ぎます。
func includeProtectedArchivePaths(
	protected map[string]struct{},
	paths []string,
) map[string]struct{} {
	if len(paths) == 0 {
		return protected
	}
	merged := make(map[string]struct{}, len(protected)+len(paths))
	for path := range protected {
		merged[path] = struct{}{}
	}
	for _, path := range paths {
		if normalized := pathidentity.AbsoluteOrCleanPath(path); normalized != "" {
			merged[normalized] = struct{}{}
		}
	}
	return merged
}

// includeProtectedArchiveGenerationPathsは、圧縮・upload taskが一つのpathを
// 保護しているgenerationについて、同時に観測した別suffixもpurgeから
// 除外します。これにより、groupのremote upload中に先行したcompressed log fileだけを
// generation・size purgeが削除することを防ぎます。
func includeProtectedArchiveGenerationPaths(
	protected map[string]struct{},
	files []fsstore.FileInfo,
	fileName logpattern.FileNamePattern,
) map[string]struct{} {
	protectedGenerations := make(map[string]struct{})
	for path := range protected {
		generation, canonical := logpattern.ParseCanonicalGenerationFilePath(path, fileName)
		if !canonical {
			continue
		}
		protectedGenerations[checkpointstore.ArchiveCheckpointGenerationKey(
			generation.Period,
			generation.Index,
		)] = struct{}{}
	}
	if len(protectedGenerations) == 0 {
		return protected
	}
	for _, file := range files {
		generation, canonical := logpattern.ParseCanonicalGenerationFilePath(file.Path, fileName)
		if !canonical {
			continue
		}
		key := checkpointstore.ArchiveCheckpointGenerationKey(generation.Period, generation.Index)
		if _, sameGeneration := protectedGenerations[key]; sameGeneration {
			protected[pathidentity.AbsoluteOrCleanPath(file.Path)] = struct{}{}
		}
	}
	return protected
}

// lockArchiveWriterForObservationは、走査が命名情報を読み取るためにwriter lockを取得します。
// rotationの切替中などでlockが使用中なら、その処理の完了を待ってから取得します。
func (f *FsArchiveHandler) lockArchiveWriterForObservation() {
	if f.Fsstore.Mu.TryLock() {
		return
	}
	if wait := f.waitArchiveWriterLock; wait != nil {
		wait()
	}
	f.Fsstore.Mu.Lock()
}

// observeArchiveDirectoryForPurgeは対象物理directoryのmanaged archiveを一回だけ
// globし、Lstatで通常fileと確認できたものを返します。リンク・directory・情報取得失敗は
// purge候補と概算使用量から除外します。Guardの保護一覧は、この観測後に
// RunPurgeWithProtectionがstates lock下で取得します。実際の削除中はstates lockを保持しません。
// writer lockは命名情報のコピーにだけ使い、
// directory I/Oと検査失敗の通知中も追記を許可します。
func (f *FsArchiveHandler) observeArchiveDirectoryForPurge(
	failureCategory failurearchive.FailureCategory,
) (archiveDirectoryInspection, error) {
	f.lockArchiveWriterForObservation()
	fileName, err := f.archiveFileNamePattern(failureCategory)
	f.Fsstore.Mu.Unlock()
	if err != nil {
		return archiveDirectoryInspection{}, err
	}

	glob := f.globArchiveFiles
	if glob == nil {
		glob = logpattern.GlobLifecycleFileNames
	}
	matches, err := glob(fileName, logpattern.GlobAllFiles)
	if err != nil {
		return archiveDirectoryInspection{}, fmt.Errorf("failed to glob log files: %w", err)
	}

	return newArchiveDirectoryInspection(f.inspectArchivePaths(matches), fileName), nil
}

// observeArchiveFileSeriesForUploadはupload再試行期限に達したfailure categoryのpathだけを
// GlobByTargetで取得します。writer lockで命名情報をコピーしてから、lock外で
// 一度だけ名前・通常file・検査errorを観測します。directory byte数やpurge判断には
// 使いません。正規世代のsymlink等は不存在として扱わず、retain前のerrorで保全します。
func (f *FsArchiveHandler) observeArchiveFileSeriesForUpload(
	failureCategory failurearchive.FailureCategory,
) (fsstore.FileInspection, logpattern.FileNamePattern, error) {
	f.lockArchiveWriterForObservation()
	fileName, err := f.archiveFileNamePattern(failureCategory)
	f.Fsstore.Mu.Unlock()
	if err != nil {
		return fsstore.FileInspection{}, logpattern.FileNamePattern{}, err
	}
	glob := f.globArchiveFiles
	if glob == nil {
		glob = logpattern.GlobFileNames
	}
	paths, err := glob(fileName, logpattern.GlobByTarget)
	if err != nil {
		return fsstore.FileInspection{}, logpattern.FileNamePattern{}, fmt.Errorf(
			"failed to glob log files: %w",
			err,
		)
	}
	return f.inspectArchivePaths(paths), fileName, nil
}

// archiveFileNamePatternはFsstore.Mu保持中、または構築中の排他的な状態でだけ呼びます。
// background処理へはこの返却値またはdirectory observationのpatternを値渡ししてください。
func (f *FsArchiveHandler) archiveFileNamePattern(
	failureCategory failurearchive.FailureCategory,
) (logpattern.FileNamePattern, error) {
	switch failureCategory {
	case failurearchive.FailureCategoryRetryable:
		return f.Fsstore.FileNameRetryable, nil
	case failurearchive.FailureCategoryPermanent:
		return f.Fsstore.FileNamePermanent, nil
	default:
		return logpattern.FileNamePattern{}, fmt.Errorf("unknown failure archive category: %q", failureCategory)
	}
}

func replaceArchiveFileInfos(
	files []fsstore.FileInfo,
	original []fsstore.FileInfo,
	replacement []fsstore.FileInfo,
) []fsstore.FileInfo {
	if len(files) == 0 || len(original) == 0 {
		return files
	}
	originalPaths := make(map[string]struct{}, len(original))
	for _, file := range original {
		originalPaths[pathidentity.AbsoluteOrCleanPath(file.Path)] = struct{}{}
	}
	remaining := fsstore.ExcludeFileInfosByPaths(files, originalPaths)
	return append(remaining, replacement...)
}

func archiveDirectoryBytes(files []fsstore.FileInfo) uint64 {
	var bytes uint64
	for _, file := range files {
		if math.MaxUint64-bytes < file.Size {
			bytes = math.MaxUint64
			continue
		}
		bytes += file.Size
	}
	return bytes
}

// archiveDirectorySizePurgeDiagnosticは、容量によるpurge後の使用量と削除候補の有無を、
// lifecycleのロックを解放した後の警告判定へ渡すための値です。
// 上限超過が残った場合に、使用中のファイルだけで上限を超えているか、
// 保護条件上の削除候補が残っていないかを判定し、OnWarnへ通知するために使います。
// 対象拡張子の通常ファイルについて、走査で得た情報から削除成功分を除いて集計します。
// 再走査は行わず、並行する追記・rotation後の最新の容量や保護状態は反映しません。
type archiveDirectorySizePurgeDiagnostic struct {
	// failureCategoryは、今回のpurgeを実行した失敗分類です。retryableまたはpermanentです。
	failureCategory failurearchive.FailureCategory
	// directoryPathは、容量上限の判定対象となったfailure archiveの保存先です。
	directoryPath string
	// directoryBytesは、purge後に残った対象ファイルの合計サイズです。単位はbyteです。
	// 同じディレクトリの別系列も含み、使用中・保護中のファイルも容量に数えます。
	directoryBytes uint64
	// activeFileBytesは、残ったファイルのうち、purge開始時にDirectoryGuardへ
	// 使用中として登録されていたファイルの合計サイズです。単位はbyteです。
	activeFileBytes uint64
	// hasRemainingDeletionCandidatesは、残ったファイルから保護中・purge開始時に未登録・
	// 削除済みを除いた候補が、一件以上あることを表します。
	// ファイル名の日付・世代番号の検証や、実際に削除できるかの判定は含みません。
	hasRemainingDeletionCandidates bool
}

func newArchiveDirectorySizePurgeDiagnostic(
	failureCategory failurearchive.FailureCategory,
	directoryPath string,
	remaining []fsstore.FileInfo,
	deletionCandidates []fsstore.FileInfo,
	activePaths map[string]struct{},
) archiveDirectorySizePurgeDiagnostic {
	diagnostic := archiveDirectorySizePurgeDiagnostic{
		failureCategory:                failureCategory,
		directoryPath:                  directoryPath,
		directoryBytes:                 archiveDirectoryBytes(remaining),
		hasRemainingDeletionCandidates: len(deletionCandidates) > 0,
	}
	activeFiles := make([]fsstore.FileInfo, 0, len(activePaths))
	for _, file := range remaining {
		if !pathidentity.Contains(activePaths, file.Path) {
			continue
		}
		activeFiles = append(activeFiles, file)
	}
	diagnostic.activeFileBytes = archiveDirectoryBytes(activeFiles)
	return diagnostic
}

func (f *FsArchiveHandler) executeArchiveAgePurge(
	ctx context.Context,
	purge *appendlog.Purge,
	files []fsstore.FileInfo,
	lastPurgedDate time.Time,
	purgeAt time.Time,
) fsstore.PurgeResult {
	ctx, span := f.Tracer.StartSpan(ctx, SpanFilePurgeByAge)
	defer span.End()

	return purge.PurgeByAge(
		files,
		f.Config.MaxFileAge(),
		purgeAt,
		lastPurgedDate,
	)
}

func (f *FsArchiveHandler) executeArchiveGenerationPurge(
	ctx context.Context,
	purge *appendlog.Purge,
	files []fsstore.FileInfo,
	baseExtension string,
) fsstore.PurgeResult {
	if logpattern.CountGenerations(purge.DeletionCandidates(files), baseExtension) <=
		uint64(f.Config.MaxFileBackups()) {
		return purge.PurgeGenerationSeries(
			files,
			baseExtension,
			f.Config.MaxFileBackups(),
		)
	}

	ctx, span := f.Tracer.StartSpan(ctx, SpanFilePurgeByGeneration)
	defer span.End()

	return purge.PurgeGenerationSeries(
		files,
		baseExtension,
		f.Config.MaxFileBackups(),
	)
}

func (f *FsArchiveHandler) executeArchiveDirectorySizePurge(
	ctx context.Context,
	purge *appendlog.Purge,
	files []fsstore.FileInfo,
) fsstore.PurgeResult {
	if !fsstore.ShouldPurgeByDirectorySize(files, f.Config.MaxDirSize()) {
		return purge.PurgeByDirectorySize(f.Config.MaxDirSize())
	}

	ctx, span := f.Tracer.StartSpan(ctx, SpanFilePurgeByDirectorySize)
	defer span.End()

	return purge.PurgeByDirectorySize(f.Config.MaxDirSize())
}

func (f *FsArchiveHandler) reportArchiveAgePurge(
	ctx context.Context,
	result fsstore.PurgeResult,
	purgeAt time.Time,
) error {
	//OnDebug パージ開始します
	if result.Ran && f.Config.IsDebug() {
		f.delegateDebug(
			ctx,
			f.internalCallerDebugEntry(DebugMsgPurgedFileAge),
		)
	}

	// デバッグモードであれば削除対象ファイルを記録
	if f.Config.IsDebug() {
		for _, removal := range result.RemovalResults {
			if removal.Err != nil {
				continue
			}
			deleted := removal.File
			entry := f.internalCallerDebugEntry(fmt.Sprintf(MsgOldArchivesDeleted, deleted.Path))
			entry.Fields = map[string]any{
				"reason":     fsstore.MsgRotationAges,
				"maxAgeDays": f.Config.MaxFileAge(),
				"modTime":    deleted.ModTime,
				"periodDate": pkgtime.StartOfDay(purgeAt).
					AddDate(0, 0, -f.Config.MaxFileAge()).
					Format(fsstore.DateFormat),
			}
			// ファイルへログ・または他のロガーで処理
			f.delegateDebug(ctx, entry)
		}
	}

	// 削除失敗fileがあった場合の処理
	if err := fsstore.JoinFileRemovalFailures(result.RemovalResults); err != nil {
		// purge失敗を記録
		f.delegateError(
			ctx,
			err,
			f.internalCallerErrorEntry("failed to purge failure archives by age", err),
		)
		return &archiveCleanupNotificationSentError{err: err}
	}

	return nil
}

func (f *FsArchiveHandler) reportArchiveGenerationPurge(
	ctx context.Context,
	result fsstore.PurgeResult,
) error {
	// パージ実行しました
	if result.Ran && f.Config.IsDebug() {
		f.delegateDebug(
			ctx,
			f.internalCallerDebugEntry(DebugMsgPurgedFileGenerations),
		)
	}

	if f.Config.IsDebug() {
		for _, removal := range result.RemovalResults {
			if removal.Err != nil {
				continue
			}
			deleted := removal.File
			entry := f.internalCallerDebugEntry(fmt.Sprintf(MsgOldArchivesDeleted, deleted.Path))
			entry.Fields = map[string]any{
				"reason":      fsstore.MsgRotationExceeded,
				"existFiles":  len(result.Remaining),
				"maxLogFiles": f.Config.MaxFileBackups(),
				"orderBy":     "file name date and index asc",
				"path":        deleted.Path,
				"modTime":     deleted.ModTime,
			}
			f.delegateDebug(ctx, entry)
		}
	}

	if err := fsstore.JoinFileRemovalFailures(result.RemovalResults); err != nil {
		entry := f.internalCallerErrorEntry(
			"failed to purge archives by generations",
			err,
		)

		f.delegateError(ctx, err, entry)
		return &archiveCleanupNotificationSentError{err: err}
	}

	return nil
}

func (f *FsArchiveHandler) reportArchiveDirectorySizePurge(
	ctx context.Context,
	result fsstore.PurgeResult,
	diagnostic archiveDirectorySizePurgeDiagnostic,
) error {
	//OnDebug パージ開始します
	if result.Ran && f.Config.IsDebug() {
		f.delegateDebug(
			ctx,
			f.internalCallerDebugEntry(DebugMsgPurgedFileDirSize),
		)
	}

	// デバッグモードであれば削除対象ファイルを記録
	if f.Config.IsDebug() {
		for _, removal := range result.RemovalResults {
			if removal.Err != nil {
				continue
			}
			deleted := removal.File
			entry := f.internalCallerDebugEntry(DebugMsgPurgedFileDirSize)
			entry.Fields = map[string]any{
				"reason":     fsstore.MsgRotationDirSize,
				"path":       deleted.Path,
				"maxDirSize": f.Config.MaxDirSize(),
				"modTime":    deleted.ModTime,
				"fileSize":   deleted.Size,
			}
			// ファイルへログ・または他のロガーで処理
			f.delegateDebug(ctx, entry)
		}
	}

	if result.Ran && diagnostic.directoryBytes > f.Config.MaxDirSize() {
		switch {
		case diagnostic.activeFileBytes > f.Config.MaxDirSize():
			entry := f.internalCallerWarnEntry(WarnMsgActiveArchiveFilesExceedMaxDirSize)
			entry.Fields = map[string]any{
				"failureCategory": diagnostic.failureCategory,
				"directoryPath":   diagnostic.directoryPath,
				"directorySize":   diagnostic.directoryBytes,
				"maxDirSize":      f.Config.MaxDirSize(),
				"activeFileSize":  diagnostic.activeFileBytes,
			}
			f.delegateWarn(ctx, entry)
		case !diagnostic.hasRemainingDeletionCandidates:
			entry := f.internalCallerWarnEntry(WarnMsgNoArchivesEligibleForDirectorySizePurge)
			entry.Fields = map[string]any{
				"failureCategory": diagnostic.failureCategory,
				"directoryPath":   diagnostic.directoryPath,
				"directorySize":   diagnostic.directoryBytes,
				"maxDirSize":      f.Config.MaxDirSize(),
				"remainingFiles":  len(result.Remaining),
			}
			f.delegateWarn(ctx, entry)
		}
	}

	// 削除失敗fileがあった場合の処理
	if err := fsstore.JoinFileRemovalFailures(result.RemovalResults); err != nil {
		// purge失敗を記録
		f.delegateError(
			ctx,
			err,
			f.internalCallerErrorEntry("failed to purge logs by directory size", err),
		)
		return &archiveCleanupNotificationSentError{err: err}
	}

	return nil
}
