package file

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// rotateWithHandlersはrotationTimeでactive fileを切り替え、inline lifecycleが
// 有効な場合は直前のfileの圧縮・uploadも起動します。rotation判定と同じ
// rotationTimeを渡し、日付境界で判定と新しいfile名がずれないようにします。
// callerはFsstore.Muと、Close開始前に登録したrotation task registration予約を
// 保持した状態で呼び出さなければなりません。
func (l *FileLogger) rotateWithHandlers(
	ctx context.Context,
	rotationTime time.Time,
	retryLog logging.RetryLogFunc,
	entryFactory logging.EntryFactory,
) error {
	oldActivePath := l.Fsstore.ActiveFilePath
	protectedRotatedFilePaths := l.pathsToProtectForRotatedFile(oldActivePath)
	archiveToken := appendlog.NewDirectoryGuardToken()
	// old pathは現在writer tokenのactiveとして保護されています。task固有tokenにも
	// 先に登録し、activeがnew pathへ切り替わる瞬間にも保護の隙間を作りません。
	l.lifecycleGuard.ProtectPaths(archiveToken, oldActivePath)
	archiveClaimed := true
	if l.inlineLifecycleEnabled() {
		archiveClaimed = l.lifecycleGuard.ProtectPathsIfNoOtherOwnerConflict(
			archiveToken,
			l.lifecycleToken,
			protectedRotatedFilePaths,
		)
	}

	oldFilePath, err := l.Fsstore.Rotate(
		ctx,
		retryLog,
		entryFactory,
		func(ctx context.Context) error {
			return l.openLogFile(ctx, rotationTime)
		},
	)
	if err != nil {
		l.lifecycleGuard.UnprotectPaths(archiveToken, protectedRotatedFilePaths...)
		return err
	}
	// rotation結果はFsstore.Muを保持する同期境界で確定させる。同じpathが再openされた場合は
	// active fileなのでarchiveしない。非同期goroutine側でActiveFilePathを再読込しないためraceもない。
	if oldFilePath == "" || oldFilePath == l.Fsstore.ActiveFilePath {
		l.lifecycleGuard.UnprotectPaths(archiveToken, protectedRotatedFilePaths...)
		return nil
	}
	inlinePurge := l.inlinePurge
	// runner・disableではinline taskを開始しません。以降のbackground archive・cleanupは
	// inline runtimeが存在することを前提とします。
	if inlinePurge == nil {
		// runner・disableではinline purgeが概算値を読まないため、更新せずに
		// rotation中の一時保護だけを解除します。
		l.lifecycleGuard.UnprotectPaths(archiveToken, protectedRotatedFilePaths...)
		return nil
	}
	// Fsstore.Muを保持している間に命名情報をコピーします。archiveとcleanupは
	// この値だけを使用し、次のrotationが更新するFsstore.FileNameを読み直しません。
	fileNameSnapshot := l.Fsstore.FileName
	archiveCtx := context.Background()
	if ctx != nil {
		// rotationの受付可否はwrite開始時に確定済みです。callerが応答待ちを
		// 打ち切っても、受理済みarchive taskを完了させます。
		archiveCtx = context.WithoutCancel(ctx)
	}
	if !archiveClaimed {
		// 同じgenerationを別のupload/recovery taskが処理中です。rotation transition
		// まではtask tokenでold sourceを保護済みです。ここでは二重処理せず保護を
		// 解除し、次の自然なdirectory観測でraw sourceを回復します。
		l.lifecycleGuard.UnprotectPaths(archiveToken, protectedRotatedFilePaths...)
		inlinePurge.invalidateDirectorySizeEstimate()
		return nil
	}
	archiveCtx, cancelArchive := l.forceWork.context(archiveCtx)

	// 直前のファイルをアーカイブ可能であれば行う
	// (ローテーション時のみトリガー)
	// write受付時のregistration予約がcounterを1以上に保っているため、Closeが
	// Startedへ遷移してWait中でも、このwriteに従属するarchive childは安全に
	// Addできます。新規cleanup用のbeginBackgroundOperationとは受付条件を分けます。
	l.beginRotationArchiveOperationFromAcceptedWrite()
	go func() {
		defer cancelArchive()
		notifications := &archiveTaskNotificationQueue{}
		defer func() {
			recovered := recover()
			// このdeferを最初に登録し、panic通知の構築またはpost-cleanup planningが
			// panicしても通知を実行し、その完了後にrotation task countを必ず解放します。
			defer func() {
				defer l.endBackgroundOperation()
				notifications.runDeferredNotifications()
			}()
			// post-cleanup planningのpanicも同じrotation archive failureとして回収します。
			// recovery deferは上のownership解放deferより後に登録するため先に実行されます。
			defer func() {
				if postCleanupPanic := recover(); postCleanupPanic != nil {
					l.notifyRotationArchivePanic(
						notifications.enqueue,
						archiveCtx,
						rotationTime,
						oldFilePath,
						postCleanupPanic,
					)
				}
			}()
			if recovered != nil {
				l.notifyRotationArchivePanic(
					notifications.enqueue,
					archiveCtx,
					rotationTime,
					oldFilePath,
					recovered,
				)
			}
			// rotation task countが残っている間にpost-cleanup childを登録し、
			// WaitGroupのcounterが0になった後のAddを防ぎます。そのrotationで
			// 処理したsourceはcleanup planから除外されるため、同じ圧縮失敗を
			// この完了経路で直ちに再通知しません。
			l.triggerPostRotationCleanup(
				archiveCtx,
				fileNameSnapshot,
				oldFilePath,
			)
		}()
		l.processRotatedLogFile(
			archiveCtx,
			oldFilePath,
			fileNameSnapshot,
			protectedRotatedFilePaths,
			archiveToken,
			notifications.enqueue,
		)
	}()
	return nil
}

// processRotatedLogFileは、rotationで書き込みを終えたログについて、設定に応じた
// 圧縮・送信・退避・削除を呼び出し、処理終了時の後始末を管理します。
// 必要な再走査の予約、今回のtokenの保護解除、ディレクトリ容量見積もりの無効化を、
// 同じlifecycle lock内で順に行います。この後始末はpanic時もdeferで実行します。
// goroutineの起動、panicの回収、終了待ち件数の解除、保留した通知の実行は呼び出し元が担当します。
func (l *FileLogger) processRotatedLogFile(
	ctx context.Context,
	oldFilePath string,
	fileNameSnapshot logpattern.FileNamePattern,
	protectedRotatedFilePaths []string,
	archiveToken *appendlog.DirectoryGuardToken,
	notify taskNotificationDelivery,
) {
	inlinePurge := l.inlinePurge
	var (
		archiveCompleted          bool
		seriesUploadRetryRequired bool
		rescanAfterPanicRequired  = l.shouldScheduleLogFileRescanAfterPanic(oldFilePath)
	)
	defer func() {
		_ = l.lifecycleGuard.RunWithLifecycleLock(func() error {
			// schedule時刻取得やUnprotectPaths自身がpanicしても、保護解除とcache
			// 無効化を可能な限り両方実行します。deferはLIFOなので正常時も従来どおり
			// unprotect後にinvalidateします。
			defer inlinePurge.invalidateDirectorySizeEstimate()
			defer l.lifecycleGuard.UnprotectPaths(
				archiveToken,
				protectedRotatedFilePaths...,
			)
			// upload失敗はdirectory Trackerではなくこのphysical seriesの
			// scheduleへ記録し、TTL後のwriteでtarget-only再発見します。
			// scheduleと保護解除を同じlifecycle順序に置くことで、
			// 先行観測の古いsnapshotが新しい失敗を完了扱いしません。
			if seriesUploadRetryRequired ||
				(rescanAfterPanicRequired && !archiveCompleted) {
				inlinePurge.seriesUploadRetry.scheduleRetry(
					l.Clock.Now(),
					inlinePurge.config.UsageCacheTTL(),
				)
			}
			return nil
		})
	}()
	seriesUploadRetryRequired = l.archiveRotatedFileWithNotifications(
		ctx,
		oldFilePath,
		fileNameSnapshot,
		notify,
	)
	archiveCompleted = true
}

// RotationArchivePanicErrorは、rotation後のログ処理で回収したpanicです。
// Valueはrecoverで得た値、Stackは回収した時点のgoroutineのstack traceです。
type RotationArchivePanicError struct {
	Value any
	Stack []byte
}

func (e *RotationArchivePanicError) Error() string {
	return fmt.Sprintf("rotation archive task panic: %v\n%s", e.Value, e.Stack)
}

// notifyRotationArchivePanicは、rotation後のログ処理で発生したpanicを内部エラーに変換し、
// OnErrorへの通知処理をnotifyへ渡します。通知の保留・即時実行はnotifyが決めます。
// FileLogger.Clockは再読込せず、rotation判定時の時刻を使って通知を構築します。
// Clock.Now自体がpanic原因でも、同じエラー通知の経路を使用できます。
func (l *FileLogger) notifyRotationArchivePanic(
	notify taskNotificationDelivery,
	ctx context.Context,
	rotationTime time.Time,
	oldFilePath string,
	recovered any,
) {
	panicErr := &RotationArchivePanicError{Value: recovered, Stack: debug.Stack()}
	wrappedErr := l.wrapErrorByLogger(panicErr, OperationRotationArchive)
	occurredAt := rotationTime.UTC()
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	entry := &logging.LogEntry{
		OccurredAt: occurredAt,
		Level:      pkglogger.LogLevelError,
		Message:    fmt.Sprintf(ErrorMsgRotationArchivePanic, oldFilePath),
		Internal:   true,
		Err:        panicErr,
		Source:     fileLoggerInternalSource(l.logger),
	}
	notify(func() {
		l.dispatchError(ctx, wrappedErr, entry)
	})
}

// pathsToProtectForRotatedFileは、書き込み先の切替後に旧ログの処理を進める間、
// purgeから保護すべきパスを返します。元ログに加え、inlineで圧縮が有効なら、
// 既存または作成予定の圧縮ファイルのパスを含めます。
// 呼び出し元は切替前に保護を登録し、後続処理の終了時に解除します。
func (l *FileLogger) pathsToProtectForRotatedFile(
	oldFilePath string,
) []string {
	if oldFilePath == "" {
		return nil
	}
	paths := []string{oldFilePath}
	// runner・disableではwriterの切り替え中のrawだけを保護します。
	// inline archiveを起動しないため、圧縮済みfileの探索も行いません。
	if !l.inlineLifecycleEnabled() {
		return paths
	}
	if l.config.Fsstore().Lifecycle().EnableFileCompress() {
		// 圧縮後pathも生成前から保護し、renameからupload開始までの隙間をなくします。
		// 設定変更前の成果物があればその実pathを保護します。rawは全archive taskの共通キーです。
		existingCompressedPath, _, err := l.findExistingCompressedArchivePath(oldFilePath)
		if err != nil {
			// 探索・型検査に失敗した場合も、新規作成先の保護は設定します。
			// 圧縮入口で同じ検査エラーを拒否するため、この予定パスへ圧縮はしません。
			existingCompressedPath = ""
		}
		compressedPath, _, resolveErr := fsstore.ResolveCompressedFilePath(
			oldFilePath, []string{existingCompressedPath}, l.Fsstore.GetCompressFormat(),
		)
		if resolveErr == nil {
			paths = append(paths, compressedPath)
		}
	}
	return paths
}
