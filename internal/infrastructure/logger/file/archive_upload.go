package file

import (
	"context"
	"errors"
	"fmt"
	"sync"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/compressedlogfailure"
	uploadercompose "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/compose"
	s3uploaderbuilder "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/s3"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// newLogFileUploaderは、通常・error・debugのログ種別に対応するobject storage送信処理を構築します。
// 全体または対象ログ種別の送信が無効なら、nilを返します。
// 送信失敗時の退避機能は、newLogFileUploaderWithFailureStorageで追加します。
func newLogFileUploader(
	uploadCfg objectstorage.LogFileUploadConfigProvider,
	loggerType loggerType,
) (uploader.DestinationUploader, error) {
	if !uploadCfg.Enabled() {
		return nil, nil
	}
	targetCfg := uploadTargetConfigForLoggerType(uploadCfg, loggerType)
	if !targetCfg.Enabled() {
		return nil, nil
	}
	return uploadercompose.FromTargets(targetCfg, s3uploaderbuilder.Builders())
}

// newLogFileUploaderWithFailureStorageは、通常の送信実装baseUploaderに、圧縮ログの送信失敗時の
// 退避処理と、FileLoggerへの内部ログ通知を組み込んだUploaderを構築します。
// 退避が無効ならbaseUploaderをそのまま返します。有効なら、指定されたarchive・spoolへの保存を追加します。
// ownerが指定されていれば、送信・退避の途中経過を記録するcheckpointと通知先を受け取り、
// spoolRuntimesが指定されていれば、構築側が用意した共有spoolを使います。
func newLogFileUploaderWithFailureStorage(
	targetCfg objectstorage.UploadTargetConfigProvider,
	fileName logpattern.FileNamePattern,
	baseUploader uploader.DestinationUploader,
	owner *FileLogger,
	spoolRuntimes ...compressedlog.RootRuntimes,
) (uploader.DestinationUploader, error) {
	failureStoreConfig := targetCfg.CompressedLogFileUploadFailureStore()
	types := failureStoreConfig.DurableType()
	if len(types) == 0 || (len(types) == 1 && types[0] == failurestore.DurableTypeNone) {
		return baseUploader, nil
	}
	managedConfig := compressedlogfailure.ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: failureStoreConfig,
		FileName:                            fileName,
	}
	if len(spoolRuntimes) > 0 {
		managedConfig.SpoolRuntimes = spoolRuntimes[0]
	}
	var maintenanceNotifications *notification.Dispatcher
	if owner != nil {
		managedConfig.CheckpointStore = owner.archiveUploadCheckpoints
		managedConfig.Clock = owner.Clock
		// spoolの再送や退避先のpurgeは、元のログ送信とは別の周期で動きます。
		// 通知を別goroutineへ渡し、通常Closeでは受理済み通知の完了まで待ちます。
		maintenanceNotifications = notification.NewDispatcher()
		// FileLoggerでは再送件数などの集計結果を使わず、発生したエラーだけを通知します。
		managedConfig.SpoolRunObserverForRoot = func(*compressedlog.RootRuntime) compressedlog.ManagedRunObserver {
			return func(
				ctx context.Context,
				_ *compressedlog.RunReport,
				_ *objectstoragespool.TerminalPurgeReport,
				err error,
			) {
				owner.notifyArchiveCompressedLogMaintenanceError(
					ctx,
					"compressed log file upload failure spool maintenance failed",
					err,
					maintenanceNotifications,
				)
			}
		}
		managedConfig.SpoolOnWarnForRoot = func(*compressedlog.RootRuntime) func(context.Context, error) {
			return func(ctx context.Context, warning error) {
				owner.notifyArchiveCompressedLogMaintenanceWarning(ctx, warning, maintenanceNotifications)
			}
		}
		// archiveのpurgeも同様に、削除件数などの集計結果を使わず、エラーだけを通知します。
		managedConfig.ArchivePurgeObserverForRoot = func(compressedlogarchive.Root) compressedlogfailure.ArchivePurgeObserver {
			return func(
				ctx context.Context,
				_ compressedlogarchive.PurgeReport,
				err error,
			) {
				owner.notifyArchiveCompressedLogMaintenanceError(
					ctx,
					"compressed log file upload failure archive lifecycle failed",
					err,
					maintenanceNotifications,
				)
			}
		}
	}
	managedUploader := &managedFileArchiveUploader{
		owner:                    owner,
		maintenanceNotifications: maintenanceNotifications,
	}
	managedConfig.Notifier = managedUploader.collectUploadFailureNotification
	managed, err := compressedlogfailure.NewManagedUploader(managedConfig, baseUploader)
	if err != nil {
		maintenanceNotifications.CloseAfterDrain()
		return nil, err
	}
	// FileLoggerは宛先IDごとに完了を管理するため、退避機能を付けた後も
	// 宛先一覧の取得と、宛先を指定した送信ができることを確認します。
	destinationUploader, ok := managed.(uploader.DestinationUploader)
	if !ok {
		maintenanceNotifications.CloseAfterDrain()
		return nil, errors.Join(
			fmt.Errorf("managed file uploader %T does not provide destination IDs", managed),
			closeArchiveUploader(managed),
		)
	}
	managedUploader.next = destinationUploader
	return managedUploader, nil
}

// managedFileArchiveUploaderは、送信・退避処理が返す結果とは別に、その処理中のエラーや警告を
// FileLoggerの内部ログ通知へつなぎます。通知内容は送信呼び出しごとに集め、委譲先が戻った後に取り出します。
// rotation・cleanupから使う場合は、通知をさらに呼び出し元へ預け、ファイル操作後に実行します。
// 通知先を呼ぶ前にファイル操作とlockを解放します。通知内からの終了要求にはRequestCloseを使います。
// spoolの再送や退避先のpurgeで発生したエラーと、保存結果の矛盾についての保全警告は、
// 専用の通知用goroutineへ渡します。
type managedFileArchiveUploader struct {
	// nextは、object storageへの送信と、失敗した圧縮ログの退避を行います。
	next uploader.DestinationUploader
	// ownerは内部ログ通知を受け持つFileLoggerです。nilの場合、この部品から通知しません。
	owner *FileLogger
	// maintenanceNotificationsは、周期保守のエラーと保全警告を別goroutineで通知します。
	maintenanceNotifications *notification.Dispatcher
	closeController          shutdown.Controller
	// uploadMuは受付停止とinFlightへの登録を直列化します。委譲先が戻った後の
	// 通知登録も受理済み処理に含め、Closeで通知先が先に閉じることを防ぎます。
	uploadMu sync.Mutex
	closing  bool
	inFlight sync.WaitGroup
}

// archiveUploadFailureNotificationsContextKeyは、一回の送信に属する通知内容をcontextで受け渡すためのキーです。
// 同時に別のファイルを送っても、それぞれの通知内容が混ざらないようにします。
type archiveUploadFailureNotificationsContextKey struct{}

var _ uploader.DestinationUploader = (*managedFileArchiveUploader)(nil)

// DestinationIDsは、委譲先に登録されているobject storageの宛先ID一覧を返します。
func (u *managedFileArchiveUploader) DestinationIDs() []objectstorage.DestinationID {
	return u.next.DestinationIDs()
}

// Uploadは登録済みの全宛先への処理を委譲します。送信成功、または必要な退避が完了すればnilを返します。
// この入口では、notify経由のエラー通知を呼び出し中に実行します。
func (u *managedFileArchiveUploader) Upload(
	ctx context.Context,
	input uploader.UploadRequest,
) error {
	return u.uploadWithNotifications(ctx, input, runNotificationImmediately)
}

// uploadWithNotificationsは全宛先への処理を委譲し、送信・退避中に集めた通知内容を後から取り出します。
// notifyには、エラー通知をその場で実行する関数、または後で実行するために保留する関数を渡します。
// 委譲先の戻り値はそのまま返し、保全警告はnotifyとは別の通知用goroutineへ渡します。
func (u *managedFileArchiveUploader) uploadWithNotifications(
	ctx context.Context,
	input uploader.UploadRequest,
	notify taskNotificationDelivery,
) error {
	if !u.acceptUpload() {
		return compressedlogfailure.ErrUploaderClosed
	}
	defer u.inFlight.Done()
	if ctx == nil {
		ctx = context.Background()
	}
	// 委譲先が実行中の間はcollectUploadFailureNotificationで内容を集め、戻ってから通知先へ渡します。
	notifications := &archiveUploadFailureNotifications{}
	err := u.next.Upload(context.WithValue(ctx, archiveUploadFailureNotificationsContextKey{}, notifications), input)
	u.notifyUploadFailureInternalLogs(ctx, notifications.drain(), notify)
	return err
}

// UploadDestinationsは指定された宛先への処理を委譲し、宛先ごとの結果を返します。
// 送信失敗後に必要な退避が完了した宛先も成功扱いです。エラー通知の扱いはUploadと同じです。
func (u *managedFileArchiveUploader) UploadDestinations(
	ctx context.Context,
	input uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	return u.uploadDestinationsWithNotifications(ctx, input, destinationIDs, runNotificationImmediately)
}

// uploadDestinationsWithNotificationsは、指定宛先への処理結果と、内部ログへの通知内容を受け取ります。
// 宛先ごとの結果はそのまま呼び出し元へ返し、エラー通知の実行時期はnotifyに従います。
// 通知内容は今回の呼び出し専用の一覧へ集め、別の送信呼び出しと共有しません。
func (u *managedFileArchiveUploader) uploadDestinationsWithNotifications(
	ctx context.Context,
	input uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
	notify taskNotificationDelivery,
) uploader.UploadReport {
	if !u.acceptUpload() {
		report := uploader.UploadReport{Results: make([]uploader.DestinationResult, len(destinationIDs))}
		for index, id := range destinationIDs {
			report.Results[index] = uploader.DestinationResult{DestinationID: id, Err: compressedlogfailure.ErrUploaderClosed}
		}
		return report
	}
	defer u.inFlight.Done()
	if ctx == nil {
		ctx = context.Background()
	}
	notifications := &archiveUploadFailureNotifications{}
	result := u.next.UploadDestinations(
		context.WithValue(ctx, archiveUploadFailureNotificationsContextKey{}, notifications), input, destinationIDs,
	)
	u.notifyUploadFailureInternalLogs(ctx, notifications.drain(), notify)
	return result
}

func (u *managedFileArchiveUploader) acceptUpload() bool {
	u.uploadMu.Lock()
	defer u.uploadMu.Unlock()
	if u.closing {
		return false
	}
	u.inFlight.Add(1)
	return true
}

func (u *managedFileArchiveUploader) stopAcceptingUploads() {
	u.uploadMu.Lock()
	u.closing = true
	u.uploadMu.Unlock()
}

// Closeは委譲先の終了後に通知の受付を閉じ、受理済み通知の実行完了まで待ちます。
// 通知内から終了を要求する場合は、通知自身を待たないRequestCloseを使います。
func (u *managedFileArchiveUploader) Close() error {
	u.stopAcceptingUploads()
	return u.closeController.Close("file upload notifications", func() (bool, error) {
		u.inFlight.Wait()
		err := closeArchiveUploader(u.next)
		loggercommon.WaitForForcedShutdown(u.next, err)
		if err != nil && !loggercommon.ShutdownComplete(u.next) {
			return false, err
		}
		u.maintenanceNotifications.CloseAfterDrain()
		if u.maintenanceNotifications != nil {
			shutdown.Wait("file upload notifications", u.maintenanceNotifications.Done())
		}
		return true, err
	})
}

// RequestCloseは通常終了を別goroutineで開始し、通知元の完了をここでは待ちません。
func (u *managedFileArchiveUploader) RequestClose() {
	u.stopAcceptingUploads()
	go u.Close()
}

// RequestForceCloseは待機を打ち切り、委譲先と通知用contextへ中止を伝えます。
// 資源の解放は、実行中の処理と通知が戻ってから通常のClose経路で行います。
func (u *managedFileArchiveUploader) RequestForceClose() error {
	u.stopAcceptingUploads()
	return u.closeController.ForceClose(func() {
		if u.owner != nil {
			u.owner.forceWork.abort()
		}
		_ = loggercommon.ForceCloseIfSupported(u.next)
	})
}

func (u *managedFileArchiveUploader) ForceClose() error {
	err := u.RequestForceClose()
	u.RequestClose()
	return err
}

func (u *managedFileArchiveUploader) ShutdownDone() <-chan struct{} { return u.closeController.Done() }
func (u *managedFileArchiveUploader) ShutdownComplete() bool        { return u.closeController.IsComplete() }

// collectUploadFailureNotificationは、委譲先から受け取った退避結果・checkpointエラーなどを、今回の送信の通知内容へ追加します。
// この時点ではFileLoggerの通知先を呼びません。今回の送信に対応する一覧がなければ何もしません。
func (u *managedFileArchiveUploader) collectUploadFailureNotification(
	ctx context.Context,
	notification compressedlogfailure.Notification,
) {
	if ctx == nil {
		return
	}
	notifications, _ := ctx.Value(archiveUploadFailureNotificationsContextKey{}).(*archiveUploadFailureNotifications)
	if notifications != nil {
		notifications.append(notification)
	}
}

// PreservedCompressedLogConflictErrorは、退避先の同名衝突で既存の圧縮ログを保全したことを、
// 管理者の確認を求めるOnErrorの内部エラーとして通知します。OriginalPathは元の配置、
// RetainedPathは保全先、DestinationIDは退避を行った送信先です。
type PreservedCompressedLogConflictError struct {
	OriginalPath  string
	RetainedPath  string
	DestinationID objectstorage.DestinationID
}

func (e *PreservedCompressedLogConflictError) Error() string {
	return fmt.Sprintf(
		"preserved conflicting compressed log file %q as %q (destination %q); operator review required",
		e.OriginalPath, e.RetainedPath, e.DestinationID,
	)
}

// SavedUploadFailureErrorは、object storageへの送信に失敗した圧縮ログを、全指定退避先へ
// 保存できたことを通知する内部エラーです。SavedTargetsは保存先、FailureCategoryは退避前の
// 送信失敗の分類、DestinationIDは送信先です。Errは送信の失敗で、errors.Is/Asで判定できます。
type SavedUploadFailureError struct {
	SavedTargets    []compressedlogfailure.SavedTargetDetails
	FailureCategory uploader.FailureCategory
	DestinationID   objectstorage.DestinationID
	Err             error
}

func (e *SavedUploadFailureError) Error() string {
	return fmt.Sprintf(
		"object storage upload failed; compressed log file was saved to durable stores %v (upload failure category %q, destination %q): %v",
		e.SavedTargets, e.FailureCategory, e.DestinationID, e.Err,
	)
}

func (e *SavedUploadFailureError) Unwrap() error { return e.Err }

// notifyUploadFailureInternalLogsは、集めた送信・退避結果のうち、エラーや管理者の確認が必要なものから内部ログを作り、
// 設定済みのハンドラーへ渡します。
// エラーはnotifyを通してOnErrorへ、保存結果の矛盾についての保全警告は別goroutineからOnWarnへ渡します。
// 退避そのものの失敗は送信処理の戻り値を受け取った呼び出し元が通知するため、ここでは重ねて通知しません。
func (u *managedFileArchiveUploader) notifyUploadFailureInternalLogs(
	ctx context.Context,
	notifications []compressedlogfailure.Notification,
	notify taskNotificationDelivery,
) {
	if u == nil || u.owner == nil {
		return
	}
	for _, notification := range notifications {
		for _, conflict := range notification.ConflictRenames {
			u.owner.notifyArchiveUploadError(ctx, notification.SourcePath, &PreservedCompressedLogConflictError{
				OriginalPath:  conflict.OriginalPath,
				RetainedPath:  conflict.RetainedPath,
				DestinationID: notification.DestinationID,
			}, notify)
		}
		if notification.ExistingFileCheckErr != nil {
			u.owner.notifyArchiveUploadError(ctx, notification.SourcePath, fmt.Errorf("check existing failure files before upload (destination %q): %w", notification.DestinationID, notification.ExistingFileCheckErr), notify)
		}
		if notification.CheckpointErr != nil {
			u.owner.notifyArchiveUploadCheckpointError(ctx, notification.SourcePath, fmt.Errorf("destination %q: %w", notification.DestinationID, notification.CheckpointErr), notify)
		}
		if notification.PreservationWarning != nil {
			// 元データの保全を終えて処理を継続した矛盾は、管理者の確認を求める警告にします。
			warning := fmt.Errorf("preserved conflicting upload progress for %q (destination %q): %w",
				notification.SourcePath, notification.DestinationID, notification.PreservationWarning)
			entry := u.owner.InternalWarnEntryWithContext(ctx, warning.Error())
			entry.Err = warning
			u.maintenanceNotifications.Enqueue(func() {
				notificationCtx, cancel := u.owner.forceWork.context(context.WithoutCancel(ctx))
				defer cancel()
				u.owner.dispatchWarn(notificationCtx, entry)
			})
		}
		// 送信失敗を退避で引き受けた場合、送信処理の戻り値は成功になり得ます。
		// その場合も実際に送信が失敗したことを知らせます。退避も失敗した場合は呼び出し元が通知します。
		if notification.UploadErr == nil || notification.SaveErr != nil {
			continue
		}
		u.owner.notifyArchiveUploadError(
			ctx,
			notification.SourcePath,
			&SavedUploadFailureError{
				SavedTargets:    append([]compressedlogfailure.SavedTargetDetails(nil), notification.SavedTargets...),
				FailureCategory: notification.FailureCategory,
				DestinationID:   notification.DestinationID,
				Err:             notification.UploadErr,
			},
			notify,
		)
	}
}

// notifyArchiveCompressedLogMaintenanceErrorは、spoolの再送や退避先のpurge中のエラーをOnErrorへ非同期で渡します。
// errがnilなら通知しません。保守処理のgoroutineでは通知先の実行を待ちません。
func (l *FileLogger) notifyArchiveCompressedLogMaintenanceError(
	ctx context.Context,
	message string,
	err error,
	dispatcher *notification.Dispatcher) {
	if l == nil || err == nil {
		return
	}
	wrappedErr := l.wrapErrorByLogger(err, OperationArchiveUpload)
	entry := l.InternalErrorEntryWithContext(ctx, message, err)
	dispatcher.Enqueue(func() {
		notificationCtx, cancel := l.forceWork.context(context.WithoutCancel(ctx))
		defer cancel()
		l.dispatchError(notificationCtx, wrappedErr, entry)
	})
}

// notifyArchiveCompressedLogMaintenanceWarningは、spoolで保全したファイルについて、管理者の確認を求める警告を
// OnWarnへ非同期で渡します。warningがnilなら通知しません。通知内の終了要求にはRequestCloseを使います。
func (l *FileLogger) notifyArchiveCompressedLogMaintenanceWarning(
	ctx context.Context,
	warning error,
	dispatcher *notification.Dispatcher,
) {
	if l == nil || warning == nil {
		return
	}
	entry := l.InternalWarnEntryWithContext(ctx, "compressed log file upload failure spool preservation warning: "+warning.Error())
	entry.Err = warning
	dispatcher.Enqueue(func() {
		notificationCtx, cancel := l.forceWork.context(context.WithoutCancel(ctx))
		defer cancel()
		l.dispatchWarn(notificationCtx, entry)
	})
}

// archiveUploadFailureNotificationsは、一回の送信呼び出し中に届いた通知内容を保持します。
// 複数宛先からの追加と取り出しをmuで排他し、drainで取り出した内容は保持し続けません。
type archiveUploadFailureNotifications struct {
	mu            sync.Mutex
	notifications []compressedlogfailure.Notification
}

// appendは、送信・退避中に受け取った通知内容を末尾へ追加します。
func (n *archiveUploadFailureNotifications) append(notification compressedlogfailure.Notification) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.notifications = append(n.notifications, notification)
}

// drainは、それまでに集めた通知内容をまとめて取り出し、保持している一覧を空にします。
func (n *archiveUploadFailureNotifications) drain() []compressedlogfailure.Notification {
	n.mu.Lock()
	defer n.mu.Unlock()
	notifications := n.notifications
	n.notifications = nil
	return notifications
}

// uploadArchiveDestinationsWithNotificationsは、指定された宛先へログファイルの送信を依頼します。
// FileLogger用の送信実装なら内部ログ通知の実行方法も渡し、それ以外なら宛先別の送信だけを呼び出します。
// 戻り値は宛先ごとの処理結果です。checkpointの完了位置の更新や元ファイルの削除は呼び出し元が行います。
func uploadArchiveDestinationsWithNotifications(
	ctx context.Context,
	archiveUploader processing.DestinationUploader,
	request uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
	notify taskNotificationDelivery,
) uploader.UploadReport {
	managed, _ := archiveUploader.(*managedFileArchiveUploader)
	if managed != nil {
		return managed.uploadDestinationsWithNotifications(ctx, request, destinationIDs, notify)
	}
	return archiveUploader.UploadDestinations(ctx, request, destinationIDs)
}

// closeArchiveUploaderは、送信実装がCloseに対応していれば呼び出し、終了時のエラーを返します。
func closeArchiveUploader(archiveUploader uploader.Uploader) error {
	closer, ok := archiveUploader.(interface{ Close() error })
	if !ok || closer == nil {
		return nil
	}
	return closer.Close()
}

// uploadTargetConfigForLoggerTypeは、通常・error・debugのログ種別に対応する送信先設定を返します。
func uploadTargetConfigForLoggerType(uploadCfg objectstorage.LogFileUploadConfigProvider, loggerType loggerType) objectstorage.UploadTargetConfigProvider {
	switch loggerType {
	case ErrorLogger:
		return uploadCfg.Error()
	case DebugLogger:
		return uploadCfg.Debug()
	default:
		return uploadCfg.Default()
	}
}

// tryUploadRotatedFileは、書き込みを終えたログファイルの送信を依頼し、全宛先分の完了を確認できればtrueを返します。
// 完了には、送信失敗後に必要な退避先すべてへの保存を終えた場合も含みます。
// 退避機能付きの送信実装では、既存の退避ファイルやcheckpointから完了済みと確認できた場合も含みます。
// 送信が無効、パスが空、送信依頼の作成や送信・退避に失敗した場合はfalseです。
// この関数は元ファイルを削除せず、発生したエラーはOnErrorへ通知します。
func (l *FileLogger) tryUploadRotatedFile(ctx context.Context, filePath string) bool {
	return l.tryUploadRotatedFileWithNotifications(ctx, filePath, runNotificationImmediately)
}

// tryUploadRotatedFileWithNotificationsは、FileLoggerに設定された送信実装を使います。
// エラー通知をその場で実行するか、後で実行するために保留するかはnotifyで指定します。
func (l *FileLogger) tryUploadRotatedFileWithNotifications(
	ctx context.Context,
	filePath string,
	notify taskNotificationDelivery,
) bool {
	return l.tryUploadRotatedFileUsingWithNotifications(
		ctx,
		l.ArchiveUploader,
		filePath,
		notify,
	)
}

// tryUploadRotatedFileUsingWithNotificationsは、指定された送信実装で処理し、完了の可否を返します。
// 依頼の作成または送信・退避のエラーはnotifyを通して通知します。再試行の登録やファイル削除は行いません。
func (l *FileLogger) tryUploadRotatedFileUsingWithNotifications(
	ctx context.Context,
	archiveUploader uploader.DestinationUploader,
	filePath string,
	notify taskNotificationDelivery,
) bool {
	if archiveUploader == nil || filePath == "" {
		return false
	}
	// 送信依頼を作成してパスを検証・絶対化し、失敗した場合は送信処理へ進みません。
	request, err := uploader.NewUploadRequest(filePath)
	if err != nil {
		l.notifyArchiveUploadError(ctx, filePath, err, notify)
		return false
	}
	if managed, ok := archiveUploader.(*managedFileArchiveUploader); ok {
		err = managed.uploadWithNotifications(ctx, request, notify)
	} else {
		err = archiveUploader.Upload(ctx, request)
	}
	if err != nil {
		l.notifyArchiveUploadError(ctx, filePath, err, notify)
		return false
	}
	return true
}

// notifyArchiveUploadErrorは、対象ファイルとエラーから内部ログを作り、OnErrorへの通知をnotifyへ渡します。
// notifyが保留用の関数なら、この呼び出し中にはOnErrorを実行しません。
func (l *FileLogger) notifyArchiveUploadError(
	ctx context.Context,
	filePath string,
	err error,
	notify taskNotificationDelivery,
) {
	message := fmt.Sprintf(ErrorMsgFileArchiveUpload, filePath)
	wrappedErr := l.wrapErrorByLogger(err, OperationArchiveUpload)
	entry := l.InternalErrorEntryWithContext(ctx, message, err)
	notify(func() {
		l.dispatchError(ctx, wrappedErr, entry)
	})
}
