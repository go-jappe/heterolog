package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/compressedlogfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// ObjectStorageUploadFailureRouteOwnerは、圧縮ファイルの送信失敗時の退避先登録と通知処理を所有します。
// 呼び出し元は送信・退避・その再試行を終えてからCloseし、登録を解放してください。
// 通知の出力先はNotificationsDoneを待ってから閉じます。
type ObjectStorageUploadFailureRouteOwner interface {
	Close() error
	RequestForceClose() error
	NotificationsDone() <-chan struct{}
}

// WrapObjectStorageUploaderは、設定された圧縮ファイルの退避先と通知先を送信処理へ接続します。
// 通常ログの.logとbackend送信失敗レコードの.jsonlについて、runnerとinlineで共用します。
// fileNameは退避する系列とspoolから再送するファイルを識別します。
// 送信先prefixの設定とcheckpointの宛先登録は、呼び出し元で先に済ませてください。
func WrapObjectStorageUploader(
	targetCfg objectstorage.UploadTargetConfig,
	spoolRuntimes compressedlog.RootRuntimes,
	rawUploader uploader.Uploader,
	fileName logpattern.FileNamePattern,
	timeSource clock.Clock,
	reporter RunReporter,
	checkpointStore checkpointstore.Store,
	manualRecovery bool,
) (uploader.Uploader, ObjectStorageUploadFailureRouteOwner, error) {
	if rawUploader == nil {
		return nil, nil, nil
	}
	failureStoreConfig := compressedlogfailure.NewConfigAdapter(targetCfg.CompressedLogFileUploadFailureStore)
	types := failureStoreConfig.DurableType()
	// 退避先がない場合は通知用goroutineも必要ありません。
	if len(types) == 0 || (len(types) == 1 && types[0] == failurestore.DurableTypeNone) {
		return rawUploader, nil, nil
	}
	var notifications *notification.Dispatcher
	var cancelNotifications context.CancelFunc
	observedReporter := reporter
	// 手動復旧は呼び出し中に通知を完了し、結果集計後へ遅延させません。
	// 再送・purgeのworkerがないため、専用の通知workerも不要です。
	if reporter != nil && !manualRecovery {
		notifications = notification.NewDispatcher()
		var notificationContext context.Context
		notificationContext, cancelNotifications = context.WithCancel(context.Background())
		// constructorのrollbackと通常Closeのどちらでも、受理済み通知を完了してから
		// contextを解放します。通知callbackからのClose再入を妨げないよう待機を分離します。
		go func(done <-chan struct{}, cancel context.CancelFunc) {
			defer cancel()
			<-done
		}(notifications.Done(), cancelNotifications)
		observedReporter = &dispatchedCompressedLogFileUploadFailureRunReporter{
			next:         reporter,
			dispatcher:   notifications,
			forceContext: notificationContext,
		}
	}

	wrapped, err := compressedlogfailure.NewManagedUploader(
		compressedlogfailure.ManagedUploaderConfig{
			DisableMaintenance: manualRecovery,
			SpoolRuntimes:      spoolRuntimes,
			CheckpointStore:    checkpointStore,
			FileName:           fileName,
			// archiveの保持期限にも、runnerと同じClockを渡します。
			Clock:                               timeSource,
			CompressedLogFileUploadFailureStore: failureStoreConfig,
			Notifier:                            compressedLogFileUploadFailureNotifier(observedReporter),
			SpoolRunObserverForRoot: func(runtime *compressedlog.RootRuntime) compressedlog.ManagedRunObserver {
				return compressedLogFileUploadFailureSpoolRunObserver(observedReporter, runtime)
			},
			SpoolOnWarnForRoot: func(runtime *compressedlog.RootRuntime) func(context.Context, error) {
				return compressedLogFileUploadFailureSpoolWarningObserver(observedReporter, runtime)
			},
			ArchivePurgeObserverForRoot: func(root compressedlogarchive.Root) compressedlogfailure.ArchivePurgeObserver {
				return compressedLogFileUploadFailureArchivePurgeRunObserver(observedReporter, root)
			},
		},
		rawUploader,
	)
	if err != nil {
		notifications.CloseAfterDrain()
		return nil, nil, err
	}
	managed, _ := wrapped.(*compressedlogfailure.ManagedUploader)
	if managed == nil {
		notifications.CloseAfterDrain()
		return wrapped, nil, nil
	}
	return wrapped, &runnerCompressedLogFileUploadFailureRouteOwner{
		managed:             managed,
		notifications:       notifications,
		cancelNotifications: cancelNotifications,
	}, nil
}

// runnerCompressedLogFileUploadFailureRouteOwnerは共有compressed log file routeと、そのrouteから受理したReporter通知を
// 同じ外側lifecycleへ結び付けます。Closeは受理済みのupload・保存処理の終了と
// 共有coordinatorの利用登録解除を待ってから、通知の受付を閉じます。
// 外側の所有者はNotificationsDoneも待ってから通知の出力先を閉じます。
type runnerCompressedLogFileUploadFailureRouteOwner struct {
	managed             *compressedlogfailure.ManagedUploader
	notifications       *notification.Dispatcher
	cancelNotifications context.CancelFunc
}

func (o *runnerCompressedLogFileUploadFailureRouteOwner) Close() error {
	if o == nil {
		return nil
	}
	err := o.managed.Close()
	// 強制終了でCloseの待機だけが解除されても、通知の発行元が戻るまでは受付を維持します。
	<-o.managed.ShutdownDone()
	o.notifications.CloseAfterDrain()
	return err
}

// RequestForceCloseは進行中の送信・退避と通知へ中止を要求します。登録の解放はCloseが行います。
func (o *runnerCompressedLogFileUploadFailureRouteOwner) RequestForceClose() error {
	if o == nil {
		return nil
	}
	if o.cancelNotifications != nil {
		o.cancelNotifications()
	}
	return o.managed.RequestForceClose()
}

func (o *runnerCompressedLogFileUploadFailureRouteOwner) NotificationsDone() <-chan struct{} {
	if o != nil && o.notifications != nil {
		return o.notifications.Done()
	}
	done := make(chan struct{})
	close(done)
	return done
}

// compressedLogFileUploadFailureSpoolRunObserverは、spoolへの退避後の再送・
// terminal整理で発生したoperational errorだけを既存maintenance Reporterへ渡します。
// 通知用の保存先パスは構築時に確定し、通知のたびに設定やfilesystemを調べ直しません。
func compressedLogFileUploadFailureSpoolRunObserver(
	reporter RunReporter,
	runtime *compressedlog.RootRuntime,
) compressedlog.ManagedRunObserver {
	if reporter == nil || runtime == nil {
		return nil
	}
	directoryPath := pathidentity.AbsoluteOrCleanPath(runtime.Root().Path())
	return func(
		ctx context.Context,
		_ *compressedlog.RunReport,
		_ *objectstoragespool.TerminalPurgeReport,
		err error,
	) {
		if err == nil {
			return
		}
		reporter.ReportRun(
			ctx,
			RunReport{DirectoryPath: directoryPath},
			fmt.Errorf("%w: %w", ErrUploadFailureSpoolMaintenance, err),
		)
	}
}

// compressedLogFileUploadFailureSpoolWarningObserverは、保全済みファイルの確認を
// 処理失敗と区別してReporterへ渡し、既存のOnWarnへ接続します。
func compressedLogFileUploadFailureSpoolWarningObserver(
	reporter RunReporter,
	runtime *compressedlog.RootRuntime,
) func(context.Context, error) {
	if reporter == nil || runtime == nil {
		return nil
	}
	directoryPath := pathidentity.AbsoluteOrCleanPath(runtime.Root().Path())
	return func(ctx context.Context, warning error) {
		if warning == nil {
			return
		}
		reporter.ReportRun(ctx, RunReport{DirectoryPath: directoryPath, SpoolWarning: warning}, nil)
	}
}

// compressedLogFileUploadFailureArchivePurgeRunObserverは、独立したarchive保守周期の
// purge結果を成功・失敗ともReporterへ渡します。通常のappendlog RunOnceとは合算しません。
// 通知先directoryは構築時に確定し、通知のたびに再解決しません。
func compressedLogFileUploadFailureArchivePurgeRunObserver(
	reporter RunReporter,
	root compressedlogarchive.Root,
) compressedlogfailure.ArchivePurgeObserver {
	if reporter == nil {
		return nil
	}
	directoryPath := pathidentity.AbsoluteOrCleanPath(root.Path())
	return func(
		ctx context.Context,
		report compressedlogarchive.PurgeReport,
		err error,
	) {
		if err != nil {
			err = fmt.Errorf("%w: %w", ErrUploadFailureArchiveLifecycle, err)
		}
		reporter.ReportRun(
			ctx,
			RunReport{
				DirectoryPath:             directoryPath,
				UploadFailureArchivePurge: &report,
			},
			err,
		)
	}
}

// compressedLogFileUploadFailureNotifierは、送信失敗と退避先の情報から内部ログ用の通知を作り、
// 設定済みのRunReporterへ渡す関数を返します。通常の一巡処理の戻り値とは別に通知します。
// 退避が成功した場合も送信失敗を通知します。退避自体の失敗は一巡処理のerrorで通知します。
// 送信前の退避済みファイル確認やcheckpoint保存が失敗した場合も通知します。
// 保存結果の矛盾をpurge対象外へ保全できた場合は、通常errorと別にwarningとして通知します。
func compressedLogFileUploadFailureNotifier(
	reporter RunReporter,
) compressedlogfailure.Notifier {
	if reporter == nil {
		return nil
	}
	return func(ctx context.Context, notification compressedlogfailure.Notification) {
		for _, conflict := range notification.ConflictRenames {
			reporter.ReportRun(ctx, RunReport{
				DirectoryPath: pathidentity.AbsoluteOrCleanPath(filepath.Dir(notification.SourcePath)),
			}, &UploadFailureConflictPreservedError{
				OriginalPath: conflict.OriginalPath, RetainedPath: conflict.RetainedPath,
				DestinationID: notification.DestinationID,
			})
		}
		if notification.ExistingFileCheckErr != nil {
			reporter.ReportRun(ctx, RunReport{
				DirectoryPath: pathidentity.AbsoluteOrCleanPath(filepath.Dir(notification.SourcePath)),
			}, &UploadFailureNotificationError{
				Kind: ErrExistingUploadFailureFileCheck, SourcePath: notification.SourcePath,
				DestinationID: notification.DestinationID, Err: notification.ExistingFileCheckErr,
			})
		}
		if notification.CheckpointErr != nil {
			reporter.ReportRun(ctx, RunReport{
				DirectoryPath: pathidentity.AbsoluteOrCleanPath(filepath.Dir(notification.SourcePath)),
			}, &UploadFailureNotificationError{
				Kind: ErrUploadProgressSave, SourcePath: notification.SourcePath,
				DestinationID: notification.DestinationID, Err: notification.CheckpointErr,
			})
		}
		if notification.PreservationWarning != nil {
			reporter.ReportRun(ctx, RunReport{
				DirectoryPath: pathidentity.AbsoluteOrCleanPath(filepath.Dir(notification.SourcePath)),
				CheckpointWarning: &UploadFailureNotificationError{
					Kind: ErrUploadProgressPreserved, SourcePath: notification.SourcePath,
					DestinationID: notification.DestinationID, Err: notification.PreservationWarning,
				},
			}, nil)
		}
		if notification.UploadErr == nil || notification.SaveErr != nil {
			return
		}
		reporter.ReportRun(
			ctx,
			RunReport{
				DirectoryPath: pathidentity.AbsoluteOrCleanPath(
					filepath.Dir(notification.SourcePath),
				),
			},
			&UploadFailureHandoffError{
				SourcePath: notification.SourcePath, DestinationID: notification.DestinationID,
				FailureCategory: notification.FailureCategory, SavedTargets: notification.SavedTargets,
				Err: notification.UploadErr,
			},
		)
	}
}

func closeObjectStorageCompressedLogFileUploadFailureRouteOwners(
	owners []ObjectStorageUploadFailureRouteOwner) error {
	var errs []error
	for _, owner := range owners {
		if owner == nil {
			continue
		}
		if err := owner.Close(); err != nil {
			errs = append(errs, err)
		}
		<-owner.NotificationsDone()
	}
	return errors.Join(errs...)
}
