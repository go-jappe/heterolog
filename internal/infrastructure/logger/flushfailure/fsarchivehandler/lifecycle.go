package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	shutdownwait "github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
)

// Shutdownは新規failure archive処理を停止し、Pipelineが所有するrunnerを
// 停止してから、handlerが保持するretryable/permanentのactive fileを閉じます。
//
// inline modeは開始済みの圧縮・uploadを待ちます。runner modeは新しい周期を止め、
// 受理済み周期、その復旧、checkpoint保存を完了して閉じます。新しい最終走査は開始しません。
// disable modeにはpost-rotation lifecycleがないため、active fileのcloseだけを行います。
// context timeoutは呼び出し元の待機だけを止め、開始済みの終了処理はbackgroundで継続します。
// 次回Shutdownは同じ終了処理の完了を待ち直せます。
// 内部ログの通知も終了待ちに含むため、通知先から終了を求める場合はRequestShutdownを使います。
func (f *FailureArchivePipeline) Shutdown(ctx context.Context) error {
	if f == nil {
		return nil
	}
	f.operationMu.Lock()
	if f.shutdownState == shutdownStateAccepting {
		// 呼出し元のcontextが既に期限切れでも、終了要求と同時に新規受付を止めます。
		f.shutdownState = shutdownStateStarted
		f.runnerRuntime.stopPeriodic()
	}
	complete := f.shutdownState == shutdownStateCompleted
	f.operationMu.Unlock()
	if complete && !f.closeController.IsForced() && f.closeController.IsComplete() {
		return nil
	}
	return f.closeController.CloseContext(contextOrBackground(ctx), "failure archive pipeline", func() (bool, error) {
		return true, f.shutdown(context.Background())
	})
}

// RequestShutdownは、新規受付を停止して通常の終了処理を開始し、完了を待たずに戻ります。
// 通知先から終了を要求する場合に使います。完了は通知先の外側でShutdownまたはShutdownDoneを待ちます。
func (f *FailureArchivePipeline) RequestShutdown() {
	if f == nil {
		return
	}
	f.operationMu.Lock()
	if f.shutdownState != shutdownStateAccepting {
		f.operationMu.Unlock()
		return
	}
	f.shutdownState = shutdownStateStarted
	f.runnerRuntime.stopPeriodic()
	f.operationMu.Unlock()
	go f.Shutdown(context.Background())
}

func (f *FailureArchivePipeline) shutdown(ctx context.Context) error {
	if f == nil {
		return nil
	}
	ctx = contextOrBackground(ctx)

	f.operationMu.Lock()
	if f.shutdownState == shutdownStateAccepting {
		// 受理済み処理や先行Shutdownを待つ前に、新規受付だけを即座に停止します。
		f.shutdownState = shutdownStateStarted
		f.runnerRuntime.stopPeriodic()
	}
	if f.shutdownState == shutdownStateCompleted {
		f.operationMu.Unlock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		f.operationMu.Unlock()
		return fmt.Errorf("failure archive shutdown canceled before waiting: %w", err)
	}
	if f.shutdownDone == nil {
		f.shutdownDone = make(chan struct{})
		if f.operationCount > 0 && f.operationDone == nil {
			// Started以後は新規受付がないため、最後の受理済み処理が閉じます。
			f.operationDone = make(chan struct{})
		}
		acceptedDone := f.operationDone
		go func() {
			err := f.drainShutdown(acceptedDone)
			f.operationMu.Lock()
			f.shutdownErr = err
			f.shutdownState = shutdownStateCompleted
			close(f.shutdownDone)
			f.operationMu.Unlock()
		}()
	}
	done := f.shutdownDone
	f.operationMu.Unlock()
	if err := waitForArchiveShutdown(ctx, "failure archive shutdown", done); err != nil {
		return err
	}
	f.operationMu.Lock()
	err := f.shutdownErr
	f.operationMu.Unlock()
	return err
}

// drainShutdownは一度だけ起動し、呼び出し元の待機期限とは独立して受理済み処理を完了します。
func (f *FailureArchivePipeline) drainShutdown(acceptedDone <-chan struct{}) error {
	ctx := context.Background()

	if acceptedDone != nil {
		if err := waitForArchiveShutdown(ctx, "accepted failure archive operations", acceptedDone); err != nil {
			return err
		}
	}
	// 受理済みwriteがrotation taskを登録し終えてからtask受付を閉じます。
	// ここまでの待機がtimeoutしても、そのwriteのtaskを破棄してはいけません。

	var logFileProcessingRecoveryErr error
	if f.handler != nil {
		done := f.handler.stopArchiveTasks()
		if done != nil {
			if err := waitForArchiveShutdown(ctx, "accepted failure archive tasks", done); err != nil {
				return err
			}
		}
		// 終了開始後も受理済みtaskからcheckpoint更新・file操作の失敗が発生します。
		// その復旧登録を失わないよう、初回taskの完了後に復旧queueを閉じます。
		// 古い世代を待っている世代も、古い順に復旧queueへ移してから閉じます。
		f.handler.closeArchiveGenerationOrderWaits()
		if recovery := f.handler.logFileProcessingRecovery; recovery != nil {
			logFileProcessingRecoveryErr = recovery.Close()
		}
	}

	// 終了時は設定modeを再判定せず、実際に構築・所有しているruntimeを正本にします。
	if f.runnerRuntime != nil {
		if err := f.runnerRuntime.waitStopped(ctx); err != nil {
			return errors.Join(err, logFileProcessingRecoveryErr)
		}
	}

	logFileProcessingRecoveryErr = errors.Join(logFileProcessingRecoveryErr,
		f.runnerRuntime.closeLogFileProcessingRecovery())
	closeErr := f.runnerRuntime.closeResources()
	if f.handler != nil {
		// 受理済みtaskと復旧が完了した後、最新のcheckpoint保存を完了してから
		// active fileとleaseを解放します。
		for _, category := range []failurearchive.FailureCategory{
			failurearchive.FailureCategoryRetryable,
			failurearchive.FailureCategoryPermanent,
		} {
			if store := f.handler.compressedArchiveUploadCheckpointStore(category); store != nil {
				if err := store.Close(); err != nil {
					closeErr = errors.Join(closeErr, fmt.Errorf("close %s archive upload checkpoint: %w", category, err))
				}
			}
		}
	}
	for _, route := range f.uploadFailureRoutes {
		closeErr = errors.Join(closeErr, route.Close())
	}
	// os.File.Closeはerrorを返してもhandleを再利用できません。closeActiveFilesが全pointerと
	// guardを解除済みなら、closeErrは呼び出し元へ返しつつShutdown自体は完了とします。
	closeErr = errors.Join(closeErr, f.waitForNotifications())
	if f.handler != nil {
		closeErr = errors.Join(closeErr, f.handler.closeActiveFiles())
	}
	f.releaseArchiveRootLease()
	return errors.Join(closeErr, logFileProcessingRecoveryErr)
}

func waitForArchiveShutdown(ctx context.Context, description string, done <-chan struct{}) error {
	if err := shutdownwait.WaitContext(ctx, description, done); err != nil {
		return fmt.Errorf("wait for %s: %w", description, err)
	}
	return nil
}

// waitForNotificationsは、受理済み処理・復旧・保存を閉じた後に、その通知をすべて待ちます。
// 上流が後段dispatcherへ通知を登録し終えてから、後段の受付を閉じて出力完了を待ちます。
func (f *FailureArchivePipeline) waitForNotifications() error {
	if f == nil {
		return nil
	}
	var upstream []<-chan struct{}
	if f.handler != nil {
		if recovery := f.handler.logFileProcessingRecovery; recovery != nil {
			upstream = append(upstream, recovery.NotificationsDone())
		}
		for _, category := range []failurearchive.FailureCategory{
			failurearchive.FailureCategoryRetryable,
			failurearchive.FailureCategoryPermanent,
		} {
			store := f.handler.compressedArchiveUploadCheckpointStore(category)
			if notifications, ok := store.(interface{ NotificationsDone() <-chan struct{} }); ok {
				upstream = append(upstream, notifications.NotificationsDone())
			}
		}
	}
	for _, owner := range f.uploadFailureRoutes {
		upstream = append(upstream, owner.NotificationsDone())
	}
	if f.runnerRuntime != nil {
		for _, runner := range f.runnerRuntime.runners {
			if runner != nil && runner.DirectoryLifecycle != nil {
				upstream = append(upstream, runner.DirectoryLifecycle.NotificationsDone())
			}
		}
	}
	for _, done := range upstream {
		if err := waitForArchiveShutdown(context.Background(), "failure archive upstream notifications", done); err != nil {
			return err
		}
	}
	if f.runnerNotifications != nil {
		f.runnerNotifications.CloseAfterDrain()
		return waitForArchiveShutdown(context.Background(), "failure archive internal log notifications", f.runnerNotifications.Done())
	}
	return nil
}

// releaseArchiveRootLeaseは、runnerの最終処理とactive fileのcloseがすべて完了した後に、
// retryable/permanent共通親directoryの排他的所有権を解放します。
func (f *FailureArchivePipeline) releaseArchiveRootLease() {
	if f.archiveRootLease == nil {
		return
	}
	f.archiveRootLease.Release()
	f.archiveRootLease = nil
}

// stopArchiveTasksは新規の圧縮・upload task登録を停止し、実行中taskがあれば
// 現在の世代の完了channelを返します。
func (f *FsArchiveHandler) stopArchiveTasks() <-chan struct{} {
	f.archiveTaskMu.Lock()
	defer f.archiveTaskMu.Unlock()
	f.archiveTaskClosing = true
	if f.archiveTaskCount == 0 {
		return nil
	}
	return f.archiveTaskGeneration
}

// closeActiveFilesはFileStore lock内で両方のactive fileを一度ずつ閉じます。
// Closeのerror有無にかかわらずhandleは再利用せず、pointerとDirectoryGuard tokenを
// 解除します。errorは呼び出し元へ返しますが、次回Shutdownで同じCloseを再試行しません。
func (f *FsArchiveHandler) closeActiveFiles() error {
	if f == nil || f.Fsstore == nil {
		f.removeDirectoryGuardTokens()
		return nil
	}

	f.Fsstore.Mu.Lock()
	defer f.Fsstore.Mu.Unlock()

	var errs []error
	if err := f.recoverArchiveAppendsLocked(); err != nil {
		errs = append(errs, err)
	}
	if file := f.Fsstore.FileRetryable; file != nil {
		if err := file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close retryable failure archive: %w", err))
		}
		f.Fsstore.FileRetryable = nil
		f.removeDirectoryGuardToken(failurearchive.FailureCategoryRetryable)
	}
	if file := f.Fsstore.FilePermanent; file != nil {
		if err := file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close permanent failure archive: %w", err))
		}
		f.Fsstore.FilePermanent = nil
		f.removeDirectoryGuardToken(failurearchive.FailureCategoryPermanent)
	}
	return errors.Join(errs...)
}
