package logfile

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
)

// CheckpointSaveRetryErrorは、受理済みcheckpointの保存再試行が失敗したことを通知するerrorです。
// SourceIDは保存対象のログ系列、Pathは構築時に解決した実体directoryのcheckpoint文書です。
// 原因はErrで保持し、errors.Is/Asで判定できます。
type CheckpointSaveRetryError struct {
	SourceID string
	Path     string
	Err      error
}

func (e *CheckpointSaveRetryError) Error() string {
	return fmt.Sprintf("retry saving log file upload checkpoint for source %q at %q: %v", e.SourceID, e.Path, e.Err)
}

func (e *CheckpointSaveRetryError) Unwrap() error { return e.Err }

// NewCheckpointStoreは、ファイル保存と保存再試行のエラー通知を一緒に管理するStoreを作ります。
// onErrorはStoreのlockと再試行goroutineの外で呼びます。
// 所有者の通常終了はNotificationsDoneも待つため、通知内の終了要求は非同期に行ってください。
// 初回操作のエラーは戻り値だけで返し、再試行エラーはCheckpointSaveRetryErrorで通知します。
// nilなら通知用goroutineを作りません。保存先は呼び出し前に準備してください。
func NewCheckpointStore(
	directory, sourceID string,
	onError func(error),
	options ...checkpointstore.FilesystemStoreOption,
) (checkpointstore.Store, error) {
	var notifications *notification.Dispatcher
	var enqueueError func(error)
	// 保存再試行は構築後の操作で始まるため、通知時には文書pathが確定しています。
	var documentPath atomic.Pointer[string]
	if onError != nil {
		notifications = notification.NewDispatcher()
		enqueueError = func(err error) {
			notifications.Enqueue(func() {
				retryErr := &CheckpointSaveRetryError{SourceID: sourceID, Err: err}
				if path := documentPath.Load(); path != nil {
					retryErr.Path = *path
				}
				onError(retryErr)
			})
		}
	}
	// 呼び出し元のoptionスライスを変更せず、通知の実行方法はこの所有者で固定します。
	storeOptions := append([]checkpointstore.FilesystemStoreOption(nil), options...)
	storeOptions = append(storeOptions, checkpointstore.WithOnError(enqueueError))
	store, err := checkpointstore.NewFilesystemStore(directory, sourceID, storeOptions...)
	if err != nil {
		notifications.CloseAfterDrain()
		return nil, err
	}
	path := store.DocumentPath()
	documentPath.Store(&path)
	if notifications == nil {
		return store, nil
	}
	return &checkpointStoreWithErrorNotifications{Store: store, notifications: notifications}, nil
}

// checkpointStoreWithErrorNotificationsはStoreと通知workerを同じ期間だけ所有します。
type checkpointStoreWithErrorNotifications struct {
	checkpointstore.Store
	notifications *notification.Dispatcher
	forceOnce     sync.Once
}

// PendingSaveErrorは、通知workerを所有するStoreの保存状況をそのまま返します。
func (s *checkpointStoreWithErrorNotifications) PendingSaveError(ctx context.Context) error {
	return s.Store.(interface{ PendingSaveError(context.Context) error }).PendingSaveError(ctx)
}

func (s *checkpointStoreWithErrorNotifications) Close() error {
	err := s.Store.Close()
	// 保存再試行が終了してから通知受付を閉じます。後段の通知受付を閉じる順序は
	// 所有者が管理し、NotificationsDoneを待ってから通知先の資源を解放します。
	s.notifications.CloseAfterDrain()
	return err
}

// ForceCloseは保存再試行へ強制終了を転送し、実行中の保存の完了を待ちません。
// 通知受付は保存goroutineが実際に終了した後に閉じます。
func (s *checkpointStoreWithErrorNotifications) ForceClose() error {
	err := s.Store.(interface{ ForceClose() error }).ForceClose()
	s.forceOnce.Do(func() {
		go func() {
			<-s.Done()
			s.notifications.CloseAfterDrain()
		}()
	})
	return err
}

// Doneは保存再試行が実際に終了したことを表します。通知はNotificationsDoneで待ちます。
func (s *checkpointStoreWithErrorNotifications) Done() <-chan struct{} {
	return s.Store.(interface{ Done() <-chan struct{} }).Done()
}

// NotificationsDoneはClose後に、受理済みの保存エラー通知がすべて戻ると閉じます。
// 通知先を所有する側は、この完了後に後段の通知受付を閉じます。
// Store.Closeと分け、上流から下流へ通知を排出する順序を所有者側で管理します。
func (s *checkpointStoreWithErrorNotifications) NotificationsDone() <-chan struct{} {
	return s.notifications.Done()
}
