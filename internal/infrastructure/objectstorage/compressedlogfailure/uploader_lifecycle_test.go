package compressedlogfailure

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

func TestManagedUploaderCloseWaitsForAcceptedCallWithoutSeparateDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		remote := &saveBudgetRemoteUploader{started: make(chan struct{}), release: make(chan struct{})}
		release := sync.OnceFunc(func() { close(remote.release) })
		defer release()
		managed := &ManagedUploader{
			next:     remote,
			fileName: logpattern.FileNamePattern{Dir: filepath.Join(t.TempDir(), "other"), Target: "app", Extension: ".log"},
		}
		uploadDone := make(chan error, 1)
		request := uploadertest.Request(t, "app_20260929_1.log.gz")
		go func() {
			uploadDone <- managed.Upload(context.Background(), request)
		}()
		<-remote.started

		closeDone := make(chan error, 1)
		go func() { closeDone <- managed.Close() }()
		synctest.Wait()
		managed.stateMu.Lock()
		closing := managed.closing
		managed.stateMu.Unlock()
		if !closing {
			t.Fatal("Close did not stop accepting uploads")
		}
		if err := managed.Upload(context.Background(), request); !errors.Is(err, ErrUploaderClosed) {
			t.Fatalf("new upload during Close = %v, want ErrUploaderClosed", err)
		}

		// 仮想時刻を進めてもCloseは未完了を返さず、受理済み呼び出しの終了を待ちます。
		// upload自体の期限を設ける試験ではなく、Close専用timerがないことの回帰試験です。
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before accepted upload ended: %v", err)
		default:
		}

		release()
		if err := <-uploadDone; err == nil {
			t.Fatal("remote failure was lost")
		}
		if err := <-closeDone; err != nil {
			t.Fatalf("Close after accepted upload ended: %v", err)
		}
		if err := managed.Close(); err != nil {
			t.Fatalf("repeated Close: %v", err)
		}
	})
}

func TestManagedUploaderForceCloseReleasesWaitButRetainsAcceptedCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		remote := &saveBudgetRemoteUploader{started: make(chan struct{}), release: make(chan struct{})}
		managed := &ManagedUploader{
			next:     remote,
			fileName: logpattern.FileNamePattern{Dir: filepath.Join(t.TempDir(), "other"), Target: "app", Extension: ".log"},
		}
		uploadDone := make(chan error, 1)
		request := uploadertest.Request(t, "app_20260929_1.log.gz")
		go func() { uploadDone <- managed.Upload(context.Background(), request) }()
		<-remote.started
		closeDone := make(chan error, 1)
		go func() { closeDone <- managed.Close() }()
		synctest.Wait()
		if err := managed.RequestForceClose(); !errors.Is(err, pkgshutdown.ErrForcedClose) {
			t.Fatalf("RequestForceClose = %v", err)
		}
		if err := <-closeDone; !errors.Is(err, pkgshutdown.ErrForcedClose) {
			t.Fatalf("Close after force = %v", err)
		}
		select {
		case <-managed.ShutdownDone():
			t.Fatal("resources were released while the accepted call remained active")
		default:
		}
		close(remote.release)
		<-uploadDone
		<-managed.ShutdownDone()
	})
}

func TestManagedUploaderNotifierReceivesOnlyExplicitForceCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan context.Context, 1)
		managed := &ManagedUploader{notifier: func(ctx context.Context, _ Notification) {
			started <- ctx
			<-ctx.Done()
		}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		done := make(chan struct{})
		go func() { managed.notify(ctx, Notification{}); close(done) }()
		notificationContext := <-started
		synctest.Wait()
		if err := notificationContext.Err(); err != nil {
			t.Fatalf("sender cancellation reached notification: %v", err)
		}
		if err := managed.RequestForceClose(); !errors.Is(err, pkgshutdown.ErrForcedClose) {
			t.Fatal(err)
		}
		<-done
		if err := notificationContext.Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("force notification context = %v", err)
		}
	})
}

func TestManagedUploaderCloseOwnsAcceptedNotificationUntilItReturns(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "normal close drains notification"
		if force {
			name = "force releases waiter and retains notification"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				base := t.TempDir()
				source, payload := writeCompressedLog(t, base, "app_20261020_1.log.gz")
				remote := &testDestinationUploader{
					ids:  []objectstorage.DestinationID{"destination-a"},
					errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("temporary upload failure")},
				}
				entered := make(chan context.Context, 1)
				finish := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(finish) })
				defer unblock()
				created, err := NewManagedUploader(ManagedUploaderConfig{
					DisableMaintenance:                  true,
					CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
					FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
					Notifier: func(ctx context.Context, _ Notification) {
						entered <- ctx
						<-finish // 中止に即応しない通知でも資源を先に解放してはいけません。
					},
				}, remote)
				if err != nil {
					t.Fatal(err)
				}
				managed := created.(*ManagedUploader)
				t.Cleanup(func() { _ = managed.RequestForceClose(); _ = managed.Close() })
				uploadDone := make(chan error, 1)
				go func() { uploadDone <- managed.Upload(context.Background(), uploadertest.Request(t, source)) }()
				notificationContext := <-entered
				closed := make(chan error, 1)
				go func() { closed <- managed.Close() }()
				synctest.Wait()
				if notificationContext.Err() != nil || len(closed) != 0 {
					t.Fatal("normal close canceled or abandoned its accepted notification")
				}
				assertFileBytes(t, source, payload)
				if force {
					if err := managed.RequestForceClose(); !errors.Is(err, pkgshutdown.ErrForcedClose) {
						t.Fatal(err)
					}
					if err := <-closed; !errors.Is(err, pkgshutdown.ErrForcedClose) {
						t.Fatalf("Close after force = %v", err)
					}
					synctest.Wait()
					if !errors.Is(notificationContext.Err(), context.Canceled) {
						t.Fatal("force did not reach the accepted notification")
					}
				}
				select {
				case <-managed.ShutdownDone():
					t.Fatal("shutdown completed before its accepted notification returned")
				default:
				}
				unblock()
				if err := <-uploadDone; err != nil {
					t.Fatalf("durable upload result = %v", err)
				}
				if !force {
					if err := <-closed; err != nil {
						t.Fatal(err)
					}
				}
				<-managed.ShutdownDone()
			})
		})
	}
}
