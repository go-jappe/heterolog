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
)

func TestSaveFailureStoresStartsBothBeforeWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		archiveStarted := make(chan struct{})
		spoolStarted := make(chan struct{})
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		completed := make(chan []error, 1)
		go func() {
			completed <- saveFailureStores([]func() error{
				func() error {
					close(archiveStarted)
					<-release
					return nil
				},
				func() error {
					close(spoolStarted)
					<-release
					return nil
				},
			})
		}()

		// 両保存処理が停止した状態まで進め、片方の完了を待たずに他方も開始したことを確認します。
		synctest.Wait()
		for name, started := range map[string]<-chan struct{}{
			"archive": archiveStarted,
			"spool":   spoolStarted,
		} {
			select {
			case <-started:
			default:
				t.Fatalf("%s publish did not start while the other publish was blocked", name)
			}
		}
		select {
		case result := <-completed:
			t.Fatalf("publish returned before either store completed: %v", result)
		default:
		}

		unblock()
		result := <-completed
		if len(result) != 2 || result[0] != nil || result[1] != nil {
			t.Fatalf("publish errors = %v, want both nil", result)
		}
	})
}

func TestSaveFailureStoresFailureWaitsForOtherStore(t *testing.T) {
	for _, failureSide := range []string{"archive", "spool"} {
		for _, failureKind := range []string{"error", "panic"} {
			t.Run(failureSide+"/"+failureKind, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					failed := errors.New("injected " + failureSide + " failure")
					failNow := make(chan struct{})
					unblockFailure := sync.OnceFunc(func() { close(failNow) })
					defer unblockFailure()
					releaseOther := make(chan struct{})
					unblockOther := sync.OnceFunc(func() { close(releaseOther) })
					defer unblockOther()
					failingStarted := make(chan struct{})
					otherStarted := make(chan struct{})
					completed := make(chan []error, 1)
					failing := func() error {
						close(failingStarted)
						<-failNow
						if failureKind == "panic" {
							panic(failed)
						}
						return failed
					}
					other := func() error {
						close(otherStarted)
						<-releaseOther
						return nil
					}
					go func() {
						archive, spool := failing, other
						if failureSide == "spool" {
							archive, spool = other, failing
						}
						completed <- saveFailureStores([]func() error{archive, spool})
					}()
					<-failingStarted
					<-otherStarted
					unblockFailure()
					synctest.Wait()
					select {
					case result := <-completed:
						t.Fatalf("publish returned after %s without waiting for the other store: %v", failureKind, result)
					default:
					}

					unblockOther()
					result := <-completed
					if len(result) != 2 {
						t.Fatalf("publish results = %v, want two slots", result)
					}
					failureIndex, otherIndex := 0, 1
					if failureSide == "spool" {
						failureIndex, otherIndex = 1, 0
					}
					if result[otherIndex] != nil {
						t.Fatalf("successful store error = %v", result[otherIndex])
					}
					if failureKind == "error" {
						if !errors.Is(result[failureIndex], failed) {
							t.Fatalf("failing store error = %v, want %v", result[failureIndex], failed)
						}
					} else {
						var panicErr *PanicError
						if !errors.As(result[failureIndex], &panicErr) || !errors.Is(panicErr, ErrDurableStoreSavePanic) || panicErr.Value != failed {
							t.Fatalf("failing store error = %v, want converted panic", result[failureIndex])
						}
					}
				})
			})
		}
	}
}

func TestSaveFailureStoresSkipsUnselectedStores(t *testing.T) {
	for _, selected := range []string{"none", "archive", "spool"} {
		t.Run(selected, func(t *testing.T) {
			calls := 0
			var archive, spool func() error
			publish := func() error {
				calls++
				return nil
			}
			if selected == "archive" {
				archive = publish
			} else if selected == "spool" {
				spool = publish
			}
			results := saveFailureStores([]func() error{archive, spool})
			if len(results) != 2 || results[0] != nil || results[1] != nil {
				t.Fatalf("publish errors = %v, want two nil slots", results)
			}
			wantCalls := 1
			if selected == "none" {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Fatalf("publish calls = %d, want %d", calls, wantCalls)
			}
		})
	}
}

func TestSaveFailureStoresKeepsBothErrors(t *testing.T) {
	archiveFailure := errors.New("archive unavailable")
	spoolFailure := errors.New("spool unavailable")
	results := saveFailureStores([]func() error{
		func() error { return archiveFailure },
		func() error { return spoolFailure },
	})
	if len(results) != 2 || !errors.Is(results[0], archiveFailure) || !errors.Is(results[1], spoolFailure) {
		t.Fatalf("publish errors = %v, want %v, %v", results, archiveFailure, spoolFailure)
	}
}

func TestSaveFailureStoresStartsThreeAndKeepsIndependentResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstFailure := errors.New("second store failed")
		secondFailure := errors.New("third store failed")
		started := [3]chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
		release := [3]chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
		unblock := [3]func(){
			sync.OnceFunc(func() { close(release[0]) }),
			sync.OnceFunc(func() { close(release[1]) }),
			sync.OnceFunc(func() { close(release[2]) }),
		}
		for _, releaseStore := range unblock {
			defer releaseStore()
		}
		completed := make(chan []error, 1)
		go func() {
			completed <- saveFailureStores([]func() error{
				func() error {
					close(started[0])
					<-release[0]
					return nil
				},
				func() error {
					close(started[1])
					<-release[1]
					return firstFailure
				},
				func() error {
					close(started[2])
					<-release[2]
					return secondFailure
				},
			})
		}()

		synctest.Wait()
		for index, signal := range started {
			select {
			case <-signal:
			default:
				t.Fatalf("store %d did not start while the other stores were blocked", index)
			}
		}
		unblock[1]()
		synctest.Wait()
		select {
		case results := <-completed:
			t.Fatalf("save returned before the other stores completed: %v", results)
		default:
		}

		unblock[0]()
		unblock[2]()
		results := <-completed
		if len(results) != 3 || results[0] != nil || !errors.Is(results[1], firstFailure) || !errors.Is(results[2], secondFailure) {
			t.Fatalf("save results = %v, want nil, %v, %v", results, firstFailure, secondFailure)
		}
	})
}

func TestManagedUploaderDoesNotStartWithCanceledUploadContext(t *testing.T) {
	for _, cancellation := range []string{"canceled", "deadline exceeded"} {
		t.Run(cancellation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if cancellation == "deadline exceeded" {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			}
			cancel()
			if ctx.Err() == nil {
				t.Fatal("upload context is not canceled")
			}
			base := t.TempDir()
			sourcePath, _ := writeCompressedLog(t, base, "app_20260915_1.log.gz")
			remoteErr := ctx.Err()
			remote := &testDestinationUploader{
				ids:  []objectstorage.DestinationID{"destination-a"},
				errs: map[objectstorage.DestinationID]error{"destination-a": remoteErr},
			}
			var receivedNotification Notification
			notifierCalls := 0
			var notifierContextErr error
			managed := newTestManagedUploader(t, ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool})),
				SpoolRuntimes:                       newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
				FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
				Notifier: func(notificationCtx context.Context, notification Notification) {
					notifierCalls++
					receivedNotification = notification
					notifierContextErr = notificationCtx.Err()
				},
			}, remote)
			if err := managed.Upload(ctx, uploadertest.Request(t, sourcePath)); !errors.Is(err, remoteErr) {
				t.Fatalf("Upload error = %v, want cancellation", err)
			}
			if err := managed.Close(); err != nil {
				t.Fatalf("Close error = %v", err)
			}
			if notifierCalls != 0 || notifierContextErr != nil || archiveDetailsForTest(receivedNotification).FilePath != "" || remote.callCount() != 0 {
				t.Fatalf("canceled input started work: reports=%d remote calls=%d", notifierCalls, remote.callCount())
			}
		})
	}
}

func TestManagedUploaderConcurrentCloseWaitsForBothStoresHandoff(t *testing.T) {
	// bubble内で全goroutineが待機状態になってから、Close・Uploadがまだ戻っていないことを確かめます。
	synctest.Test(t, func(t *testing.T) {
		base := t.TempDir()
		sourcePath, payload := writeCompressedLog(t, base, "app_20260915_2.log.gz")
		remote := &testDestinationUploader{
			ids:  []objectstorage.DestinationID{"destination-a"},
			errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("temporary outage")},
		}
		notifications := make(chan Notification, 1)
		releaseNotifier := make(chan struct{})
		unblockNotifier := sync.OnceFunc(func() { close(releaseNotifier) })
		managed := newTestManagedUploader(t, ManagedUploaderConfig{
			CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool})),
			SpoolRuntimes:                       newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
			FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
			Notifier: func(_ context.Context, notification Notification) {
				notifications <- notification
				<-releaseNotifier
			},
		}, remote)
		// 途中で失敗しても、Closeの後片付けより先に通知を解放してbubbleを終了させます。
		t.Cleanup(unblockNotifier)
		uploadDone := make(chan error, 1)
		go func() {
			uploadDone <- managed.Upload(context.Background(), uploadertest.Request(t, sourcePath))
		}()

		var notification Notification
		select {
		case notification = <-notifications:
		case <-time.After(2 * time.Second):
			t.Fatal("notifier did not start")
		}
		// 通知の時点で両保存は完了していますが、producerへの引渡しはまだ終了していません。
		assertStoredTypes(t, savedKindsForTest(notification), failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool)
		assertFileBytes(t, archiveDetailsForTest(notification).FilePath, payload)
		assertFileBytes(t, spoolDetailsForTest(notification).FilePath, payload)
		assertFileBytes(t, sourcePath, payload)
		closeDone := make(chan error, 2)
		for range 2 {
			go func() { closeDone <- managed.Close() }()
		}
		// 両方のCloseが引渡しの完了待ちに入るまで進めます。時刻は進めません。
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before handoff completed: %v", err)
		default:
		}
		select {
		case err := <-uploadDone:
			t.Fatalf("Upload returned before its notification completed: %v", err)
		default:
		}

		unblockNotifier()
		for name, done := range map[string]<-chan error{
			"Upload":       uploadDone,
			"first Close":  closeDone,
			"second Close": closeDone,
		} {
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("%s error after handoff = %v", name, err)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%s did not finish after handoff", name)
			}
		}
	})
}
