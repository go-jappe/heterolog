package compressedlogfailure

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestManagedUploaderSaveTimeoutStopsBeforeReturningAndCanRetry(t *testing.T) {
	// bubble内の時刻は、全goroutineが待機状態になるまで進みません。spool保存のfile I/Oと同期が
	// 終わり、lock待ちのarchive保存だけが残ってから保存期限が切れるため、実時間の速さに依存しません。
	synctest.Test(t, func(t *testing.T) {
		base := t.TempDir()
		source, payload := writeCompressedLog(t, base, "app_20260915_1.log.gz")
		remoteErr := errors.New("remote upload failed")
		remote := &testDestinationUploader{
			ids:  []objectstorage.DestinationID{"destination-a"},
			errs: map[objectstorage.DestinationID]error{"destination-a": remoteErr},
		}
		var notifications []Notification
		managed := newTestManagedUploader(t, ManagedUploaderConfig{
			SaveTimeout:                         200 * time.Millisecond,
			CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool})),
			SpoolRuntimes:                       newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
			FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
			Notifier: func(_ context.Context, notification Notification) {
				notifications = append(notifications, notification)
			},
		}, remote)
		// 起動時の保守処理を終えてから始めます。時刻は進めません。
		synctest.Wait()
		// 送信前の既存ファイル確認は通し、送信後の保存だけをlock待ちで期限切れにします。
		remote.afterUpload = archiveRouteForTest(t, managed, "destination-a").owner.coordinator.storeOrPurgeMu.Lock
		release := sync.OnceFunc(archiveRouteForTest(t, managed, "destination-a").owner.coordinator.storeOrPurgeMu.Unlock)
		defer release()
		err := managed.Upload(context.Background(), uploadertest.Request(t, source))
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, remoteErr) {
			t.Fatalf("Upload error = %v, want save deadline and remote failure", err)
		}
		if len(notifications) != 1 || !errors.Is(notifications[0].SaveErr, context.DeadlineExceeded) {
			t.Fatalf("report = %+v, want synchronous save cancellation", notifications)
		}
		assertFileBytes(t, source, payload)
		assertStoredTypes(t, savedKindsForTest(notifications[0]), failurestore.DurableTypeSpool)
		assertFileBytes(t, spoolDetailsForTest(notifications[0]).FilePath, payload)
		spooledBefore, statErr := os.Stat(spoolDetailsForTest(notifications[0]).FilePath)
		if statErr != nil {
			t.Fatal(statErr)
		}
		release()
		// 戻った保存処理は残っていません。lockを解放して全goroutineが待機状態になっても、
		// 次の保存開始前にfinal fileが存在しないままです。
		synctest.Wait()
		archivePath := filepath.Join(archiveRouteForTest(t, managed, "destination-a").store.Root().RetryableDir(), filepath.Base(source))
		if _, err := os.Lstat(archivePath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("canceled save published a file: %v", err)
		}
		// 保存処理だけを再試行できます。ここからS3を呼ぶことはありません。
		managed.saveTimeout = time.Second
		if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err != nil {
			t.Fatalf("retry save: %v", err)
		}
		result := notifications[len(notifications)-1]
		assertStoredTypes(t, savedKindsForTest(result), failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool)
		assertFileBytes(t, archiveDetailsForTest(result).FilePath, payload)
		if spoolDetailsForTest(result).FilePath != "" {
			t.Fatalf("previously saved spool file was reported as a new save: %q", spoolDetailsForTest(result).FilePath)
		}
		spooledAfter, statErr := os.Stat(spoolDetailsForTest(notifications[0]).FilePath)
		if statErr != nil || !os.SameFile(spooledBefore, spooledAfter) {
			t.Fatalf("retry replaced the successfully saved spool file: %v", statErr)
		}
		if len(notifications) != 2 || remote.callCount() != 1 {
			t.Fatalf("notifications=%d remote calls=%d, want 2 and 1", len(notifications), remote.callCount())
		}
	})
}

func TestPublishFailureStoresCancellationWaitsForCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		cleanupStarted := make(chan struct{})
		releaseCleanup := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(releaseCleanup) })
		defer unblock()
		completed := make(chan []error, 1)
		go func() {
			completed <- saveFailureStores([]func() error{
				func() error {
					<-ctx.Done()
					close(cleanupStarted)
					<-releaseCleanup
					return ctx.Err()
				},
				func() error { return nil },
			})
		}()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		select {
		case <-cleanupStarted:
		default:
			t.Fatal("save did not begin cancellation cleanup")
		}
		select {
		case result := <-completed:
			t.Fatalf("save returned before cleanup ended: %v", result)
		default:
		}
		unblock()
		result := <-completed
		if !errors.Is(result[0], context.DeadlineExceeded) || result[1] != nil {
			t.Fatalf("results = %v, want timeout and preserved successful store", result)
		}
	})
}

func TestNewManagedUploaderRejectsNegativeSaveTimeout(t *testing.T) {
	base := t.TempDir()
	_, err := NewManagedUploader(ManagedUploaderConfig{
		SaveTimeout:                         -time.Second,
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
	}, &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative SaveTimeout: %v", err)
	}
}

func TestManagedUploaderDoesNotApplySaveDeadlineToRemote(t *testing.T) {
	base := t.TempDir()
	source, payload := writeCompressedLog(t, base, "app_20260915_2.log.gz")
	remote := &saveBudgetRemoteUploader{started: make(chan struct{}), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(remote.release) })
	defer unblock()
	var notification Notification
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		SaveTimeout:                         time.Second,
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		Notifier:                            func(_ context.Context, result Notification) { notification = result },
	}, remote)
	// remoteへ渡すctxに保存用の期限を付けず、remote失敗後の保存には独立した期限を与えます。
	done := make(chan error, 1)
	go func() { done <- managed.Upload(context.Background(), uploadertest.Request(t, source)) }()
	<-remote.started
	// remote uploaderへ保存期限付きctxを渡していないことを、remote側で確認します。
	if remote.hasDeadline {
		t.Error("save deadline was applied to the remote uploader")
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatalf("save after remote failure: %v", err)
	}
	assertFileBytes(t, archiveDetailsForTest(notification).FilePath, payload)
}

type saveBudgetRemoteUploader struct {
	started     chan struct{}
	release     chan struct{}
	hasDeadline bool
}

func (u *saveBudgetRemoteUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"destination-a"}
}
func (u *saveBudgetRemoteUploader) DestinationService(id objectstorage.DestinationID) (objectstorage.UploadTarget, bool) {
	return objectstorage.UploadTargetS3, id == "destination-a"
}
func (u *saveBudgetRemoteUploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	return u.UploadDestinations(ctx, input, u.DestinationIDs()).Err()
}
func (u *saveBudgetRemoteUploader) UploadDestinations(ctx context.Context, _ uploader.UploadRequest, ids []objectstorage.DestinationID) uploader.UploadReport {
	_, u.hasDeadline = ctx.Deadline()
	close(u.started)
	<-u.release
	return failedUploadReport(ids, errors.New("remote failure"))
}
