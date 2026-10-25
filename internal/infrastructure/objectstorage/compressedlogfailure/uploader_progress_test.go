package compressedlogfailure

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

func newUploadProgressStore(t *testing.T, directory string) *checkpoint.FilesystemStore {
	t.Helper()
	store, err := checkpoint.NewFilesystemStore(directory, "test-source")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.RegisterDestinations(context.Background(), []objectstorage.DestinationID{"destination-a"}, nil); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestManagedUploaderPreservesSuccessfulUploadBeforeCursorAdvance(t *testing.T) {
	for _, rejectSave := range []bool{false, true} {
		name := "saved immediately"
		if rejectSave {
			name = "save initially rejected"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			base := t.TempDir()
			source, _ := writeCompressedLog(t, base, "app_20260915_1.log.gz")
			directory := t.TempDir()
			store := &progressSaveTestStore{Store: newUploadProgressStore(t, directory)}
			injected := errors.New("checkpoint did not accept success")
			if rejectSave {
				store.hardError = injected
			}
			config := ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
				CheckpointStore:                     store,
				FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
			}
			remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}}
			managed := newTestManagedUploader(t, config, remote)
			key := checkpoint.CheckpointKey{Generation: "20260915-1", Compressed: true}
			input := uploadertest.Request(t, source)
			// 送信前のキャンセルでは、成功記録を作りません。
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if err := managed.Upload(canceled, input); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled upload: %v", err)
			}
			if _, found, err := store.LoadUploadProgress(ctx, key, "destination-a"); err != nil || found || remote.callCount() != 0 {
				t.Fatalf("before upload: found=%v error=%v calls=%d", found, err, remote.callCount())
			}
			err := managed.Upload(ctx, input)
			if rejectSave {
				if !errors.Is(err, injected) {
					t.Fatalf("rejected save: %v", err)
				}
				if _, found, err := store.LoadUploadProgress(ctx, key, "destination-a"); err != nil || found {
					t.Fatalf("rejected result persisted: found=%v error=%v", found, err)
				}
				store.hardError = nil
				err = managed.Upload(ctx, input)
			}
			if err != nil || remote.callCount() != 1 {
				t.Fatalf("upload: error=%v calls=%d", err, remote.callCount())
			}
			progress, found, err := store.LoadUploadProgress(ctx, key, "destination-a")
			if err != nil || !found || progress.UploadResult != checkpoint.UploadResultSuccess || len(progress.SavedTargets) != 0 {
				t.Fatalf("success record: progress=%+v found=%v error=%v", progress, found, err)
			}
			if err := managed.Close(); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			// AdvanceCompletionCursor前に再起動しても、成功記録を未試行と取り違えません。
			config.CheckpointStore = newUploadProgressStore(t, directory)
			nextRemote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}}
			reopened := newTestManagedUploader(t, config, nextRemote)
			if err := reopened.Upload(ctx, input); err != nil || nextRemote.callCount() != 0 {
				t.Fatalf("restarted upload: error=%v calls=%d", err, nextRemote.callCount())
			}
			assertDirectoryEmpty(t, archiveRouteForTest(t, reopened, "destination-a").store.Root().RetryableDir())
			assertDirectoryEmpty(t, archiveRouteForTest(t, reopened, "destination-a").store.Root().PermanentDir())
		})
	}
}

func TestManagedUploaderRestartsFromPersistedPartialHandoff(t *testing.T) {
	for _, scenario := range []struct {
		name          string
		spoolFails    bool
		changeArchive bool
		changeSpool   bool
	}{
		{name: "archive saved and spool failed", spoolFails: true},
		{name: "spool already drained and archive failed"},
		{name: "changed archive root is a new target", spoolFails: true, changeArchive: true},
		{name: "changed spool root is a new target", changeSpool: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			base := t.TempDir()
			source, payload := writeCompressedLog(t, base, "app_20260915_1.log.gz")
			checkpointDirectory := t.TempDir()
			store := newUploadProgressStore(t, checkpointDirectory)
			archiveBase := filepath.Join(base, "archive")
			spoolBase := filepath.Join(base, "spool")
			runtime := newTestSpoolRuntimes(t, spoolBase)
			config := ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(archiveBase, []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool})),
				SpoolRuntimes:                       runtime,
				CheckpointStore:                     store,
				FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
			}
			remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}, errs: map[objectstorage.DestinationID]error{
				"destination-a": &pkgs3.HTTPStatusError{Code: http.StatusForbidden},
			}}
			managed := newTestManagedUploader(t, config, remote)
			archivePath := filepath.Join(newArchiveRoot(t, archiveBase).PermanentDir(), filepath.Base(source))
			spoolPath := filepath.Join(newSpoolRoot(t, spoolBase).DeadDir(), filepath.Base(source))
			failedPath, savedPath := archivePath, spoolPath
			if scenario.spoolFails {
				failedPath, savedPath = spoolPath, archivePath
			}
			// 送信失敗後に保存先の障害を起こして途中失敗を再現します。
			// spool内の不正なエントリは自動保全できるため、spoolの保存失敗には
			// staging directory自体を通常ファイルへ置き換える障害を使います。
			blockedPath := failedPath
			if scenario.spoolFails {
				blockedPath = filepath.Join(filepath.Dir(filepath.Dir(spoolPath)), ".staging")
			}
			var injectionErr error
			remote.afterUpload = func() {
				if scenario.spoolFails {
					if injectionErr = os.Remove(blockedPath); injectionErr == nil {
						injectionErr = os.WriteFile(blockedPath, []byte("blocked"), 0o600)
					}
				} else {
					injectionErr = os.Mkdir(blockedPath, 0o700)
				}
			}
			if err := managed.Upload(ctx, uploadertest.Request(t, source)); err == nil {
				t.Fatal("partial handoff unexpectedly completed")
			}
			if injectionErr != nil {
				t.Fatalf("inject invalid destination entry: %v", injectionErr)
			}
			key := checkpoint.CheckpointKey{Generation: "20260915-1", Compressed: true}
			progress, found, err := store.LoadUploadProgress(ctx, key, "destination-a")
			if err != nil || !found || progress.UploadResult != checkpoint.UploadResultPermanentFailure || len(progress.SavedTargets) != 1 {
				t.Fatalf("progress=%+v error=%v, want permanent failure and one saved target", progress, err)
			}
			assertFileBytes(t, savedPath, payload)
			if err := managed.Close(); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{blockedPath, savedPath} {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			if scenario.spoolFails {
				if err := os.Mkdir(blockedPath, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			// 保存済みファイルはpurgeやspool再送で消えていても、再作成しません。
			reopenedStore := newUploadProgressStore(t, checkpointDirectory)
			config.CheckpointStore = reopenedStore
			if scenario.changeArchive {
				archiveBase = filepath.Join(base, "replacement-archive")
				config.CompressedLogFileUploadFailureStore = NewConfigAdapter(testFailureStoreConfig(archiveBase, []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool}))
			}
			if scenario.changeSpool {
				spoolBase = filepath.Join(base, "replacement-spool")
				config.SpoolRuntimes = newTestSpoolRuntimes(t, spoolBase)
			}
			nextRemote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}}
			var resumedNotifications []Notification
			config.Notifier = func(_ context.Context, notification Notification) {
				resumedNotifications = append(resumedNotifications, notification)
			}
			reopened := newTestManagedUploader(t, config, nextRemote)
			if err := reopened.Upload(ctx, uploadertest.Request(t, source)); err != nil {
				t.Fatal(err)
			}
			if len(resumedNotifications) != 1 || resumedNotifications[0].UploadErr != nil {
				t.Fatalf("resumed handoff repeated the earlier remote failure: %+v", resumedNotifications)
			}
			if remote.callCount() != 1 || nextRemote.callCount() != 0 {
				t.Fatalf("remote attempts=%d+%d, want original attempt only", remote.callCount(), nextRemote.callCount())
			}
			if _, err := os.Lstat(savedPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("already saved target was recreated: %v", err)
			}
			assertFileBytes(t, failedPath, payload)
			if scenario.changeArchive {
				assertFileBytes(t, filepath.Join(newArchiveRoot(t, archiveBase).PermanentDir(), filepath.Base(source)), payload)
			}
			if scenario.changeSpool {
				assertFileBytes(t, filepath.Join(newSpoolRoot(t, spoolBase).DeadDir(), filepath.Base(source)), payload)
			}
			if _, err := reopenedStore.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"destination-a"}); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Upload(ctx, uploadertest.Request(t, source)); err != nil || nextRemote.callCount() != 0 {
				t.Fatalf("completed cursor retried upload: %v, calls=%d", err, nextRemote.callCount())
			}
		})
	}
}

type progressSaveTestStore struct {
	checkpoint.Store
	mu        sync.Mutex
	pending   bool
	hardError error
	onSave    func(checkpoint.UploadProgress)
}

func (s *progressSaveTestStore) SaveUploadProgress(ctx context.Context, progress checkpoint.UploadProgress) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hardError != nil {
		return s.hardError
	}
	if err := s.Store.SaveUploadProgress(ctx, progress); err != nil {
		return err
	}
	if s.onSave != nil {
		s.onSave(progress)
	}
	if s.pending {
		return &checkpoint.PendingSaveError{Err: errors.New("injected checkpoint sync failure")}
	}
	return nil
}

func TestManagedUploaderContinuesAfterAcceptedCheckpointSaveFailure(t *testing.T) {
	base := t.TempDir()
	source, payload := writeCompressedLog(t, base, "app_20260915_1.log.gz")
	store := &progressSaveTestStore{Store: newUploadProgressStore(t, t.TempDir()), pending: true}
	remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}, errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("remote failure")}}
	var receivedNotification Notification
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		CheckpointStore:                     store,
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		Notifier:                            func(_ context.Context, notification Notification) { receivedNotification = notification },
	}, remote)
	if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err != nil {
		t.Fatal(err)
	}
	var pending *checkpoint.PendingSaveError
	if !errors.As(receivedNotification.CheckpointErr, &pending) || receivedNotification.SaveErr != nil {
		t.Fatalf("report=%+v, want successful handoff and pending checkpoint notice", receivedNotification)
	}
	assertFileBytes(t, archiveDetailsForTest(receivedNotification).FilePath, payload)
	if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err != nil || remote.callCount() != 1 {
		t.Fatalf("retry repeated upload: error=%v calls=%d", err, remote.callCount())
	}
}

func TestManagedUploaderDoesNotLoseOutcomeWhenCheckpointRejectsUpdate(t *testing.T) {
	base := t.TempDir()
	source, _ := writeCompressedLog(t, base, "app_20260915_1.log.gz")
	injected := errors.New("checkpoint did not accept update")
	store := &progressSaveTestStore{Store: newUploadProgressStore(t, t.TempDir()), hardError: injected}
	remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}, errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("remote failure")}}
	var receivedNotification Notification
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		CheckpointStore:                     store,
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		Notifier:                            func(_ context.Context, notification Notification) { receivedNotification = notification },
	}, remote)
	if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); !errors.Is(err, injected) || !errors.Is(err, remote.errs["destination-a"]) {
		t.Fatalf("Upload error=%v", err)
	}
	if !errors.Is(receivedNotification.CheckpointErr, injected) || receivedNotification.SourcePath != source || receivedNotification.UploadErr != nil || receivedNotification.FailureCategory != "" || receivedNotification.SaveErr != nil || len(savedKindsForTest(receivedNotification)) != 0 {
		t.Fatalf("checkpoint-only notification claimed a handoff result: %+v", receivedNotification)
	}
	assertDirectoryEmpty(t, archiveRouteForTest(t, managed, "destination-a").store.Root().RetryableDir())
	store.hardError = nil
	if err := managed.Upload(context.Background(), uploadertest.Request(t, source)); err != nil || remote.callCount() != 1 {
		t.Fatalf("retry repeated upload: error=%v calls=%d", err, remote.callCount())
	}
}

func TestManagedUploaderPersistsOneSavedTargetWhileOtherIsBlocked(t *testing.T) {
	base := t.TempDir()
	source, _ := writeCompressedLog(t, base, "app_20260915_1.log.gz")
	savedSpool := make(chan struct{}, 1)
	store := &progressSaveTestStore{Store: newUploadProgressStore(t, t.TempDir()), onSave: func(progress checkpoint.UploadProgress) {
		for _, saved := range progress.SavedTargets {
			if saved.Kind() == failurestore.DurableTypeSpool {
				select {
				case savedSpool <- struct{}{}:
				default:
				}
			}
		}
	}}
	remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}, errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("remote failure")}}
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool})),
		SpoolRuntimes:                       newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
		CheckpointStore:                     store,
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
	}, remote)
	// 既存ファイルの確認順によらず、送信後のarchive保存だけを待機させます。
	remote.afterUpload = archiveRouteForTest(t, managed, "destination-a").owner.coordinator.storeOrPurgeMu.Lock
	release := sync.OnceFunc(archiveRouteForTest(t, managed, "destination-a").owner.coordinator.storeOrPurgeMu.Unlock)
	defer release()
	finished := make(chan error, 2)
	go func() { finished <- managed.Upload(context.Background(), uploadertest.Request(t, source)) }()
	select {
	case <-savedSpool:
	case <-time.After(5 * time.Second):
		t.Fatal("spool completion not persisted while archive blocked")
	}
	go func() { finished <- managed.Upload(context.Background(), uploadertest.Request(t, source)) }()
	release()
	for range 2 {
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
	}
	if remote.callCount() != 1 {
		t.Fatalf("concurrent retry repeated remote: %d calls", remote.callCount())
	}
}

type cancelDuringUpload struct {
	*testDestinationUploader
	cancel context.CancelFunc
}

func (u cancelDuringUpload) UploadDestinations(ctx context.Context, input uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
	u.cancel()
	return u.testDestinationUploader.UploadDestinations(ctx, input, destinations)
}

func TestManagedUploaderSavesAfterActualUploadCancelsContext(t *testing.T) {
	base := t.TempDir()
	source, payload := writeCompressedLog(t, base, "app_20260915_1.log.gz")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	remote := cancelDuringUpload{testDestinationUploader: &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}, errs: map[objectstorage.DestinationID]error{"destination-a": context.Canceled}}, cancel: cancel}
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
	}, remote)
	if err := managed.Upload(ctx, uploadertest.Request(t, source)); err != nil {
		t.Fatal(err)
	}
	assertFileBytes(t, filepath.Join(archiveRouteForTest(t, managed, "destination-a").store.Root().RetryableDir(), filepath.Base(source)), payload)
}
