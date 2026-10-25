package compressedlogfailure

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func newReuseTestUploader(t *testing.T, types []failurestore.DurableType, ids ...objectstorage.DestinationID) (*ManagedUploader, *testDestinationUploader, string, []byte) {
	t.Helper()
	base := t.TempDir()
	source, payload := writeCompressedLog(t, base, "app_20260929_1.log.gz")
	store := newUploadProgressStore(t, t.TempDir())
	require.NoError(t, store.RegisterDestinations(context.Background(), ids, nil))
	remote := &testDestinationUploader{ids: ids}
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), types)),
		SpoolRuntimes:                       newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
		CheckpointStore:                     store,
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
	}, remote)
	return managed, remote, source, payload
}

func TestManagedUploaderReusesCompleteHandoffWithoutUploadProgress(t *testing.T) {
	for _, test := range []struct {
		state   pkgspool.State
		archive uploader.FailureCategory
	}{
		{state: pkgspool.StateActive},
		{state: pkgspool.StateProcessing},
		{state: pkgspool.StateDone},
		{state: pkgspool.StateDead},
		{state: pkgspool.StateQuarantine},
		{state: pkgspool.StateActive, archive: uploader.FailureCategoryRetryable},
		{state: pkgspool.StateDead, archive: uploader.FailureCategoryPermanent},
	} {
		t.Run(string(test.state)+"/"+string(test.archive), func(t *testing.T) {
			ctx := context.Background()
			types := []failurestore.DurableType{failurestore.DurableTypeSpool}
			if test.archive != "" {
				types = append(types, failurestore.DurableTypeFileArchive)
			}
			managed, remote, source, payload := newReuseTestUploader(t, types, "destination-a")
			directory, err := spoolRouteForTest(t, managed, "destination-a").Root().StateDir(test.state)
			require.NoError(t, err)
			spoolPath := filepath.Join(directory, filepath.Base(source))
			// 前回のプロセスが退避したものの、checkpointに記録を残せなかった状況。
			require.NoError(t, os.WriteFile(spoolPath, payload, 0o600))
			archivePath := ""
			if test.archive != "" {
				directory, err = archiveRouteForTest(t, managed, "destination-a").store.Root().FailureCategoryDir(test.archive)
				require.NoError(t, err)
				archivePath = filepath.Join(directory, filepath.Base(source))
				require.NoError(t, os.WriteFile(archivePath, payload, 0o600))
			}
			var notifications []Notification
			managed.notifier = func(_ context.Context, notification Notification) {
				notifications = append(notifications, notification)
			}
			require.NoError(t, managed.Upload(ctx, uploadertest.Request(t, source)))
			require.Zero(t, remote.callCount())
			require.Empty(t, notifications, "clean reuse does not report an error or conflict")
			key := checkpoint.CheckpointKey{Generation: "20260929-1", Compressed: true}
			_, found, err := managed.checkpointStore.LoadUploadProgress(ctx, key, "destination-a")
			require.NoError(t, err)
			require.False(t, found, "the original upload failure must not be fabricated from the current spool state")
			// 通常と同じくproducerが完了を記録した後は、退避ファイルが消えても再送しません。
			_, err = managed.checkpointStore.(*checkpoint.FilesystemStore).AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"destination-a"})
			require.NoError(t, err)
			require.NoError(t, os.Remove(spoolPath))
			if archivePath != "" {
				require.NoError(t, os.Remove(archivePath))
			}
			require.NoError(t, managed.Upload(ctx, uploadertest.Request(t, source)))
			require.Zero(t, remote.callCount())
		})
	}
}

func TestManagedUploaderPreservesConflictsAndBlocksInvalidCopies(t *testing.T) {
	for _, test := range []struct {
		name        string
		spool       string
		archive     string
		wantRenamed bool
		wantError   bool
	}{
		{name: "no copies"},
		{name: "archive alone", archive: "matching"},
		{name: "missing required archive", spool: "matching"},
		{name: "purged spool", spool: "purged", archive: "matching"},
		{name: "different spool bytes", spool: "different", archive: "matching", wantRenamed: true},
		{name: "spool is directory", spool: "directory", archive: "matching", wantRenamed: true},
		{name: "different archive bytes", spool: "matching", archive: "different", wantRenamed: true},
		{name: "archive is directory", spool: "matching", archive: "directory", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			managed, remote, source, payload := newReuseTestUploader(t,
				[]failurestore.DurableType{failurestore.DurableTypeSpool, failurestore.DurableTypeFileArchive}, "destination-a")
			for path, kind := range map[string]string{
				filepath.Join(spoolRouteForTest(t, managed, "destination-a").Root().ActiveDir(), filepath.Base(source)):            test.spool,
				filepath.Join(archiveRouteForTest(t, managed, "destination-a").store.Root().RetryableDir(), filepath.Base(source)): test.archive,
			} {
				switch kind {
				case "matching", "purged":
					require.NoError(t, os.WriteFile(path, payload, 0o600))
					if kind == "purged" {
						require.NoError(t, os.Remove(path))
					}
				case "different":
					require.NoError(t, os.WriteFile(path, []byte("different bytes"), 0o600))
				case "directory":
					require.NoError(t, os.Mkdir(path, 0o700))
					require.NoError(t, os.WriteFile(filepath.Join(path, "keep.txt"), []byte("keep nested data"), 0o600))
				}
			}
			var notifications []Notification
			managed.notifier = func(_ context.Context, notification Notification) {
				notifications = append(notifications, notification)
			}
			err := managed.Upload(context.Background(), uploadertest.Request(t, source))
			if test.wantError {
				require.Error(t, err)
				require.Zero(t, remote.callCount())
				require.Len(t, notifications, 1)
				require.Error(t, notifications[0].ExistingFileCheckErr)
				require.Empty(t, notifications[0].ConflictRenames)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, remote.callCount())
			if test.wantRenamed {
				require.Len(t, notifications, 1)
				require.NoError(t, notifications[0].ExistingFileCheckErr)
				require.Len(t, notifications[0].ConflictRenames, 1)
				conflict := notifications[0].ConflictRenames[0]
				if test.spool == "directory" {
					retainedBytes, err := os.ReadFile(filepath.Join(conflict.RetainedPath, "keep.txt"))
					require.NoError(t, err)
					require.Equal(t, []byte("keep nested data"), retainedBytes)
				} else {
					retainedBytes, err := os.ReadFile(conflict.RetainedPath)
					require.NoError(t, err)
					require.Equal(t, []byte("different bytes"), retainedBytes)
				}
				_, err = os.Lstat(conflict.OriginalPath)
				require.ErrorIs(t, err, os.ErrNotExist)
			} else {
				require.Empty(t, notifications)
			}
		})
	}
}

func TestManagedUploaderReportsConflictWhileReusingAllFailureTargets(t *testing.T) {
	ctx := context.Background()
	destination := objectstorage.DestinationID("destination-a")
	managed, remote, source, payload := newReuseTestUploader(t,
		[]failurestore.DurableType{failurestore.DurableTypeSpool, failurestore.DurableTypeFileArchive}, destination)
	fileName := filepath.Base(source)
	archivePath := filepath.Join(archiveRouteForTest(t, managed, destination).store.Root().RetryableDir(), fileName)
	spoolPath := filepath.Join(spoolRouteForTest(t, managed, destination).Root().ActiveDir(), fileName)
	conflictingPath := filepath.Join(spoolRouteForTest(t, managed, destination).Root().DeadDir(), fileName)
	conflictingBytes := []byte("older, different copy")
	require.NoError(t, os.WriteFile(archivePath, payload, 0o600))
	require.NoError(t, os.WriteFile(spoolPath, payload, 0o600))
	require.NoError(t, os.WriteFile(conflictingPath, conflictingBytes, 0o600))
	var notifications []Notification
	managed.notifier = func(_ context.Context, notification Notification) {
		notifications = append(notifications, notification)
	}

	require.NoError(t, managed.Upload(ctx, uploadertest.Request(t, source)))
	require.Zero(t, remote.callCount())
	require.Len(t, notifications, 1, "the preserved conflict still needs a notification")
	require.NoError(t, notifications[0].UploadErr)
	require.NoError(t, notifications[0].SaveErr)
	require.NoError(t, notifications[0].ExistingFileCheckErr)
	require.Equal(t, destination, notifications[0].DestinationID)
	require.Equal(t, source, notifications[0].SourcePath)
	require.Len(t, notifications[0].ConflictRenames, 1)
	conflict := notifications[0].ConflictRenames[0]
	require.Equal(t, conflictingPath, conflict.OriginalPath)
	require.NoFileExists(t, conflictingPath)
	retainedBytes, err := os.ReadFile(conflict.RetainedPath)
	require.NoError(t, err)
	require.Equal(t, conflictingBytes, retainedBytes)
	key := checkpoint.CheckpointKey{Generation: "20260929-1", Compressed: true}
	_, found, err := managed.checkpointStore.LoadUploadProgress(ctx, key, destination)
	require.NoError(t, err)
	require.False(t, found, "reuse must not invent an upload failure result")
}

func TestManagedUploaderExistingSpoolOnlySkipsItsDestination(t *testing.T) {
	ctx := context.Background()
	ids := []objectstorage.DestinationID{"destination-a", "destination-b"}
	managed, remote, source, payload := newReuseTestUploader(t,
		[]failurestore.DurableType{failurestore.DurableTypeSpool, failurestore.DurableTypeFileArchive}, ids...)
	require.NoError(t, os.WriteFile(filepath.Join(spoolRouteForTest(t, managed, ids[0]).Root().ActiveDir(), filepath.Base(source)), payload, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(archiveRouteForTest(t, managed, "destination-a").store.Root().RetryableDir(), filepath.Base(source)), payload, 0o600))
	require.NoError(t, managed.UploadDestinations(ctx, uploadertest.Request(t, source), ids).Err())
	require.Equal(t, 1, remote.callCount())
	key := checkpoint.CheckpointKey{Generation: "20260929-1", Compressed: true}
	_, found, err := managed.checkpointStore.LoadUploadProgress(ctx, key, ids[0])
	require.NoError(t, err)
	require.False(t, found)
	progress, found, err := managed.checkpointStore.LoadUploadProgress(ctx, key, ids[1])
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, checkpoint.UploadResultSuccess, progress.UploadResult, "the other destination must actually be uploaded")
}

func TestManagedUploaderArchiveOnlyReusesMatchingFileForItsDestination(t *testing.T) {
	for _, category := range []uploader.FailureCategory{uploader.FailureCategoryRetryable, uploader.FailureCategoryPermanent} {
		t.Run(string(category), func(t *testing.T) {
			ctx := context.Background()
			managed, remote, source, payload := newReuseTestUploader(t, []failurestore.DurableType{failurestore.DurableTypeFileArchive}, "destination-a")
			directory, err := archiveRouteForTest(t, managed, "destination-a").store.Root().FailureCategoryDir(category)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(directory, filepath.Base(source)), payload, 0o600))
			var notifications []Notification
			managed.notifier = func(_ context.Context, notification Notification) {
				notifications = append(notifications, notification)
			}

			require.NoError(t, managed.Upload(ctx, uploadertest.Request(t, source)))
			require.Zero(t, remote.callCount())
			require.Empty(t, notifications, "clean archive reuse does not report an error or conflict")
			key := checkpoint.CheckpointKey{Generation: "20260929-1", Compressed: true}
			_, found, err := managed.checkpointStore.LoadUploadProgress(ctx, key, "destination-a")
			require.NoError(t, err)
			require.False(t, found, "the unknown upload failure must not be fabricated")
		})
	}
}

func TestManagedUploaderArchiveOnlyDoesNotReuseAnotherDestinationsFile(t *testing.T) {
	ctx := context.Background()
	ids := []objectstorage.DestinationID{"destination-a", "destination-b"}
	managed, remote, source, payload := newReuseTestUploader(t, []failurestore.DurableType{failurestore.DurableTypeFileArchive}, ids...)
	require.NoError(t, os.WriteFile(filepath.Join(archiveRouteForTest(t, managed, ids[0]).store.Root().PermanentDir(), filepath.Base(source)), payload, 0o600))

	require.NoError(t, managed.UploadDestinations(ctx, uploadertest.Request(t, source), ids).Err())
	require.Equal(t, 1, remote.callCount(), "only the destination without an archive copy needs upload")
	key := checkpoint.CheckpointKey{Generation: "20260929-1", Compressed: true}
	_, found, err := managed.checkpointStore.LoadUploadProgress(ctx, key, ids[0])
	require.NoError(t, err)
	require.False(t, found)
	progress, found, err := managed.checkpointStore.LoadUploadProgress(ctx, key, ids[1])
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, checkpoint.UploadResultSuccess, progress.UploadResult)
}

func TestManagedUploaderArchiveOnlyRenamesDifferentContentBeforeUpload(t *testing.T) {
	managed, remote, source, _ := newReuseTestUploader(t, []failurestore.DurableType{failurestore.DurableTypeFileArchive}, "destination-a")
	archivePath := filepath.Join(archiveRouteForTest(t, managed, "destination-a").store.Root().RetryableDir(), filepath.Base(source))
	oldBytes := []byte("different archive content")
	require.NoError(t, os.WriteFile(archivePath, oldBytes, 0o600))
	var notifications []Notification
	managed.notifier = func(_ context.Context, notification Notification) {
		notifications = append(notifications, notification)
	}

	require.NoError(t, managed.Upload(context.Background(), uploadertest.Request(t, source)))
	require.Equal(t, 1, remote.callCount())
	require.Len(t, notifications, 1)
	require.Len(t, notifications[0].ConflictRenames, 1)
	conflict := notifications[0].ConflictRenames[0]
	require.Equal(t, archivePath, conflict.OriginalPath)
	require.NoFileExists(t, archivePath)
	retainedBytes, err := os.ReadFile(conflict.RetainedPath)
	require.NoError(t, err)
	require.Equal(t, oldBytes, retainedBytes)
}

// 保存済みの退避先を確かめる再利用の判定は、archiveの保存・purgeのlockを待っている間も
// contextの取消で中止でき、object storageへは送信しません。bubble内でUploadがlockの取得待ちに
// 入ったことを確かめてから取り消します。
func TestManagedUploaderReuseCheckCanCancelBeforeUpload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		managed, remote, source, payload := newReuseTestUploader(t,
			[]failurestore.DurableType{failurestore.DurableTypeSpool, failurestore.DurableTypeFileArchive}, "destination-a")
		require.NoError(t, os.WriteFile(filepath.Join(spoolRouteForTest(t, managed, "destination-a").Root().ActiveDir(), filepath.Base(source)), payload, 0o600))
		coordinator := archiveRouteForTest(t, managed, "destination-a").owner.coordinator
		coordinator.storeOrPurgeMu.Lock()
		// 途中で失敗しても、Closeの後片付けより先にlockを解放し、待っているUploadとbubbleを終了させます。
		unlock := sync.OnceFunc(coordinator.storeOrPurgeMu.Unlock)
		t.Cleanup(unlock)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		request := uploadertest.Request(t, source)
		done := make(chan error, 1)
		go func() { done <- managed.Upload(ctx, request) }()

		// Uploadがlockの取得待ちに入るまで進めます。時刻は進めません。
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Upload returned before waiting for the archive lock: %v", err)
		default:
		}
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(10 * time.Second):
			t.Fatal("Upload kept waiting for the archive lock after its context was canceled")
		}
		require.Zero(t, remote.callCount())
	})
}
