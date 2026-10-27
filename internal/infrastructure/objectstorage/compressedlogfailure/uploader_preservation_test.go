package compressedlogfailure

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// conflictingResultStoreは、targetの結果を受理する直前に、それより前の異なるreceiptを注入します。
// 通常のspool状態遷移がこれを行うことはありません。
type conflictingResultStore struct {
	checkpoint.Store
	mu              sync.Mutex
	kind            failurestore.DurableType
	injected        bool
	recorded        checkpoint.SavedFailureTarget
	pendingConflict bool
}

func (s *conflictingResultStore) SaveUploadProgress(ctx context.Context, progress checkpoint.UploadProgress) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.injected {
		for index, saved := range progress.SavedTargets {
			if saved.Kind() != s.kind {
				continue
			}
			previous := progress
			previous.SavedTargets = slices.Clone(progress.SavedTargets)
			switch target := saved.(type) {
			case checkpoint.SavedArchiveTarget:
				target.FailureCategory = uploader.FailureCategoryPermanent
				s.recorded = target
			case checkpoint.SavedSpoolTarget:
				target.State = pkgspool.StateDead
				s.recorded = target
			}
			previous.SavedTargets[index] = s.recorded
			if err := s.Store.SaveUploadProgress(ctx, previous); err != nil {
				return err
			}
			s.injected = true
			break
		}
	}
	err := s.Store.SaveUploadProgress(ctx, progress)
	var conflict *checkpoint.SavedTargetConflictError
	if s.pendingConflict && errors.As(err, &conflict) {
		return errors.Join(err, &checkpoint.PendingSaveError{Err: errors.New("pending conflicting checkpoint save")})
	}
	return err
}

func TestManagedUploaderPreservesSourceBeforeAcceptingConflictingSavedResult(t *testing.T) {
	for _, test := range []struct {
		name    string
		kind    failurestore.DurableType
		targets []failurestore.DurableType
	}{
		{"archive only", failurestore.DurableTypeFileArchive, []failurestore.DurableType{failurestore.DurableTypeFileArchive}},
		{"spool only", failurestore.DurableTypeSpool, []failurestore.DurableType{failurestore.DurableTypeSpool}},
		{"archive and parallel spool", failurestore.DurableTypeFileArchive, []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool}},
		{"spool and parallel archive", failurestore.DurableTypeSpool, []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			base := t.TempDir()
			source, payload := writeCompressedLog(t, base, "app_20260915_1.log.gz")
			store := &conflictingResultStore{Store: newUploadProgressStore(t, t.TempDir()), kind: test.kind}
			remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}, errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("temporary outage")}}
			var notifications []Notification
			managed := newTestManagedUploader(t, ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), test.targets)),
				SpoolRuntimes:                       newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
				CheckpointStore:                     store, FileName: logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
				Notifier: func(_ context.Context, notification Notification) {
					if notification.PreservationWarning != nil {
						// 保全完了前の警告ではなく、すでに読み戻せるファイルを通知します。
						assertFileBytes(t, preservedFileForTest(t, store.recorded.Root()), payload)
					}
					notifications = append(notifications, notification)
				},
			}, remote)
			require.NoError(t, managed.Upload(ctx, uploadertest.Request(t, source)))
			require.Equal(t, 1, remote.callCount())
			require.Len(t, notifications, 1)
			notification := notifications[0]
			require.NoError(t, notification.SaveErr)
			require.NoError(t, notification.CheckpointErr, "resolved conflict is a warning, not a checkpoint save failure")
			require.Len(t, notification.SavedTargets, len(test.targets))
			preserved := preservedFileForTest(t, store.recorded.Root())
			requireConflictPreservationWarning(t, notification.PreservationWarning, preserved)
			progress, found, err := store.LoadUploadProgress(ctx, checkpoint.CheckpointKey{Generation: "20260915-1", Compressed: true}, "destination-a")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, progress.SavedTargets, len(test.targets))
			require.Contains(t, progress.SavedTargets, store.recorded)
			// 通常の保存先・元ファイルは変更せず、保全コピーだけを追加します。
			assertFileBytes(t, source, payload)
			assertFileBytes(t, preserved, payload)
			require.NoError(t, managed.Upload(ctx, uploadertest.Request(t, source)))
			require.Equal(t, 1, remote.callCount())
			require.Len(t, notifications, 2)
			require.NoError(t, notifications[1].PreservationWarning, "handled conflict must not warn repeatedly")
			require.Equal(t, preserved, preservedFileForTest(t, store.recorded.Root()))
		})
	}
}

func TestManagedUploaderRetriesFailedPreservationWithoutReupload(t *testing.T) {
	for _, test := range []struct {
		name    string
		kind    failurestore.DurableType
		pending bool
	}{
		{"archive", failurestore.DurableTypeFileArchive, false},
		{"spool", failurestore.DurableTypeSpool, false},
		{"archive with pending checkpoint save", failurestore.DurableTypeFileArchive, true},
		{"spool with pending checkpoint save", failurestore.DurableTypeSpool, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			kind := test.kind
			ctx := context.Background()
			base := t.TempDir()
			source, payload := writeCompressedLog(t, base, "app_20260915_1.log.gz")
			store := &conflictingResultStore{Store: newUploadProgressStore(t, t.TempDir()), kind: kind, pendingConflict: test.pending}
			remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}, errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("temporary outage")}}
			var notification Notification
			managed := newTestManagedUploader(t, ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{kind})),
				SpoolRuntimes:                       newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
				CheckpointStore:                     store,
				FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
				Notifier:                            func(_ context.Context, received Notification) { notification = received },
			}, remote)
			var target failureTarget
			for _, candidate := range managed.failureTargets["destination-a"] {
				if candidate.kind() == kind {
					target = candidate
				}
			}
			require.NotNil(t, target)
			// 通常保存先の初期化後に、空の保全directoryだけをfileへ置換して遮断します。
			if kind == failurestore.DurableTypeFileArchive {
				require.NoError(t, archiveRouteForTest(t, managed, "destination-a").store.EnsureDirectories(ctx))
			}
			blocked := filepath.Join(target.root(), ".preserved")
			blockPreservedDirectoryForTest(t, blocked)
			requireConflictPreservationFailure(t, managed.Upload(ctx, uploadertest.Request(t, source)), source)
			require.Error(t, notification.SaveErr)
			require.Error(t, notification.CheckpointErr)
			require.NoError(t, notification.PreservationWarning)
			assertFileBytes(t, source, payload)
			loaded, err := store.Load(ctx, checkpoint.CheckpointKey{Generation: "20260915-1", Compressed: true})
			require.NoError(t, err)
			require.Empty(t, loaded.CompletedDestinationIDs, "preservation failure cannot advance the completion cursor")
			require.NoError(t, os.Remove(blocked))
			require.NoError(t, managed.Upload(ctx, uploadertest.Request(t, source)))
			require.Equal(t, 1, remote.callCount(), "retry preservation, not object storage upload")
			require.NoError(t, notification.SaveErr)
			require.NoError(t, notification.CheckpointErr)
			requireConflictPreservationWarning(t, notification.PreservationWarning, preservedFileForTest(t, target.root()))
			assertFileBytes(t, source, payload)
		})
	}
}

func TestManagedUploaderDoesNotPreserveNormalSpoolStateChanges(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateProcessing, pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			base := t.TempDir()
			source, _ := writeCompressedLog(t, base, "app_20260915_1.log.gz")
			store := newUploadProgressStore(t, t.TempDir())
			remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}, errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("temporary outage")}}
			var notification Notification
			managed := newTestManagedUploader(t, ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeSpool})),
				SpoolRuntimes:                       newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
				CheckpointStore:                     store,
				FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
				Notifier:                            func(_ context.Context, received Notification) { notification = received },
			}, remote)
			require.NoError(t, managed.Upload(ctx, uploadertest.Request(t, source)))
			root := spoolRouteForTest(t, managed, "destination-a").Root()
			directory, err := root.StateDir(state)
			require.NoError(t, err)
			require.NoError(t, os.Rename(filepath.Join(root.ActiveDir(), filepath.Base(source)), filepath.Join(directory, filepath.Base(source))))
			require.NoError(t, managed.Upload(ctx, uploadertest.Request(t, source)))
			require.Equal(t, 1, remote.callCount())
			require.NoError(t, notification.PreservationWarning)
			preserved, err := os.ReadDir(filepath.Join(root.Path(), ".preserved"))
			require.NoError(t, err)
			require.Empty(t, preserved, "normal spool state transitions must not preserve files")
		})
	}
}

func TestManagedUploaderRestartsUnresolvedPreservationBeforeCompletingHandoff(t *testing.T) {
	for _, kind := range []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			base := t.TempDir()
			source, payload := writeCompressedLog(t, base, "app_20260915_1.log.gz")
			directory := t.TempDir()
			store := &conflictingResultStore{Store: newUploadProgressStore(t, directory), kind: kind}
			remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}, errs: map[objectstorage.DestinationID]error{"destination-a": errors.New("temporary outage")}}
			var notification Notification
			config := ManagedUploaderConfig{
				CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{kind})),
				SpoolRuntimes:                       newTestSpoolRuntimes(t, filepath.Join(base, "spool")),
				CheckpointStore:                     store,
				FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
				Notifier:                            func(_ context.Context, received Notification) { notification = received },
			}
			managed := newTestManagedUploader(t, config, remote)
			var target failureTarget
			for _, candidate := range managed.failureTargets["destination-a"] {
				if candidate.kind() == kind {
					target = candidate
				}
			}
			if kind == failurestore.DurableTypeFileArchive {
				require.NoError(t, archiveRouteForTest(t, managed, "destination-a").store.EnsureDirectories(ctx))
			}
			blocked := filepath.Join(target.root(), ".preserved")
			blockPreservedDirectoryForTest(t, blocked)
			require.Error(t, managed.Upload(ctx, uploadertest.Request(t, source)))
			require.NoError(t, managed.Close())
			require.NoError(t, store.Close())
			// 新しいownerの通常初期化を許可した後、保全を再び遮断します。
			// 起動時のdirectory検査失敗ではなく、永続化済みconflictの再試行を検証します。
			require.NoError(t, os.Remove(blocked))
			require.NoError(t, os.Mkdir(blocked, 0o750))
			config.CheckpointStore = newUploadProgressStore(t, directory)
			nextRemote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}}
			restarted := newTestManagedUploader(t, config, nextRemote)
			blockPreservedDirectoryForTest(t, blocked)
			requireConflictPreservationFailure(t, restarted.Upload(ctx, uploadertest.Request(t, source)), source,
				"restart must not bypass the unresolved preservation using the earlier saved receipt")
			require.Equal(t, 0, nextRemote.callCount())
			require.NoError(t, notification.PreservationWarning)
			assertFileBytes(t, source, payload)
			require.NoError(t, os.Remove(blocked))
			require.NoError(t, restarted.Upload(ctx, uploadertest.Request(t, source)))
			require.Equal(t, 0, nextRemote.callCount())
			requireConflictPreservationWarning(t, notification.PreservationWarning, preservedFileForTest(t, target.root()))
			progress, found, err := config.CheckpointStore.LoadUploadProgress(ctx, checkpoint.CheckpointKey{Generation: "20260915-1", Compressed: true}, "destination-a")
			require.NoError(t, err)
			require.True(t, found)
			require.Empty(t, progress.SavedTargetConflicts)
			assertFileBytes(t, source, payload)
		})
	}
}

func blockPreservedDirectoryForTest(t *testing.T, directory string) {
	t.Helper()
	if err := os.Remove(directory); !errors.Is(err, os.ErrNotExist) {
		require.NoError(t, err)
	}
	require.NoError(t, os.WriteFile(directory, []byte("not a directory"), 0o600))
}

func preservedFileForTest(t *testing.T, root string) string {
	t.Helper()
	directory := filepath.Join(root, ".preserved")
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	return filepath.Join(directory, entries[0].Name())
}

// requireConflictPreservationWarningは、警告がpreservedPathへの元ファイルの保全を通知することを確認します。
// 期待値は保存結果のTarget（比較用の識別子）から求めるため、Windowsでは警告の実際のpathと
// 大小文字が異なり得ます。同じfileを指すかをos.SameFileで照合します。
func requireConflictPreservationWarning(t *testing.T, warning error, preservedPath string) {
	t.Helper()
	require.ErrorIs(t, warning, ErrConflictingSavedFailureResults)
	want, err := os.Stat(preservedPath)
	require.NoError(t, err)
	_, found := errortree.Find(warning, func(got *ConflictPreservationWarning) bool {
		info, err := os.Stat(got.PreservedPath)
		return err == nil && os.SameFile(want, info)
	})
	require.True(t, found, "preservation warning = %v, want preserved file %q", warning, preservedPath)
}

// requireConflictPreservationFailureは、errが保存結果の矛盾に備えたsourceの保全の失敗であることを確認します。
func requireConflictPreservationFailure(t *testing.T, err error, source string, msgAndArgs ...any) {
	t.Helper()
	var preservationErr *ConflictPreservationError
	require.ErrorAs(t, err, &preservationErr, msgAndArgs...)
	require.Equal(t, source, preservationErr.SourcePath, msgAndArgs...)
}
