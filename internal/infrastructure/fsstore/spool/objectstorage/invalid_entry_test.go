package objectstoragespool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestStoreStateScanPreservesInvalidNamesAndListsValidFiles(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateActive, pkgspool.StateProcessing, pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		t.Run(string(state), func(t *testing.T) {
			stores, store := newTestDestinationStores(t)
			ctx := context.Background()
			require.NoError(t, store.EnsureLayout(ctx))
			directory, err := store.Root().StateDir(state)
			require.NoError(t, err)
			validPath := filepath.Join(directory, "app_20261004_1.log.gz")
			invalidPath := filepath.Join(directory, "manifest.json")
			require.NoError(t, os.WriteFile(validPath, []byte("valid"), payloadMode))
			require.NoError(t, os.WriteFile(invalidPath, []byte("invalid-named bytes"), payloadMode))

			files, err := store.ListFiles(ctx, state)
			require.NoError(t, err)
			require.Len(t, files, 1)
			require.Equal(t, validPath, files[0].Path)
			require.NoFileExists(t, invalidPath)
			entries, err := os.ReadDir(store.root.preservedDir())
			require.NoError(t, err)
			require.Len(t, entries, 1)
			preservedPath := filepath.Join(store.root.preservedDir(), entries[0].Name())
			got, err := os.ReadFile(preservedPath)
			require.NoError(t, err)
			require.Equal(t, "invalid-named bytes", string(got))
			warnings, err := stores.TakePreservationWarnings(ctx)
			require.NoError(t, err)
			require.Len(t, warnings, 1)
			require.ErrorIs(t, warnings[0], ErrInvalidCompressedLogFileName)
			var preservation *InvalidEntryPreservationWarning
			require.ErrorAs(t, warnings[0], &preservation)
			require.Equal(t, invalidPath, preservation.SourcePath)
			require.Equal(t, preservedPath, preservation.PreservedPath)
			files, err = store.ListFiles(ctx, state)
			require.NoError(t, err)
			require.Len(t, files, 1)
			warnings, err = stores.TakePreservationWarnings(ctx)
			require.NoError(t, err)
			require.Empty(t, warnings)
		})
	}
}

func TestStoreClaimsAndRecoversValidFilesAfterInvalidNamePreservation(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	valid, err := store.SaveRetryableFailure(ctx, writeTestSource(t, t.TempDir(), "app_20261004_1.log.gz", "valid"))
	require.NoError(t, err)
	invalidPath := filepath.Join(store.Root().ActiveDir(), "uncompressed.log")
	require.NoError(t, os.WriteFile(invalidPath, []byte("invalid active"), payloadMode))
	claimed, found, err := store.ClaimOldestActive(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, valid.FileName, claimed.FileName)
	require.Equal(t, pkgspool.StateProcessing, claimed.State)
	require.NoFileExists(t, invalidPath)
	invalidPath = filepath.Join(store.Root().ProcessingDir(), "app.log.gz.tmp")
	require.NoError(t, os.WriteFile(invalidPath, []byte("invalid processing"), payloadMode))
	recovered, err := store.RecoverProcessing(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, recovered)
	require.FileExists(t, valid.Path)
	require.NoFileExists(t, invalidPath)
	warnings, err := store.takePreservationWarnings(ctx)
	require.NoError(t, err)
	require.Len(t, warnings, 2)
}

func TestTerminalPurgePreservesInvalidNameAndRemovesValidExpiredFile(t *testing.T) {
	ctx := context.Background()
	stores, store := newTestDestinationStores(t)
	valid := saveTerminalPurgeTestFile(t, store, "app_20260901_1.log.gz", "valid", pkgspool.StateDone)
	old := time.Now().AddDate(0, 0, -10)
	require.NoError(t, os.Chtimes(valid.Path, old, old))
	invalidPath := filepath.Join(store.Root().DoneDir(), "unknown.log.gz.tmp")
	require.NoError(t, os.WriteFile(invalidPath, []byte("preserve this"), payloadMode))
	purger := newTestTerminalPurger(t, stores, enabledPurgeConfig(1, 1<<20))
	report, err := purger.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, report.ByState[pkgspool.StateDone].DeletedFileCount)
	require.NoFileExists(t, valid.Path)
	require.NoFileExists(t, invalidPath)
	entries, err := os.ReadDir(store.root.preservedDir())
	require.NoError(t, err)
	require.Len(t, entries, 1)
	got, err := os.ReadFile(filepath.Join(store.root.preservedDir(), entries[0].Name()))
	require.NoError(t, err)
	require.Equal(t, "preserve this", string(got))
}

func TestInvalidNamePreservationFailureRetainsSourceAndStopsClaim(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	valid, err := store.SaveRetryableFailure(ctx, writeTestSource(t, t.TempDir(), "app_20261004_1.log.gz", "valid"))
	require.NoError(t, err)
	invalidPath := filepath.Join(store.Root().ActiveDir(), "invalid.txt")
	require.NoError(t, os.WriteFile(invalidPath, []byte("retain"), payloadMode))
	// 保存先が使えない場合を権限設定に依存せず再現します。
	require.NoError(t, os.Remove(store.root.preservedDir()))
	require.NoError(t, os.WriteFile(store.root.preservedDir(), []byte("blocked"), payloadMode))
	_, found, err := store.ClaimOldestActive(ctx)
	require.Error(t, err)
	require.False(t, found)
	require.FileExists(t, valid.Path)
	got, err := os.ReadFile(invalidPath)
	require.NoError(t, err)
	require.Equal(t, "retain", string(got))
	warnings, err := store.takePreservationWarnings(ctx)
	require.NoError(t, err)
	require.Empty(t, warnings)
	// 保存先が復旧すれば、次回は保全して正常ファイルのclaimを再開します。
	require.NoError(t, os.Remove(store.root.preservedDir()))
	require.NoError(t, os.Mkdir(store.root.preservedDir(), directoryMode))
	_, found, err = store.ClaimOldestActive(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.NoFileExists(t, invalidPath)
}

func TestConcurrentStateScansPreserveInvalidFileOnce(t *testing.T) {
	ctx := context.Background()
	stores, store := newTestDestinationStores(t)
	require.NoError(t, store.EnsureLayout(ctx))
	invalidPath := filepath.Join(store.Root().ActiveDir(), "invalid.txt")
	require.NoError(t, os.WriteFile(invalidPath, []byte("preserve once"), payloadMode))
	var scans sync.WaitGroup
	for range 8 {
		scans.Add(1)
		go func() {
			defer scans.Done()
			files, err := store.ListFiles(ctx, pkgspool.StateActive)
			if err != nil || len(files) != 0 {
				t.Errorf("ListFiles() = %v, %v", files, err)
			}
		}()
	}
	scans.Wait()
	warnings, err := stores.TakePreservationWarnings(ctx)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	entries, err := os.ReadDir(store.root.preservedDir())
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestStateScanCancelsWhileWaitingForOperationLock(t *testing.T) {
	store := newTestStore(t)
	synctest.Test(t, func(t *testing.T) {
		store.operationMu.Lock()
		defer store.operationMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := store.ListFiles(ctx, pkgspool.StateActive)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ListFiles() = %v", err)
		}
	})
	_, err := os.Lstat(store.Root().ActiveDir())
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestInvalidSaveInputDoesNotMoveProducerFile(t *testing.T) {
	store := newTestStore(t)
	source := writeTestSource(t, t.TempDir(), "not-compressed.log", "source")
	_, err := store.SaveRetryableFailure(context.Background(), source)
	require.ErrorIs(t, err, ErrInvalidCompressedLogFileName)
	got, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, "source", string(got))
}

func TestStateScanPreservesNonRegularEntriesInEveryState(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateActive, pkgspool.StateProcessing, pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		t.Run(string(state), func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			require.NoError(t, store.EnsureLayout(ctx))
			directory, err := store.Root().StateDir(state)
			require.NoError(t, err)
			bundle := filepath.Join(directory, "legacy-bundle")
			require.NoError(t, os.Mkdir(bundle, directoryMode))
			require.NoError(t, os.WriteFile(filepath.Join(bundle, "keep.txt"), []byte("keep nested data"), payloadMode))
			link := filepath.Join(directory, "app_20261004_1.log.gz")
			missingTarget := filepath.Join(t.TempDir(), "missing")
			testutil.Symlink(t, missingTarget, link)

			files, err := store.ListFiles(ctx, state)
			require.NoError(t, err)
			require.Empty(t, files)
			_, err = os.Lstat(bundle)
			require.ErrorIs(t, err, os.ErrNotExist)
			_, err = os.Lstat(link)
			require.ErrorIs(t, err, os.ErrNotExist)
			entries, err := os.ReadDir(store.root.preservedDir())
			require.NoError(t, err)
			require.Len(t, entries, 2)
			for _, entry := range entries {
				path := filepath.Join(store.root.preservedDir(), entry.Name())
				info, err := os.Lstat(path)
				require.NoError(t, err)
				if info.IsDir() {
					body, err := os.ReadFile(filepath.Join(path, "keep.txt"))
					require.NoError(t, err)
					require.Equal(t, "keep nested data", string(body))
				} else {
					require.NotZero(t, info.Mode()&os.ModeSymlink)
					target, err := os.Readlink(path)
					require.NoError(t, err)
					require.Equal(t, missingTarget, target)
				}
			}
			_, err = os.Lstat(missingTarget)
			require.ErrorIs(t, err, os.ErrNotExist)
			warnings, err := store.takePreservationWarnings(ctx)
			require.NoError(t, err)
			require.Len(t, warnings, 2)
			for _, warning := range warnings {
				require.ErrorIs(t, warning, ErrInvalidCompressedLogPath)
			}
		})
	}
}

func TestPreservationRejectsPathsOutsideManagedState(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	require.NoError(t, store.EnsureLayout(ctx))
	outside := filepath.Join(store.Root().Path(), "outside.log.gz")
	require.NoError(t, os.WriteFile(outside, []byte("keep outside"), payloadMode))
	store.operationMu.Lock()
	_, err := store.preserveStateEntryLocked(ctx, pkgspool.StateActive, "../outside.log.gz")
	store.operationMu.Unlock()
	require.ErrorIs(t, err, ErrInvalidCompressedLogPath)
	body, err := os.ReadFile(outside)
	require.NoError(t, err)
	require.Equal(t, "keep outside", string(body))
	entries, err := os.ReadDir(store.root.preservedDir())
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestSaveAndReusePreserveNonRegularExistingEntry(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		name := "save"
		if reuse {
			name = "reuse"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			require.NoError(t, store.EnsureLayout(ctx))
			source := writeTestSource(t, t.TempDir(), "app_20261004_1.log.gz", "new payload")
			// doneのpurgeが無効でも、保存・再利用の確認時点で保全できます。
			invalidPath := filepath.Join(store.Root().DoneDir(), filepath.Base(source))
			target := writeTestSource(t, t.TempDir(), "outside.log.gz", "keep outside")
			testutil.Symlink(t, target, invalidPath)
			var file CompressedLogFile
			var err error
			if reuse {
				var found bool
				file, found, err = store.ConfirmSavedFile(ctx, source)
				require.NoError(t, err)
				require.False(t, found)
			} else {
				file, err = store.SaveRetryableFailure(ctx, source)
				require.NoError(t, err)
				require.Equal(t, pkgspool.StateActive, file.State)
				body, err := os.ReadFile(file.Path)
				require.NoError(t, err)
				require.Equal(t, "new payload", string(body))
			}
			require.Len(t, file.ConflictRenames, 1)
			require.Equal(t, invalidPath, file.ConflictRenames[0].OriginalPath)
			retainedTarget, err := os.Readlink(file.ConflictRenames[0].RetainedPath)
			require.NoError(t, err)
			require.Equal(t, target, retainedTarget)
			body, err := os.ReadFile(target)
			require.NoError(t, err)
			require.Equal(t, "keep outside", string(body))
			body, err = os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, "new payload", string(body))
			warnings, err := store.takePreservationWarnings(ctx)
			require.NoError(t, err)
			require.Len(t, warnings, 1)
			require.ErrorIs(t, warnings[0], ErrInvalidCompressedLogPath)
		})
	}
}

func TestMissingPreservationDirectoryReturnsErrorAndRetainsEntry(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		name := "list"
		if reuse {
			name = "reuse"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			require.NoError(t, store.EnsureLayout(ctx))
			source := writeTestSource(t, t.TempDir(), "app_20261004_1.log.gz", "source")
			entry := filepath.Join(store.Root().ActiveDir(), filepath.Base(source))
			require.NoError(t, os.Mkdir(entry, directoryMode))
			require.NoError(t, os.Remove(store.root.preservedDir()))
			if reuse {
				_, found, err := store.ConfirmSavedFile(ctx, source)
				require.Error(t, err)
				require.False(t, found)
			} else {
				_, err := store.ListFiles(ctx, pkgspool.StateActive)
				require.Error(t, err)
			}
			require.DirExists(t, entry)
			warnings, err := store.takePreservationWarnings(ctx)
			require.NoError(t, err)
			require.Empty(t, warnings)
		})
	}
}
