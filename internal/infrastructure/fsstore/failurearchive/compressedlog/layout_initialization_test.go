package compressedlogarchive

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestStoreEnsureDirectoriesSkipsFilesystemAfterSuccess(t *testing.T) {
	for _, replacedByFile := range []bool{false, true} {
		name := "missing destination directory"
		if replacedByFile {
			name = "destination directory replaced by file"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t, pkglogger.LoggerContractSync)
			require.NoError(t, store.EnsureDirectories(context.Background()))
			directory := store.Root().RetryableDir()
			require.NoError(t, os.Remove(directory)) // Only this empty test directory is removed.
			if replacedByFile {
				require.NoError(t, os.WriteFile(directory, []byte("external replacement"), 0o600))
			}
			for attempt := 0; attempt < 2; attempt++ {
				require.NoError(t, store.EnsureDirectories(context.Background()), "successful initialization must not inspect or repair the directories again")
			}

			payload := []byte("source survives unavailable destination")
			source := writeTestCompressedLog(t, "cached-layout.log.gz", payload)
			input := testSaveInput(source, uploader.FailureCategoryRetryable)
			published, err := store.Save(context.Background(), input)
			require.Error(t, err)
			if !replacedByFile {
				// 削除された保存先を作り直さず、その中への一時file作成で失敗します。
				require.ErrorIs(t, err, os.ErrNotExist)
				var pathErr *os.PathError
				require.ErrorAs(t, err, &pathErr)
				require.Equal(t, directory, filepath.Dir(pathErr.Path))
			}
			require.Empty(t, published.Path)
			purger := newTestPurger(t, store, PurgeConfig{})
			_, err = purger.Run(context.Background())
			var purgeErr *FailureCategoryPurgeError
			require.ErrorAs(t, err, &purgeErr)
			require.Equal(t, uploader.FailureCategoryRetryable, purgeErr.FailureCategory)
			assertFileBytes(t, source, payload)
			if replacedByFile {
				assertFileBytes(t, directory, []byte("external replacement"))
			} else {
				_, err = os.Lstat(directory)
				require.ErrorIs(t, err, os.ErrNotExist)
			}

			// A different Store still validates and creates its directories.
			restarted, err := NewStore(store.Root())
			require.NoError(t, err)
			if replacedByFile {
				require.Error(t, restarted.EnsureDirectories(context.Background()))
				require.NoError(t, os.Remove(directory)) // Remove only the replacement fixture above.
			}
			require.NoError(t, restarted.EnsureDirectories(context.Background()))
			require.DirExists(t, directory)
			published, err = store.Save(context.Background(), input)
			require.NoError(t, err, "the original Store must work after external directory repair")
			require.Equal(t, filepath.Join(store.Root().RetryableDir(), filepath.Base(source)), published.Path)
			assertFileBytes(t, published.Path, payload)
			_, err = purger.Run(context.Background())
			require.NoError(t, err)
		})
	}
}

func TestStoreEnsureDirectoriesRetriesPartialInitialization(t *testing.T) {
	store := newTestStore(t, pkglogger.LoggerContractSync)
	require.NoError(t, os.MkdirAll(store.Root().Path(), directoryMode))
	blocker := store.Root().PermanentDir()
	require.NoError(t, os.WriteFile(blocker, []byte("initialization blocker"), 0o600))
	for attempt := 0; attempt < 2; attempt++ {
		require.Error(t, store.EnsureDirectories(context.Background()))
		require.DirExists(t, store.Root().RetryableDir())
		assertFileBytes(t, blocker, []byte("initialization blocker"))
	}
	require.NoError(t, os.Remove(blocker))
	require.NoError(t, store.EnsureDirectories(context.Background()))
	for _, directory := range []string{store.Root().RetryableDir(), store.Root().PermanentDir()} {
		require.DirExists(t, directory)
	}
}

func TestStoreConcurrentEnsureSaveAndPurgerInitializeDirectories(t *testing.T) {
	store := newTestStore(t, pkglogger.LoggerContractSync)
	payload := []byte("one archived payload")
	source := writeTestCompressedLog(t, "concurrent-layout_"+time.Now().UTC().Format("20060102")+"_1.log.gz", payload)
	input := testSaveInput(source, uploader.FailureCategoryRetryable)
	purger := newTestPurger(t, store, PurgeConfig{
		Retryable: enabledArchivePurgeConfig(365, 1<<20),
		Permanent: enabledArchivePurgeConfig(365, 1<<20),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	operations := []struct {
		name string
		run  func() error
	}{
		{name: "ensure", run: func() error { return store.EnsureDirectories(ctx) }},
		{name: "publish", run: func() error {
			_, err := store.Save(ctx, input)
			return err
		}},
		{name: "purger", run: func() error {
			_, err := purger.Run(ctx)
			return err
		}},
	}
	type result struct {
		operation string
		err       error
	}
	const callersPerOperation = 6
	results := make(chan result, callersPerOperation*len(operations))
	start := make(chan struct{})
	for range callersPerOperation {
		for _, operation := range operations {
			go func() {
				<-start
				results <- result{operation: operation.name, err: operation.run()}
			}()
		}
	}
	close(start)
	for range callersPerOperation * len(operations) {
		select {
		case got := <-results:
			require.NoError(t, got.err, got.operation)
		case <-ctx.Done():
			t.Fatal("concurrent initialization, publication, and purger did not finish")
		}
	}
	entries, err := os.ReadDir(store.Root().RetryableDir())
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, filepath.Base(source), entries[0].Name())
	assertFileBytes(t, filepath.Join(store.Root().RetryableDir(), entries[0].Name()), payload)
	assertDirectoryEmpty(t, store.Root().PermanentDir())
	assertNoArchiveTemporaryFiles(t, store)
}
