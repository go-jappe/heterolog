package compressedlogarchive

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestStorePreserveFileReusesContentAndKeepsChangedContentSeparate(t *testing.T) {
	store := newTestStore(t, pkglogger.LoggerContractSync)
	body := []byte("first compressed bytes")
	source := writeTestCompressedLog(t, "app_20261003_1.log.gz", body)
	preserved, err := store.PreserveFile(context.Background(), source)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(store.Root().Path(), ".preserved"), filepath.Dir(preserved))
	assertFileBytes(t, preserved, body)
	assertFileBytes(t, source, body)
	before, err := os.Stat(preserved)
	require.NoError(t, err)

	// A retry with nil context reuses the existing file without replacing it.
	retried, err := store.PreserveFile(nil, source)
	require.NoError(t, err)
	require.Equal(t, preserved, retried)
	after, err := os.Stat(retried)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))

	changedBody := []byte("different compressed bytes")
	require.NoError(t, os.WriteFile(source, changedBody, 0o600))
	changed, err := store.PreserveFile(context.Background(), source)
	require.NoError(t, err)
	require.NotEqual(t, preserved, changed)
	assertFileBytes(t, preserved, body)
	assertFileBytes(t, changed, changedBody)
	assertFileBytes(t, source, changedBody)
	entries, err := os.ReadDir(filepath.Dir(preserved))
	require.NoError(t, err)
	require.Len(t, entries, 2)
	for _, directory := range []string{store.Root().RetryableDir(), store.Root().PermanentDir()} {
		_, err := os.Lstat(directory)
		require.ErrorIs(t, err, os.ErrNotExist, "preservation does not initialize normal archive directories")
	}
}

func TestStorePreserveFileSurvivesNormalCategoryPurge(t *testing.T) {
	for _, category := range []uploader.FailureCategory{uploader.FailureCategoryRetryable, uploader.FailureCategoryPermanent} {
		t.Run(string(category), func(t *testing.T) {
			store := newTestStore(t, pkglogger.LoggerContractSync)
			body := []byte("preserved bytes exceed normal archive capacity")
			source := writeTestCompressedLog(t, "app_20261003_1.log.gz", body)
			archived, err := store.Save(context.Background(), testSaveInput(source, category))
			require.NoError(t, err)
			preserved, err := store.PreserveFile(context.Background(), source)
			require.NoError(t, err)
			assertFileBytes(t, archived.Path, body)
			assertFileBytes(t, preserved, body)
			purger := newTestPurger(t, store, PurgeConfig{
				Retryable: enabledArchivePurgeConfig(365, 1),
				Permanent: enabledArchivePurgeConfig(365, 1),
			})

			report, err := purger.Run(context.Background())
			require.NoError(t, err)
			require.Equal(t, 1, report.PreservedFileCount, "copied preservation is reported once at archive-root scope")
			require.Equal(t, 1, report.ByFailureCategory[category].DeletedFileCount)
			_, err = os.Lstat(archived.Path)
			require.ErrorIs(t, err, os.ErrNotExist)
			assertFileBytes(t, preserved, body)
			assertFileBytes(t, source, body)
			retried, err := store.PreserveFile(context.Background(), source)
			require.NoError(t, err)
			require.Equal(t, preserved, retried)

			// 件数はprocess内の記憶ではなく.preservedの走査から再構築します。
			restarted, err := NewStore(store.Root())
			require.NoError(t, err)
			restartedPurger := newTestPurger(t, restarted, PurgeConfig{
				Retryable: enabledArchivePurgeConfig(365, 1),
				Permanent: enabledArchivePurgeConfig(365, 1),
			})
			restartedReport, err := restartedPurger.Run(context.Background())
			require.NoError(t, err)
			require.Equal(t, 1, restartedReport.PreservedFileCount)
		})
	}
}

func TestPurgerCountsPreservedDirectoryAndKeepsCategoryCapacitySeparate(t *testing.T) {
	store := newTestStore(t, pkglogger.LoggerContractSync)
	body := []byte("ordinary archive bytes")
	source := writeTestCompressedLog(t, "app_20261003_1.log.gz", body)
	archived, err := store.Save(context.Background(), testSaveInput(source, uploader.FailureCategoryRetryable))
	require.NoError(t, err)
	preserved, err := store.PreserveFile(context.Background(), source)
	require.NoError(t, err)
	retainedDirectory := filepath.Join(filepath.Dir(preserved), "retained-directory")
	require.NoError(t, os.Mkdir(retainedDirectory, 0o700))
	nestedPath := filepath.Join(retainedDirectory, "keep.txt")
	require.NoError(t, os.WriteFile(nestedPath, []byte("retained"), 0o600))
	purger := newTestPurger(t, store, PurgeConfig{
		Retryable: enabledArchivePurgeConfig(365, 1<<20),
		Permanent: enabledArchivePurgeConfig(365, 1<<20),
	})

	report, err := purger.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, report.PreservedFileCount)
	retryable := report.ByFailureCategory[uploader.FailureCategoryRetryable]
	require.Equal(t, 1, retryable.ScannedEntryCount)
	require.Equal(t, uint64(len(body)), retryable.ScannedBytes)
	require.Equal(t, uint64(len(body)), retryable.RemainingBytes)
	require.False(t, retryable.CapacityLimitExceeded)
	assertFileBytes(t, archived.Path, body)
	assertFileBytes(t, preserved, body)
	assertFileBytes(t, nestedPath, []byte("retained"))
}

func TestStorePreserveFileCancelsWhileWaitingForOperationLock(t *testing.T) {
	store := newTestStore(t, pkglogger.LoggerContractSync)
	body := []byte("source retained during canceled preservation")
	source := writeTestCompressedLog(t, "app_20261003_1.log.gz", body)
	synctest.Test(t, func(t *testing.T) {
		store.operationMu.Lock()
		defer store.operationMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		preserved, err := store.PreserveFile(ctx, source)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Empty(t, preserved)
	})
	_, err := os.Lstat(store.Root().Path())
	require.ErrorIs(t, err, os.ErrNotExist)
	assertFileBytes(t, source, body)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	preserved, err := store.PreserveFile(ctx, source)
	require.NoError(t, err)
	assertFileBytes(t, preserved, body)
}

func TestStorePreserveFileRejectsNilStoreAndInvalidSourceNames(t *testing.T) {
	var absent *Store
	path, err := absent.PreserveFile(context.Background(), "app.log.gz")
	require.ErrorIs(t, err, ErrInvalidRoot)
	require.Empty(t, path)
	store := newTestStore(t, pkglogger.LoggerContractSync)
	path, err = store.PreserveFile(context.Background(), "")
	require.ErrorIs(t, err, ErrInvalidCompressedLogPath)
	require.Empty(t, path)
	for _, name := range []string{"raw.log", "record.jsonl", "record.jsonl.gz.tmp", ".log.gz", ".jsonl.gz"} {
		source := writeTestCompressedLog(t, name, []byte("not a compressed log filename"))
		path, err := store.PreserveFile(context.Background(), source)
		require.ErrorIs(t, err, ErrUnsupportedCompressedLog)
		require.Empty(t, path)
	}
	_, err = os.Lstat(store.Root().Path())
	require.ErrorIs(t, err, os.ErrNotExist)
}
