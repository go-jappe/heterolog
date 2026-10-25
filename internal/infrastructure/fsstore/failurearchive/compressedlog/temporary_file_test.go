package compressedlogarchive

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestPurgerCleansTemporaryFilesInBothCategoriesWithoutCountingThemAsArchives(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "purge disabled"
		if enabled {
			name = "purge enabled"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t, pkglogger.LoggerContractSync)
			payload := "completed archive"
			var completed, temporary []string
			for _, category := range archiveFailureCategories {
				archived := publishPurgeTestCompressedLog(t, store, string(category)+"_"+time.Now().UTC().Format("20060102")+"_1", category, payload)
				completed = append(completed, archived.Path)
				directory, err := store.Root().FailureCategoryDir(category)
				require.NoError(t, err)
				file, err := os.CreateTemp(directory, temporaryFilePattern)
				require.NoError(t, err)
				temporary = append(temporary, file.Name())
				_, writeErr := file.WriteString(strings.Repeat("x", 4096))
				closeErr := file.Close()
				require.NoError(t, writeErr)
				require.NoError(t, closeErr)
			}
			config := PurgeConfig{}
			if enabled {
				config.Retryable = enabledArchivePurgeConfig(365, 1024)
				config.Permanent = enabledArchivePurgeConfig(365, 1024)
			}
			report, err := newTestPurger(t, store, config).Run(context.Background())
			require.NoError(t, err)
			for _, path := range temporary {
				require.NoFileExists(t, path)
			}
			for _, path := range completed {
				assertFileBytes(t, path, []byte(payload))
			}
			for _, category := range archiveFailureCategories {
				result := report.ByFailureCategory[category]
				require.Equal(t, enabled, result.Enabled)
				require.Zero(t, result.DeletedFileCount)
				if enabled {
					require.Equal(t, 1, result.ScannedEntryCount)
					require.Equal(t, uint64(len(payload)), result.RemainingBytes)
				}
			}
		})
	}
}

func TestTemporaryCleanupPreservesUnrelatedNamesAndCompletedLookingNames(t *testing.T) {
	store := newTestStore(t, pkglogger.LoggerContractSync)
	require.NoError(t, store.EnsureDirectories(context.Background()))
	for _, name := range []string{"operator.tmp", ".compressed-log-.tmp", ".compressed-log-app_20260928_1.log.tmp"} {
		path := filepath.Join(store.Root().RetryableDir(), name)
		require.NoError(t, os.WriteFile(path, []byte("preserve"), archivedFileMode))
	}
	_, err := newTestPurger(t, store, PurgeConfig{}).Run(context.Background())
	require.NoError(t, err)
	for _, name := range []string{"operator.tmp", ".compressed-log-.tmp", ".compressed-log-app_20260928_1.log.tmp"} {
		assertFileBytes(t, filepath.Join(store.Root().RetryableDir(), name), []byte("preserve"))
	}
}

func TestTemporaryCleanupRejectsSymlinks(t *testing.T) {
	store := newTestStore(t, pkglogger.LoggerContractSync)
	require.NoError(t, store.EnsureDirectories(context.Background()))
	target := writeTestCompressedLog(t, "outside.log.gz", []byte("preserve"))
	link := filepath.Join(store.Root().PermanentDir(), ".compressed-log-link.tmp")
	testutil.Symlink(t, target, link)
	_, err := newTestPurger(t, store, PurgeConfig{}).Run(context.Background())
	var notRegular *fsstore.NotRegularFileError
	require.ErrorAs(t, err, &notRegular)
	require.Equal(t, link, notRegular.Path)
	info, err := os.Lstat(link)
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSymlink)
	assertFileBytes(t, target, []byte("preserve"))
}

func TestArchiveTemporaryFileSharesDestinationDirectory(t *testing.T) {
	for _, category := range archiveFailureCategories {
		t.Run(string(category), func(t *testing.T) {
			store := newTestStore(t, pkglogger.LoggerContractSync)
			require.NoError(t, store.EnsureDirectories(context.Background()))
			directory, err := store.Root().FailureCategoryDir(category)
			require.NoError(t, err)
			source := writeTestCompressedLog(t, "app_20260928_1.log.custom", []byte("payload"))
			observed := false
			ctx := &archiveCopyObservedContext{Context: context.Background(), beforeErr: func() {
				entries, err := os.ReadDir(directory)
				require.NoError(t, err)
				for _, entry := range entries {
					if isArchiveTemporaryFileName(entry.Name()) {
						observed = true
					}
				}
			}}
			archived, err := store.Save(ctx, testSaveInput(source, category))
			require.NoError(t, err)
			require.True(t, observed, "copy must use a temporary name in its final directory")
			require.Equal(t, directory, filepath.Dir(archived.Path))
			assertNoArchiveTemporaryFiles(t, store)
			require.NoDirExists(t, filepath.Join(store.Root().Path(), ".staging"))
		})
	}
}
