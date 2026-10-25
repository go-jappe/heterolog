package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func cacheVerifiedPeerForRemovalTest(t *testing.T, fixture logFileProcessingRecoveryFixture) {
	t.Helper()
	valid, err := fixture.manager.existingCompressedFileIsValid(fixture.raw, fixture.path)
	require.NoError(t, err)
	require.True(t, valid)
	require.True(t, hasVerifiedPeerForRemovalTest(fixture.manager, fixture.path))
}

func hasVerifiedPeerForRemovalTest(manager *FileSeriesManager, path string) bool {
	manager.compressedPeerCacheMu.Lock()
	defer manager.compressedPeerCacheMu.Unlock()
	_, exists := manager.verifiedCompressedPeers[pathidentity.ComparisonKey(path)]
	return exists
}

func TestUploadedRemovalForgetsVerifiedPeer(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "normal upload"
		if recovery {
			name = "log file processing recovery"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const destination = objectstorage.DestinationID("archive")
			fixture := newLogFileProcessingRecoveryFixture(t, destination)
			cacheVerifiedPeerForRemovalTest(t, fixture)
			fixture.store.failLoad.Store(recovery)
			_, err := fixture.lifecycle.RunOnce(context.Background())
			if recovery {
				require.ErrorIs(t, err, errRecoveryCheckpoint)
				fixture.store.failLoad.Store(false)
			} else {
				require.NoError(t, err)
			}
			if recovery {
				waitLogFileProcessingRecovery(t, fixture.manager, fixture.cursor)
			}
			require.NoFileExists(t, fixture.path)
			require.False(t, hasVerifiedPeerForRemovalTest(fixture.manager, fixture.path))
			require.Equal(t, []string{fixture.path}, fixture.remote.pathsFor(destination))
		})
	}
}

func TestRemoveCompressedFileAndSyncDirectoryInvalidatesOnlyAbsentPeer(t *testing.T) {
	for _, outcome := range []string{"removed", "already absent", "directory sync failed", "removal denied"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			fixture := newLogFileProcessingRecoveryFixture(t, "archive")
			cacheVerifiedPeerForRemovalTest(t, fixture)
			switch outcome {
			case "already absent":
				require.NoError(t, os.Remove(fixture.path))
			case "directory sync failed":
				require.NoError(t, removeRunnerFixtureDirectory(fixture))
			case "removal denied":
				restore := testutil.DenyFileRemoval(t, fixture.path)
				defer restore()
			}

			result, directorySyncErr := fixture.manager.removeCompressedFileAndSyncDirectory(fixture.path)
			switch outcome {
			case "directory sync failed":
				require.NoError(t, result.Err, "directory sync failure is not a deletion failure")
				require.ErrorIs(t, directorySyncErr, pkgfsstore.ErrDurabilityUnconfirmed)
			case "removal denied":
				require.ErrorIs(t, result.Err, os.ErrPermission)
				require.NoError(t, directorySyncErr, "sync must not run after deletion fails")
				require.FileExists(t, fixture.path)
				require.True(t, hasVerifiedPeerForRemovalTest(fixture.manager, fixture.path),
					"the unchanged valid compressed file must keep its successful verification")
				return
			default:
				require.NoError(t, result.Err)
				require.NoError(t, directorySyncErr)
			}
			require.NoFileExists(t, fixture.path)
			require.False(t, hasVerifiedPeerForRemovalTest(fixture.manager, fixture.path),
				"absence must discard the cache without another file scan")
		})
	}
}

func TestRemovedPeerCacheUsesRemovalResult(t *testing.T) {
	for _, scenario := range []struct {
		name string
		err  error
	}{
		{name: "removed or already absent"},
		{name: "removal failed", err: os.ErrPermission},
		{name: "inspection failed", err: errors.New("stat failed")},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(resolvedTempDirForTest(t), "app_20260925_1.log.gz")
			key := pathidentity.ComparisonKey(path)
			manager := &FileSeriesManager{
				verifiedCompressedPeers: map[string]compressedPeerFingerprint{key: {size: 1}},
			}
			result := fsstore.FileRemovalResult{
				File: fsstore.FileInfo{Path: path}, Err: scenario.err,
			}
			manager.forgetRemovedCompressedPeers(result)
			require.Equal(t, scenario.err != nil, hasVerifiedPeerForRemovalTest(manager, path))
			require.Equal(t, scenario.err, result.Err, "discarding cache does not suppress the error report")
		})
	}
}
