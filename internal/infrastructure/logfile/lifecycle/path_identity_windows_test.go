package lifecycle

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/objectstorage"

	"github.com/stretchr/testify/require"
)

func TestObjectStorageUploadManagerWindowsPathAliasesKeepOriginalUploadPath(t *testing.T) {
	path := filepath.Join(resolvedTempDirForTest(t), "App_20260911_1.log.gz")
	alias := strings.ToUpper(path)
	recorder := &objectStorageSingleUploadRecorder{}
	manager := newObjectStorageUploadManager(newTestDestinationUploader(t, recorder), newTestFileCheckpointStore(t))
	ctx := context.Background()
	state, err := manager.prepareRun(ctx)
	require.NoError(t, err)
	generation := fileGeneration{Period: "20260911", Index: 1}
	result, err := manager.uploadIfPending(ctx, path, generation, state, true)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{"primary"}, result.uploadedDestinationIDs)
	result, err = manager.uploadIfPending(ctx, alias, generation, state, true)
	require.NoError(t, err)
	require.Empty(t, result.uploadedDestinationIDs)
	require.Equal(t, []string{path}, recorder.pathsSnapshot())
	require.False(t, manager.uploadPending(generation, true, true))
	manager.forgetUploadedPath(alias)
	require.False(t, manager.uploadPending(generation, true, true), "forgetting a path cache must preserve checkpoint completion")
}

func TestObjectStorageUploadManagerWindowsDestinationCacheMatchesAliases(t *testing.T) {
	path := filepath.Join(resolvedTempDirForTest(t), "App_20260911_1.log.gz")
	alias := strings.ToUpper(path)
	manager := newObjectStorageUploadManager(newDestinationUploaderRecorder("primary"), newTestFileCheckpointStore(t))
	manager.recordUploadedPathDestinations(path, []objectstorage.DestinationID{"primary"})
	require.Contains(t, manager.uploadedPathDestinationIDs(alias), objectstorage.DestinationID("primary"))
	fileManager := &FileSeriesManager{objectStorageUpload: manager}
	fileManager.prunePathCaches([]string{alias})
	require.Contains(t, manager.uploadedPathDestinationIDs(path), objectstorage.DestinationID("primary"))
	manager.forgetUploadedPath(alias)
	require.Empty(t, manager.uploadedPathDestinationIDs(path))
}

func TestFileSeriesManagerWindowsRetainSeesOtherOwnerPathAlias(t *testing.T) {
	path := filepath.Join(resolvedTempDirForTest(t), "App_20260911_1.log.gz")
	guard := appendlog.DirectoryGuardForPath(filepath.Dir(path))
	otherOwner := appendlog.NewDirectoryGuardToken()
	guard.ProtectPaths(otherOwner, strings.ToUpper(path))
	defer guard.UnprotectPaths(otherOwner, strings.ToUpper(path))
	acquired := guard.ProtectPathsIfNoOtherOwnerConflict(appendlog.NewDirectoryGuardToken(), nil, []string{path})
	require.False(t, acquired, "same physical file must remain owned by the other token")
}

func TestDirectoryLifecycleRejectsWindowsCaseAliasedFileSeries(t *testing.T) {
	root := resolvedTempDirForTest(t)
	cfg := testFileSeriesManagerConfig(root, "app", 0, 0, 0, false)
	directory := filepath.Join(root, "app", "logs")
	managers := make([]*FileSeriesManager, 0, 2)
	for _, prefix := range []string{"Err_", "err_"} {
		managers = append(managers, NewFileSeriesManager(FileSeriesManagerOptions{
			Config: cfg,
			FileName: logpattern.FileNamePattern{
				Dir: directory, Prefix: prefix, Target: "app", Extension: "log",
			},
		}))
	}
	_, err := NewDirectoryLifecycle(managers...)
	require.ErrorIs(t, err, ErrDuplicateLifecyclePattern)
}
