package lifecycle

import (
	"context"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// newTestFileCheckpointStoreは、試験終了時に閉じるcheckpoint Storeを作ります。成功した試験では
// 終了処理がerrorなく終わることを確認します。共有fixtureのため、bubble外の実時間でもbubble内の
// 仮想時間でもboundedwait.HangTimeoutで待ち、期限を過ぎたらForceCloseを通知して失敗させます。
func newTestFileCheckpointStore(t *testing.T) *checkpointstore.FilesystemStore {
	t.Helper()
	store, err := checkpointstore.NewFilesystemStore(resolvedTempDirForTest(t), "file-series")
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, store.ForceClose, boundedwait.NoError)
	return store
}

func TestObjectStorageUploadManagerSharesCompletionBetweenRawAndCompressedFiles(t *testing.T) {
	ctx := context.Background()
	const id = objectstorage.DestinationID("primary")
	store := newTestFileCheckpointStore(t)
	remote := newDestinationUploaderRecorder(id)
	manager := newObjectStorageUploadManager(remote, store)
	state, err := manager.prepareRun(ctx)
	require.NoError(t, err)
	generation := fileGeneration{Period: "20260915", Index: 1}
	raw := filepath.Join(resolvedTempDirForTest(t), "app_20260915_1.log")
	result, err := manager.uploadIfPending(ctx, raw, generation, state, false)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptComplete, result.state)
	require.Empty(t, result.localCompressedPathToRemove)
	compressedBefore, err := manager.loadFileCheckpoint(ctx, raw+".gz", generation, true)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{id}, compressedBefore.CompletedDestinationIDs)
	// 次の走査で圧縮ファイルだけが残っていても、元ログの削除やcacheの整理で
	// 永続化した世代の完了記録は失われません。
	manager.pruneUploadedPaths(map[string]struct{}{pathidentity.ComparisonKey(raw + ".gz"): {}})
	state, err = manager.prepareRun(ctx)
	require.NoError(t, err)
	_, err = manager.loadFileCheckpoint(ctx, raw+".gz", generation, true)
	require.NoError(t, err)
	require.False(t, manager.uploadPending(generation, true, true))
	result, err = manager.uploadIfPending(ctx, raw+".gz", generation, state, true)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptComplete, result.state)
	require.Empty(t, result.uploadedDestinationIDs)
	require.Equal(t, raw+".gz", result.localCompressedPathToRemove)
	for _, compressed := range []bool{false, true} {
		record, err := manager.loadFileCheckpoint(ctx, raw, generation, compressed)
		require.NoError(t, err)
		require.True(t, record.Complete([]objectstorage.DestinationID{id}))
	}
	require.Equal(t, []string{raw}, remote.pathsFor(id))
	require.Empty(t, manager.uploadedPathDestinations)
}

func TestObjectStorageUploadManagerWithoutUploaderLeavesSuccessRecordsUntouched(t *testing.T) {
	ctx := context.Background()
	store := newTestFileCheckpointStore(t)
	key := checkpointstore.CheckpointKey{Generation: "20260915-1", Compressed: true}
	_, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	manager := newObjectStorageUploadManager(nil, store)
	state, err := manager.prepareRun(ctx)
	require.NoError(t, err)
	require.Empty(t, state.destinations)
	result, err := manager.uploadIfPending(ctx, "app_20260915_1.log.gz", fileGeneration{Period: "20260915", Index: 1}, state, true)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptInactive, result.state)
	require.Zero(t, result.state)
	require.Empty(t, result.uploadedDestinationIDs)
	require.Empty(t, result.localCompressedPathToRemove)
	record, err := store.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{"primary"}, record.CompletedDestinationIDs)
	require.False(t, manager.uploadPending(fileGeneration{Period: "20260915", Index: 1}, true, true))
}

func TestObjectStorageUploadManagerRejectsRawPrefixGenerationBeforeUpload(t *testing.T) {
	ctx := context.Background()
	const destination = objectstorage.DestinationID("primary")
	store := newTestFileCheckpointStore(t)
	remote := newDestinationUploaderRecorder(destination)
	manager := newObjectStorageUploadManager(remote, store)
	state, err := manager.prepareRun(ctx)
	require.NoError(t, err)
	directory := resolvedTempDirForTest(t)
	raw := filepath.Join(directory, "app_20260915_1.log")
	compressed := filepath.Join(directory, "prefixed-generation.log.gz")
	generation := fileGeneration{Period: "20260915", Index: 1}
	prefixedGeneration := fileGeneration{Period: "raw:20260915", Index: 1}

	// 未圧縮の完了記録があっても、不正な世代キーを別の有効なキーとして扱いません。
	result, err := manager.uploadIfPending(ctx, raw, generation, state, false)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptComplete, result.state)
	require.True(t, manager.uploadPending(prefixedGeneration, true, true))
	result, err = manager.uploadIfPending(ctx, compressed, prefixedGeneration, state, true)
	require.ErrorIs(t, err, checkpointstore.ErrInvalidGroupKey)
	require.Empty(t, result.uploadedDestinationIDs)
	require.Equal(t, uploadAttemptResumeUpload, result.state)
	require.Equal(t, []string{raw}, remote.pathsFor(destination))
}

func TestObjectStorageUploadManagerRequiresStoreAndDestinationUploader(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		uploader uploader.Uploader
		store    checkpointstore.Store
	}{
		{name: "single uploader without store", uploader: &objectStorageSingleUploadRecorder{}},
		{name: "destination uploader without store", uploader: newDestinationUploaderRecorder("primary")},
		{name: "single uploader with store", uploader: &objectStorageSingleUploadRecorder{}, store: newTestFileCheckpointStore(t)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager := newObjectStorageUploadManager(testCase.uploader, testCase.store)
			require.ErrorIs(t, manager.validate(), ErrInvalidCheckpointConfiguration)
			state, err := manager.prepareRun(context.Background())
			require.ErrorIs(t, err, ErrInvalidCheckpointConfiguration)
			require.Nil(t, state)
		})
	}
}

func TestObjectStorageUploadManagerWithoutUploaderAllowsOptionalStore(t *testing.T) {
	for _, store := range []checkpointstore.Store{nil, newTestFileCheckpointStore(t)} {
		manager := newObjectStorageUploadManager(nil, store)
		require.NoError(t, manager.validate())
		_, err := manager.prepareRun(context.Background())
		require.NoError(t, err)
	}
}

func TestFileSeriesManagerRejectsInvalidUploadBeforeProcessingFiles(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		destinationAware bool
		withStore        bool
	}{
		{name: "single uploader without store"},
		{name: "destination uploader without store", destinationAware: true},
		{name: "single uploader with store", withStore: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root, target := resolvedTempDirForTest(t), "invalid-upload"
			directory := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(directory, 0o750))
			source := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
			latest := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
			recorder := &objectStorageSingleUploadRecorder{}
			opts := FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
				Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, true), Uploader: recorder,
			}
			if testCase.destinationAware {
				opts.Uploader = newTestDestinationUploader(t, recorder)
			}
			if testCase.withStore {
				opts.CheckpointStore = newTestFileCheckpointStore(t)
			}
			manager := NewFileSeriesManager(opts)
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)
			report, err := manager.RunOnce(context.Background())
			require.ErrorIs(t, err, ErrInvalidCheckpointConfiguration)
			require.Empty(t, report.Compressed)
			require.Empty(t, report.ObjectStorageUploads)
			require.Empty(t, report.UploadedFileRemovals)
			require.Empty(t, recorder.pathsSnapshot())
			require.FileExists(t, source)
			require.FileExists(t, latest)
			require.NoFileExists(t, source+".gz")
		})
	}
}

func TestObjectStorageUploadManagerRequiresPreparedDestinationState(t *testing.T) {
	recorder := &objectStorageSingleUploadRecorder{}
	manager := newObjectStorageUploadManager(newTestDestinationUploader(t, recorder), newTestFileCheckpointStore(t))
	result, err := manager.uploadIfPending(context.Background(), filepath.Join(resolvedTempDirForTest(t), "app_20260829_1.log.gz"), fileGeneration{Period: "20260829", Index: 1}, nil, true)
	require.ErrorIs(t, err, ErrInvalidCheckpointConfiguration)
	require.Empty(t, result.uploadedDestinationIDs)
	require.Empty(t, result.localCompressedPathToRemove)
	require.Empty(t, recorder.pathsSnapshot())
}

func TestObjectStorageUploadManagerWithCheckpointUploadsNormalizedPathOnce(t *testing.T) {
	uploadRecorder := &objectStorageSingleUploadRecorder{}
	manager := newObjectStorageUploadManager(newTestDestinationUploader(t, uploadRecorder), newTestFileCheckpointStore(t))
	ctx := context.Background()
	state, err := manager.prepareRun(ctx)
	require.NoError(t, err)
	generation := fileGeneration{Period: "20260829", Index: 1}
	path := filepath.Join(resolvedTempDirForTest(t), "logs", "app_20260829_1.log.gz")
	pathAlias := filepath.Dir(path) + string(os.PathSeparator) + "." + string(os.PathSeparator) + filepath.Base(path)

	require.True(t, manager.uploadPending(generation, true, true))
	first, err := manager.uploadIfPending(
		ctx, path, generation, state, true,
	)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptComplete, first.state)
	require.Equal(t, []objectstorage.DestinationID{"primary"}, first.uploadedDestinationIDs)

	second, err := manager.uploadIfPending(
		ctx, pathAlias, generation, state, true,
	)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptComplete, second.state)
	require.Empty(t, second.uploadedDestinationIDs)
	require.False(t, manager.uploadPending(generation, true, true))
	require.Equal(t, []string{filepath.Clean(path)}, uploadRecorder.pathsSnapshot())
}

func TestObjectStorageUploadManagerPrunesUploadedPathsMissingFromScan(t *testing.T) {
	manager := newObjectStorageUploadManager(newDestinationUploaderRecorder("primary"), newTestFileCheckpointStore(t))
	livePath := pathidentity.ComparisonKey("/logs/app_20260829_1.log")
	stalePath := pathidentity.ComparisonKey("/logs/app_20260829_2.log")
	manager.uploadedPathDestinations[livePath] = map[objectstorage.DestinationID]struct{}{
		"primary": {},
	}
	manager.uploadedPathDestinations[stalePath] = map[objectstorage.DestinationID]struct{}{
		"primary": {},
	}

	manager.pruneUploadedPaths(map[string]struct{}{livePath: {}})

	require.Equal(t, map[string]map[objectstorage.DestinationID]struct{}{
		livePath: {"primary": {}},
	}, manager.uploadedPathDestinations)
}

type objectStorageSingleUploadRecorder struct {
	mu    sync.Mutex
	paths []string
}

func (r *objectStorageSingleUploadRecorder) Upload(
	_ context.Context,
	input uploader.UploadRequest,
) error {
	r.mu.Lock()
	r.paths = append(r.paths, input.FilePath())
	r.mu.Unlock()
	return nil
}

func (r *objectStorageSingleUploadRecorder) pathsSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.paths...)
}
