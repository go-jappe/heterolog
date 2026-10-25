package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestManagerCloseDrainsAcceptedCompressionAfterInitialPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root, target := resolvedTempDirForTest(t), "compression-panic"
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		period := currentLogDate()
		source := writeFileSeriesCheckpointLog(t, directory, target, period, 1)
		latest := writeFileSeriesCheckpointLog(t, directory, target, period, 2)
		remote := &recordingUploader{}
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true), Uploader: remote,
		})
		t.Cleanup(func() {
			_ = manager.ForceClose()
			_ = boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose)
		})
		compress := manager.compressFile
		attempts := 0
		manager.compressFile = func(ctx context.Context, path string) (string, error) {
			attempts++
			if attempts == 1 {
				panic("initial compression panic")
			}
			return compress(ctx, path)
		}
		require.PanicsWithValue(t, "initial compression panic", func() { _, _ = manager.RunOnce(context.Background()) })
		// panic後に増えたfileや、今回未選択の最新世代を終了処理で新たに受理しません。
		newLatest := writeFileSeriesCheckpointLog(t, directory, target, period, 3)
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		require.Equal(t, 2, attempts)
		require.Equal(t, []string{source + ".gz"}, remote.pathsSnapshot())
		require.NoFileExists(t, source)
		require.NoFileExists(t, source+".gz")
		require.FileExists(t, latest)
		require.FileExists(t, newLatest)
	})
}

type panicOnceRunnerUploader struct {
	calls   atomic.Int64
	onFirst func()
}

func (u *panicOnceRunnerUploader) Upload(context.Context, uploader.UploadRequest) error {
	if u.calls.Add(1) == 1 {
		if u.onFirst != nil {
			u.onFirst()
		}
		panic("initial uploader panic")
	}
	return nil
}

func (*panicOnceRunnerUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"primary"}
}

func (u *panicOnceRunnerUploader) UploadDestinations(ctx context.Context, request uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
	report := uploader.UploadReport{}
	for _, destination := range destinations {
		err := u.Upload(ctx, request)
		report.Results = append(report.Results, uploader.DestinationResult{DestinationID: destination, Err: err})
	}
	return report
}

func TestManagerCloseDrainsAcceptedSingleUploadAfterInitialPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root, target := resolvedTempDirForTest(t), "upload-panic"
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		path := filepath.Join(directory, target+"_"+currentLogDate()+"_1.log.gz")
		writeGzipFile(t, path, []byte("accepted upload"))
		writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
		remote := &panicOnceRunnerUploader{}
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, false), Uploader: remote,
		})
		t.Cleanup(func() {
			_ = manager.ForceClose()
			_ = boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose)
		})
		require.PanicsWithValue(t, "initial uploader panic", func() { _, _ = manager.RunOnce(context.Background()) })
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		require.EqualValues(t, 2, remote.calls.Load())
		require.NoFileExists(t, path)
	})
}

type panicOnceRunnerCheckpointStore struct {
	checkpointstore.Store
	panicked atomic.Bool
}

// ForceCloseは、包んだ実Storeへ所有者のForceCloseを届けます（forceCloseWrappedCheckpointStore）。
func (s *panicOnceRunnerCheckpointStore) ForceClose() error {
	return forceCloseWrappedCheckpointStore(s.Store)
}

func (s *panicOnceRunnerCheckpointStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, destinations []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	if s.panicked.CompareAndSwap(false, true) {
		panic("initial checkpoint panic")
	}
	return s.Store.AdvanceCompletionCursor(ctx, key, destinations)
}

func TestManagerClosePreservesSuccessfulUploadAfterInitialCheckpointPanic(t *testing.T) {
	f := newLogFileProcessingRecoveryFixture(t, "archive")
	f.manager.objectStorageUpload.checkpointStore = &panicOnceRunnerCheckpointStore{Store: f.store}
	require.PanicsWithValue(t, "initial checkpoint panic", func() { _, _ = f.manager.RunOnce(context.Background()) })
	require.NoError(t, boundedwait.Close(t, "f.manager.Close()", f.manager.Close, boundedwait.ForceCloserOf(f.manager)))
	require.Equal(t, []string{f.path}, f.remote.pathsFor("archive"))
	require.NoFileExists(t, f.path)
}
