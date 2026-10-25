package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestManagerCloseDrainsOrdinaryUploadFailure(t *testing.T) {
	f := newLogFileProcessingRecoveryFixture(t, "primary", "secondary")
	f.remote.failNext("secondary", errors.New("remote unavailable"))
	_, err := f.manager.RunOnce(context.Background())
	require.Error(t, err)
	require.NoError(t, boundedwait.Close(t, "f.manager.Close()", f.manager.Close, boundedwait.ForceCloserOf(f.manager)))
	require.NoFileExists(t, f.path)
	require.Equal(t, []string{f.path}, f.remote.pathsFor("primary"))
	require.Equal(t, []string{f.path, f.path}, f.remote.pathsFor("secondary"))
}

type retryingSingleRunnerUploader struct {
	calls atomic.Int64
}

func (u *retryingSingleRunnerUploader) Upload(context.Context, uploader.UploadRequest) error {
	if u.calls.Add(1) == 1 {
		return errors.New("temporary single uploader failure")
	}
	return nil
}

func TestManagerCloseDrainsSingleDestinationUploaderWithCheckpoint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root, target := resolvedTempDirForTest(t), "single-upload"
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		path := filepath.Join(directory, target+"_"+currentLogDate()+"_1.log.gz")
		writeGzipFile(t, path, []byte("single upload"))
		writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
		remote := &retryingSingleRunnerUploader{}
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, false), Uploader: remote,
		})
		// Closeより前に失敗しても、受理済みの再試行を中止してから閉じ、bubbleにgoroutineを残しません。
		forceCloseManagerOnFailure(t, manager)
		_, err := manager.RunOnce(context.Background())
		require.Error(t, err)
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		require.Equal(t, int64(2), remote.calls.Load())
		require.NoFileExists(t, path)
	})
}

func TestManagerCloseDrainsAcceptedCompressionAndBlockedUploadOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root, target := resolvedTempDirForTest(t), "accepted-compression"
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		period := currentLogDate()
		first := writeFileSeriesCheckpointLog(t, directory, target, period, 1)
		second := writeFileSeriesCheckpointLog(t, directory, target, period, 2)
		latest := writeFileSeriesCheckpointLog(t, directory, target, period, 3)
		remote := newDestinationUploaderRecorder("archive")
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true), Uploader: remote,
			CheckpointStore: newTestFileCheckpointStore(t),
		})
		// Closeより前に失敗しても、受理済みの圧縮・送信の再試行と通知を止めてから閉じ、
		// bubbleにgoroutineを残しません。
		forceCloseManagerOnFailure(t, manager)
		realCompress := manager.compressFile
		var attempts int
		manager.compressFile = func(ctx context.Context, path string) (string, error) {
			if path == first {
				attempts++
				if attempts == 1 {
					return "", errors.New("temporary compression failure")
				}
			}
			return realCompress(ctx, path)
		}
		_, err := manager.RunOnce(context.Background())
		require.Error(t, err)
		require.Empty(t, remote.pathsFor("archive"))
		// 終了をきっかけに新しい走査周期を開始しないことも確認します。
		newLatest := writeFileSeriesCheckpointLog(t, directory, target, period, 4)
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		require.Equal(t, []string{first + ".gz", second + ".gz"}, remote.pathsFor("archive"))
		require.Equal(t, 2, attempts)
		require.NoFileExists(t, first)
		require.NoFileExists(t, second+".gz")
		require.FileExists(t, latest)
		require.FileExists(t, newLatest)
	})
}

func TestManagerCloseDrainsCommittedCompressionSourceCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root, target := resolvedTempDirForTest(t), "accepted-cleanup"
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		source := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
		writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
		manager := NewFileSeriesManager(FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true),
		})
		forceCloseManagerOnFailure(t, manager)
		realCompress := manager.compressFile
		pending := logpattern.CompressionSourceDeletePendingPath(source)
		manager.compressFile = func(ctx context.Context, path string) (string, error) {
			compressed, err := realCompress(ctx, path)
			if err != nil {
				return compressed, err
			}
			// 圧縮のcommit後に削除待ちだけが残る状態を再現します。
			if err := os.WriteFile(pending, []byte("retired source"), 0o600); err != nil {
				return compressed, err
			}
			return compressed, appendlog.ErrCompressionSourceCleanup
		}
		_, err := manager.RunOnce(context.Background())
		require.ErrorIs(t, err, appendlog.ErrCompressionSourceCleanup)
		require.FileExists(t, pending)
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		require.NoFileExists(t, pending)
		require.FileExists(t, source+".gz")
	})
}

type forceAwareRunnerUploader struct {
	entered chan struct{}
	calls   atomic.Int64
}

func (u *forceAwareRunnerUploader) Upload(ctx context.Context, _ uploader.UploadRequest) error {
	if u.calls.Add(1) == 1 {
		close(u.entered)
	}
	<-ctx.Done()
	return ctx.Err()
}
