package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

func TestGenerationContinuationWaitsForEarlierCompressionFailureAcrossRestart(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const target = "continue-after-compression-failure"
		const firstDestination = objectstorage.DestinationID("first")
		const secondDestination = objectstorage.DestinationID("second")
		logDirectory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDirectory, 0o750))
		checkpointDirectory := newCheckpointDirectoryForTest(t, root)
		remote := newDestinationUploaderRecorder(firstDestination, secondDestination)
		manager, compressedStore := newGenerationContinuationManager(t, root, target, checkpointDirectory, remote)

		// 圧縮自体は後続へ進めても、送信位置が先行する失敗世代を飛び越えません。
		period := currentLogDate()
		failedRaw := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 1)
		laterRaw := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 2)
		previousActive := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 3)
		laterCompressed := laterRaw + ".gz"
		compressionErr := errors.New("first generation cannot be compressed")
		realCompress := manager.compressFile
		manager.compressFile = func(ctx context.Context, path string) (string, error) {
			if path == failedRaw {
				return "", compressionErr
			}
			return realCompress(ctx, path)
		}

		report, err := manager.RunOnce(ctx)
		require.ErrorIs(t, err, compressionErr)
		require.Len(t, report.Compressed, 1)
		require.Empty(t, remote.pathsFor(firstDestination))
		require.Empty(t, remote.pathsFor(secondDestination))
		require.FileExists(t, failedRaw)
		require.NoFileExists(t, laterRaw)
		require.FileExists(t, laterCompressed)
		key := fileCheckpointKey(fileGeneration{Period: period, Index: 2}, true)
		checkpoint, err := compressedStore.Load(ctx, key)
		require.NoError(t, err)
		require.Empty(t, checkpoint.CompletedDestinationIDs)
		// 障害が続くまま再起動する試験では、明示的に待機を打ち切ります。
		require.ErrorIs(t, manager.ForceClose(), pkgshutdown.ErrForcedClose)
		require.ErrorIs(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose), pkgshutdown.ErrForcedClose)

		// Storeを新規instanceにしても、残る古い失敗世代が後続を止めます。
		restartedRemote := newDestinationUploaderRecorder(firstDestination, secondDestination)
		restarted, _ := newGenerationContinuationManager(t, root, target, checkpointDirectory, restartedRemote)
		restartedCompress := restarted.compressFile
		restarted.compressFile = func(ctx context.Context, path string) (string, error) {
			if path == failedRaw {
				return "", compressionErr
			}
			return restartedCompress(ctx, path)
		}
		_, err = restarted.RunOnce(ctx)
		require.ErrorIs(t, err, compressionErr)
		require.Empty(t, restartedRemote.pathsFor(firstDestination))
		require.Empty(t, restartedRemote.pathsFor(secondDestination))
		require.FileExists(t, laterCompressed)
		require.FileExists(t, failedRaw)

		// 管理者が未処理の元fileを削除した後は、消えた世代を永久に待ちません。
		require.NoError(t, os.Remove(failedRaw))
		latest := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 4)
		_, err = restarted.RunOnce(ctx)
		require.NoError(t, err)
		require.NoError(t, boundedwait.CloseInBubble(t, "restarted.Close()", restarted.Close, restarted.ForceClose))
		require.Equal(t, []string{laterCompressed, previousActive + ".gz"}, restartedRemote.pathsFor(firstDestination))
		require.Equal(t, []string{laterCompressed, previousActive + ".gz"}, restartedRemote.pathsFor(secondDestination))
		require.NoFileExists(t, previousActive)
		require.NoFileExists(t, previousActive+".gz")
		require.FileExists(t, latest)
	})
}

func TestGenerationContinuationSkipsUnsafePeerAndRetriesAfterManualRemoval(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const target = "continue-after-unsafe-peer"
		const destination = objectstorage.DestinationID("archive")
		logDirectory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDirectory, 0o750))
		checkpointDirectory := newCheckpointDirectoryForTest(t, root)
		remote := newDestinationUploaderRecorder(destination)
		manager, _ := newGenerationContinuationManager(t, root, target, checkpointDirectory, remote)
		period := currentLogDate()
		failedRaw := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 1)
		laterRaw := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 2)
		latest := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 3)
		peer := failedRaw + ".gz"
		externalFile := filepath.Join(resolvedTempDirForTest(t), "external.log.gz")
		writeGzipFile(t, externalFile, []byte("must not be touched"))
		testutil.Symlink(t, externalFile, peer)
		externalContents, err := os.ReadFile(externalFile)
		require.NoError(t, err)

		_, err = manager.RunOnce(ctx)
		require.ErrorIs(t, err, appendlog.ErrCompressedPeerNotRegular)
		require.Empty(t, remote.pathsFor(destination))
		require.FileExists(t, failedRaw)
		require.NoFileExists(t, laterRaw)
		require.FileExists(t, laterRaw+".gz")
		info, err := os.Lstat(peer)
		require.NoError(t, err)
		require.NotZero(t, info.Mode()&os.ModeSymlink)

		// 障害の原因であるリンクだけを削除すれば、元ログを再発見して圧縮・送信します。
		require.NoError(t, os.Remove(peer))
		_, err = manager.RunOnce(ctx)
		require.NoError(t, err)
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		require.Equal(t, []string{peer, laterRaw + ".gz"}, remote.pathsFor(destination))
		require.NoFileExists(t, failedRaw)
		require.NoFileExists(t, peer)
		require.FileExists(t, latest)
		contentsAfter, err := os.ReadFile(externalFile)
		require.NoError(t, err)
		require.Equal(t, externalContents, contentsAfter)
	})
}

func TestGenerationContinuationCompressesLaterFilesWithoutUploader(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const target = "continue-without-upload"
		logDirectory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDirectory, 0o750))
		period := currentLogDate()
		failedRaw := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 1)
		laterRaw := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 2)
		latest := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 3)
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: logDirectory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true),
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, boundedwait.NoError)
		realCompress := manager.compressFile
		defer func() { manager.compressFile = realCompress }()
		compressionErr := errors.New("first generation compression failed")
		manager.compressFile = func(ctx context.Context, path string) (string, error) {
			if path == failedRaw {
				return "", compressionErr
			}
			return realCompress(ctx, path)
		}

		report, err := manager.RunOnce(ctx)
		require.ErrorIs(t, err, compressionErr)
		require.Equal(t, []CompressionReport{{SourcePath: laterRaw, CompressedPath: laterRaw + ".gz"}}, report.Compressed)
		require.Empty(t, report.ObjectStorageUploads)
		require.FileExists(t, failedRaw)
		require.NoFileExists(t, laterRaw)
		require.FileExists(t, laterRaw+".gz")
		require.FileExists(t, latest)
	})
}

func newGenerationContinuationManager(
	t *testing.T,
	root, target, checkpointDirectory string,
	remote *destinationUploaderRecorder,
) (*FileSeriesManager, *checkpointstore.FilesystemStore) {
	t.Helper()
	compressedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, target)
	require.NoError(t, err)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: filepath.Join(root, target, "logs"), Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 0, 0, 0, true),
		Uploader:        remote,
		CheckpointStore: compressedStore,
	})
	// bubble内の試験だけが使うfixtureです。圧縮の失敗が続く途中で試験が失敗しても、受理済みの
	// 再試行をForceCloseで中止してからbubble時間の期限付きで閉じ、試験の失敗理由を残します。
	// 成功した試験では、本文で強制終了したmanagerのErrForcedCloseだけを許します。
	boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose,
		func(t testing.TB, err error) {
			t.Helper()
			if err != nil {
				require.ErrorIs(t, err, pkgshutdown.ErrForcedClose)
			}
		})
	return manager, compressedStore
}
