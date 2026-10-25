package lifecycle

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// preservedCompressionSourcesForTestは、ログdirectoryの.preservedへ退避した元ログを返します。
func preservedCompressionSourcesForTest(t *testing.T, logDir string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(logDir, infrafsstore.PreservedDirectoryName, "*.compressed-content-mismatch-*.preserved"))
	require.NoError(t, err)
	return paths
}

func TestExistingCompressedPeerCacheRevalidatesWhenSourceChanges(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "peer-cache-source-change"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	source := writeFileSeriesCheckpointLog(t, logDir, target, currentLogDate(), 1)
	peer := source + pkgfsstore.ExtensionCompressGzip
	content, err := os.ReadFile(source)
	require.NoError(t, err)
	writeGzipFile(t, peer, content)
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true),
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)

	valid, err := manager.existingCompressedFileIsValid(source, peer)
	require.NoError(t, err)
	require.True(t, valid)

	// 一致を確認した後に元ログへ追記されたら、以前の検証記録を根拠に元ログを片付けません。
	file, err := os.OpenFile(source, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.WriteString("\nrecord appended after verification")
	require.NoError(t, errors.Join(err, file.Close()))
	valid, err = manager.existingCompressedFileIsValid(source, peer)
	require.ErrorIs(t, err, infrafsstore.ErrCompressedContentMismatch)
	require.False(t, valid)
	// 通常purgeの判定でも、内容の異なるpeerと共存する元ログは保護します。
	require.True(t, manager.shouldProtectFromLifecyclePurge(source))
}

func TestFileSeriesManagerRunOncePreservesSourceWhenCompressedPeerContentDiffers(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "mismatched-compressed-peer"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	sourcePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	latestPath := filepath.Join(logDir, target+"_"+date+"_2.log")
	sourceContent := []byte("compressed record\nrecord written after the peer was published\n")
	require.NoError(t, os.WriteFile(sourcePath, sourceContent, 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))
	// 完全なgzipですが、元ログ後半のrecordを含みません。
	writeGzipFile(t, compressedPath, []byte("compressed record\n"))

	recorder := &recordingUploader{}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true),
		Uploader: recorder,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	manager.compressFile = func(context.Context, string) (string, error) {
		t.Fatal("an intact compressed peer must not be overwritten by recompression")
		return "", nil
	}

	report, runErr := manager.RunOnce(ctx)
	require.ErrorIs(t, runErr, infrafsstore.ErrCompressedContentMismatch)
	require.NotErrorIs(t, runErr, pkgfsstore.ErrCorruptCompressedFile)
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, recorder.pathsSnapshot())
	// 元ログは削除せず.preservedへ退避し、退避先をerrorで報告します。完全なpeerは残します。
	require.NoFileExists(t, sourcePath)
	require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
	require.FileExists(t, compressedPath)
	preserved := preservedCompressionSourcesForTest(t, logDir)
	require.Len(t, preserved, 1)
	var preservedErr *processing.CompressionSourcePreservedError
	require.ErrorAs(t, runErr, &preservedErr)
	require.Equal(t, sourcePath, preservedErr.SourcePath)
	require.Equal(t, preserved[0], preservedErr.PreservedPath)
	got, err := os.ReadFile(preserved[0])
	require.NoError(t, err)
	require.Equal(t, sourceContent, got)

	// 次周期は残った圧縮ファイルをその世代の圧縮済みfileとして送信し、退避した元ログは扱いません。
	report, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, report.Compressed)
	require.Equal(t, []string{compressedPath}, recorder.pathsSnapshot())
	require.Len(t, report.UploadedFileRemovals, 1)
	require.NoError(t, report.UploadedFileRemovals[0].Err)
	require.NoFileExists(t, compressedPath)
	require.Equal(t, preserved, preservedCompressionSourcesForTest(t, logDir))
	require.FileExists(t, latestPath)
}

func TestCompressionRecoveryPreservesSourceWhenPublishedPeerContentDiffers(t *testing.T) {
	// 受理済み処理の再試行間隔は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const (
			target      = "resumed-mismatched-peer"
			destination = objectstorage.DestinationID("archive")
		)
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))
		period := currentLogDate()
		sourcePath := writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
		latestPath := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)
		sourceContent, err := os.ReadFile(sourcePath)
		require.NoError(t, err)
		checkpointStore, err := checkpointstore.NewFilesystemStore(newCheckpointDirectoryForTest(t, root), "default-log-series")
		require.NoError(t, err)
		uploadRecorder := newDestinationUploaderRecorder(destination)
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config:          testFileSeriesManagerConfig(root, target, 1, 1, 1, true),
			Uploader:        uploadRecorder,
			CheckpointStore: checkpointStore,
		})
		forceCloseManagerOnFailure(t, manager)
		compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
		directorySyncErr := errors.New("sync compressed file parent directory")
		manager.compressFile = func(_ context.Context, path string) (string, error) {
			require.Equal(t, sourcePath, path)
			// 公開後の同期に失敗し、元ログの退避前に回復へ渡した状態です。peerは完全ですが、
			// 元ログとは別の内容です。
			writeGzipFile(t, compressedPath, []byte("records that differ from the source"))
			return compressedPath, directorySyncErr
		}

		_, err = manager.RunOnce(ctx)
		require.ErrorIs(t, err, directorySyncErr)
		require.FileExists(t, sourcePath)
		require.FileExists(t, compressedPath)

		// 受理済みの回復は、両fileを保護したまま元ログを.preservedへ退避し、
		// 残った完全なpeerの送信を再開します。元ログを削除しません。
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		require.NoFileExists(t, sourcePath)
		preserved := preservedCompressionSourcesForTest(t, logDir)
		require.Len(t, preserved, 1)
		got, err := os.ReadFile(preserved[0])
		require.NoError(t, err)
		require.Equal(t, sourceContent, got)
		require.Equal(t, []string{compressedPath}, uploadRecorder.pathsFor(destination))
		checkpoint, err := checkpointStore.Load(ctx, checkpointstore.CheckpointKey{
			Generation: checkpointstore.ArchiveCheckpointGenerationKey(period, 1), Compressed: true,
		})
		require.NoError(t, err)
		require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)
		require.FileExists(t, latestPath)
		require.NoError(t, boundedwait.CloseInBubble(t, "checkpointStore.Close()", checkpointStore.Close, checkpointStore.ForceClose))
	})
}

func TestCompressionRecoveryRemovesCorruptPublishedPeerAndRecompressesBeforeClose(t *testing.T) {
	// 受理済み処理の再試行間隔は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const target = "resumed-corrupt-peer"
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))
		period := currentLogDate()
		sourcePath := writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
		latestPath := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)
		sourceContent, err := os.ReadFile(sourcePath)
		require.NoError(t, err)
		manager := NewFileSeriesManager(FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, true),
		})
		forceCloseManagerOnFailure(t, manager)
		compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
		directorySyncErr := errors.New("sync compressed file parent directory")
		compress := manager.compressFile
		compressCalls := 0
		manager.compressFile = func(ctx context.Context, path string) (string, error) {
			compressCalls++
			if compressCalls > 1 {
				return compress(ctx, path)
			}
			// 公開後の同期に失敗して回復へ渡した後、公開済みのpeerが壊れた状態です。
			require.NoError(t, os.WriteFile(compressedPath, []byte("not a gzip stream"), 0o600))
			return compressedPath, directorySyncErr
		}

		_, err = manager.RunOnce(ctx)
		require.ErrorIs(t, err, directorySyncErr)
		require.FileExists(t, sourcePath)

		// 受理済みの回復は破損したpeerを削除し、保護した元ログから再圧縮して完了します。
		closed := make(chan error, 1)
		go func() { closed <- manager.Close() }()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(10 * time.Minute):
			_ = manager.ForceClose()
			boundedwait.ReceiveInBubble(t, closed, "receive from closed")
			t.Fatal("accepted compression recovery kept retrying the corrupt peer")
		}
		require.Equal(t, 2, compressCalls)
		require.NoFileExists(t, sourcePath)
		require.Equal(t, sourceContent, readGzipForPeerContentTest(t, compressedPath))
		require.Empty(t, preservedCompressionSourcesForTest(t, logDir))
		require.FileExists(t, latestPath)
	})
}

func readGzipForPeerContentTest(t *testing.T, path string) []byte {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	return content
}
