package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFileSeriesManagerRestartReusesPublishedGzipAfterChangingToZstd(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const target = "single-archive-restart"
		const destination = objectstorage.DestinationID("archive")
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		raw := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
		latest := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
		gzipPath := raw + pkgfsstore.ExtensionCompressGzip
		checkpointDirectory := newCheckpointDirectoryForTest(t, root)
		store, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "restart-single")
		require.NoError(t, err)

		first := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
			Uploader: newDestinationUploaderRecorder(destination), CheckpointStore: store,
		})
		publicationErr := errors.New("process stopped after gzip publication before raw retirement")
		first.compressFile = func(_ context.Context, path string) (string, error) {
			content, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			writeGzipFile(t, gzipPath, content)
			return gzipPath, publicationErr
		}
		_, err = first.RunOnce(ctx)
		require.ErrorIs(t, err, publicationErr)
		originalGzip, err := os.ReadFile(gzipPath)
		require.NoError(t, err)
		require.FileExists(t, raw)
		require.ErrorIs(t, first.ForceClose(), shutdown.ErrForcedClose)
		require.ErrorIs(t, boundedwait.CloseInBubble(t, "first.Close()", first.Close, first.ForceClose), shutdown.ErrForcedClose)

		restartedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "restart-single")
		require.NoError(t, err)
		remote := newDestinationUploaderRecorder(destination)
		uploadErr := errors.New("keep reused gzip pending for inspection")
		remote.failNext(destination, uploadErr)
		restarted := NewFileSeriesManager(FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config: testFileSeriesManagerConfigWithCompressionFormat(
				root, target, pkgfsstore.FileRotationDaily, 10, 0, 0, true, pkgfsstore.CompressZstd,
			),
			Uploader: remote, CheckpointStore: restartedStore,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "restarted.Close()", restarted.Close, restarted.ForceClose, boundedwait.NoError)
		restarted.compressFile = func(context.Context, string) (string, error) {
			t.Fatal("published old-format archive must not be recompressed")
			return "", nil
		}
		report, err := restarted.RunOnce(ctx)
		require.ErrorIs(t, err, uploadErr)
		require.Empty(t, report.Compressed)
		require.Equal(t, []string{gzipPath}, remote.pathsFor(destination))
		require.NoFileExists(t, raw)
		require.NoFileExists(t, raw+pkgfsstore.ExtensionCompressZstd)
		require.FileExists(t, latest)
		reusedGzip, err := os.ReadFile(gzipPath)
		require.NoError(t, err)
		require.Equal(t, originalGzip, reusedGzip)
		generation, valid := restarted.parseFileGeneration(gzipPath)
		require.True(t, valid)
		checkpoint, err := restarted.objectStorageUpload.checkpointStore.Load(
			ctx, fileCheckpointKey(generation, true),
		)
		require.NoError(t, err)
		require.Empty(t, checkpoint.CompletedDestinationIDs)

		require.NoError(t, boundedwait.CloseInBubble(t, "restarted.Close()", restarted.Close, restarted.ForceClose))
		require.NoFileExists(t, gzipPath)
		require.NoFileExists(t, raw+pkgfsstore.ExtensionCompressZstd)
	})
}

func TestFileSeriesManagerKeepsRawAndUnknownOldCodec(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "unknown-old-codec"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	raw := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
	latest := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
	peer := raw + ".unknown"
	require.NoError(t, os.WriteFile(peer, []byte("preserved legacy codec"), 0o600))
	remote := &recordingUploader{}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, true), Uploader: remote,
	})
	report, err := manager.RunOnce(context.Background())
	require.ErrorIs(t, err, infrafsstore.ErrUnsupportedCompressionFormat)
	require.Empty(t, report.Compressed)
	require.Empty(t, remote.pathsSnapshot())
	require.Empty(t, report.UploadedFileRemovals)
	for _, path := range []string{raw, peer, latest} {
		require.FileExists(t, path)
	}
	require.NoFileExists(t, raw+pkgfsstore.ExtensionCompressGzip)
}

func TestFileSeriesManagerKeepsRawAndPeerWhenVerificationHasIOError(t *testing.T) {
	codec := &fileSeriesConcurrencyCodec{
		runnerTestCompressionCodec: &runnerTestCompressionCodec{},
		beforeVerify:               func() error { return os.ErrPermission },
	}
	manager, source := newCompressionConcurrencyManager(t, codec, true)
	report, err := manager.RunOnce(context.Background())
	require.ErrorIs(t, err, os.ErrPermission)
	require.NotErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
	require.Empty(t, report.Compressed)
	require.Empty(t, report.ObjectStorageUploads)
	require.FileExists(t, source)
	require.FileExists(t, source+".br")
}

func TestFileSeriesManagerPreservesUnselectedArchiveThroughEveryPurge(t *testing.T) {
	for _, layout := range []string{"raw-present", "compressed-only"} {
		t.Run(layout, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			const target = "first-observed-archive"
			directory := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(directory, 0o750))
			// 保持期間1日を過ぎた期間の世代です。
			raw := filepath.Join(directory, target+"_"+logDateDaysBefore(10)+"_1.log")
			gzipPath, zstdPath := raw+".gz", raw+".zst"
			writeGzipFile(t, gzipPath, []byte("original source"))
			require.NoError(t, os.WriteFile(zstdPath, []byte("unselected archive"), 0o600))
			old := time.Now().Add(-48 * time.Hour)
			require.NoError(t, os.Chtimes(zstdPath, old, old))
			if layout == "raw-present" {
				require.NoError(t, os.WriteFile(raw, []byte("original source"), 0o600))
			}
			latest := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
			remote := &recordingUploader{}
			manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
				Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, layout == "raw-present"),
				Uploader: remote,
			})
			report, err := manager.RunOnce(context.Background())
			require.NoError(t, err)
			require.Empty(t, report.Compressed)
			require.Equal(t, []string{gzipPath}, remote.pathsSnapshot())
			require.Len(t, report.UploadedFileRemovals, 1)
			require.Equal(t, gzipPath, report.UploadedFileRemovals[0].File.Path)
			require.NoFileExists(t, raw)
			require.NoFileExists(t, gzipPath)
			require.True(t, report.Purge.ByAge.Ran)
			require.Empty(t, report.Purge.ByAge.RemovalResults)
			require.False(t, report.Purge.ByGeneration.Ran)
			require.Empty(t, report.Purge.ByGeneration.RemovalResults)
			require.True(t, report.Purge.ByDirSize.Ran)
			require.Empty(t, report.Purge.ByDirSize.RemovalResults)
			content, err := os.ReadFile(zstdPath)
			require.NoError(t, err)
			require.Equal(t, "unselected archive", string(content))
			require.FileExists(t, latest)

			_, err = manager.RunOnce(context.Background())
			require.NoError(t, err)
			require.Equal(t, []string{gzipPath}, remote.pathsSnapshot(), "the selected alternate codec shares the completed generation")
			require.NoFileExists(t, zstdPath)
		})
	}
}

func TestFileSeriesManagerCompressedOnlyArchiveConflictsWithRawProtection(t *testing.T) {
	for _, ownership := range []string{"active", "retained"} {
		t.Run(ownership, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			const target = "compressed-only-protection"
			directory := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(directory, 0o750))
			raw := filepath.Join(directory, target+"_"+currentLogDate()+"_1.log")
			peer := raw + pkgfsstore.ExtensionCompressGzip
			writeGzipFile(t, peer, []byte("published"))
			latest := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
			remote := &recordingUploader{}
			manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
				Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, true), Uploader: remote,
			})
			owner := appendlog.NewDirectoryGuardToken()
			protectedRaw := raw
			if runtime.GOOS == "windows" {
				protectedRaw = strings.ToUpper(raw)
			}
			guard := manager.directoryGuard
			if ownership == "active" {
				require.NoError(t, guard.TransitionActive(owner, func() (string, error) { return protectedRaw, nil }))
			} else {
				guard.ProtectPaths(owner, protectedRaw)
			}
			t.Cleanup(func() {
				guard.RemoveToken(owner)
				guard.UnprotectPaths(owner, protectedRaw)
			})
			report, err := manager.RunOnce(context.Background())
			require.NoError(t, err)
			require.Empty(t, report.ObjectStorageUploads)
			require.Empty(t, remote.pathsSnapshot())
			require.FileExists(t, peer)
			require.FileExists(t, latest)
			guard.RemoveToken(owner)
			guard.UnprotectPaths(owner, protectedRaw)
			_, err = manager.RunOnce(context.Background())
			require.NoError(t, err)
			require.Equal(t, []string{peer}, remote.pathsSnapshot())
			require.NoFileExists(t, peer)
		})
	}
}
