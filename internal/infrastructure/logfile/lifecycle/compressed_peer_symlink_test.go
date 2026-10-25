package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestFileSeriesManagerRunOnceDoesNotTreatCompressedPeerSymlinkAsCompleted(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "compressed-peer-symlink"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	sourcePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	compressedTargetPath := filepath.Join(resolvedTempDirForTest(t), "valid-target.log.gz")
	latestPath := filepath.Join(logDir, target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("must be compressed"), 0o600))
	writeGzipFile(t, compressedTargetPath, []byte("unrelated valid compressed file"))
	testutil.Symlink(t, compressedTargetPath, compressedPath)
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true),
	})
	realCompress := manager.compressFile
	compressCalls := 0
	manager.compressFile = func(ctx context.Context, path string) (string, error) {
		compressCalls++
		return realCompress(ctx, path)
	}

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, appendlog.ErrCompressedPeerNotRegular)
	require.Empty(t, report.Compressed)
	require.Zero(t, compressCalls)
	require.FileExists(t, sourcePath)
	peerInfo, lstatErr := os.Lstat(compressedPath)
	require.NoError(t, lstatErr)
	require.NotZero(t, peerInfo.Mode()&os.ModeSymlink)

	// symlinkは圧縮完了としてskipされていません。占有pathを取り除けば、
	// 保持したrawを次のlifecycle周期で通常どおり圧縮します。
	require.NoError(t, os.Remove(compressedPath))
	report, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Len(t, report.Compressed, 1)
	require.Equal(t, 1, compressCalls)
	require.NoFileExists(t, sourcePath)
	require.FileExists(t, compressedPath)
	peerInfo, lstatErr = os.Lstat(compressedPath)
	require.NoError(t, lstatErr)
	require.True(t, peerInfo.Mode().IsRegular())
	require.FileExists(t, latestPath)
}

func TestFileSeriesManagerRejectsOldFormatNonRegularPeerBeforeResolving(t *testing.T) {
	for _, kind := range []string{"symlink", "dangling-symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			const target = "old-format-nonregular"
			logDir := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(logDir, 0o750))
			raw := writeFileSeriesCheckpointLog(t, logDir, target, currentLogDate(), 1)
			latest := writeFileSeriesCheckpointLog(t, logDir, target, currentLogDate(), 2)
			peer := raw + pkgfsstore.ExtensionCompressGzip
			var linkTarget string
			switch kind {
			case "directory":
				require.NoError(t, os.Mkdir(peer, 0o750))
			case "symlink", "dangling-symlink":
				linkTarget = filepath.Join(resolvedTempDirForTest(t), "old-format.gz")
				if kind == "symlink" {
					writeGzipFile(t, linkTarget, []byte("unrelated source"))
				}
				testutil.Symlink(t, linkTarget, peer)
			}
			remote := &recordingUploader{}
			manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
				Config: testFileSeriesManagerConfigWithCompressionFormat(
					root, target, pkgfsstore.FileRotationDaily, 1, 1, 1, true, pkgfsstore.CompressZstd,
				),
				Uploader: remote,
			})
			manager.compressFile = func(context.Context, string) (string, error) {
				t.Fatal("an occupied old-format peer must not be treated as missing")
				return "", nil
			}
			scan, err := manager.scanLifecycleTargetFiles()
			require.NoError(t, err)
			require.Equal(t, []string{raw, peer}, scan.files)
			require.ElementsMatch(t, fsstore.InspectFilePaths([]string{raw, latest}).RegularFiles, scan.sourceGroups.Files.RegularFiles)
			require.ElementsMatch(t, []string{raw, peer, latest}, scan.sourceGroups.Files.Paths)
			require.Len(t, scan.sourceGroups.Files.PathErrors, 1)
			sourceGroup, found := scan.sourceGroups.Source(raw)
			require.True(t, found)
			require.ErrorIs(t, sourceGroup.Err, appendlog.ErrCompressedPeerNotRegular)

			report, err := manager.RunOnce(context.Background())
			require.ErrorIs(t, err, appendlog.ErrCompressedPeerNotRegular)
			require.Empty(t, report.Compressed)
			require.Empty(t, remote.pathsSnapshot())
			require.False(t, report.Purge.ByAge.Ran)
			require.False(t, report.Purge.ByGeneration.Ran)
			require.False(t, report.Purge.ByDirSize.Ran)
			require.FileExists(t, raw)
			require.FileExists(t, latest)
			require.NoFileExists(t, raw+pkgfsstore.ExtensionCompressZstd)
			info, err := os.Lstat(peer)
			require.NoError(t, err)
			require.False(t, info.Mode().IsRegular())
			if kind == "symlink" {
				require.FileExists(t, linkTarget)
			}
		})
	}
}

func TestFileSeriesManagerScanPassesOnlyRegularPathsAndKeepsLatestFilenameAuthority(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "regular-observed-paths"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	date := currentLogDate()
	raw := writeFileSeriesCheckpointLog(t, logDir, target, date, 1)
	peer := raw + pkgfsstore.ExtensionCompressGzip
	writeGzipFile(t, peer, []byte("source"))
	latest := filepath.Join(logDir, target+"_"+date+"_99.log")
	unrelatedPeer := filepath.Join(logDir, target+"_"+date+"_2.log.gz")
	unrelatedDir := filepath.Join(logDir, target+"_"+date+"_3.log.zst")
	testutil.Symlink(t, filepath.Join(resolvedTempDirForTest(t), "absent-raw"), latest)
	testutil.Symlink(t, filepath.Join(resolvedTempDirForTest(t), "absent-peer"), unrelatedPeer)
	require.NoError(t, os.Mkdir(unrelatedDir, 0o750))
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true),
	})
	scan, err := manager.scanLifecycleTargetFiles()
	require.NoError(t, err)
	// raw1をlatestへ繰り上げず、名前上のlatest99だけを処理一覧から除外します。
	// 不正な2・3も名前を残し、cursorをその失敗世代より先へ進めないようにします。
	require.Equal(t, []string{raw, peer, unrelatedPeer, unrelatedDir}, scan.files)
	require.Equal(t, fsstore.InspectFilePaths([]string{raw, peer}).RegularFiles, scan.sourceGroups.Files.RegularFiles)
	require.ElementsMatch(t, []string{raw, peer, unrelatedPeer, unrelatedDir, latest}, scan.sourceGroups.Files.Paths)
	require.Len(t, scan.sourceGroups.Files.PathErrors, 2)
	for _, path := range []string{unrelatedPeer, unrelatedDir} {
		source, ok := fsstore.CompressionSourcePath(path, "log")
		require.True(t, ok)
		sourceGroup, found := scan.sourceGroups.Source(source)
		require.True(t, found)
		require.ErrorIs(t, sourceGroup.Err, appendlog.ErrCompressedPeerNotRegular)
	}
	for _, file := range scan.sourceGroups.Files.RegularFiles {
		info, err := os.Lstat(file.Path)
		require.NoError(t, err)
		require.True(t, info.Mode().IsRegular())
	}
}
