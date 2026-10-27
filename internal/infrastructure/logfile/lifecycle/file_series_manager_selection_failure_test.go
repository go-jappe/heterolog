package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

func TestFileSeriesManagerContinuesPastFailedGenerationAndProtectsOnlyUnprocessedArchives(t *testing.T) {
	for _, blocker := range []string{"compression-failure", "raw-owner-protection"} {
		for _, uploadEnabled := range []bool{true, false} {
			uploadMode := "uploads-enabled"
			if !uploadEnabled {
				uploadMode = "uploads-disabled"
			}
			t.Run(blocker+"/"+uploadMode, func(t *testing.T) {
				ctx := context.Background()
				root := resolvedTempDirForTest(t)
				const (
					target      = "selection-break"
					period      = "20000101"
					destination = objectstorage.DestinationID("archive")
				)
				directory := filepath.Join(root, target, "logs")
				require.NoError(t, os.MkdirAll(directory, 0o750))
				firstSource := filepath.Join(directory, target+"_"+period+"_1.log")
				firstPath := firstSource
				if blocker == "raw-owner-protection" {
					firstPath += ".gz"
					writeGzipFile(t, firstPath, []byte("blocked compressed generation"))
				} else {
					require.Equal(t, firstSource, writeFileSeriesCheckpointLog(t, directory, target, period, 1))
				}
				laterSource := filepath.Join(directory, target+"_"+period+"_2.log")
				selectedPeer, unselectedPeer := laterSource+".gz", laterSource+".zst"
				writeGzipFile(t, selectedPeer, []byte("selected later archive"))
				require.NoError(t, os.WriteFile(unselectedPeer, []byte("unselected later archive"), 0o600))
				laterRaw := writeFileSeriesCheckpointLog(t, directory, target, period, 3)
				latest := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 4)
				observed := []string{firstPath, selectedPeer, unselectedPeer, laterRaw}
				old := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
				for _, path := range observed {
					require.NoError(t, os.Chtimes(path, old, old))
				}

				store, err := checkpointstore.NewFilesystemStore(newCheckpointDirectoryForTest(t, root), "selection-break")
				require.NoError(t, err)

				remote := newDestinationUploaderRecorder(destination)
				options := FileSeriesManagerOptions{
					FileName:        logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
					Config:          testFileSeriesManagerConfig(root, target, 1, 1, 1, true),
					CheckpointStore: store,
				}
				if uploadEnabled {
					options.Uploader = remote
				}
				manager := newFileSeriesManagerWithExistingDestinations(t, options)
				t.Cleanup(func() {
					// この試験は障害を残すため、受理済みの処理を明示的に打ち切ります。
					require.ErrorIs(t, manager.ForceClose(), pkgshutdown.ErrForcedClose)
					require.ErrorIs(t, boundedwait.Close(t, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager)), pkgshutdown.ErrForcedClose)
				})
				if blocker == "raw-owner-protection" {
					owner := appendlog.NewDirectoryGuardToken()
					manager.directoryGuard.ProtectPaths(owner, firstSource)
					t.Cleanup(func() { manager.directoryGuard.UnprotectPaths(owner, firstSource) })
				}
				compressionErr := errors.New("compression failed before publication")
				var compressionPaths []string
				realCompress := manager.compressFile
				manager.compressFile = func(ctx context.Context, path string) (string, error) {
					compressionPaths = append(compressionPaths, path)
					if path == firstSource {
						return "", compressionErr
					}
					return realCompress(ctx, path)
				}

				// blockされたgenerationと選択されなかった代替fileは保護されたまま残ります。
				// 後続の選択済みfileは、uploadを待つか通常のlocal retentionの対象になります。
				report, err := manager.RunOnce(ctx)
				if blocker == "compression-failure" {
					require.ErrorIs(t, err, compressionErr)
					require.Equal(t, []string{firstSource, laterRaw}, compressionPaths)
				} else {
					require.NoError(t, err)
					require.Equal(t, []string{laterRaw}, compressionPaths)
				}
				require.Equal(t, len(observed), report.ScannedFiles)
				require.Equal(t, []CompressionReport{{SourcePath: laterRaw, CompressedPath: laterRaw + ".gz"}}, report.Compressed)
				completedPaths := []string{selectedPeer, laterRaw + ".gz"}
				if uploadEnabled {
					require.Empty(t, remote.pathsFor(destination))
					require.Empty(t, report.ObjectStorageUploads)
					require.Empty(t, report.UploadedFileRemovals)
					require.Empty(t, report.Purge.ByAge.RemovalResults)
				} else {
					require.Empty(t, report.ObjectStorageUploads)
					require.Empty(t, remote.pathsFor(destination))
					require.Empty(t, report.UploadedFileRemovals)
					require.Len(t, report.Purge.ByAge.RemovalResults, 2)
					for index, removal := range report.Purge.ByAge.RemovalResults {
						require.Equal(t, completedPaths[index], removal.File.Path)
						require.NoError(t, removal.Err)
					}
				}
				require.True(t, report.Purge.ByAge.Ran)
				require.False(t, report.Purge.ByGeneration.Ran)
				require.Empty(t, report.Purge.ByGeneration.RemovalResults)
				require.True(t, report.Purge.ByDirSize.Ran)
				require.Empty(t, report.Purge.ByDirSize.RemovalResults)
				for _, path := range []string{firstPath, unselectedPeer, latest} {
					require.FileExists(t, path)
				}
				require.NoFileExists(t, laterRaw)
				for _, path := range completedPaths {
					if uploadEnabled {
						require.FileExists(t, path)
					} else {
						require.NoFileExists(t, path)
					}
				}
				if uploadEnabled {
					for _, index := range []uint64{1, 2, 3} {
						checkpoint, err := store.Load(ctx, checkpointstore.CheckpointKey{Generation: checkpointstore.ArchiveCheckpointGenerationKey(period, index), Compressed: true})
						require.NoError(t, err)
						require.Empty(t, checkpoint.CompletedDestinationIDs, "failed generations keep all following generations pending")
					}
				}
			})
		}
	}
}
