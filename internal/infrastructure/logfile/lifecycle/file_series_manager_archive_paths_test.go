package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestLifecycleCompressedPathsBySource(t *testing.T) {
	for _, test := range []struct {
		name     string
		existing bool
		compress bool
	}{
		{"new compression", false, true},
		{"reuse old format", true, true},
		{"compression disabled", false, false},
		{"existing file with compression disabled", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			const target = "path-roles"
			directory := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(directory, 0o750))
			raw := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
			observed := []string{raw}
			if test.existing {
				writeGzipFile(t, raw+".gz", []byte("existing gzip"))
				observed = append(observed, raw+".gz")
			}
			manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
				Config: testFileSeriesManagerConfigWithCompressionFormat(
					root, target, pkgfsstore.FileRotationDaily, 10, 0, 0, test.compress, pkgfsstore.CompressZstd,
				),
			})
			sourceGroups := fsstore.GroupFilesByCompressionSource(fsstore.InspectFilePaths(observed), "log")
			compressedPathsBySource, err := manager.resolveLifecycleCompressedPaths(observed, sourceGroups)
			require.NoError(t, err)
			require.Len(t, compressedPathsBySource, 1, "raw and compressed files share the same source key")
			compressedPath, registered := compressedPathsBySource[pathidentity.ComparisonKey(raw)]
			require.True(t, registered, "an empty compressed path must still have a registered source")
			if test.existing {
				require.Equal(t, raw+".gz", compressedPath)
			} else if test.compress {
				require.Equal(t, raw+".zst", compressedPath)
			} else {
				require.Empty(t, compressedPath)
			}
			require.NoFileExists(t, raw+".zst", "planning must not create a file")
		})
	}
}

func TestLifecycleProtectsPlannedOutputBeforeCompression(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "planned-output"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	raw := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
	writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
	})
	realCompress := manager.compressFile
	called := false
	manager.compressFile = func(ctx context.Context, source string) (string, error) {
		called = true
		require.Equal(t, raw, source)
		outputPath := raw + ".gz"
		require.NoFileExists(t, outputPath)
		competing := appendlog.NewDirectoryGuardToken()
		defer manager.directoryGuard.RemoveToken(competing)
		require.False(t, manager.directoryGuard.ProtectPathsIfNoOtherOwnerConflict(
			competing, nil, []string{outputPath},
		), "new output must be protected before compression starts")
		return realCompress(ctx, source)
	}
	report, err := manager.RunOnce(context.Background())
	require.NoError(t, err)
	require.True(t, called)
	require.Len(t, report.Compressed, 1)
	require.Equal(t, raw+".gz", report.Compressed[0].CompressedPath)
}

func TestLifecycleDefersNewFormatWhenObservedCompressedFileDisappears(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "disappeared-peer"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	raw := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
	writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
	writeGzipFile(t, raw+".gz", []byte("previously observed file"))
	const destination = objectstorage.DestinationID("archive")
	upload := newDestinationUploaderRecorder(destination)
	store, err := checkpointstore.NewFilesystemStore(resolvedTempDirForTest(t), "disappeared-peer")
	require.NoError(t, err)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config: testFileSeriesManagerConfigWithCompressionFormat(
			root, target, pkgfsstore.FileRotationDaily, 10, 0, 0, true, pkgfsstore.CompressZstd,
		),
		Uploader:        upload,
		CheckpointStore: &removePeerDuringCheckpointRegistration{Store: store, path: raw + ".gz"},
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	// scan後の宛先登録中に旧形式fileを取り除き、圧縮直前の再確認を試験します。
	report, err := manager.RunOnce(context.Background())
	require.ErrorIs(t, err, processing.ErrObservedCompressedFileDisappeared)
	require.Empty(t, report.Compressed)
	require.Empty(t, upload.pathsFor(destination))
	require.FileExists(t, raw)
	require.NoFileExists(t, raw+".zst", "the new format was not protected in this cycle")

	// 次の走査では未圧縮sourceだけを観測し、新しい出力先を保護して圧縮できます。
	report, err = manager.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Compressed, 1)
	require.Equal(t, raw+".zst", report.Compressed[0].CompressedPath)
	require.Equal(t, []string{raw + ".zst"}, upload.pathsFor(destination))
	require.NoFileExists(t, raw+".zst")
}

type removePeerDuringCheckpointRegistration struct {
	checkpointstore.Store
	path string
}

func (s *removePeerDuringCheckpointRegistration) RegisterDestinations(ctx context.Context, ids []objectstorage.DestinationID, keys []checkpointstore.CheckpointKey) error {
	if s.path != "" && len(keys) > 0 {
		if err := os.Remove(s.path); err != nil {
			return err
		}
		s.path = ""
	}
	return s.Store.RegisterDestinations(ctx, ids, keys)
}
