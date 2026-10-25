package lifecycle

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"

	"github.com/stretchr/testify/require"
)

func TestFileSeriesManagerRunOnceCompressesAndUploadsInactiveLogOnly(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "delivery-lifecycle"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	currentDate := currentLogDate()
	inactivePath := filepath.Join(logDir, target+"_"+currentDate+"_1.log")
	activePath := filepath.Join(logDir, target+"_"+currentDate+"_2.log")
	require.NoError(t, os.WriteFile(inactivePath, []byte("inactive"), 0o600))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	recorder := &recordingUploader{}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader: recorder,
	})

	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)

	compressedPath := inactivePath + pkgfsstore.ExtensionCompressGzip
	require.NoFileExists(t, inactivePath)
	require.NoFileExists(t, compressedPath)
	require.FileExists(t, activePath)
	require.Len(t, report.Compressed, 1)
	require.Equal(t, compressedPath, report.Compressed[0].CompressedPath)
	require.Len(t, report.UploadedFileRemovals, 1)
	require.Equal(t, compressedPath, report.UploadedFileRemovals[0].File.Path)
	require.NoError(t, report.UploadedFileRemovals[0].Err)
	require.Equal(t, []string{compressedPath}, recorder.pathsSnapshot())
}

func TestFileSeriesManagerRunOnceRetriesPreCommitCompressionFailureAndUploadsAfterSuccess(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "compression-retry"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	inactivePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	latestPath := filepath.Join(logDir, target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(inactivePath, []byte("retry me"), 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

	recorder := &recordingUploader{}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader: recorder,
	})
	useShortLogFileProcessingRecovery(t, manager)
	realCompress := manager.compressFile
	compressionErr := errors.New("temporary compression failure")
	attempts := 0
	manager.compressFile = func(ctx context.Context, path string) (string, error) {
		attempts++
		if attempts <= 2 {
			return "", compressionErr
		}
		return realCompress(ctx, path)
	}

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, compressionErr)
	require.Empty(t, report.Compressed)
	require.Empty(t, report.ObjectStorageUploads)
	require.FileExists(t, inactivePath)
	require.NoFileExists(t, inactivePath+pkgfsstore.ExtensionCompressGzip)
	require.Empty(t, recorder.pathsSnapshot())

	require.NoError(t, boundedwait.Close(t, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager)))
	require.Equal(t, 3, attempts)

	compressedPath := inactivePath + pkgfsstore.ExtensionCompressGzip
	require.NoFileExists(t, inactivePath)
	require.NoFileExists(t, compressedPath)
	require.FileExists(t, latestPath)
	require.Equal(t, []string{compressedPath}, recorder.pathsSnapshot())
}

func TestFileSeriesManagerRunOnceCompressionFailureDoesNotBlockLaterArchiveRetention(t *testing.T) {
	tests := []struct {
		name       string
		maxBackups int
		maxAge     int
		maxDirSize uint64
	}{
		{name: "age purge", maxAge: 1},
		{name: "generation purge", maxBackups: 1},
		{name: "directory size purge", maxDirSize: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			root := resolvedTempDirForTest(t)
			target := "compression-failure-frontier"
			logDir := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(logDir, 0o750))

			date := logDateDaysBefore(3)
			firstInactive := filepath.Join(logDir, target+"_"+date+"_1.log")
			secondInactive := filepath.Join(logDir, target+"_"+date+"_2.log"+pkgfsstore.ExtensionCompressGzip)
			thirdInactive := filepath.Join(logDir, target+"_"+date+"_3.log"+pkgfsstore.ExtensionCompressGzip)
			latestPath := filepath.Join(logDir, target+"_"+currentLogDate()+"_4.log")
			require.NoError(t, os.WriteFile(firstInactive, []byte("12345678"), 0o600))
			writeGzipFile(t, secondInactive, []byte("second compressed file"))
			writeGzipFile(t, thirdInactive, []byte("third compressed file"))
			require.NoError(t, os.WriteFile(latestPath, []byte("12345678"), 0o600))
			old := time.Now().Add(-72 * time.Hour)
			for _, path := range []string{firstInactive, secondInactive, thirdInactive} {
				require.NoError(t, os.Chtimes(path, old, old))
			}

			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
				Config: testFileSeriesManagerConfig(
					root,
					target,
					test.maxBackups,
					test.maxAge,
					test.maxDirSize,
					true,
				),
			})
			compressionErr := errors.New("compression unavailable")
			attemptedPaths := make([]string, 0, 1)
			manager.compressFile = func(_ context.Context, path string) (string, error) {
				attemptedPaths = append(attemptedPaths, path)
				return "", compressionErr
			}

			report, err := manager.RunOnce(ctx)
			require.ErrorIs(t, err, compressionErr)
			require.Equal(t, []string{firstInactive}, attemptedPaths)
			for _, path := range []string{firstInactive, latestPath} {
				require.FileExists(t, path)
			}
			require.NoFileExists(t, secondInactive)
			removals := report.Purge.ByAge.RemovalResults
			removals = append(removals, report.Purge.ByGeneration.RemovalResults...)
			removals = append(removals, report.Purge.ByDirSize.RemovalResults...)
			if test.maxBackups > 0 {
				require.FileExists(t, thirdInactive)
				require.Len(t, removals, 1)
			} else {
				require.NoFileExists(t, thirdInactive)
				require.Len(t, removals, 2)
			}
			for _, removal := range removals {
				require.NoError(t, removal.Err)
				require.NotEqual(t, firstInactive, removal.File.Path)
				require.NotEqual(t, latestPath, removal.File.Path)
			}
		})
	}
}

func TestFileSeriesManagerRunOnceReportsSourceCleanupFailureAndContinuesCommittedFiles(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "compression-cleanup-failure"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	firstInactivePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	secondInactivePath := filepath.Join(logDir, target+"_"+date+"_2.log")
	latestPath := filepath.Join(logDir, target+"_"+date+"_3.log")
	require.NoError(t, os.WriteFile(firstInactivePath, []byte("first source remains"), 0o600))
	require.NoError(t, os.WriteFile(secondInactivePath, []byte("second source remains"), 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

	recorder := &recordingUploader{}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true),
		Uploader: recorder,
	})
	cleanupErr := errors.New("source cleanup failed")
	compressCalls := 0
	manager.compressFile = func(_ context.Context, path string) (string, error) {
		compressCalls++
		compressedPath := path + pkgfsstore.ExtensionCompressGzip
		writeGzipFile(t, compressedPath, []byte(filepath.Base(path)))
		pendingSourcePath := logpattern.CompressionSourceDeletePendingPath(path)
		require.NoError(t, os.Rename(path, pendingSourcePath))
		return compressedPath, errors.Join(
			appendlog.ErrCompressionSourceCleanup,
			cleanupErr,
		)
	}

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, cleanupErr)
	require.Equal(t, 2, compressCalls)
	require.Len(t, report.Compressed, 2)
	require.Len(t, report.ObjectStorageUploads, 2)
	require.Empty(t, report.RecoveredCompressionSourceDeletePendingPaths)

	firstCompressedPath := firstInactivePath + pkgfsstore.ExtensionCompressGzip
	secondCompressedPath := secondInactivePath + pkgfsstore.ExtensionCompressGzip
	require.Equal(t, []string{firstCompressedPath, secondCompressedPath}, recorder.pathsSnapshot())
	for _, path := range []string{
		firstInactivePath,
		secondInactivePath,
	} {
		require.NoFileExists(t, path)
		require.FileExists(t, logpattern.CompressionSourceDeletePendingPath(path))
	}
	require.NoFileExists(t, firstCompressedPath)
	require.NoFileExists(t, secondCompressedPath)
	require.Len(t, report.UploadedFileRemovals, 2)
	require.FileExists(t, latestPath)

	// process再起動を新しいmanagerで再現します。圧縮済みfileがupload後に削除されても、
	// 正常圧縮済みsourceはcanonical `.log`へ戻しません。次周期はpending sourceの削除だけを
	// filesystemから再発見し、再圧縮・再uploadしません。
	restartedManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true),
		Uploader: recorder,
	})
	report, err = restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, compressCalls)
	require.Empty(t, report.Compressed)
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	require.Equal(t, []string{
		logpattern.CompressionSourceDeletePendingPath(firstInactivePath),
		logpattern.CompressionSourceDeletePendingPath(secondInactivePath),
	}, report.RecoveredCompressionSourceDeletePendingPaths)
	require.Equal(t, []string{
		firstCompressedPath,
		secondCompressedPath,
	}, recorder.pathsSnapshot())
	require.NoFileExists(t, firstInactivePath)
	require.NoFileExists(t, secondInactivePath)
	require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(firstInactivePath))
	require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(secondInactivePath))
	require.NoFileExists(t, firstCompressedPath)
	require.NoFileExists(t, secondCompressedPath)
}

func TestFileSeriesManagerRunOnceRetiresRawSourceBeforeUploadedCompressedPeerIsRemoved(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "compression-cleanup-purge"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	sourcePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	latestPath := filepath.Join(logDir, target+"_"+date+"_2.log")
	for path, content := range map[string]string{
		sourcePath: "source",
		latestPath: "active",
	} {
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	}
	writeGzipFile(t, compressedPath, []byte("source"))
	oldest := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(sourcePath, oldest, oldest))
	require.NoError(t, os.Chtimes(compressedPath, oldest.Add(time.Hour), oldest.Add(time.Hour)))

	recorder := &recordingUploader{}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 1, 0, 0, true),
		Uploader: recorder,
	})
	manager.compressFile = func(context.Context, string) (string, error) {
		t.Fatal("source with a committed compressed peer must not be recompressed")
		return "", nil
	}

	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, report.Compressed)
	require.Equal(t, []string{compressedPath}, recorder.pathsSnapshot())
	require.Len(t, report.UploadedFileRemovals, 1)
	require.Equal(t, compressedPath, report.UploadedFileRemovals[0].File.Path)
	require.NoError(t, report.UploadedFileRemovals[0].Err)
	require.False(t, report.Purge.ByGeneration.Ran)
	require.Empty(t, report.Purge.ByGeneration.RemovalResults)
	require.NoFileExists(t, sourcePath)
	require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
	require.NoFileExists(t, compressedPath)
	require.FileExists(t, latestPath)

	// local圧縮fileがupload後に消えても、一度valid peerとして確認したsourceを
	// 次周期に再圧縮しません。
	report, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, report.Compressed)
	require.Equal(t, []string{compressedPath}, recorder.pathsSnapshot())
}

func TestFileSeriesManagerRunOnceProtectsSourceWhenCompressedPeerIsNotARegularFile(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "invalid-compressed-peer"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	sourcePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	latestPath := filepath.Join(logDir, target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("must remain"), 0o600))
	require.NoError(t, os.Mkdir(compressedPath, 0o750))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 1, true),
	})
	manager.compressFile = func(context.Context, string) (string, error) {
		t.Fatal("an invalid compressed peer must not be overwritten")
		return "", nil
	}

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, appendlog.ErrCompressedPeerNotRegular)
	// 非通常peerは候補解決より上流で拒否し、この周期のpurgeにも進みません。
	require.False(t, report.Purge.ByDirSize.Ran)
	require.Empty(t, report.Purge.ByDirSize.RemovalResults)
	require.FileExists(t, sourcePath)
	require.DirExists(t, compressedPath)
	require.FileExists(t, latestPath)
}

func TestFileSeriesManagerRunOnceRemovesCorruptCompressedPeerAndRetriesSourceNextCycle(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "corrupt-compressed-peer"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	sourcePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	latestPath := filepath.Join(logDir, target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("valid source"), 0o600))
	require.NoError(t, os.WriteFile(compressedPath, []byte("not gzip"), 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

	recorder := &recordingUploader{}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, true),
		Uploader: recorder,
	})

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, infrafsstore.ErrInvalidCompressedFile)
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, recorder.pathsSnapshot())
	require.FileExists(t, sourcePath)
	require.NoFileExists(t, compressedPath)

	// sourceを保持したまま現在周期を止めるため、次周期は通常の圧縮経路でpeerを
	// 再生成し、検証に成功したfileだけをuploadします。
	report, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Len(t, report.Compressed, 1)
	require.Len(t, report.ObjectStorageUploads, 1)
	require.Equal(t, []string{compressedPath}, recorder.pathsSnapshot())
	require.NoFileExists(t, sourcePath)
	require.NoFileExists(t, compressedPath)
	require.Len(t, report.UploadedFileRemovals, 1)
	require.NoError(t, report.UploadedFileRemovals[0].Err)
	require.FileExists(t, latestPath)
}

func TestFileSeriesManagerRunOncePurgesInactiveLogsByGeneration(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "delivery-purge"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	currentDate := currentLogDate()
	oldest := filepath.Join(logDir, target+"_"+currentDate+"_1.log")
	middle := filepath.Join(logDir, target+"_"+currentDate+"_2.log")
	newest := filepath.Join(logDir, target+"_"+currentDate+"_3.log")
	activePath := filepath.Join(logDir, target+"_"+currentDate+"_4.log")
	for _, path := range []string{oldest, middle, newest, activePath} {
		require.NoError(t, os.WriteFile(path, []byte(filepath.Base(path)), 0o600))
	}
	baseTime := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(oldest, baseTime, baseTime))
	require.NoError(t, os.Chtimes(middle, baseTime.Add(time.Minute), baseTime.Add(time.Minute)))
	require.NoError(t, os.Chtimes(newest, baseTime.Add(2*time.Minute), baseTime.Add(2*time.Minute)))
	require.NoError(t, os.Chtimes(activePath, baseTime.Add(3*time.Minute), baseTime.Add(3*time.Minute)))

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 2, 0, 0, false),
	})

	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)

	require.True(t, report.Purge.ByGeneration.Ran)
	require.Len(t, report.Purge.ByGeneration.RemovalResults, 1)
	require.NoError(t, report.Purge.ByGeneration.RemovalResults[0].Err)
	require.NoFileExists(t, oldest)
	require.FileExists(t, middle)
	require.FileExists(t, newest)
	require.FileExists(t, activePath)
}

// MaxFileBackupsは圧縮形式ごとではなく、日付・番号で決まる世代単位で数えます。
// 超過した世代は、未圧縮fileと圧縮fileをまとめて削除します。
func TestFileSeriesManagerRunOnceCountsGenerationsAcrossCompressionFormats(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "generation-across-compression"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	period := currentLogDate()
	rawOldest := filepath.Join(logDir, target+"_"+period+"_1.log")
	rawNewest := filepath.Join(logDir, target+"_"+period+"_2.log")
	gzipOldest := rawOldest + pkgfsstore.ExtensionCompressGzip
	gzipMiddle := rawNewest + pkgfsstore.ExtensionCompressGzip
	gzipNewest := filepath.Join(logDir, target+"_"+period+"_3.log"+pkgfsstore.ExtensionCompressGzip)
	activePath := filepath.Join(logDir, target+"_"+period+"_4.log")

	for _, path := range []string{rawOldest, rawNewest, activePath} {
		require.NoError(t, os.WriteFile(path, []byte(filepath.Base(path)), 0o600))
	}
	for _, path := range []string{gzipOldest, gzipMiddle, gzipNewest} {
		writeGzipFile(t, path, []byte(filepath.Base(path)))
	}
	baseTime := time.Now().Add(-time.Hour)
	for index, path := range []string{
		rawOldest,
		gzipOldest,
		rawNewest,
		gzipMiddle,
		gzipNewest,
		activePath,
	} {
		at := baseTime.Add(time.Duration(index) * time.Minute)
		require.NoError(t, os.Chtimes(path, at, at))
	}

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 2, 0, 0, false),
	})
	report, err := manager.RunOnce(ctx)

	require.NoError(t, err)
	require.True(t, report.Purge.ByGeneration.Ran)
	// activeを除く3世代のうち、最も古い第1世代の未圧縮・圧縮fileだけを削除します。
	removedPaths := make([]string, 0, len(report.Purge.ByGeneration.RemovalResults))
	for _, removal := range report.Purge.ByGeneration.RemovalResults {
		require.NoError(t, removal.Err)
		removedPaths = append(removedPaths, removal.File.Path)
	}
	require.ElementsMatch(t, []string{rawOldest, gzipOldest}, removedPaths)
	require.NoFileExists(t, rawOldest)
	require.NoFileExists(t, gzipOldest)
	require.FileExists(t, rawNewest)
	require.FileExists(t, gzipMiddle)
	require.FileExists(t, gzipNewest)
	require.FileExists(t, activePath)
}

// 同じ世代の未圧縮fileと圧縮fileは一世代として数え、件数を合算しません。
func TestFileSeriesManagerRunOnceCountsRawAndCompressedFilesOfGenerationOnce(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "separate-compression-generation-counts"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	period := currentLogDate()
	rawFirst := filepath.Join(logDir, target+"_"+period+"_1.log")
	rawSecond := filepath.Join(logDir, target+"_"+period+"_2.log")
	gzipFirst := rawFirst + pkgfsstore.ExtensionCompressGzip
	gzipSecond := rawSecond + pkgfsstore.ExtensionCompressGzip
	activePath := filepath.Join(logDir, target+"_"+period+"_3.log")
	for _, path := range []string{rawFirst, rawSecond, activePath} {
		require.NoError(t, os.WriteFile(path, []byte(filepath.Base(path)), 0o600))
	}
	for _, path := range []string{gzipFirst, gzipSecond} {
		writeGzipFile(t, path, []byte(filepath.Base(path)))
	}

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 2, 0, 0, false),
	})
	report, err := manager.RunOnce(ctx)

	require.NoError(t, err)
	require.False(t, report.Purge.ByGeneration.Ran)
	require.Empty(t, report.Purge.ByGeneration.RemovalResults)
	for _, path := range []string{rawFirst, rawSecond, gzipFirst, gzipSecond, activePath} {
		require.FileExists(t, path)
	}
}

func TestFileSeriesManagerRunOnceReturnsGenerationPurgeFailureAsError(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "generation-purge-failure"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	undeletablePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	secondInactivePath := filepath.Join(logDir, target+"_"+date+"_2.log")
	latestPath := filepath.Join(logDir, target+"_"+date+"_3.log")
	require.NoError(t, os.WriteFile(undeletablePath, []byte("retained"), 0o600))
	require.NoError(t, os.WriteFile(secondInactivePath, []byte("inactive"), 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

	oldest := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(undeletablePath, oldest, oldest))

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 1, 0, 0, false),
	})
	restoreRemoval := testutil.DenyFileRemoval(t, undeletablePath)
	defer restoreRemoval()

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, ErrPurgeByGeneration)
	require.Len(t, report.Purge.ByGeneration.RemovalResults, 1)
	require.Equal(t, undeletablePath, report.Purge.ByGeneration.RemovalResults[0].File.Path)
	require.ErrorIs(t, report.Purge.ByGeneration.RemovalResults[0].Err, os.ErrPermission)
	// 返されたerrorは、削除できなかったfileの削除結果を原因として保持します。
	require.ErrorIs(t, err, report.Purge.ByGeneration.RemovalResults[0].Err)
	require.FileExists(t, undeletablePath)
	require.FileExists(t, secondInactivePath)
	require.FileExists(t, latestPath)
}

func TestFileSeriesManagerStopsLaterPurgeStepsAfterAgeDeleteFailure(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "age-purge-failure"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	date := now.Format("20060102")
	old := now.AddDate(0, 0, -2)
	failingPath := filepath.Join(logDir, target+"_"+old.Format("20060102")+"_1.log")
	require.NoError(t, os.WriteFile(failingPath, []byte("retained"), 0o600))
	require.NoError(t, os.Chtimes(failingPath, old, old))

	var recentPaths []string
	for index := 2; index <= 4; index++ {
		path := filepath.Join(logDir, fmt.Sprintf("%s_%s_%d.log", target, date, index))
		require.NoError(t, os.WriteFile(path, []byte("recent"), 0o600))
		recentPaths = append(recentPaths, path)
	}

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 2, 1, 1, false),
		Clock:    pkgclock.NewFixedClock(now),
	})
	restoreRemoval := testutil.DenyFileRemoval(t, failingPath)
	defer restoreRemoval()

	report, err := manager.RunOnce(ctx)

	require.ErrorIs(t, err, ErrPurgeByAge)
	require.True(t, report.Purge.ByAge.Ran)
	require.Len(t, report.Purge.ByAge.RemovalResults, 1)
	require.Equal(t, failingPath, report.Purge.ByAge.RemovalResults[0].File.Path)
	require.ErrorIs(t, report.Purge.ByAge.RemovalResults[0].Err, os.ErrPermission)
	require.ErrorIs(t, err, report.Purge.ByAge.RemovalResults[0].Err)
	require.False(t, report.Purge.ByGeneration.Ran)
	require.False(t, report.Purge.ByDirSize.Ran)
	require.Equal(t, now, manager.lastAgePurgeAt)
	require.FileExists(t, failingPath)
	for _, path := range recentPaths {
		require.FileExists(t, path)
	}
}

// NewFileSeriesManagerを直接使う場合も、MaxDirSizeは削除候補だけでなくlatest fileを含む
// 物理directory全体で判定します。latestは容量には含めますが削除対象にはしません。
func TestStandaloneFileSeriesManagerCountsLatestFileForDirectorySizePurge(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "standalone-directory-size"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	currentDate := currentLogDate()
	inactivePath := filepath.Join(logDir, target+"_"+currentDate+"_1.log")
	latestPath := filepath.Join(logDir, target+"_"+currentDate+"_2.log")
	require.NoError(t, os.WriteFile(inactivePath, []byte("12345678"), 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("abcdefgh"), 0o600))

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 10, false),
	})

	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, report.Purge.ByDirSize.Ran)
	require.Len(t, report.Purge.ByDirSize.RemovalResults, 1)
	require.Equal(t, inactivePath, report.Purge.ByDirSize.RemovalResults[0].File.Path)
	require.NoError(t, report.Purge.ByDirSize.RemovalResults[0].Err)
	require.NoFileExists(t, inactivePath)
	require.FileExists(t, latestPath)
}

func TestFileSeriesManagerRunOnceKeepsLatestExistingLogAcrossRotationBoundary(t *testing.T) {
	ctx := context.Background()

	for _, rotationType := range []pkgfsstore.FileRotationType{
		pkgfsstore.FileRotationDaily,
		pkgfsstore.FileRotationWeekly,
		pkgfsstore.FileRotationMonthly,
	} {
		t.Run(rotationType.String(), func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			target := "delivery-boundary"
			logDir := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(logDir, 0o750))

			oldDate := "20200101"
			inactivePath := filepath.Join(logDir, target+"_"+oldDate+"_1.log")
			activeCandidatePath := filepath.Join(logDir, target+"_"+oldDate+"_2.log")
			require.NoError(t, os.WriteFile(inactivePath, []byte("inactive"), 0o600))
			require.NoError(t, os.WriteFile(activeCandidatePath, []byte("active candidate"), 0o600))

			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
				Config:   testFileSeriesManagerConfigWithRotation(root, target, rotationType, 10, 0, 0, true),
			})

			report, err := manager.RunOnce(ctx)
			require.NoError(t, err)

			require.Len(t, report.Compressed, 1)
			require.Equal(t, inactivePath, report.Compressed[0].SourcePath)
			require.NoFileExists(t, inactivePath)
			require.FileExists(t, inactivePath+pkgfsstore.ExtensionCompressGzip)
			require.FileExists(t, activeCandidatePath)
		})
	}
}

func TestNewLogFileLifecyclesGroupsSharedDirectoryFileSeries(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	target := "regular-lifecycle"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	currentDate := currentLogDate()
	defaultInactive := filepath.Join(logDir, target+"_"+currentDate+"_1.log")
	defaultLatest := filepath.Join(logDir, target+"_"+currentDate+"_2.log")
	errorInactive := filepath.Join(logDir, "error_"+target+"_"+currentDate+"_1.log")
	errorLatest := filepath.Join(logDir, "error_"+target+"_"+currentDate+"_2.log")
	debugInactive := filepath.Join(logDir, "debug_"+target+"_"+currentDate+"_1.log")
	debugLatest := filepath.Join(logDir, "debug_"+target+"_"+currentDate+"_2.log")
	for _, path := range []string{defaultInactive, defaultLatest, errorInactive, errorLatest, debugInactive, debugLatest} {
		require.NoError(t, os.WriteFile(path, []byte(filepath.Base(path)), 0o600))
	}

	cfg := pkgfsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:                     target,
				LogPath:                    root,
				LogDir:                     "logs",
				ErrorLogDir:                "logs",
				IsSeparateErrorLogsEnabled: true,
				SeparateErrorLogsPrefix:    "error_",
				IsSeparateDebugLogsEnabled: true,
				SeparateDebugLogsPrefix:    "debug_",
				FileNameFormat:             pkgfsstore.FileNameFormat,
				FileDateFormat:             pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL:      time.Hour,
				EnableRotate:       true,
				RotationType:       pkgfsstore.FileRotationDaily,
				MaxFileBackups:     10,
				MaxFileAge:         36500,
				MaxFileSize:        1 << 20,
				MaxDirSize:         20 << 20,
				EnableFileCompress: true,
				CompressFormat:     pkgfsstore.CompressGzip,
			},
		},
	}

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, lifecycles, 1)
	require.Len(t, lifecycles[0].fileSeriesManagers, 3)

	report, err := lifecycles[0].RunOnce(ctx)
	require.NoError(t, err)
	require.Len(t, report.Compressed, 3)

	for _, path := range []string{defaultInactive, errorInactive, debugInactive} {
		require.NoFileExists(t, path)
		require.FileExists(t, path+pkgfsstore.ExtensionCompressGzip)
	}
	for _, path := range []string{defaultLatest, errorLatest, debugLatest} {
		require.FileExists(t, path)
	}
}

func TestNewLogFileLifecyclesLeavesPreviousSeparatedFileSeriesUnmanagedAfterCollapseToDefault(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "collapsed-file-series"
	logDir := filepath.Join(root, target, "logs")
	errorLogDir := filepath.Join(root, target, "error-logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	require.NoError(t, os.MkdirAll(errorLogDir, 0o750))

	date := currentLogDate()
	defaultInactive := filepath.Join(logDir, target+"_"+date+"_1.log")
	defaultLatest := filepath.Join(logDir, target+"_"+date+"_2.log")
	oldDebugInactive := filepath.Join(logDir, "debug_"+target+"_"+date+"_1.log")
	oldDebugLatest := filepath.Join(logDir, "debug_"+target+"_"+date+"_2.log")
	oldErrorInactive := filepath.Join(errorLogDir, "error_"+target+"_"+date+"_1.log")
	oldErrorLatest := filepath.Join(errorLogDir, "error_"+target+"_"+date+"_2.log")
	for _, path := range []string{
		defaultInactive,
		defaultLatest,
		oldDebugInactive,
		oldDebugLatest,
		oldErrorInactive,
		oldErrorLatest,
	} {
		require.NoError(t, os.WriteFile(path, []byte(filepath.Base(path)), 0o600))
	}

	cfg := pkgfsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:                     target,
				LogPath:                    root,
				LogDir:                     "logs",
				ErrorLogDir:                "error-logs",
				IsSeparateErrorLogsEnabled: true,
				SeparateErrorLogsPrefix:    "error_",
				IsSeparateDebugLogsEnabled: true,
				SeparateDebugLogsPrefix:    "debug_",
				FileNameFormat:             pkgfsstore.FileNameFormat,
				FileDateFormat:             pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL:      time.Hour,
				EnableRotate:       true,
				RotationType:       pkgfsstore.FileRotationDaily,
				MaxFileBackups:     10,
				MaxFileAge:         36500,
				MaxFileSize:        1 << 20,
				MaxDirSize:         20 << 20,
				EnableFileCompress: true,
				CompressFormat:     pkgfsstore.CompressGzip,
			},
		},
	}

	// 変更前はdefault/debugとerrorの二つのdirectoryに、三つのfile seriesを持ちます。
	separatedLifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, separatedLifecycles, 2)
	require.Len(t, fileSeriesManagersForLifecycles(separatedLifecycles), 3)

	// 分離を無効にした後は、以後のerror/debug eventもdefaultへ書き込まれるため、
	// lifecycleもdefaultだけを管理します。既存の分離fileは自動移行しません。
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	collapsedLifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, collapsedLifecycles, 1)
	require.Len(t, collapsedLifecycles[0].fileSeriesManagers, 1)
	require.Empty(t, collapsedLifecycles[0].fileSeriesManagers[0].FileNamePattern().Prefix)

	report, err := collapsedLifecycles[0].RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []CompressionReport{{
		SourcePath:     defaultInactive,
		CompressedPath: defaultInactive + pkgfsstore.ExtensionCompressGzip,
	}}, report.Compressed)
	require.NoFileExists(t, defaultInactive)
	require.FileExists(t, defaultInactive+pkgfsstore.ExtensionCompressGzip)
	require.FileExists(t, defaultLatest)

	for _, path := range []string{
		oldDebugInactive,
		oldDebugLatest,
		oldErrorInactive,
		oldErrorLatest,
	} {
		require.FileExists(t, path)
		require.NoFileExists(t, path+pkgfsstore.ExtensionCompressGzip)
	}
}

func TestNewLogFileLifecyclesUseOneExecutionUnitPerDirectory(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "directory-execution-units"
	cfg := pkgfsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:                     target,
				LogPath:                    root,
				LogDir:                     "logs",
				ErrorLogDir:                "error_logs",
				IsSeparateErrorLogsEnabled: true,
				SeparateErrorLogsPrefix:    "error_",
				IsSeparateDebugLogsEnabled: true,
				SeparateDebugLogsPrefix:    "debug_",
				FileNameFormat:             pkgfsstore.FileNameFormat,
				FileDateFormat:             pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL:      time.Hour,
				EnableRotate:       true,
				RotationType:       pkgfsstore.FileRotationDaily,
				MaxFileBackups:     10,
				MaxFileAge:         36500,
				MaxFileSize:        1 << 20,
				MaxDirSize:         20 << 20,
				CompressFormat:     pkgfsstore.CompressGzip,
				EnableFileCompress: false,
			},
		},
		RunnerConfig: pkgfsstore.AppendLogRunnerConfig{Interval: time.Minute},
	}

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, lifecycles, 2)
	require.Len(t, lifecycles[0].fileSeriesManagers, 2)
	require.Equal(t, "", lifecycles[0].fileSeriesManagers[0].FileNamePattern().Prefix)
	require.Equal(t, "debug_", lifecycles[0].fileSeriesManagers[1].FileNamePattern().Prefix)
	require.Len(t, lifecycles[1].fileSeriesManagers, 1)
	require.Equal(t, "error_", lifecycles[1].fileSeriesManagers[0].FileNamePattern().Prefix)

}

func TestNewDirectoryLifecycleRejectsDuplicateFileSeriesPattern(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "duplicate-file-series-pattern"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	pattern := logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"}
	first := NewFileSeriesManager(FileSeriesManagerOptions{
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, false),
		FileName: pattern,
	})
	second := NewFileSeriesManager(FileSeriesManagerOptions{
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, false),
		FileName: pattern,
	})

	_, err := NewDirectoryLifecycle(first, second)
	require.ErrorIs(t, err, ErrDuplicateLifecyclePattern)
}

func TestDirectoryLifecycleUploadsFileSeriesBeforeDirectorySizePurge(t *testing.T) {
	ctx := context.Background()
	// factoryの保持設定検証を満たしつつ、元の4/5/14の容量比率を維持します。
	const sizeUnit = 1 << 20
	root := resolvedTempDirForTest(t)
	target := "shared-file-series-size"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	runAt := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	currentDate := runAt.Format(pkgfsstore.FileDateFormat)
	previousDate := runAt.AddDate(0, 0, -1).Format(pkgfsstore.FileDateFormat)
	defaultLatest := filepath.Join(logDir, target+"_"+currentDate+"_2.log")
	errorInactive := filepath.Join(logDir, "error_"+target+"_"+previousDate+"_1.log")
	errorLatest := filepath.Join(logDir, "error_"+target+"_"+currentDate+"_2.log")
	debugLatest := filepath.Join(logDir, "debug_"+target+"_"+currentDate+"_2.log")
	for path, content := range map[string]string{
		defaultLatest: "1234",
		errorInactive: "12345",
		errorLatest:   "1234",
		debugLatest:   "1234",
	} {
		require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte(content), sizeUnit), 0o600))
	}

	cfg := pkgfsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:                     target,
				LogPath:                    root,
				LogDir:                     "logs",
				ErrorLogDir:                "logs",
				IsSeparateErrorLogsEnabled: true,
				SeparateErrorLogsPrefix:    "error_",
				IsSeparateDebugLogsEnabled: true,
				SeparateDebugLogsPrefix:    "debug_",
				FileNameFormat:             pkgfsstore.FileNameFormat,
				FileDateFormat:             pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL:      time.Hour,
				EnableRotate:       true,
				RotationType:       pkgfsstore.FileRotationDaily,
				MaxFileBackups:     10,
				MaxFileAge:         36500,
				MaxFileSize:        4 * sizeUnit,
				MaxDirSize:         14 * sizeUnit,
				EnableFileCompress: false,
				CompressFormat:     pkgfsstore.CompressGzip,
			},
		},
	}

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{
		Config: cfg,
		Clock:  pkgclock.NewFixedClock(runAt),
	})
	require.NoError(t, err)
	require.Len(t, lifecycles, 1)
	require.Len(t, lifecycles[0].fileSeriesManagers, 3)
	require.Same(t, lifecycles[0].fileSeriesManagers[0].directoryGuard, lifecycles[0].fileSeriesManagers[1].directoryGuard)
	require.Same(t, lifecycles[0].fileSeriesManagers[0].directoryGuard, lifecycles[0].fileSeriesManagers[2].directoryGuard)

	uploaderRecorder := &recordingUploader{}
	configuredErrorSeries := lifecycles[0].fileSeriesManagers[1]
	// upload依存もconstructorで確定させ、構築後のFileSeriesManagerを書き換えません。
	lifecycles[0].fileSeriesManagers[1] = newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		Config:   configuredErrorSeries.Config,
		FileName: configuredErrorSeries.FileNamePattern(),
		Uploader: uploaderRecorder,
		Clock:    pkgclock.NewFixedClock(runAt),
	})

	report, err := lifecycles[0].RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{errorInactive}, uploaderRecorder.pathsSnapshot())
	require.True(t, report.Purge.ByDirSize.Ran)
	require.Len(t, report.Purge.ByDirSize.RemovalResults, 1)
	require.Equal(t, errorInactive, report.Purge.ByDirSize.RemovalResults[0].File.Path)
	require.NoError(t, report.Purge.ByDirSize.RemovalResults[0].Err)
	require.NoFileExists(t, errorInactive)
	for _, activePath := range []string{defaultLatest, errorLatest, debugLatest} {
		require.FileExists(t, activePath)
	}
}

func TestDirectoryLifecycleMaxDirSizeDoesNotDependOnFirstFileSeries(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "first-file-series-is-error"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	errorInactive := filepath.Join(logDir, "error_"+target+"_"+date+"_1.log")
	errorLatest := filepath.Join(logDir, "error_"+target+"_"+date+"_2.log")
	defaultLatest := filepath.Join(logDir, target+"_"+date+"_1.log")
	require.NoError(t, os.WriteFile(errorInactive, []byte("1234567"), 0o600))
	require.NoError(t, os.WriteFile(errorLatest, []byte("1234"), 0o600))
	require.NoError(t, os.WriteFile(defaultLatest, []byte("1234"), 0o600))

	cfg := testFileSeriesManagerConfig(root, target, 0, 0, 10, false)
	errorManager := NewFileSeriesManager(FileSeriesManagerOptions{
		Config: cfg,
		FileName: logpattern.FileNamePattern{
			Dir:       logDir,
			Prefix:    "error_",
			Target:    target,
			Extension: defaultLogFileExtension,
		},
	})
	defaultManager := NewFileSeriesManager(FileSeriesManagerOptions{
		Config: cfg,
		FileName: logpattern.FileNamePattern{
			Dir:       logDir,
			Target:    target,
			Extension: defaultLogFileExtension,
		},
	})
	directoryLifecycle, err := NewDirectoryLifecycle(errorManager, defaultManager)
	require.NoError(t, err)

	report, err := directoryLifecycle.RunOnce(context.Background())
	require.NoError(t, err)
	require.True(t, report.Purge.ByDirSize.Ran)
	require.Len(t, report.Purge.ByDirSize.RemovalResults, 1)
	require.Equal(t, errorInactive, report.Purge.ByDirSize.RemovalResults[0].File.Path)
	require.NoError(t, report.Purge.ByDirSize.RemovalResults[0].Err)
	require.NoFileExists(t, errorInactive)
	require.FileExists(t, errorLatest)
	require.FileExists(t, defaultLatest)
}

func TestDirectoryLifecycleMaxDirSizeProtectsFilesFromDisabledFileSeries(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "removed-debug-file-series"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	oldDebugFile := filepath.Join(logDir, "debug_"+target+"_"+date+"_1.log")
	defaultActive := filepath.Join(logDir, target+"_"+date+"_1.log")
	require.NoError(t, os.WriteFile(oldDebugFile, []byte("12345678"), 0o600))
	require.NoError(t, os.WriteFile(defaultActive, []byte("abcdefgh"), 0o600))

	// debug分離を無効にした後も、同じdirectoryに残る旧debug fileは物理容量へ
	// 含めます。ただしdefault FileSeriesManagerはそのcheckpoint完了を証明できないため、
	// MaxDirSizeの削除候補にはしません。
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 10, false),
	})
	directoryLifecycle, err := NewDirectoryLifecycle(manager)
	require.NoError(t, err)

	report, err := directoryLifecycle.RunOnce(context.Background())
	require.NoError(t, err)
	require.True(t, report.Purge.ByDirSize.Ran)
	require.Empty(t, report.Purge.ByDirSize.RemovalResults)
	require.FileExists(t, oldDebugFile)
	require.FileExists(t, defaultActive)
}

func TestDirectoryLifecycleCountsAndProtectsPendingSourceFromDisabledFileSeries(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "removed-debug-pending-source"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	oldDebugSource := filepath.Join(logDir, "debug_"+target+"_"+date+"_1.log")
	oldDebugPendingSource := logpattern.CompressionSourceDeletePendingPath(oldDebugSource)
	defaultActive := filepath.Join(logDir, target+"_"+date+"_1.log")
	require.NoError(t, os.WriteFile(oldDebugPendingSource, []byte("12345678"), 0o600))
	require.NoError(t, os.WriteFile(defaultActive, []byte("abcdefgh"), 0o600))

	// debug分離を無効にすると、このpending sourceをcleanupするdebug managerは
	// 構築されません。directory-wide size scanで物理容量には含めますが、正常な
	// generation fileではないためgeneration purgeとdirectory purgeの削除候補にはしません。
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 1, 0, 10, false),
	})
	directoryLifecycle, err := NewDirectoryLifecycle(manager)
	require.NoError(t, err)

	fileInfo, protectedPaths, err := directoryLifecycle.directorySizePurgeInput(nil)
	require.NoError(t, err)
	require.Len(t, fileInfo, 2)
	require.ElementsMatch(t, []string{oldDebugPendingSource, defaultActive}, []string{
		fileInfo[0].Path,
		fileInfo[1].Path,
	})
	_, pendingProtected := protectedPaths[pathidentity.AbsoluteOrCleanPath(oldDebugPendingSource)]
	require.True(t, pendingProtected)

	report, err := directoryLifecycle.RunOnce(context.Background())
	require.NoError(t, err)
	require.True(t, report.Purge.ByDirSize.Ran)
	require.Empty(t, report.Purge.ByDirSize.RemovalResults)
	require.Empty(t, report.Purge.ByGeneration.RemovalResults)
	require.FileExists(t, oldDebugPendingSource)
	require.FileExists(t, defaultActive)
}

func TestDirectoryLifecycleCountsButProtectsNonCanonicalTargetFiles(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "non-canonical-directory-file"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	period := currentLogDate()
	nonCanonicalPaths := []string{
		filepath.Join(logDir, target+"_memo_1.log"),
		filepath.Join(logDir, target+"_memo_1.log.gz"),
		filepath.Join(logDir, target+"_"+period+"_01.log"),
		filepath.Join(logDir, target+"_"+period+"_01.log.gz"),
		filepath.Join(logDir, target+"_20260230_1.log"),
		filepath.Join(logDir, target+"_20260230_1.log.gz"),
	}
	for _, path := range nonCanonicalPaths {
		require.NoError(t, os.WriteFile(path, []byte("unknown"), 0o600))
	}
	activePath := filepath.Join(logDir, target+"_"+period+"_1.log")
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 1, 0, 1, false),
	})
	directoryLifecycle, err := NewDirectoryLifecycle(manager)
	require.NoError(t, err)

	fileInfo, protectedPaths, err := directoryLifecycle.directorySizePurgeInput(nil)
	require.NoError(t, err)
	require.Len(t, fileInfo, len(nonCanonicalPaths)+1)
	for _, path := range nonCanonicalPaths {
		_, protected := protectedPaths[pathidentity.AbsoluteOrCleanPath(path)]
		require.True(t, protected, "non-canonical path must not be a directory-size purge candidate: %s", path)
	}

	report, err := directoryLifecycle.RunOnce(context.Background())
	require.NoError(t, err)
	require.True(t, report.Purge.ByDirSize.Ran)
	require.Empty(t, report.Purge.ByDirSize.RemovalResults)
	for _, path := range nonCanonicalPaths {
		require.FileExists(t, path)
	}
	require.FileExists(t, activePath)
}

func TestDirectoryLifecycleKeepsUnarchivedFilesOutOfEveryPurgePath(t *testing.T) {
	testCases := []struct {
		name       string
		maxBackups int
		maxAge     int
		maxDirSize uint64
	}{
		{name: "age", maxAge: 1},
		{name: "generation", maxBackups: 1},
		{name: "directory_size", maxDirSize: 10},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			target := "pending-archive-" + testCase.name
			logDir := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(logDir, 0o750))

			date := currentLogDate()
			firstInactive := filepath.Join(logDir, target+"_"+date+"_1.log")
			secondInactive := filepath.Join(logDir, target+"_"+date+"_2.log")
			latest := filepath.Join(logDir, target+"_"+date+"_3.log")
			for _, path := range []string{firstInactive, secondInactive, latest} {
				require.NoError(t, os.WriteFile(path, []byte("12345678"), 0o600))
			}
			oldTime := time.Now().Add(-72 * time.Hour)
			for _, path := range []string{firstInactive, secondInactive} {
				require.NoError(t, os.Chtimes(path, oldTime, oldTime))
			}

			manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
				Config: testFileSeriesManagerConfig(
					root,
					target,
					testCase.maxBackups,
					testCase.maxAge,
					testCase.maxDirSize,
					false,
				),
				Uploader: failingUploader{err: errTestArchiveUpload},
			})

			report, err := manager.RunOnce(context.Background())
			require.ErrorIs(t, err, errTestArchiveUpload)
			require.FileExists(t, firstInactive)
			require.FileExists(t, secondInactive)
			require.FileExists(t, latest)
			require.Empty(t, report.Purge.ByAge.RemovalResults)
			require.Empty(t, report.Purge.ByGeneration.RemovalResults)
			require.Empty(t, report.Purge.ByDirSize.RemovalResults)
			if testCase.maxDirSize > 0 {
				require.True(t, report.Purge.ByDirSize.Ran)
			}
		})
	}
}

func TestDirectoryLifecycleSkipsDirectorySizeWhenFileSeriesScanFails(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "incomplete-file-series-scan"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	inactive := filepath.Join(logDir, target+"_"+date+"_1.log")
	latest := filepath.Join(logDir, target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(inactive, []byte("12345678"), 0o600))
	require.NoError(t, os.WriteFile(latest, []byte("abcdefgh"), 0o600))

	cfg := testFileSeriesManagerConfig(root, target, 0, 0, 10, false)
	// `[`は有効なliteral名です。NUL入りのtest入力で、OSによらずopen自体を失敗させます。
	// 通常fileへのReadDirはWindowsで空一覧になる場合があるため、失敗fixtureには使いません。
	unreadableDirectory := logDir + "\x00"
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		Config: cfg,
		FileName: logpattern.FileNamePattern{
			Dir:       logDir,
			Target:    target,
			Extension: defaultLogFileExtension,
		},
	})
	directoryLifecycle, err := NewDirectoryLifecycle(manager)
	require.NoError(t, err)
	// 初期化時の保存先解決は正常に通し、周期中のscan失敗だけを注入します。
	manager.fileName.Dir = unreadableDirectory

	// directory-size自身も同じI/O失敗で止まるため、scan結果が明示的に後段を禁止したことを確認します。
	err = directoryLifecycle.directoryGuard.RunWithLifecycleLock(func() error {
		result, scanErr := manager.runFileSeriesCompressIfNeededThenUploadAndPurge(context.Background())
		require.False(t, result.directoryPurgeSafe)
		return scanErr
	})
	var scanPathErr *os.PathError
	require.ErrorAs(t, err, &scanPathErr)
	require.Equal(t, unreadableDirectory, scanPathErr.Path)

	report, err := directoryLifecycle.RunOnce(context.Background())
	require.ErrorIs(t, err, scanPathErr.Err)
	require.False(t, report.Purge.ByDirSize.Ran)
	inactiveBody, err := os.ReadFile(inactive)
	require.NoError(t, err)
	require.Equal(t, "12345678", string(inactiveBody))
	latestBody, err := os.ReadFile(latest)
	require.NoError(t, err)
	require.Equal(t, "abcdefgh", string(latestBody))
}

func TestFileSeriesManagerUploaderCanReenterSameDirectoryGuard(t *testing.T) {
	root := resolvedTempDirForTest(t)
	target := "reentrant-uploader"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	inactivePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	activePath := filepath.Join(logDir, target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(inactivePath, []byte("inactive"), 0o600))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	guard := appendlog.DirectoryGuardForPath(logDir)
	uploadToken := appendlog.NewDirectoryGuardToken()
	uploadPath := filepath.Join(logDir, "fallback-during-upload.log")
	reentrant := &guardReentrantUploader{
		guard: guard,
		token: uploadToken,
		path:  uploadPath,
	}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader: reentrant,
	})

	done := make(chan error, 1)
	go func() {
		_, err := manager.RunOnce(context.Background())
		done <- err
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("FileSeriesManager.RunOnce() deadlocked when uploader reentered the directory guard")
	}
	require.Equal(t, 1, reentrant.callCount())
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Contains(t, protected, pathidentity.AbsoluteOrCleanPath(uploadPath))
	})
	guard.RemoveToken(uploadToken)
}

func TestFileSeriesManagerConcurrentFirstRunInitializesDirectoryGuardSafely(t *testing.T) {
	root := resolvedTempDirForTest(t)
	target := "concurrent-file-series-manager-init"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	activePath := filepath.Join(logDir, target+"_"+currentLogDate()+"_1.log")
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	// 明示patternで直接構築した未初期化managerも、初回RunOnceでguardを安全に共有する。
	manager := &FileSeriesManager{
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		fileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
	}
	require.Nil(t, manager.directoryGuard)

	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			_, err := manager.RunOnce(context.Background())
			results <- err
		}()
	}
	close(start)
	for range 2 {
		require.NoError(t, boundedwait.Receive(t, results, "concurrent first RunOnce"))
	}
	require.NotNil(t, manager.directoryGuard)
}

func TestFileSeriesManagerScanPrunesCompressionPeerCacheToFilesThatStillExist(t *testing.T) {
	t.Parallel()

	root := resolvedTempDirForTest(t)
	target := "cache-prune"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	liveCompressedPath := filepath.Join(logDir, target+"_"+date+"_1.log"+pkgfsstore.ExtensionCompressGzip)
	latestPath := filepath.Join(logDir, target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(liveCompressedPath, []byte("compressed"), 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
	})
	normalizedLivePath := pathidentity.ComparisonKey(liveCompressedPath)
	liveFingerprint := compressedPeerFingerprint{
		size:            int64(len("compressed")),
		modTimeUnixNano: time.Now().UnixNano(),
	}
	manager.verifiedCompressedPeers[normalizedLivePath] = liveFingerprint
	for index := range 100 {
		stalePath := pathidentity.ComparisonKey(
			filepath.Join(logDir, fmt.Sprintf("%s_stale_%d.log.gz", target, index)),
		)
		manager.verifiedCompressedPeers[stalePath] = compressedPeerFingerprint{
			size:            int64(index),
			modTimeUnixNano: int64(index),
		}
	}

	_, err := manager.listLifecycleTargetFiles()
	require.NoError(t, err)
	require.Equal(
		t,
		map[string]compressedPeerFingerprint{
			normalizedLivePath: liveFingerprint,
		},
		manager.verifiedCompressedPeers,
	)

	require.NoError(t, os.Remove(liveCompressedPath))
	_, err = manager.listLifecycleTargetFiles()
	require.NoError(t, err)
	require.Empty(t, manager.verifiedCompressedPeers)
}

func testFileSeriesManagerConfig(
	root string,
	target string,
	maxBackups int,
	maxAge int,
	maxDirSize uint64,
	compress bool,
) appendlog.ConfigProvider {
	return testFileSeriesManagerConfigWithRotation(root, target, pkgfsstore.FileRotationDaily, maxBackups, maxAge, maxDirSize, compress)
}

func testFileSeriesManagerConfigWithRotation(
	root string,
	target string,
	rotationType pkgfsstore.FileRotationType,
	maxBackups int,
	maxAge int,
	maxDirSize uint64,
	compress bool,
) appendlog.ConfigProvider {
	return testFileSeriesManagerConfigWithCompressionFormat(
		root,
		target,
		rotationType,
		maxBackups,
		maxAge,
		maxDirSize,
		compress,
		pkgfsstore.CompressGzip,
	)
}

func testFileSeriesManagerConfigWithCompressionFormat(
	root string,
	target string,
	rotationType pkgfsstore.FileRotationType,
	maxBackups int,
	maxAge int,
	maxDirSize uint64,
	compress bool,
	compressFormat pkgfsstore.CompressFormat,
) appendlog.ConfigProvider {
	return appendlog.NewConfigAdapter(pkgfsstore.Config{
		Layout: pkgfsstore.FsstoreConfig{
			Target:         target,
			LogPath:        root,
			LogDir:         "logs",
			FileNameFormat: pkgfsstore.FileNameFormat,
			FileDateFormat: pkgfsstore.FileDateFormat,
		},
		Lifecycle: pkgfsstore.FileLifecycleConfig{
			EnableRotate:       true,
			RotationType:       rotationType,
			MaxFileBackups:     maxBackups,
			MaxFileAge:         maxAge,
			MaxFileSize:        1024,
			MaxDirSize:         maxDirSize,
			EnableFileCompress: compress,
			CompressFormat:     compressFormat,
		},
	})
}

// currentLogDateは、現在時刻が属する期間をログfile名の日付で返します。
// managerの既定の時計（clock.NewUTCClock）と同じUTCで求め、試験を実行するprocessの
// local time zoneに依存させません。localの日付がUTCと異なる時間帯でも、試験で作る
// 「現在の期間」のfileをmanagerも現在の期間として扱います。
func currentLogDate() string {
	return time.Now().UTC().Format(pkgfsstore.FileDateFormat)
}

// logDateDaysBeforeは、現在時刻のdays日前が属する期間をログfile名の日付で返します。
// 保持期限を過ぎた世代や過去の期間の世代を、固定の日付ではなく実行時の時計から作ります。
// currentLogDateと同じく、managerの既定の時計と同じUTCで求めます。
func logDateDaysBefore(days int) string {
	return time.Now().UTC().AddDate(0, 0, -days).Format(pkgfsstore.FileDateFormat)
}

func writeGzipFile(t *testing.T, path string, content []byte) {
	t.Helper()

	file, err := os.Create(path)
	require.NoError(t, err)
	writer := gzip.NewWriter(file)
	_, err = writer.Write(content)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())
}

type recordingUploader struct {
	mu    sync.Mutex
	paths []string
}

type noopLifecycleRunReporter struct{}

func (*noopLifecycleRunReporter) ReportRun(context.Context, RunReport, error) {}

var errTestArchiveUpload = errors.New("test archive upload failed")

type failingUploader struct {
	err error
}

func (u failingUploader) Upload(context.Context, uploader.UploadRequest) error {
	return u.err
}

type guardReentrantUploader struct {
	guard *appendlog.DirectoryGuard
	token *appendlog.DirectoryGuardToken
	path  string

	mu    sync.Mutex
	calls int
}

func (r *guardReentrantUploader) Upload(context.Context, uploader.UploadRequest) error {
	err := r.guard.TransitionActive(r.token, func() (string, error) {
		return r.path, nil
	})
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	return nil
}

func (r *guardReentrantUploader) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *recordingUploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths = append(r.paths, input.FilePath())
	return nil
}

func (r *recordingUploader) pathsSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.paths...)
}
