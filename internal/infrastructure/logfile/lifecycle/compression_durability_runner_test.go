package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestInitialCompletionDoesNotRetireRawUntilCompressedPeerIsVerified(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "completed-generation-peer-verification"
	const destination = objectstorage.DestinationID("archive")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	raw := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
	latest := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
	compressed := raw + ".gz"
	require.NoError(t, os.WriteFile(compressed, []byte("corrupt gzip"), 0o600))
	remote := newDestinationUploaderRecorder(destination)
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader: remote, CheckpointStore: newTestFileCheckpointStore(t),
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
	require.Empty(t, report.UploadedFileRemovals)
	require.FileExists(t, raw, "generation completion must not bypass compressed peer validation")
	require.NoFileExists(t, compressed)
	require.Empty(t, remote.pathsFor(destination))

	report, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Len(t, report.Compressed, 1)
	// 登録前から存在した世代は、再圧縮後も送信せず、upload完了を理由に削除しません。
	require.Empty(t, report.UploadedFileRemovals)
	require.Empty(t, remote.pathsFor(destination))
	require.NoFileExists(t, raw)
	require.FileExists(t, compressed)
	require.FileExists(t, latest)
}

func TestFileSeriesManagerDoesNotUploadOrRecordCompletionWhenCompressionPublishIsNotDurable(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const (
			target           = "compression-publish-not-durable"
			checkpointSource = "default-log-series"
			destination      = objectstorage.DestinationID("archive")
		)
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))
		// 保持期間1日を過ぎた期間の世代にし、保護がなければ同じ周期のage purgeで削除される状態にします。
		period := logDateDaysBefore(10)
		sourcePath := writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
		latestPath := writeFileSeriesCheckpointLog(t, logDir, target, currentLogDate(), 1)

		checkpointStore, err := checkpointstore.NewFilesystemStore(
			newCheckpointDirectoryForTest(t, root),
			checkpointSource,
		)
		require.NoError(t, err)

		uploadRecorder := newDestinationUploaderRecorder(destination)
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config: testFileSeriesManagerConfig(
				root,
				target,
				1,
				1,
				1,
				true,
			),
			Uploader:        uploadRecorder,
			CheckpointStore: checkpointStore,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, boundedwait.NoError)

		compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
		directorySyncErr := errors.New("sync compressed file parent directory")
		manager.compressFile = func(_ context.Context, path string) (string, error) {
			require.Equal(t, sourcePath, path)
			// final renameは成功した一方、その直後の親directory syncが失敗した状態を再現します。
			// source retirement前なのでraw sourceはcanonical名のまま残ります。
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			writeGzipFile(t, compressedPath, content)
			return compressedPath, directorySyncErr
		}

		report, err := manager.RunOnce(ctx)
		require.ErrorIs(t, err, directorySyncErr)
		require.Empty(t, report.Compressed)
		require.Empty(t, report.ObjectStorageUploads)
		require.Empty(t, uploadRecorder.pathsFor(destination))
		checkpoint, checkpointErr := checkpointStore.Load(ctx, checkpointstore.CheckpointKey{Generation: checkpointstore.ArchiveCheckpointGenerationKey(period, 1), Compressed: true})
		require.NoError(t, checkpointErr)
		require.Empty(t, checkpoint.CompletedDestinationIDs)

		// final pathが見えていても、そのdirectory syncが失敗した周期では圧縮commitとして
		// 扱いません。保持期限を過ぎたsourceとpeerを同じ周期のage purgeから保護し、
		// directory-size purgeは実行せず、次周期にfilesystemから回復判定できる状態を残します。
		// 世代数purgeからの保護は、非activeの世代を二つ以上置く別の試験で確認します。
		require.FileExists(t, sourcePath)
		require.FileExists(t, compressedPath)
		require.FileExists(t, latestPath)
		require.True(t, report.Purge.ByAge.Ran)
		require.Empty(t, report.Purge.ByAge.RemovalResults)
		require.False(t, report.Purge.ByDirSize.Ran)
		require.Empty(t, report.Purge.ByDirSize.RemovalResults)
	})
}

func TestFileSeriesManagerKeepsPublishedPeerWhenSourceRetirementDirectorySyncFails(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const (
			target           = "source-retirement-not-durable"
			checkpointSource = "default-log-series"
			destination      = objectstorage.DestinationID("archive")
		)
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))
		// 保持期間1日を過ぎた期間の世代にし、保護がなければ同じ周期のage purgeで削除される状態にします。
		period := logDateDaysBefore(10)
		sourcePath := writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
		latestPath := writeFileSeriesCheckpointLog(t, logDir, target, currentLogDate(), 1)

		checkpointStore, err := checkpointstore.NewFilesystemStore(
			newCheckpointDirectoryForTest(t, root),
			checkpointSource,
		)
		require.NoError(t, err)

		uploadRecorder := newDestinationUploaderRecorder(destination)
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config: testFileSeriesManagerConfig(
				root,
				target,
				1,
				1,
				1,
				true,
			),
			Uploader:        uploadRecorder,
			CheckpointStore: checkpointStore,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, boundedwait.NoError)

		compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
		pendingSourcePath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
		directorySyncErr := errors.New("sync directory after retiring compression source")
		manager.compressFile = func(_ context.Context, path string) (string, error) {
			require.Equal(t, sourcePath, path)
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			writeGzipFile(t, compressedPath, content)
			require.NoError(t, os.Rename(sourcePath, pendingSourcePath))
			return compressedPath, directorySyncErr
		}

		report, err := manager.RunOnce(ctx)
		require.ErrorIs(t, err, directorySyncErr)
		require.Empty(t, report.Compressed)
		require.Empty(t, report.ObjectStorageUploads)
		require.Empty(t, uploadRecorder.pathsFor(destination))
		checkpoint, checkpointErr := checkpointStore.Load(ctx, checkpointstore.CheckpointKey{Generation: checkpointstore.ArchiveCheckpointGenerationKey(period, 1), Compressed: true})
		require.NoError(t, checkpointErr)
		require.Empty(t, checkpoint.CompletedDestinationIDs)

		// raw→pending rename後のdirectory syncが失敗した場合は、retirementを未commitとして
		// uploadへ進めません。同時に保持期限を過ぎた公開済みpeerも同じ周期のage purgeへ渡さず、
		// directory-size purgeも実行せず、次周期にfilesystem上のpending sourceとpeerから
		// 回復判定できる状態を残します。
		require.NoFileExists(t, sourcePath)
		require.FileExists(t, pendingSourcePath)
		require.FileExists(t, compressedPath)
		require.FileExists(t, latestPath)
		require.True(t, report.Purge.ByAge.Ran)
		require.Empty(t, report.Purge.ByAge.RemovalResults)
		require.False(t, report.Purge.ByDirSize.Ran)
		require.Empty(t, report.Purge.ByDirSize.RemovalResults)
	})
}

func TestDirectoryLifecycleSkipsDirectorySizePurgeWhenExistingPeerSourceRetirementFails(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "existing-peer-retirement-failure"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()

	sourcePath := writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	// 停止前に公開した、元ログと同じ内容の圧縮済みpeerです。
	sourceContent, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	writeGzipFile(t, compressedPath, sourceContent)
	latestPath := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 1, true),
	})
	retirementErr := errors.New("retire existing compressed peer source")
	// この試験は終了処理ではなく障害時の保護を確認するため、未完了の回復を明示的に止めます。
	t.Cleanup(func() {
		_ = manager.ForceClose()
		_ = manager.Close()
	})
	manager.removeSourceAfterCompression = func(gotSourcePath, gotCompressedPath string) error {
		require.Equal(t, sourcePath, gotSourcePath)
		require.Equal(t, compressedPath, gotCompressedPath)
		return retirementErr
	}

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, retirementErr)

	// RemoveSourceAfterCompressionのerrorだけからは、rawがcanonical名のままか、通常globへ
	// 入らないpending名へ移動済みかを判別できません。不完全なdirectory snapshotで
	// MaxDirSize purgeへ進まず、次周期のfull scanへ判断を送ります。
	require.False(t, report.Purge.ByDirSize.Ran)
	require.Empty(t, report.Purge.ByDirSize.RemovalResults)
	require.FileExists(t, sourcePath)
	require.FileExists(t, compressedPath)
	require.FileExists(t, latestPath)
}

func TestDirectoryLifecycleSkipsDirectorySizePurgeWhenCompressionSourceCleanupFailsMidRun(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "mid-run-source-cleanup-failure"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()

	sourcePath := writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
	latestPath := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	pendingSourcePath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	cleanupErr := errors.New("remove retired compression source")

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 1, true),
	})
	// 残した削除待ちは試験専用です。TempDirを削除する前に回復処理の実終了を待ちます。
	t.Cleanup(func() {
		_ = manager.ForceClose()
		_ = manager.Close()
	})
	manager.compressFile = func(_ context.Context, gotSourcePath string) (string, error) {
		require.Equal(t, sourcePath, gotSourcePath)
		writeGzipFile(t, compressedPath, []byte("committed compressed output"))
		require.NoError(t, os.Rename(sourcePath, pendingSourcePath))
		return compressedPath, errors.Join(
			appendlog.ErrCompressionSourceCleanup,
			cleanupErr,
		)
	}

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, cleanupErr)

	// RunOnce冒頭のpending cleanupより後でpending sourceが生じても、同じ周期の
	// directory-size purgeへ進めません。圧縮成果物はcommit済みなので保持します。
	require.False(t, report.Purge.ByDirSize.Ran)
	require.Empty(t, report.Purge.ByDirSize.RemovalResults)
	require.NoFileExists(t, sourcePath)
	require.FileExists(t, pendingSourcePath)
	require.FileExists(t, compressedPath)
	require.FileExists(t, latestPath)
}

func TestDirectoryLifecycleSkipsDirectorySizePurgeWhenPendingSourceCleanupFails(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "pending-source-cleanup-failure"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()

	pendingSourcePath := logpattern.CompressionSourceDeletePendingPath(filepath.Join(
		logDir,
		target+"_"+period+"_0.log",
	))
	require.NoError(t, os.Mkdir(pendingSourcePath, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(pendingSourcePath, "remove-blocker"),
		[]byte("keep the pending path non-empty"),
		0o600,
	))

	eligiblePath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	latestPath := filepath.Join(logDir, target+"_"+period+"_2.log")
	require.NoError(t, os.WriteFile(eligiblePath, []byte("eligible for size purge"), 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("active file"), 0o600))
	old := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(eligiblePath, old, old))

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 0, 0, 1, false),
	})
	report, err := manager.RunOnce(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, appendlog.ErrCompressionSourceCleanup)

	// pending cleanupが失敗した周期は、後段のfile series purgeが成功してもsafeへ
	// 戻しません。MaxDirSizeを超えた削除可能fileがあってもdirectory-size purge全体を
	// 次周期へ送り、cleanup中に観測した不完全なdirectory状態では削除しません。
	require.False(t, report.Purge.ByDirSize.Ran)
	require.Empty(t, report.Purge.ByDirSize.RemovalResults)
	require.FileExists(t, eligiblePath)
	require.FileExists(t, latestPath)
	require.DirExists(t, pendingSourcePath)
}

func TestLifecycleReportsPendingSourceConflictAndPreservesConflictingBytes(t *testing.T) {
	const sourceContent = "canonical source data"
	for _, test := range []struct {
		name           string
		pendingContent string
		wantConflict   bool
	}{
		{name: "identical pending is removed", pendingContent: sourceContent},
		{name: "different pending is preserved", pendingContent: "different retired source data", wantConflict: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), boundedwait.HangTimeout)
			t.Cleanup(cancel)
			root := resolvedTempDirForTest(t)
			const target = "pending-source-conflict"
			logDir := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(logDir, 0o750))
			period := currentLogDate()
			sourcePath := filepath.Join(logDir, target+"_"+period+"_1.log")
			latestPath := filepath.Join(logDir, target+"_"+period+"_2.log")
			pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
			require.NoError(t, os.WriteFile(sourcePath, []byte(sourceContent), 0o600))
			require.NoError(t, os.WriteFile(latestPath, []byte("active source"), 0o600))
			require.NoError(t, os.WriteFile(pendingPath, []byte(test.pendingContent), 0o600))
			old := time.Now().Add(-48 * time.Hour)
			require.NoError(t, os.Chtimes(pendingPath, old, old))

			recorder := &recordingUploader{}
			manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
				// 保全fileがage・generation・directory-sizeの対象へ混入しないことも確認します。
				Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, true),
				Uploader: recorder,
			})
			realCompress := manager.compressFile
			manager.compressFile = func(ctx context.Context, path string) (string, error) {
				require.Equal(t, sourcePath, path, "preserved data must not enter compression")
				compressedPath, err := realCompress(ctx, path)
				if err == nil {
					require.Equal(t, sourceContent, readGzipFile(t, compressedPath))
				}
				return compressedPath, err
			}
			lifecycle, err := NewDirectoryLifecycle(manager)
			require.NoError(t, err)
			var reported []lifecycleRunnerReportCall
			for range 2 {
				report, runErr := lifecycle.RunOnce(ctx)
				reported = append(reported, lifecycleRunnerReportCall{report: report, err: runErr})
			}
			require.Len(t, reported, 2, "a reported conflict must not stop the next lifecycle pass")

			preserved, err := filepath.Glob(filepath.Join(logDir, infrafsstore.PreservedDirectoryName, "*.preserved"))
			require.NoError(t, err)
			if test.wantConflict {
				require.ErrorIs(t, reported[0].err, appendlog.ErrCompressionSourceDeletePendingConflict)
				require.Len(t, preserved, 1)
				var conflict *appendlog.CompressionSourceDeletePendingConflictError
				require.ErrorAs(t, reported[0].err, &conflict)
				require.True(t, conflict.ContentsDiffer)
				require.Equal(t, preserved[0], conflict.PreservedPath, "report must identify the preserved data")
				content, err := os.ReadFile(preserved[0])
				require.NoError(t, err)
				require.Equal(t, test.pendingContent, string(content))
				require.Empty(t, reported[0].report.RecoveredCompressionSourceDeletePendingPaths,
					"preservation is an error report, not successful pending-source cleanup")
			} else {
				require.NoError(t, reported[0].err)
				require.Empty(t, preserved)
				require.Equal(t, []string{pendingPath}, reported[0].report.RecoveredCompressionSourceDeletePendingPaths)
			}
			// pendingを解消できれば、元のrawは従来どおり同周期で回復・uploadできます。
			// 次周期でも隔離fileは圧縮・upload・purgeされず、そのまま保全されます。
			require.Len(t, reported[0].report.Compressed, 1)
			require.Len(t, reported[0].report.ObjectStorageUploads, 1)
			require.NoError(t, reported[1].err)
			require.Empty(t, reported[1].report.Compressed)
			require.Empty(t, reported[1].report.ObjectStorageUploads)
			require.Empty(t, reported[1].report.Purge.ByAge.RemovalResults)
			require.Empty(t, reported[1].report.Purge.ByGeneration.RemovalResults)
			require.Empty(t, reported[1].report.Purge.ByDirSize.RemovalResults)
			require.Equal(t, []string{sourcePath + pkgfsstore.ExtensionCompressGzip}, recorder.pathsSnapshot())
			require.NoFileExists(t, sourcePath)
			require.NoFileExists(t, pendingPath)
			require.FileExists(t, latestPath)
		})
	}
}
