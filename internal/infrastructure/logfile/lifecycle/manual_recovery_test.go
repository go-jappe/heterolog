package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestManualRecoveryDurableStorageNotifiesSynchronously(t *testing.T) {
	directory := resolvedTempDirForTest(t)
	path := filepath.Join(directory, "offline_20260101_1.log.gz")
	writeGzipFile(t, path, []byte("recover"))
	runtime := runnerCompressedLogFileUploadSpoolTestRuntimes(t,
		runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), "manual-handoff", pkglogger.LoggerContractSync))
	remote := newDestinationUploaderRecorder("s3-current")
	remoteErr := errors.New("remote unavailable")
	remote.failNext("s3-current", remoteErr)
	var reportedErr error
	wrapped, owner, err := WrapObjectStorageUploader(
		objectstorage.UploadTargetConfig{
			Enabled:                             true,
			CompressedLogFileUploadFailureStore: runnerCompressedLogFileUploadFailureStoreTestConfig(),
		},
		runtime, remote, logpattern.FileNamePattern{Dir: directory, Target: "offline", Extension: "log"},
		clock.NewUTCClock(), runReporterFunc(func(_ context.Context, _ RunReport, err error) {
			reportedErr = errors.Join(reportedErr, err)
		}), nil, true,
	)
	require.NoError(t, err)
	require.Nil(t, owner.(*runnerCompressedLogFileUploadFailureRouteOwner).notifications)
	request, err := uploader.NewUploadRequest(path)
	require.NoError(t, err)
	require.NoError(t, wrapped.Upload(context.Background(), request))
	require.ErrorIs(t, reportedErr, remoteErr)
	require.NoError(t, boundedwait.Close(t, "owner.Close()", owner.Close, boundedwait.ForceCloserOf(owner)))
	require.Equal(t, []string{path}, remote.pathsFor("s3-current"))
}

func TestManualRecoveryFactoryReusesRuntimeSourceAndCursor(t *testing.T) {
	ctx := context.Background()
	cfg := configuredCheckpointFactoryTestConfig(t)
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	runtime, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	manager := runtime[0].fileSeriesManagers[0]
	store := checkpointStoreForFactoryTest(t, manager)
	sourceID := store.SourceID()
	destinations := manager.objectStorageUpload.uploader.(uploader.DestinationUploader).DestinationIDs()
	period := currentLogDate()
	first := writeFileSeriesCheckpointLog(t, manager.fileName.Dir, cfg.FsstoreConfig.Layout.Target, period, 1)
	latest := writeFileSeriesCheckpointLog(t, manager.fileName.Dir, cfg.FsstoreConfig.Layout.Target, period, 2)
	payload, err := os.ReadFile(latest)
	require.NoError(t, err)
	writeGzipFile(t, latest+".gz", payload)
	_, err = store.AdvanceCompletionCursor(ctx, fileCheckpointKey(fileGeneration{Period: period, Index: 1}, false), destinations)
	require.NoError(t, err)
	require.NoError(t, boundedwait.Close(t, "runtime[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes()", runtime[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(runtime[0])))

	recovery, err := NewLogFileRecovery(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	manager = recovery[0].fileSeriesManagers[0]
	require.Equal(t, sourceID, checkpointStoreForFactoryTest(t, manager).SourceID())
	remote := newDestinationUploaderRecorder(destinations...)
	manager.objectStorageUpload.uploader = remote
	report, err := recovery[0].RecoverOnce(ctx)
	require.NoError(t, err)
	require.Len(t, report.UploadedFileRemovals, 2)
	for _, destination := range destinations {
		require.Equal(t, []string{latest + ".gz"}, remote.pathsFor(destination))
	}
	for _, path := range []string{first, first + ".gz", latest, latest + ".gz"} {
		require.NoFileExists(t, path)
	}
	require.NoError(t, boundedwait.Close(t, "recovery[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes()", recovery[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(recovery[0])))
}

func TestManualRecoveryIncludesLatestAndNeverPurges(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "offline"
	lifecycles, err := NewLogFileRecovery(LogFileOptions{Config: pkgfsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target: target, LogPath: root, LogDir: "logs",
				FileNameFormat: pkgfsstore.FileNameFormat, FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				EnableRotate: true, RotationType: pkgfsstore.FileRotationDaily,
				MaxFileBackups: 1, MaxFileAge: 1, MaxDirSize: 1,
				UsageCacheTTL:      time.Hour,
				EnableFileCompress: true, CompressFormat: pkgfsstore.CompressGzip,
			},
		},
	}})
	require.NoError(t, err)
	require.Len(t, lifecycles, 1)
	lifecycle := lifecycles[0]
	closeDirectoryLifecycleOnCleanup(t, lifecycle)
	directory, err := lifecycle.DirectoryPath()
	require.NoError(t, err)
	var sources []string
	// 保持期間1日を過ぎた期間の世代も、手動復旧ではpurgeしません。
	for _, period := range []string{logDateDaysBefore(10), currentLogDate()} {
		source := writeFileSeriesCheckpointLog(t, directory, target, period, 1)
		sources = append(sources, source)
	}
	foreign := filepath.Join(directory, "operator-notes.txt")
	require.NoError(t, os.WriteFile(foreign, []byte("keep"), 0o600))

	report, err := lifecycle.RecoverOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, report.ScannedFiles)
	require.Len(t, report.Compressed, 2)
	require.Empty(t, report.RecoveryErrors)
	require.Equal(t, PurgeReport{}, report.Purge)
	for _, source := range sources {
		require.NoFileExists(t, source)
		require.FileExists(t, source+".gz")
	}
	require.FileExists(t, foreign)

	report, err = lifecycle.RecoverOnce(context.Background())
	require.NoError(t, err)
	require.Empty(t, report.Compressed)
	require.Equal(t, PurgeReport{}, report.Purge)
	for _, source := range sources {
		require.FileExists(t, source+".gz")
	}
}

func TestManualRecoveryUploadFailureDoesNotRetryOrAdvanceFailedDestination(t *testing.T) {
	// 手動復旧のCloseが再試行を待たないことは、実時間の待機ではなく、bubble内の全goroutineが
	// 停止した時点のCloseの戻りで確認します。
	synctest.Test(t, testManualRecoveryUploadFailureDoesNotRetryOrAdvanceFailedDestination)
}

func testManualRecoveryUploadFailureDoesNotRetryOrAdvanceFailedDestination(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "offline-upload"
	const primary, secondary = objectstorage.DestinationID("primary"), objectstorage.DestinationID("secondary")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	store, err := checkpointstore.NewFilesystemStore(resolvedTempDirForTest(t), "offline-upload", checkpointstore.WithSaveRetryDisabled())
	require.NoError(t, err)
	remote := newDestinationUploaderRecorder(primary, secondary)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, true),
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Uploader: remote, CheckpointStore: store,
	})
	manager.manualRecovery = true
	lifecycle, err := NewDirectoryLifecycle(manager)
	require.NoError(t, err)
	// bubbleに残ったgoroutineはbubbleの終了を止めるため、途中で失敗した場合は受理済みの処理を
	// 中止してから期限付きで閉じます。再試行を登録してしまう退行でも、この試験の失敗で終わります。
	forceCloseLifecycleOnFailure(t, lifecycle)
	first := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
	second := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
	failure := errors.New("primary unavailable")
	remote.failNext(primary, failure)

	report, err := lifecycle.RecoverOnce(ctx)
	require.ErrorIs(t, err, failure)
	require.Len(t, report.Compressed, 2)
	require.Len(t, report.RecoveryErrors, 2)
	require.Equal(t, first+".gz", report.RecoveryErrors[0].Path)
	require.Equal(t, second+".gz", report.RecoveryErrors[1].Path)
	require.ErrorIs(t, report.RecoveryErrors[1].Err, ErrOfflineRecoveryAwaitsEarlierGeneration)
	require.Equal(t, []string{first + ".gz"}, remote.pathsFor(primary))
	require.Equal(t, []string{first + ".gz", second + ".gz"}, remote.pathsFor(secondary))
	require.FileExists(t, first+".gz")
	require.FileExists(t, second+".gz")
	require.Equal(t, PurgeReport{}, report.Purge)
	require.False(t, manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(fileGeneration{Period: currentLogDate(), Index: 1}, true)))
	closed := make(chan error, 1)
	go func() { closed <- lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes() }()
	// 再試行を待つCloseはbubble内のtimerで止まるため、全goroutineの停止時点で戻り値がありません。
	synctest.Wait()
	select {
	case err := <-closed:
		require.NoError(t, err)
	default:
		_ = lifecycle.ForceClose()
		boundedwait.ReceiveInBubble(t, closed, "manual recovery Close after ForceClose")
		t.Fatal("manual recovery Close waited for a retry")
	}
	require.Equal(t, []string{first + ".gz"}, remote.pathsFor(primary))

	// 次回実行は宛先別の永続cursorを使い、失敗した宛先だけを世代順に再開します。
	store, err = checkpointstore.NewFilesystemStore(store.Directory(), "offline-upload", checkpointstore.WithSaveRetryDisabled())
	require.NoError(t, err)
	manager = NewFileSeriesManager(FileSeriesManagerOptions{
		Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, true),
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Uploader: remote, CheckpointStore: store,
	})
	manager.manualRecovery = true
	lifecycle, err = NewDirectoryLifecycle(manager)
	require.NoError(t, err)
	forceCloseLifecycleOnFailure(t, lifecycle)
	report, err = lifecycle.RecoverOnce(ctx)
	require.NoError(t, err)
	require.Len(t, report.UploadedFileRemovals, 2)
	require.Equal(t, []string{first + ".gz", first + ".gz", second + ".gz"}, remote.pathsFor(primary))
	require.Equal(t, []string{first + ".gz", second + ".gz"}, remote.pathsFor(secondary))
	require.NoFileExists(t, first+".gz")
	require.NoFileExists(t, second+".gz")
	require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
}

func TestManualRecoveryCompressionFailureAndPanicContinueWithoutRetry(t *testing.T) {
	for _, panicFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%t", panicFailure), func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			const target = "offline-compress"
			directory := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(directory, 0o750))
			first := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
			second := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, true),
				FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			})
			manager.manualRecovery = true
			original := manager.compressFile
			attempts := make(map[string]int)
			failure := errors.New("compression unavailable")
			manager.compressFile = func(ctx context.Context, path string) (string, error) {
				attempts[path]++
				if path == first {
					if panicFailure {
						panic(failure)
					}
					return "", failure
				}
				return original(ctx, path)
			}
			lifecycle, err := NewDirectoryLifecycle(manager)
			require.NoError(t, err)
			report, err := lifecycle.RecoverOnce(context.Background())
			require.ErrorIs(t, err, failure)
			if panicFailure {
				require.ErrorIs(t, err, ErrFileProcessingPanicked)
			}
			require.Len(t, report.RecoveryErrors, 1)
			require.Equal(t, first, report.RecoveryErrors[0].Path)
			require.Len(t, report.Compressed, 1)
			require.FileExists(t, first)
			require.FileExists(t, second+".gz")
			require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
			require.Equal(t, map[string]int{first: 1, second: 1}, attempts)
		})
	}
}

func TestManualRecoveryCheckpointSaveFailureKeepsFileAndCloseReturns(t *testing.T) {
	// 手動復旧のCloseがcheckpoint保存の再試行を待たないことは、実時間の待機ではなく、
	// bubble内の全goroutineが停止した時点のCloseの戻りで確認します。
	synctest.Test(t, testManualRecoveryCheckpointSaveFailureKeepsFileAndCloseReturns)
}

func testManualRecoveryCheckpointSaveFailureKeepsFileAndCloseReturns(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target, sourceID = "offline-save", "manual-save"
	const destination = objectstorage.DestinationID("primary")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	store, err := checkpointstore.NewFilesystemStore(resolvedTempDirForTest(t), sourceID, checkpointstore.WithSaveRetryDisabled())
	require.NoError(t, err)
	remote := newDestinationUploaderRecorder(destination)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, true),
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Uploader: remote, CheckpointStore: store,
	})
	manager.manualRecovery = true
	lifecycle, err := NewDirectoryLifecycle(manager)
	require.NoError(t, err)
	// 途中で失敗した場合は、保存の再試行などbubbleに残る処理を中止してから期限付きで閉じます。
	forceCloseLifecycleOnFailure(t, lifecycle)
	source := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
	blockRunnerCheckpointDocument(t, filepath.Join(store.Directory(), sourceID+".log-file-uploads.json"))

	report, err := lifecycle.RecoverOnce(ctx)
	var pending *checkpointstore.PendingSaveError
	require.ErrorAs(t, err, &pending)
	require.Len(t, report.ObjectStorageUploads, 1)
	require.Len(t, report.RecoveryErrors, 1)
	require.Empty(t, report.UploadedFileRemovals)
	require.FileExists(t, source+".gz")
	closed := make(chan error, 1)
	go func() { closed <- lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes() }()
	// 保存の再試行を待つCloseはbubble内のtimerで止まるため、全goroutineの停止時点で戻り値がありません。
	synctest.Wait()
	select {
	case err := <-closed:
		require.Error(t, err)
	default:
		_ = lifecycle.ForceClose()
		boundedwait.ReceiveInBubble(t, closed, "manual Close after ForceClose")
		t.Fatal("manual Close waited for checkpoint persistence")
	}
	require.FileExists(t, source+".gz")
	require.Equal(t, []string{source + ".gz"}, remote.pathsFor(destination))
}

func TestManualFailureArchiveRecoverySupportsInlineModeAndLatestJSONL(t *testing.T) {
	cfg := newFailureArchiveLifecycleDurableConfig(t, objectstorage.UploadTargetConfig{})
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.EnableFileCompress = true
	cfg.CompressFormat = pkgfsstore.CompressGzip
	lifecycles, err := NewFailureArchiveRecovery(FailureArchiveOptions{Config: newFailureArchiveTestConfigAdapter(cfg)})
	require.NoError(t, err)
	require.Len(t, lifecycles, 2)
	for _, lifecycle := range lifecycles {
		directory, err := lifecycle.DirectoryPath()
		require.NoError(t, err)
		path := filepath.Join(directory, cfg.Target+"_"+currentLogDate()+"_1.jsonl")
		require.NoError(t, os.WriteFile(path, []byte("{\"message\":\"recover\"}\n"), 0o600))
		report, err := lifecycle.RecoverOnce(context.Background())
		require.NoError(t, err)
		require.Len(t, report.Compressed, 1)
		require.FileExists(t, path+".gz")
		require.NoFileExists(t, path)
		require.Equal(t, PurgeReport{}, report.Purge)
		require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
	}
}
