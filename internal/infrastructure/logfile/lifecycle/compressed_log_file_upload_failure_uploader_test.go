package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/compressedlogfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

func TestLifecycleUploadWithoutFailureStoreReturnsOriginalUploader(t *testing.T) {
	for _, types := range [][]failurestore.DurableType{nil, {failurestore.DurableTypeNone}} {
		t.Run(fmt.Sprint(types), func(t *testing.T) {
			remote := newDestinationUploaderRecorder("s3-current")
			wrapped, owner, err := WrapObjectStorageUploader(
				objectstorage.UploadTargetConfig{
					CompressedLogFileUploadFailureStore: objectstorage.CompressedLogFileUploadFailureStoreConfig{DurableType: types},
				},
				nil, remote, logpattern.FileNamePattern{}, clock.NewUTCClock(),
				runReporterFunc(func(context.Context, RunReport, error) {
					t.Error("disabled failure store must not report")
				}),
				nil,
				false,
			)
			require.NoError(t, err)
			require.Same(t, remote, wrapped)
			require.Nil(t, owner)
		})
	}
}

func TestLifecycleUploadStillRejectsInvalidFailureStoreTypes(t *testing.T) {
	wrapped, owner, err := WrapObjectStorageUploader(
		objectstorage.UploadTargetConfig{
			CompressedLogFileUploadFailureStore: objectstorage.CompressedLogFileUploadFailureStoreConfig{
				DurableType: []failurestore.DurableType{failurestore.DurableTypeNone, failurestore.DurableTypeSpool},
			},
		},
		nil, newDestinationUploaderRecorder("s3-current"), logpattern.FileNamePattern{}, clock.NewUTCClock(),
		runReporterFunc(func(context.Context, RunReport, error) {}),
		nil,
		false,
	)
	require.Error(t, err)
	require.Nil(t, wrapped)
	require.Nil(t, owner)
}

func TestLifecycleCompressedLogFileUploadSpoolHandoffUsesContractRootAndDistinctFileSeriesRoutes(t *testing.T) {
	ctx := context.Background()
	spoolPath := resolvedTempDirForTest(t)
	spoolCfg := runnerCompressedLogFileUploadSpoolTestConfig(
		spoolPath,
		"runner-compressed_log_files",
		pkglogger.LoggerContractSync,
	)
	spoolRuntime := runnerCompressedLogFileUploadSpoolTestRuntimes(t, spoolCfg)
	root, err := objectstoragespool.NewRoot(
		spoolPath,
		"runner-compressed_log_files",
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(
		spoolPath,
		"runner-compressed_log_files",
		"spool",
		"objectstorage",
		"s3",
		"logger",
	), root.Path())

	type routeCase struct {
		class      string
		fileName   logpattern.FileNamePattern
		compressed string
	}
	logDirectory := filepath.Join(resolvedTempDirForTest(t), "logs")
	routes := []routeCase{
		{
			class: string(fileClassDefault),
			fileName: logpattern.FileNamePattern{
				Dir: logDirectory, Target: "app", Extension: "log",
			},
			compressed: "app_20260904_1.log.gz",
		},
		{
			class: string(fileClassError),
			fileName: logpattern.FileNamePattern{
				Dir: logDirectory, Prefix: "error_", Target: "app", Extension: "log",
			},
			compressed: "error_app_20260904_1.log.zst",
		},
		{
			class: string(fileClassDebug),
			fileName: logpattern.FileNamePattern{
				Dir: logDirectory, Prefix: "debug_", Target: "app", Extension: "log",
			},
			compressed: "debug_app_20260904_1.log.gz",
		},
	}

	owners := make([]ObjectStorageUploadFailureRouteOwner, 0, len(routes))
	remotes := make([]*destinationUploaderRecorder, 0, len(routes))
	wantFileNames := make(map[string]struct{}, len(routes))
	for _, route := range routes {
		require.NoError(t, os.MkdirAll(route.fileName.Dir, 0o750))
		path := filepath.Join(route.fileName.Dir, route.compressed)
		require.NoError(t, os.WriteFile(path, []byte(route.class), 0o640))

		remote := newDestinationUploaderRecorder("s3-current")
		remote.failNext("s3-current", errors.New("temporary upload failure"))
		wrapped, owner, wrapErr := WrapObjectStorageUploader(
			objectstorage.UploadTargetConfig{
				Enabled:                             true,
				CompressedLogFileUploadFailureStore: runnerCompressedLogFileUploadFailureStoreTestConfig(),
			},
			spoolRuntime,
			remote,
			route.fileName,
			clock.NewUTCClock(),
			nil,
			nil,
			false,
		)
		require.NoError(t, wrapErr)
		require.IsType(t, &compressedlogfailure.ManagedUploader{}, wrapped)
		require.NotNil(t, owner)
		owners = append(owners, owner)
		remotes = append(remotes, remote)

		require.NoError(t, wrapped.Upload(ctx, uploadertest.Request(t, path)))
		wantFileNames[route.compressed] = struct{}{}
	}
	require.NoError(t, closeObjectStorageCompressedLogFileUploadFailureRouteOwners(owners))

	store, err := objectstoragespool.NewDestinationStores(root)
	require.NoError(t, err)
	active, err := store.ListFiles(ctx, pkgspool.StateActive)
	require.NoError(t, err)
	done, err := store.ListFiles(ctx, pkgspool.StateDone)
	require.NoError(t, err)
	// Replay is ticker-only and this test config uses a one-hour interval. A
	// direct failure therefore performs exactly one PUT and leaves every handed-
	// off file active until a later runner tick.
	require.Len(t, active, len(routes))
	require.Empty(t, done)
	gotFileNames := make(map[string]struct{}, len(active))
	for _, file := range active {
		gotFileNames[file.FileName] = struct{}{}
	}
	require.Equal(t, wantFileNames, gotFileNames)
	for _, remote := range remotes {
		require.Len(t, remote.pathsFor("s3-current"), 1, "direct uploader must run once")
	}
}

func TestLifecycleCompressedLogFileUploadSpoolLeavesRawAndNonCanonicalPathsWithProducer(t *testing.T) {
	ctx := context.Background()
	spoolPath := resolvedTempDirForTest(t)
	directory := resolvedTempDirForTest(t)
	fileName := logpattern.FileNamePattern{
		Dir: directory, Target: "app", Extension: "log",
	}
	remoteErr := errors.New("remote unavailable")
	remote := newDestinationUploaderRecorder("s3-current")
	wrapped, owner, err := WrapObjectStorageUploader(
		objectstorage.UploadTargetConfig{
			Enabled:                             true,
			CompressedLogFileUploadFailureStore: runnerCompressedLogFileUploadFailureStoreTestConfig(),
		},
		runnerCompressedLogFileUploadSpoolTestRuntimes(t, runnerCompressedLogFileUploadSpoolTestConfig(spoolPath, "raw-exclusion", pkglogger.LoggerContractSync)),
		remote,
		fileName,
		clock.NewUTCClock(),
		nil,
		nil,
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, owner)
	defer func() {
		require.NoError(t, boundedwait.Close(t, "owner.Close()", owner.Close, boundedwait.ForceCloserOf(owner)))
	}()

	paths := []string{
		filepath.Join(directory, "app_20260904_1.log"),
		filepath.Join(directory, "app_20260904_01.log.gz"),
		filepath.Join(directory, "app_20260904_1.log.gz.tmp"),
		filepath.Join(resolvedTempDirForTest(t), "app_20260904_1.log.gz"),
	}
	for _, path := range paths {
		require.NoError(t, os.WriteFile(path, []byte("must stay local"), 0o640))
		remote.failNext("s3-current", remoteErr)
		uploadErr := wrapped.Upload(ctx, uploadertest.Request(t, path))
		require.ErrorIs(t, uploadErr, remoteErr)
	}

	root, err := objectstoragespool.NewRoot(
		spoolPath,
		"raw-exclusion",
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	require.NoError(t, err)
	store, err := objectstoragespool.NewStore(root)
	require.NoError(t, err)
	active, err := store.ListFiles(ctx, pkgspool.StateActive)
	require.NoError(t, err)
	require.Empty(t, active)
}

func TestLifecycleFactoryWrapsNormalLogFileSeries(t *testing.T) {
	spoolPath := resolvedTempDirForTest(t)
	normalCfg := configuredCheckpointFactoryTestConfig(t)
	normalCfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	normalCfg.FsstoreConfig.Layout.ErrorLogDir = "error-logs"
	normalCfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error_"
	normalCfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
	normalCfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug_"
	normalCfg.ObjectStorageUploadConfig.Error = normalCfg.ObjectStorageUploadConfig.Default
	normalCfg.ObjectStorageUploadConfig.Debug = normalCfg.ObjectStorageUploadConfig.Default
	spoolCfg := runnerCompressedLogFileUploadSpoolTestConfig(
		spoolPath,
		normalCfg.FsstoreConfig.Layout.Target,
		pkglogger.LoggerContractSync,
	)
	normalCfg.ObjectStorageUploadConfig.CompressedLogFileUploadSpool = objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(spoolCfg)
	normalCfg.ObjectStorageUploadConfig.Default.CompressedLogFileUploadFailureStore = runnerCompressedLogFileUploadFailureStoreTestConfig()
	normalCfg.ObjectStorageUploadConfig.Error.CompressedLogFileUploadFailureStore = runnerCompressedLogFileUploadFailureStoreTestConfig()
	normalCfg.ObjectStorageUploadConfig.Debug.CompressedLogFileUploadFailureStore = runnerCompressedLogFileUploadFailureStoreTestConfig()

	runtimes := runnerCompressedLogFileUploadSpoolTestRuntimes(t, spoolCfg)
	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: normalCfg, SpoolRuntimes: runtimes})
	require.NoError(t, err)
	require.Len(t, fileSeriesManagersForLifecycles(lifecycles), 3)
	for _, lifecycle := range lifecycles {
		closeDirectoryLifecycleOnCleanup(t, lifecycle)
		for _, manager := range lifecycle.fileSeriesManagers {
			require.IsType(
				t,
				&compressedlogfailure.ManagedUploader{},
				manager.objectStorageUpload.uploader,
			)
		}
	}
	for _, lifecycle := range lifecycles {
		require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
	}
	// 全系列のrouteを閉じても、構築側の所有権は明示的にCloseするまで残ります。
	_, acquireErr := compressedlog.NewRootRuntimes(compressedlog.NewConfigAdapters(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(spoolCfg), []objectstorage.UploadTarget{objectstorage.UploadTargetS3})...)
	require.ErrorIs(t, acquireErr, compressedlog.ErrSpoolRootAlreadyOwned)
	require.NoError(t, boundedwait.Close(t, "runtimes.Close()", runtimes.Close, boundedwait.ForceCloserOf(runtimes)))
	replacement := runnerCompressedLogFileUploadSpoolTestRuntimes(t, spoolCfg)
	require.NoError(t, boundedwait.Close(t, "replacement.Close()", replacement.Close, boundedwait.ForceCloserOf(replacement)))
}

func TestLifecycleFactoryKeepsCallerOwnedSpoolRuntimeOpen(t *testing.T) {
	config := configuredCheckpointFactoryTestConfig(t)
	spoolConfig := runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), config.FsstoreConfig.Layout.Target, pkglogger.LoggerContractSync)
	config.ObjectStorageUploadConfig.CompressedLogFileUploadSpool = objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(spoolConfig)
	config.ObjectStorageUploadConfig.Default.CompressedLogFileUploadFailureStore = runnerCompressedLogFileUploadFailureStoreTestConfig()
	runtime := runnerCompressedLogFileUploadSpoolTestRuntimes(t, spoolConfig)
	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: config, SpoolRuntimes: runtime})
	require.NoError(t, err)
	for _, lifecycle := range lifecycles {
		require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
	}
	// runnerのrouteを閉じても、同じ呼び出し元がlazy系列を追加できる状態を保ちます。
	_, route, err := WrapObjectStorageUploader(
		config.ObjectStorageUploadConfig.Default,
		runtime,
		newDestinationUploaderRecorder("s3-current"),
		logpattern.FileNamePattern{Dir: resolvedTempDirForTest(t), Target: "app", Extension: "log"},
		clock.NewUTCClock(),
		nil,
		nil,
		false,
	)
	require.NoError(t, err)
	require.NoError(t, boundedwait.Close(t, "route.Close()", route.Close, boundedwait.ForceCloserOf(route)))
}

func TestLifecycleFactoryKeepsCallerOwnedSpoolRuntimeOnCheckpointFailure(t *testing.T) {
	config := configuredCheckpointFactoryTestConfig(t)
	spoolConfig := runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), config.FsstoreConfig.Layout.Target, pkglogger.LoggerContractSync)
	config.ObjectStorageUploadConfig.CompressedLogFileUploadSpool = objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(spoolConfig)
	config.ObjectStorageUploadConfig.Default.CompressedLogFileUploadFailureStore = runnerCompressedLogFileUploadFailureStoreTestConfig()
	config.ArchiveCheckpointConfig.SourcePrefix = ""
	runtimes := runnerCompressedLogFileUploadSpoolTestRuntimes(t, spoolConfig)
	_, err := NewLogFileLifecycles(LogFileOptions{Config: config, SpoolRuntimes: runtimes})
	require.ErrorIs(t, err, ErrInvalidCheckpointConfiguration)
	// 構築失敗でも、呼び出し元が渡したruntimeの所有権は変更しません。
	_, acquireErr := compressedlog.NewRootRuntimes(compressedlog.NewConfigAdapters(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(spoolConfig), []objectstorage.UploadTarget{objectstorage.UploadTargetS3})...)
	require.ErrorIs(t, acquireErr, compressedlog.ErrSpoolRootAlreadyOwned)
	require.NoError(t, boundedwait.Close(t, "runtimes.Close()", runtimes.Close, boundedwait.ForceCloserOf(runtimes)))
	replacement := runnerCompressedLogFileUploadSpoolTestRuntimes(t, spoolConfig)
	require.NoError(t, boundedwait.Close(t, "replacement.Close()", replacement.Close, boundedwait.ForceCloserOf(replacement)))
}

func TestLifecycleFactoryRequiresSharedSpoolRuntime(t *testing.T) {
	config := configuredCheckpointFactoryTestConfig(t)
	spoolConfig := runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), config.FsstoreConfig.Layout.Target, pkglogger.LoggerContractSync)
	config.ObjectStorageUploadConfig.CompressedLogFileUploadSpool = objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(spoolConfig)
	config.ObjectStorageUploadConfig.Default.CompressedLogFileUploadFailureStore = runnerCompressedLogFileUploadFailureStoreTestConfig()
	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: config})
	require.ErrorIs(t, err, compressedlogfailure.ErrInvalidConfig)
	require.ErrorIs(t, err, compressedlogfailure.ErrNoMatchingSpoolRuntime)
	require.Nil(t, lifecycles)
	entries, err := os.ReadDir(spoolConfig.SpoolPath)
	require.NoError(t, err)
	require.Empty(t, entries, "missing runtime must not trigger spool construction")
}

func TestLifecycleCloseObjectStorageCompressedLogFileUploadFailureRoutesReleasesOwnershipOnce(t *testing.T) {
	owner := &countingCompressedLogFileUploadFailureRouteOwner{}
	lifecycle := &DirectoryLifecycle{
		objectStorageCompressedLogFileUploadFailureRouteOwners: []ObjectStorageUploadFailureRouteOwner{owner},
	}
	require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
	require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
	require.Equal(t, 1, owner.closeCalls)
}

func TestLifecycleCloseCompressedLogRoutesClosesAllOwnersOnceAndKeepsErrors(t *testing.T) {
	finalErr := errors.New("completed resource close error")
	otherErr := errors.New("another resource close error")
	completed := &countingCompressedLogFileUploadFailureRouteOwner{}
	failed := &sequenceCompressedLogRouteOwner{errors: []error{finalErr}}
	otherFailed := &sequenceCompressedLogRouteOwner{errors: []error{otherErr}}
	lifecycle := &DirectoryLifecycle{
		objectStorageCompressedLogFileUploadFailureRouteOwners: []ObjectStorageUploadFailureRouteOwner{failed, nil, completed, otherFailed},
	}
	err := lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()
	require.ErrorIs(t, err, finalErr)
	require.ErrorIs(t, err, otherErr)
	require.Len(t, lifecycle.objectStorageCompressedLogFileUploadFailureRouteOwners, 4, "closed owners remain available for notification drain and force cancellation")

	err = lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()
	require.ErrorIs(t, err, finalErr)
	require.ErrorIs(t, err, otherErr)
	require.Equal(t, 1, completed.closeCalls)
	require.Equal(t, 1, failed.closeCalls)
	require.Equal(t, 1, otherFailed.closeCalls)
}

func TestLifecycleCompressedLogRouteClosesNotificationsAfterManagedResources(t *testing.T) {
	_, owner, err := WrapObjectStorageUploader(
		objectstorage.UploadTargetConfig{CompressedLogFileUploadFailureStore: runnerCompressedLogFileUploadFailureStoreTestConfig()},
		runnerCompressedLogFileUploadSpoolTestRuntimes(t, runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), "close-notifications", pkglogger.LoggerContractSync)),
		newDestinationUploaderRecorder("s3-current"),
		logpattern.FileNamePattern{Dir: resolvedTempDirForTest(t), Target: "app", Extension: "log"},
		clock.NewUTCClock(),
		&compressedLogFileUploadFailureNotificationRunReporter{},
		nil,
		false,
	)
	require.NoError(t, err)
	route := owner.(*runnerCompressedLogFileUploadFailureRouteOwner)
	require.NoError(t, boundedwait.Close(t, "route.Close()", route.Close, boundedwait.ForceCloserOf(route)))
	require.NoError(t, boundedwait.Close(t, "route.Close()", route.Close, boundedwait.ForceCloserOf(route)))
	select {
	case <-route.notifications.Done():
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("notifications did not stop after resources closed")
	}
	request := uploadertest.Request(t, filepath.Join(resolvedTempDirForTest(t), "app_20260904_1.log.gz"))
	require.ErrorIs(t, route.managed.Upload(context.Background(), request), compressedlogfailure.ErrUploaderClosed)
}

func TestLifecycleCompressedLogFileUploadSpoolNotifiesUploadFailureAndSavedState(t *testing.T) {
	directory := resolvedTempDirForTest(t)
	fileName := logpattern.FileNamePattern{
		Dir: directory, Target: "app", Extension: "log",
	}
	path := filepath.Join(directory, "app_20260904_1.log.gz")
	require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o640))
	remoteErr := errors.New("remote unavailable")
	remote := newDestinationUploaderRecorder("s3-current")
	remote.failNext("s3-current", remoteErr)
	reporter := &compressedLogFileUploadFailureNotificationRunReporter{}
	wrapped, owner, err := WrapObjectStorageUploader(
		objectstorage.UploadTargetConfig{
			Enabled:                             true,
			CompressedLogFileUploadFailureStore: runnerCompressedLogFileUploadFailureStoreTestConfig(),
		},
		runnerCompressedLogFileUploadSpoolTestRuntimes(t, runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), "upload-failure-notification", pkglogger.LoggerContractSync)),
		remote,
		fileName,
		clock.NewUTCClock(),
		reporter,
		nil,
		false,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, boundedwait.Close(t, "owner.Close()", owner.Close, boundedwait.ForceCloserOf(owner)))
	}()

	require.NoError(t, wrapped.Upload(context.Background(), uploadertest.Request(t, path)))
	require.Len(t, remote.pathsFor("s3-current"), 1, "direct uploader must run once")
	require.Eventually(t, func() bool {
		_, reportErr := reporter.snapshot()
		return reportErr != nil
	}, boundedwait.HangTimeout, time.Millisecond)
	report, reportErr := reporter.snapshot()
	require.Equal(t, pathidentity.AbsoluteOrCleanPath(directory), report.DirectoryPath)
	require.ErrorIs(t, reportErr, remoteErr)
	var handoff *UploadFailureHandoffError
	require.ErrorAs(t, reportErr, &handoff)
	require.Equal(t, path, handoff.SourcePath)
	require.Len(t, handoff.SavedTargets, 1)
	spool, ok := handoff.SavedTargets[0].(compressedlogfailure.SavedSpoolDetails)
	require.True(t, ok, "the failed upload is handed off to the spool: %#v", handoff.SavedTargets[0])
	require.Equal(t, pkgspool.StateActive, spool.State)
}

func TestLifecycleCompressedLogFileUploadSpoolMaintenanceObserverReportsOperationalError(t *testing.T) {
	reporter := &compressedLogFileUploadFailureNotificationRunReporter{}
	config := runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), "maintenance-report", pkglogger.LoggerContractSync)
	observer := compressedLogFileUploadFailureSpoolRunObserver(reporter, runnerCompressedLogFileUploadSpoolTestRuntimes(t, config)[objectstorage.UploadTargetS3])
	require.NotNil(t, observer)

	observer(context.Background(), nil, nil, nil)
	_, reportErr := reporter.snapshot()
	require.NoError(t, reportErr)

	wantErr := errors.New("cannot scan compressed log file spool")
	observer(context.Background(), nil, nil, wantErr)
	root, err := objectstoragespool.NewRoot(
		config.SpoolPath,
		config.Target,
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	require.NoError(t, err)
	report, reportErr := reporter.snapshot()
	require.Equal(t, root.Path(), report.DirectoryPath)
	require.ErrorIs(t, reportErr, wantErr)
}

func TestLifecycleCompressedLogFileUploadSpoolMaintenanceObserverKeepsResolvedDirectory(t *testing.T) {
	reporter := &compressedLogFileUploadFailureNotificationRunReporter{}
	config := runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), "maintenance-report", pkglogger.LoggerContractSync)
	applicationDirectory := filepath.Join(config.SpoolPath, config.Target)
	require.NoError(t, os.MkdirAll(applicationDirectory, 0o750))
	root, err := objectstoragespool.NewRoot(config.SpoolPath, config.Target, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	require.NoError(t, err)
	observer := compressedLogFileUploadFailureSpoolRunObserver(reporter, runnerCompressedLogFileUploadSpoolTestRuntimes(t, config)[objectstorage.UploadTargetS3])
	require.NotNil(t, observer)

	// 構築後にパスを再解決できなくなっても、通知では最初に確定した保存先を使います。
	// spoolの位置を通常ファイルで塞ぎ、再解決すれば失敗する状態を作ります。
	require.NoError(t, os.WriteFile(filepath.Join(applicationDirectory, "spool"), []byte("not a directory"), 0o600))
	_, err = objectstoragespool.NewRoot(config.SpoolPath, config.Target, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	require.Error(t, err)

	wantErr := errors.New("cannot scan compressed log file spool")
	observer(context.Background(), nil, nil, wantErr)
	report, reportErr := reporter.snapshot()
	require.Equal(t, pathidentity.AbsoluteOrCleanPath(root.Path()), report.DirectoryPath)
	require.ErrorIs(t, reportErr, wantErr)
}

func TestLifecycleCompressedLogFileUploadSpoolMaintenanceObserverSkipsMissingRuntime(t *testing.T) {
	reporter := &compressedLogFileUploadFailureNotificationRunReporter{}
	require.Nil(t, compressedLogFileUploadFailureSpoolRunObserver(reporter, nil))
	require.Nil(t, compressedLogFileUploadFailureSpoolRunObserver(nil, nil))
}

func TestLifecycleCompressedLogFileUploadFailureArchivePurgeObserverPreservesResults(
	t *testing.T,
) {
	config := objectstorage.CompressedLogFileUploadFailureArchiveConfig{
		RootPath:       resolvedTempDirForTest(t),
		LoggerContract: pkglogger.LoggerContractSync.String(),
	}
	wantRoot, err := compressedlogarchive.NewRoot(
		config.RootPath,
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	require.NoError(t, err)
	failureCategoryErr := errors.New("unsafe archive entry retained")
	for _, tc := range []struct {
		name   string
		report compressedlogarchive.PurgeReport
		err    error
	}{
		{
			name: "successful purge",
			report: compressedlogarchive.PurgeReport{PreservedFileCount: 3, ByFailureCategory: map[uploader.FailureCategory]compressedlogarchive.FailureCategoryPurgeReport{
				uploader.FailureCategoryRetryable: {
					Enabled: true, ScannedEntryCount: 5, ScannedBytes: 500,
					DeletedFileCount: 2, DeletedBytes: 200, RemainingEntryCount: 3, RemainingBytes: 300,
				},
				uploader.FailureCategoryPermanent: {Enabled: false},
			}},
		},
		{
			name: "failure category error and exceeded capacity",
			report: compressedlogarchive.PurgeReport{ByFailureCategory: map[uploader.FailureCategory]compressedlogarchive.FailureCategoryPurgeReport{
				uploader.FailureCategoryRetryable: {
					Enabled: true, ScannedEntryCount: 8, ScannedBytes: 900,
					DeletedFileCount: 2, DeletedBytes: 300, RemainingEntryCount: 6, RemainingBytes: 600,
					RetainedUnsafeEntryCount: 1, CapacityLimitExceeded: true, Errors: []error{failureCategoryErr},
				},
				uploader.FailureCategoryPermanent: {
					Enabled: true, ScannedEntryCount: 1, ScannedBytes: 100,
					DeletedFileCount: 1, DeletedBytes: 100,
				},
			}},
			err: failureCategoryErr,
		},
		{name: "empty result"},
		{
			name: "disabled purge",
			report: compressedlogarchive.PurgeReport{ByFailureCategory: map[uploader.FailureCategory]compressedlogarchive.FailureCategoryPurgeReport{
				uploader.FailureCategoryRetryable: {Enabled: false},
				uploader.FailureCategoryPermanent: {Enabled: false},
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), struct{}{}, tc.name)
			var gotContext context.Context
			var gotReport RunReport
			var gotErr error
			calls := 0
			reporter := runReporterFunc(func(ctx context.Context, report RunReport, err error) {
				calls++
				gotContext, gotReport, gotErr = ctx, report, err
			})
			observer := compressedLogFileUploadFailureArchivePurgeRunObserver(reporter, wantRoot)
			require.NotNil(t, observer)
			observer(ctx, tc.report, tc.err)

			require.Equal(t, 1, calls)
			require.Same(t, ctx, gotContext)
			require.Equal(t, wantRoot.Path(), gotReport.DirectoryPath)
			require.NotNil(t, gotReport.UploadFailureArchivePurge)
			require.Equal(t, tc.report, *gotReport.UploadFailureArchivePurge)
			require.Equal(t, PurgeReport{}, gotReport.Purge, "archive purge must remain separate from normal appendlog purge")
			if tc.err == nil {
				require.NoError(t, gotErr)
			} else {
				require.ErrorIs(t, gotErr, tc.err)
				require.ErrorIs(t, gotErr, ErrUploadFailureArchiveLifecycle)
			}
		})
	}
}

type runnerTestSavedTargetDetails string

func (runnerTestSavedTargetDetails) Kind() failurestore.DurableType { return "third-test-store" }
func (r runnerTestSavedTargetDetails) String() string               { return string(r) }

func TestLifecycleCompressedLogNotifierNotifiesSavedTargetsWithoutKnowingTheirKind(t *testing.T) {
	var received []error
	notify := compressedLogFileUploadFailureNotifier(runReporterFunc(func(_ context.Context, _ RunReport, err error) {
		received = append(received, err)
	}))
	uploadErr := errors.New("upload unavailable")
	notify(context.Background(), compressedlogfailure.Notification{
		SourcePath:      "app_20260929_1.log.gz",
		DestinationID:   "destination-a",
		FailureCategory: uploader.FailureCategoryRetryable,
		UploadErr:       uploadErr,
		SavedTargets: []compressedlogfailure.SavedTargetDetails{
			runnerTestSavedTargetDetails("third-test-store: /other/file.log.gz"),
		},
	})
	require.Len(t, received, 1)
	require.ErrorIs(t, received[0], uploadErr)
	var handoff *UploadFailureHandoffError
	require.ErrorAs(t, received[0], &handoff)
	require.Equal(t, []compressedlogfailure.SavedTargetDetails{
		runnerTestSavedTargetDetails("third-test-store: /other/file.log.gz"),
	}, handoff.SavedTargets)
	require.Equal(t, uploader.FailureCategoryRetryable, handoff.FailureCategory)
	require.Equal(t, objectstorage.DestinationID("destination-a"), handoff.DestinationID)
	require.Equal(t, "app_20260929_1.log.gz", handoff.SourcePath)
}

func TestLifecycleCompressedLogNotifierDoesNotClaimFailedStorage(t *testing.T) {
	reporter := &compressedLogFileUploadFailureNotificationRunReporter{}
	notify := compressedLogFileUploadFailureNotifier(reporter)
	require.NotNil(t, notify)

	notify(context.Background(), compressedlogfailure.Notification{
		SourcePath: "app_20260904_1.log.gz",
		UploadErr:  errors.New("remote unavailable"),
		SaveErr:    errors.New("durable publish failed"),
	})
	_, reportErr := reporter.snapshot()
	require.NoError(t, reportErr)
}

func TestLifecycleCompressedLogNotifierNotifiesExistingFileCheckError(t *testing.T) {
	var received []error
	notify := compressedLogFileUploadFailureNotifier(runReporterFunc(func(_ context.Context, _ RunReport, err error) {
		received = append(received, err)
	}))
	injected := errors.New("cannot inspect existing spool")
	notify(context.Background(), compressedlogfailure.Notification{
		SourcePath: "app_20260929_1.log.gz", DestinationID: "destination-a", ExistingFileCheckErr: injected,
	})
	require.Len(t, received, 1)
	require.ErrorIs(t, received[0], injected)
	require.ErrorIs(t, received[0], ErrExistingUploadFailureFileCheck)
	var handoff *UploadFailureHandoffError
	require.False(t, errors.As(received[0], &handoff), "a failed check must not claim that the file was saved")
}

func TestLifecycleCompressedLogNotifierNotifiesPreservationWarningWithoutUploadError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var gotContext context.Context
	var gotReport RunReport
	var gotErr error
	calls := 0
	notify := compressedLogFileUploadFailureNotifier(runReporterFunc(func(ctx context.Context, report RunReport, err error) {
		calls++
		gotContext, gotReport, gotErr = ctx, report, err
	}))
	warning := errors.New("conflicting saved target was retained outside purge")
	source := filepath.Join(resolvedTempDirForTest(t), "app_20261003_1.log.gz")
	notify(ctx, compressedlogfailure.Notification{
		SourcePath: source, DestinationID: "destination-a", PreservationWarning: warning,
	})
	require.Equal(t, 1, calls, "warning-only notifications must be delivered before the no-upload-error return")
	require.Same(t, ctx, gotContext)
	require.NoError(t, gotErr, "synchronous preservation allows the upload to continue")
	require.Equal(t, pathidentity.AbsoluteOrCleanPath(filepath.Dir(source)), gotReport.DirectoryPath)
	require.ErrorIs(t, gotReport.CheckpointWarning, warning)
	var preserved *UploadFailureNotificationError
	require.ErrorAs(t, gotReport.CheckpointWarning, &preserved)
	require.ErrorIs(t, preserved, ErrUploadProgressPreserved)
	require.Equal(t, source, preserved.SourcePath)
	require.Equal(t, objectstorage.DestinationID("destination-a"), preserved.DestinationID)
	require.Equal(t, RunReport{DirectoryPath: gotReport.DirectoryPath, CheckpointWarning: gotReport.CheckpointWarning}, gotReport,
		"a preservation warning must not fabricate a successful lifecycle result")
}

func TestLifecycleCompressedLogSpoolWarningObserverUsesStandaloneWarningReport(t *testing.T) {
	runtimes := runnerCompressedLogFileUploadSpoolTestRuntimes(t, runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), "spool-warning", pkglogger.LoggerContractSync))
	runtime := runtimes[objectstorage.UploadTargetS3]
	var gotReport RunReport
	var gotErr error
	calls := 0
	observe := compressedLogFileUploadFailureSpoolWarningObserver(runReporterFunc(func(_ context.Context, report RunReport, err error) {
		calls++
		gotReport, gotErr = report, err
	}), runtime)
	warning := errors.New("invalid spool file retained in .preserved")
	observe(context.Background(), warning)
	observe(context.Background(), nil)
	require.Equal(t, 1, calls)
	require.NoError(t, gotErr)
	require.Equal(t, RunReport{
		DirectoryPath: pathidentity.AbsoluteOrCleanPath(runtime.Root().Path()),
		SpoolWarning:  warning,
	}, gotReport)
}

func TestLifecycleCompressedLogNotifierNotifiesPreservationFailureAsCheckpointError(t *testing.T) {
	var received []error
	notify := compressedLogFileUploadFailureNotifier(runReporterFunc(func(_ context.Context, report RunReport, err error) {
		require.Nil(t, report.CheckpointWarning, "failed preservation must not claim a safely retained conflict")
		received = append(received, err)
	}))
	failure := errors.New("cannot preserve conflicting saved target")
	notify(context.Background(), compressedlogfailure.Notification{
		SourcePath: "app_20261003_1.log.gz", DestinationID: "destination-a",
		CheckpointErr: failure, SaveErr: failure,
	})
	require.Len(t, received, 1, "the unsuccessful handoff must not also be reported as accepted")
	require.ErrorIs(t, received[0], failure)
	require.ErrorIs(t, received[0], ErrUploadProgressSave)
	var saveErr *UploadFailureNotificationError
	require.ErrorAs(t, received[0], &saveErr)
	require.Equal(t, "app_20261003_1.log.gz", saveErr.SourcePath)
	require.Equal(t, objectstorage.DestinationID("destination-a"), saveErr.DestinationID)
}

func TestLifecycleCompressedLogNotifierNotifiesPreservedConflictEvenWhenSaveFails(t *testing.T) {
	var received []error
	notify := compressedLogFileUploadFailureNotifier(runReporterFunc(func(_ context.Context, _ RunReport, err error) {
		received = append(received, err)
	}))
	notify(context.Background(), compressedlogfailure.Notification{
		SourcePath:    "app_20260929_1.log.gz",
		DestinationID: "destination-a",
		SaveErr:       errors.New("save failed after preserving conflict"),
		ConflictRenames: []infrafsstore.ConflictRename{{
			OriginalPath: "/archive/retryable/app_20260929_1.log.gz",
			RetainedPath: "/archive/retryable/.compressed-log-conflict-preserved",
		}},
	})
	require.Len(t, received, 1)
	// 保全したファイルは自動削除・再送しないため、運用者の確認が必要な保全として通知します。
	var conflict *UploadFailureConflictPreservedError
	require.ErrorAs(t, received[0], &conflict)
	require.Equal(t, UploadFailureConflictPreservedError{
		OriginalPath:  "/archive/retryable/app_20260929_1.log.gz",
		RetainedPath:  "/archive/retryable/.compressed-log-conflict-preserved",
		DestinationID: "destination-a",
	}, *conflict)
}

func TestLifecycleCompressedLogNotificationCanCloseOwnerAfterCurrentUploadReturns(t *testing.T) {
	directory := resolvedTempDirForTest(t)
	fileName := logpattern.FileNamePattern{
		Dir: directory, Target: "app", Extension: "log",
	}
	path := filepath.Join(directory, "app_20260904_2.log.gz")
	require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o640))
	remote := newDestinationUploaderRecorder("s3-current")
	remote.failNext("s3-current", errors.New("remote unavailable"))

	uploadReturned := make(chan struct{})
	reporterReturned := make(chan error, 1)
	var owner ObjectStorageUploadFailureRouteOwner
	reporter := runReporterFunc(func(context.Context, RunReport, error) {
		// 外側runtimeのCloseが先に現在のrunner終了を待つ状況を再現します。
		// observerがupload goroutine上で同期実行されると、ここで自己待機します。
		<-uploadReturned
		reporterReturned <- owner.Close()
	})
	wrapped, acquiredOwner, err := WrapObjectStorageUploader(
		objectstorage.UploadTargetConfig{
			Enabled:                             true,
			CompressedLogFileUploadFailureStore: runnerCompressedLogFileUploadFailureStoreTestConfig(),
		},
		runnerCompressedLogFileUploadSpoolTestRuntimes(t, runnerCompressedLogFileUploadSpoolTestConfig(resolvedTempDirForTest(t), "close-reentry", pkglogger.LoggerContractSync)),
		remote,
		fileName,
		clock.NewUTCClock(),
		reporter,
		nil,
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, acquiredOwner)
	owner = acquiredOwner
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "owner.Close()", owner.Close, boundedwait.ForceCloserOf(owner), nil)

	uploadResult := make(chan error, 1)
	request := uploadertest.Request(t, path)
	go func() {
		uploadResult <- wrapped.Upload(context.Background(), request)
		close(uploadReturned)
	}()
	select {
	case uploadErr := <-uploadResult:
		require.NoError(t, uploadErr)
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("compressed log file upload waited for its own Reporter callback")
	}
	select {
	case closeErr := <-reporterReturned:
		require.NoError(t, closeErr)
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("Reporter callback did not finish owner Close")
	}
}

func runnerCompressedLogFileUploadFailureStoreTestConfig() objectstorage.CompressedLogFileUploadFailureStoreConfig {
	return objectstorage.CompressedLogFileUploadFailureStoreConfig{
		DurableType: []failurestore.DurableType{failurestore.DurableTypeSpool},
	}
}

func runnerCompressedLogFileUploadSpoolTestConfig(
	spoolPath string,
	target string,
	contract pkglogger.LoggerContractKind,
) objectstorage.CompressedLogFileUploadSpoolConfig {
	return objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath:                  spoolPath,
		Target:                     target,
		LoggerContract:             contract.String(),
		RunnerInterval:             time.Hour,
		LifecycleInterval:          time.Hour,
		CircuitBreakerOpenCooldown: 5 * time.Minute,
	}
}

func runnerCompressedLogFileUploadSpoolTestRuntimes(t *testing.T, config objectstorage.CompressedLogFileUploadSpoolConfig) compressedlog.RootRuntimes {
	t.Helper()
	runtime, err := compressedlog.NewRootRuntimes(compressedlog.NewConfigAdapters(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(config), []objectstorage.UploadTarget{objectstorage.UploadTargetS3})...)
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "runtime.Close()", runtime.Close, boundedwait.ForceCloserOf(runtime), boundedwait.NoError)
	return runtime
}

type countingCompressedLogFileUploadFailureRouteOwner struct {
	closeCalls int
}

func (*countingCompressedLogFileUploadFailureRouteOwner) RequestForceClose() error { return nil }
func (*countingCompressedLogFileUploadFailureRouteOwner) NotificationsDone() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

type sequenceCompressedLogRouteOwner struct {
	errors     []error
	closeCalls int
}

func (*sequenceCompressedLogRouteOwner) RequestForceClose() error { return nil }
func (*sequenceCompressedLogRouteOwner) NotificationsDone() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (o *sequenceCompressedLogRouteOwner) Close() error {
	err := o.errors[o.closeCalls]
	o.closeCalls++
	return err
}

func (o *countingCompressedLogFileUploadFailureRouteOwner) Close() error {
	o.closeCalls++
	return nil
}

type compressedLogFileUploadFailureNotificationRunReporter struct {
	mu     sync.Mutex
	report RunReport
	err    error
}

type runReporterFunc func(context.Context, RunReport, error)

func (f runReporterFunc) ReportRun(ctx context.Context, report RunReport, err error) {
	f(ctx, report, err)
}

func (r *compressedLogFileUploadFailureNotificationRunReporter) ReportRun(
	_ context.Context,
	report RunReport,
	err error,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report = report
	r.err = err
}

func (r *compressedLogFileUploadFailureNotificationRunReporter) snapshot() (RunReport, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.report, r.err
}
