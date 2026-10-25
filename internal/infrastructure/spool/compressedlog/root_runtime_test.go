package compressedlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestManagedRouteRetryablePublishDoesNotWakeReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval = time.Hour
		ready := make(chan struct{}, 1)
		remote := &replaytest.Uploader{Ready: ready}
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		route, err := runtime.RegisterRoute(ManagedRouteConfig{
			FileName: replaytest.FileNamePattern("app_"),
		}, remote)
		if err != nil {
			t.Fatal(err)
		}
		if got := route.rootRuntime.runner.config.(rootRuntimeConfigProvider).CapacityPurgeMinimumFileAge(); got != cfg.LifecycleInterval {
			t.Fatalf("capacity purge minimum file age = %s, want lifecycle interval %s", got, cfg.LifecycleInterval)
		}
		t.Cleanup(func() {
			if closeErr := route.Close(); closeErr != nil {
				t.Errorf("Close() error = %v", closeErr)
			}
		})

		sourcePath := replaytest.WriteSource(t, "app_20260906_1.log.gz")
		file, err := route.SaveRetryableFailure(context.Background(), sourcePath)
		if err != nil {
			t.Fatalf("SaveRetryableFailure() error = %v", err)
		}
		if file.FileName != filepath.Base(sourcePath) || file.State != pkgspool.StateActive {
			t.Fatalf("published file = %+v", file)
		}
		// 保存を契機に動き出す処理があれば、時計を進めずにすべて終わるまで待ちます。
		synctest.Wait()
		select {
		case <-ready:
			t.Fatal("retryable publish woke replay and caused an immediate duplicate PUT")
		default:
		}
		// Replay uses the copied flat spool file, so the producer can remove its
		// original immediately after a successful handoff.
		if err := os.Remove(sourcePath); err != nil {
			t.Fatal(err)
		}

		waitForManagedState(t, route.rootRuntime.store, pkgspool.StateActive, 1)
		if got := remote.CallCount(); got != 0 {
			t.Fatalf("upload call count before runner tick = %d, want 0", got)
		}
	})
}

func TestManagedRouteRestartRetriesActiveFileOnNextRunnerTick(t *testing.T) {
	spoolPath := t.TempDir()
	cfg := managedUploaderTestConfig(spoolPath)
	cfg.RunnerInterval = time.Hour
	firstUpload := &replaytest.Uploader{}
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	first, err := runtime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
	}, firstUpload)
	if err != nil {
		t.Fatal(err)
	}
	sourcePath := replaytest.WriteSource(t, "app_20260906_1.log.gz")
	if _, err := first.SaveRetryableFailure(context.Background(), sourcePath); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if got := firstUpload.CallCount(); got != 0 {
		t.Fatalf("first-process upload calls = %d, want 0", got)
	}

	ready := make(chan struct{}, 1)
	restartedUpload := &replaytest.Uploader{Ready: ready}
	restartedConfig := cfg
	restartedConfig.RunnerInterval = 10 * time.Millisecond
	restartedRuntime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(restartedConfig), objectstorage.UploadTargetS3))
	restarted, err := restartedRuntime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
	}, restartedUpload)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	select {
	case <-ready:
	case <-time.After(hangDetectionTimeout):
		t.Fatal("restart did not replay the active file on a runner tick")
	}
	waitForManagedState(t, restarted.rootRuntime.store, pkgspool.StateDone, 1)
}

func TestManagedRouteSameFilenameIsIsolatedByDestination(t *testing.T) {
	cfg := managedUploaderTestConfig(t.TempDir())
	cfg.RunnerInterval = time.Hour
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	firstUpload := &replaytest.Uploader{DestinationID: "first"}
	secondUpload := &replaytest.Uploader{DestinationID: "second"}
	config := ManagedRouteConfig{FileName: replaytest.FileNamePattern("app_")}
	first, err := runtime.RegisterRoute(config, firstUpload)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := runtime.RegisterRoute(config, secondUpload)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	source := replaytest.WriteSource(t, "app_20260906_1.log.gz")
	one, err := first.SaveRetryableFailure(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	two, err := second.SaveRetryableFailure(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if one.Path == two.Path {
		t.Fatal("destinations share the same file")
	}
	for range 2 {
		report, err := runtime.runner.RunOnce(context.Background())
		if err != nil || len(report.CompressedLogFiles) != 1 {
			t.Fatalf("run = %+v, %v", report, err)
		}
	}
	if firstUpload.CallCount() != 1 || secondUpload.CallCount() != 1 {
		t.Fatal("wrong destination or duplicate upload")
	}
}

func TestManagedRouteDuplicateOwnersForSameDestinationReplayOnce(t *testing.T) {
	spoolPath := t.TempDir()
	cfg := managedUploaderTestConfig(spoolPath)
	firstUpload := &replaytest.Uploader{}
	secondUpload := &replaytest.Uploader{}
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	managedConfig := ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
	}
	first, err := runtime.RegisterRoute(managedConfig, firstUpload)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	second, err := runtime.RegisterRoute(managedConfig, secondUpload)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()

	sourcePath := replaytest.WriteSource(t, "app_20260906_1.log.gz")
	if _, err := first.SaveRetryableFailure(context.Background(), sourcePath); err != nil {
		t.Fatal(err)
	}
	waitForManagedState(t, first.rootRuntime.store, pkgspool.StateDone, 1)
	if got := firstUpload.CallCount() + secondUpload.CallCount(); got != 1 {
		t.Fatalf("duplicate owner upload calls = %d, want 1", got)
	}
}

func TestManagedRouteRegistrationChangeKeepsSharedDestinationCircuitOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval = time.Hour
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		upload := &replaytest.Uploader{Err: &pkgs3.HTTPStatusError{Code: 503}}
		first, err := runtime.RegisterRoute(ManagedRouteConfig{FileName: replaytest.FileNamePattern("app_")}, upload)
		if err != nil {
			t.Fatal(err)
		}
		defer first.Close()
		primeRuntimeTestCircuit(t, runtime, first)
		second, err := runtime.RegisterRoute(ManagedRouteConfig{FileName: replaytest.FileNamePattern("error_app_")}, &replaytest.Uploader{})
		if err != nil {
			t.Fatal(err)
		}
		defer second.Close()
		report, err := runtime.runner.RunOnce(context.Background())
		if err != nil || len(report.CompressedLogFiles) != 1 || report.CompressedLogFiles[0].UploadAttempted || report.CompressedLogFiles[0].DeferredReason != compressedlogworker.DeferredReasonCircuitBreaker {
			t.Fatalf("replay after registration = %+v/%v, want circuit suppression", report, err)
		}
		if upload.CallCount() != 13 {
			t.Fatalf("upload count = %d, want 13", upload.CallCount())
		}
	})
}

func TestRootRuntimeKeepsCircuitAndStagingAcrossZeroRouteGap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval = time.Hour
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		config := ManagedRouteConfig{FileName: replaytest.FileNamePattern("app_")}
		first, err := runtime.RegisterRoute(config, &replaytest.Uploader{Err: &pkgs3.HTTPStatusError{Code: 503}})
		if err != nil {
			t.Fatal(err)
		}
		primeRuntimeTestCircuit(t, runtime, first)
		if err := first.Close(); err != nil {
			t.Fatal(err)
		}
		if runtime.isShutdownStarted() {
			t.Fatal("last route closed the open root")
		}
		duplicate, err := NewRootRuntime(NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		if duplicate != nil {
			_ = duplicate.Close()
		}
		if !errors.Is(err, ErrSpoolRootAlreadyOwned) {
			t.Fatalf("duplicate root: %v", err)
		}
		staging := filepath.Join(first.Root().Path(), ".staging", ".compressed-log-gap.tmp")
		if err := os.WriteFile(staging, []byte("in progress"), 0o600); err != nil {
			t.Fatal(err)
		}
		upload := &replaytest.Uploader{}
		second, err := runtime.RegisterRoute(config, upload)
		if err != nil {
			t.Fatal(err)
		}
		defer second.Close()
		if second.rootRuntime != runtime {
			t.Fatal("root runtime replaced")
		}
		contents, err := os.ReadFile(staging)
		if err != nil || string(contents) != "in progress" {
			t.Fatalf("staging changed: %q/%v", contents, err)
		}
		report, err := runtime.runner.RunOnce(context.Background())
		if err != nil || len(report.CompressedLogFiles) != 1 || report.CompressedLogFiles[0].UploadAttempted || report.CompressedLogFiles[0].DeferredReason != compressedlogworker.DeferredReasonCircuitBreaker {
			t.Fatalf("replay after gap = %+v/%v, want circuit suppression", report, err)
		}
		if upload.CallCount() != 0 {
			t.Fatal("open circuit reset during zero-route gap")
		}
	})
}

// 内部のbreakerを操作せず、仮想時間上で実際の再送失敗を重ねてopenにします。
func primeRuntimeTestCircuit(t *testing.T, runtime *RootRuntime, route *ManagedRoute) {
	t.Helper()
	if _, err := route.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, "app_20260906_1.log.gz")); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 13; attempt++ {
		report, err := runtime.runner.RunOnce(context.Background())
		if err == nil || len(report.CompressedLogFiles) != 1 || report.CompressedLogFiles[0].FailureCategory != uploader.FailureCategoryRetryable {
			t.Fatalf("failure %d: %+v/%v", attempt, report, err)
		}
		if attempt < 12 {
			time.Sleep(5 * time.Second)
		}
	}
}

func TestRootRuntimeDefersFilesystemInitializationUntilFirstRegistration(t *testing.T) {
	spoolPath := filepath.Join(t.TempDir(), "not-created")
	cfg := managedUploaderTestConfig(spoolPath)
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	if _, err := os.Stat(spoolPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("root construction touched spool filesystem: %v", err)
	}
	for name, fileName := range map[string]logpattern.FileNamePattern{
		"zero value":        {},
		"missing target":    {Extension: "log"},
		"missing extension": {Target: "app"},
		"blank target":      {Target: " \t", Extension: "log"},
		"blank extension":   {Target: "app", Extension: " \t"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := runtime.RegisterRoute(ManagedRouteConfig{FileName: fileName}, &replaytest.Uploader{}); !errors.Is(err, compressedlogworker.ErrInvalidRouteFileName) {
				t.Fatalf("invalid registration = %v, want compressedlogworker.ErrInvalidRouteFileName", err)
			}
			if _, err := os.Stat(spoolPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid registration touched spool filesystem: %v", err)
			}
		})
	}
	route, err := runtime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
	}, &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = route.Close() }()
	if _, err := os.Stat(route.Root().ActiveDir()); err != nil {
		t.Fatalf("first registration did not initialize spool layout: %v", err)
	}
}

func TestRootRuntimeUninitializedRejectsRegistrationAndAllowsClose(t *testing.T) {
	for name, runtime := range map[string]*RootRuntime{"nil": nil, "zero value": new(RootRuntime)} {
		t.Run(name, func(t *testing.T) {
			route, err := runtime.RegisterRoute(ManagedRouteConfig{
				FileName: replaytest.FileNamePattern("app_"),
			}, &replaytest.Uploader{})
			if route != nil || !errors.Is(err, ErrRootRuntimeClosed) {
				t.Fatalf("uninitialized RegisterRoute() = %v, %v, want nil, ErrRootRuntimeClosed", route, err)
			}
			if err := runtime.Close(); err != nil {
				t.Fatalf("uninitialized Close() = %v", err)
			}
		})
	}
}

func TestManagedRouteNoMatchingRouteNotifiesObserverAndStaysActive(t *testing.T) {
	errCh := make(chan error, 1)
	remote := &replaytest.Uploader{}
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(managedUploaderTestConfig(t.TempDir())), objectstorage.UploadTargetS3))
	route, err := runtime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("error_app_"),
		RunObserver: func(
			_ context.Context,
			_ *RunReport,
			_ *objectstoragespool.TerminalPurgeReport,
			runErr error,
		) {
			if runErr != nil {
				select {
				case errCh <- runErr:
				default:
				}
			}
		},
	}, remote)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = route.Close() }()

	sourcePath := replaytest.WriteSource(t, "app_20260906_1.log.gz")
	if _, err := route.SaveRetryableFailure(context.Background(), sourcePath); err != nil {
		t.Fatal(err)
	}
	select {
	case observed := <-errCh:
		if !errors.Is(observed, compressedlogworker.ErrNoMatchingRoute) {
			t.Fatalf("observer error = %v, want compressedlogworker.ErrNoMatchingRoute", observed)
		}
	case <-time.After(hangDetectionTimeout):
		t.Fatal("observer did not receive unmatched-route error")
	}
	waitForManagedState(t, route.rootRuntime.store, pkgspool.StateActive, 1)
	if remote.CallCount() != 0 {
		t.Fatal("unmatched file was uploaded")
	}
}

func TestManagedRoutePermanentPublishStaysDead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		remote := &replaytest.Uploader{}
		var replayRuns atomic.Int32
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		route, err := runtime.RegisterRoute(ManagedRouteConfig{
			FileName: replaytest.FileNamePattern("debug_app_"),
			RunObserver: func(_ context.Context, replay *RunReport, _ *objectstoragespool.TerminalPurgeReport, _ error) {
				if replay != nil {
					replayRuns.Add(1)
				}
			},
		}, remote)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = route.Close() })

		sourcePath := replaytest.WriteSource(t, "debug_app_20260906_1.log.gz")
		if _, err := route.SavePermanentFailure(context.Background(), sourcePath); err != nil {
			t.Fatal(err)
		}
		runsBeforeWait := replayRuns.Load()
		// 再送周期を何度か完了させても、配送不能のfileは再送対象へ戻りません。
		time.Sleep(4 * cfg.RunnerInterval)
		synctest.Wait()
		if replayRuns.Load() == runsBeforeWait {
			t.Fatal("no replay cycle completed after the permanent file was saved")
		}
		waitForManagedState(t, route.rootRuntime.store, pkgspool.StateDead, 1)
		if remote.CallCount() != 0 {
			t.Fatal("permanent file was reactivated")
		}
	})
}

func TestManagedRouteKeepsCoordinatorUntilEveryOwnerCloses(t *testing.T) {
	spoolPath := t.TempDir()
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(managedUploaderTestConfig(spoolPath)), objectstorage.UploadTargetS3))
	cfg := ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
	}
	first, err := runtime.RegisterRoute(cfg, &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.RegisterRoute(cfg, &replaytest.Uploader{})
	if err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	defer func() { _ = second.Close() }()
	coordinator := first.rootRuntime
	coordinator.mu.Lock()
	_, firstRegistered := coordinator.routes[first]
	_, secondRegistered := coordinator.routes[second]
	coordinator.mu.Unlock()
	if !firstRegistered || !secondRegistered {
		t.Fatal("each ManagedRoute must identify its own registration")
	}
	rootKey := pathidentity.ComparisonKey(first.rootRuntime.store.Root().Path())
	rootRuntimeRegistry.Lock()
	registeredRuntime := rootRuntimeRegistry.byRoot[rootKey]
	rootRuntimeRegistry.Unlock()
	if registeredRuntime != runtime {
		t.Fatal("registry must directly reference the constructed root runtime")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.RegisterRoute(cfg, &replaytest.Uploader{}); !errors.Is(err, ErrRootRuntimeClosed) {
		t.Fatalf("RegisterRoute() after root Close() = %v, want ErrRootRuntimeClosed", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	coordinator.mu.Lock()
	_, firstRegistered = coordinator.routes[first]
	_, secondRegistered = coordinator.routes[second]
	remainingRoutes := len(coordinator.routes)
	coordinator.mu.Unlock()
	if firstRegistered || !secondRegistered || remainingRoutes != 1 {
		t.Fatal("repeated Close must remove only its own registration once")
	}
	rootRuntimeRegistry.Lock()
	_, existsAfterFirstClose := rootRuntimeRegistry.byRoot[rootKey]
	rootRuntimeRegistry.Unlock()
	if !existsAfterFirstClose {
		t.Fatal("coordinator stopped while another owner remained")
	}
	duplicate, err := NewRootRuntime(NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(managedUploaderTestConfig(spoolPath)), objectstorage.UploadTargetS3))
	if duplicate != nil {
		_ = duplicate.Close()
	}
	if !errors.Is(err, ErrSpoolRootAlreadyOwned) {
		t.Fatalf("root reservation while route remains = %v, want ErrSpoolRootAlreadyOwned", err)
	}
	if _, err := second.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, "app_20260906_41.log.gz")); err != nil {
		t.Fatalf("surviving route after root Close() cannot publish: %v", err)
	}
	waitForManagedState(t, second.rootRuntime.store, pkgspool.StateDone, 1)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	rootRuntimeRegistry.Lock()
	_, existsAfterLastClose := rootRuntimeRegistry.byRoot[rootKey]
	rootRuntimeRegistry.Unlock()
	if existsAfterLastClose {
		t.Fatal("coordinator remains registered after every owner closed")
	}
}

func TestManagedRouteCloseAndSameRootConstructionHandOffToNewCoordinator(t *testing.T) {
	testManagedRouteReplacementWaitsForRetirement(t, func(path string) string { return path })
}

func testManagedRouteReplacementWaitsForRetirement(t *testing.T, aliasRoot func(string) string) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		spoolPath := t.TempDir()
		cfg := managedUploaderTestConfig(spoolPath)
		blockUpload := make(chan struct{})
		var releaseOnce sync.Once
		releaseUpload := func() { releaseOnce.Do(func() { close(blockUpload) }) }
		uploadStarted := make(chan struct{}, 1)
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		oldRoute, err := runtime.RegisterRoute(ManagedRouteConfig{
			FileName: replaytest.FileNamePattern("app_"),
		}, &replaytest.Uploader{
			Block:         blockUpload,
			Ready:         uploadStarted,
			IgnoreContext: true,
		})
		if err != nil {
			t.Fatal(err)
		}

		type routeResult struct {
			runtime *RootRuntime
			route   *ManagedRoute
			err     error
		}
		newResult := make(chan routeResult, 1)
		var replacementOnce sync.Once
		var replacement routeResult
		takeReplacement := func() routeResult {
			replacementOnce.Do(func() { replacement = <-newResult })
			return replacement
		}
		replacementStarted := false
		// 試験が途中で失敗しても、停止中のuploadを解放してから旧routeと後継を閉じます。
		t.Cleanup(func() {
			releaseUpload()
			_ = oldRoute.Close()
			if !replacementStarted {
				return
			}
			if result := takeReplacement(); result.runtime != nil {
				if result.route != nil {
					_ = result.route.Close()
				}
				_ = result.runtime.Close()
			}
		})

		oldCoordinator := oldRoute.rootRuntime
		if _, err := oldRoute.store.SaveRetryableFailure(
			context.Background(),
			replaytest.WriteSource(t, "app_20260906_40.log.gz"),
		); err != nil {
			t.Fatal(err)
		}
		select {
		case <-uploadStarted:
		case <-time.After(time.Minute):
			t.Fatal("old coordinator did not start the blocking upload")
		}

		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}
		closeResult := make(chan error, 1)
		go func() {
			closeResult <- oldRoute.Close()
		}()
		// 旧routeのCloseは、受理済みのuploadが終わるまで停止開始の状態で待ちます。
		synctest.Wait()
		if !oldCoordinator.isShutdownStarted() {
			t.Fatal("old coordinator did not enter shutdown-started state")
		}

		replacementConfig := cfg
		replacementConfig.SpoolPath = aliasRoot(cfg.SpoolPath)
		replacementStarted = true
		go func() {
			replacementRuntime, runtimeErr := NewRootRuntime(NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(replacementConfig), objectstorage.UploadTargetS3))
			if runtimeErr != nil {
				newResult <- routeResult{err: runtimeErr}
				return
			}
			route, routeErr := replacementRuntime.RegisterRoute(ManagedRouteConfig{
				FileName: replaytest.FileNamePattern("app_"),
			}, &replaytest.Uploader{})
			newResult <- routeResult{runtime: replacementRuntime, route: route, err: routeErr}
		}()

		// 後継の構築は、旧管理処理の送信と引き継ぎが終わるまで待ちます。
		synctest.Wait()
		select {
		case early := <-newResult:
			replacementOnce.Do(func() { replacement = early })
			t.Fatalf("same-root constructor returned before predecessor retired: %v", early.err)
		default:
		}
		releaseUpload()
		select {
		case closeErr := <-closeResult:
			if closeErr != nil {
				t.Fatalf("old Close() error = %v", closeErr)
			}
		case <-time.After(time.Minute):
			t.Fatal("old Close() did not finish after upload was released")
		}

		result := takeReplacement()
		if result.err != nil {
			t.Fatalf("replacement root registration error = %v", result.err)
		}
		if result.route.rootRuntime == oldCoordinator {
			t.Fatal("replacement reused the stopped coordinator")
		}
		if err := result.route.Close(); err != nil {
			t.Fatalf("replacement Close() error = %v", err)
		}
	})
}

func TestManagedRouteActiveStateDebrisIsPreservedAndNotifiesWarning(t *testing.T) {
	synctest.Test(t, testManagedRouteActiveStateDebrisIsPreserved)
}

func testManagedRouteActiveStateDebrisIsPreserved(t *testing.T) {
	warnCh := make(chan error, 1)
	errCh := make(chan error, 1)
	cfg := managedUploaderTestConfig(t.TempDir())
	cfg.RunnerInterval = time.Minute
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	route, err := runtime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
		OnWarn: func(_ context.Context, warning error) {
			if errors.Is(warning, objectstoragespool.ErrInvalidCompressedLogPath) {
				select {
				case warnCh <- warning:
				default:
				}
			}
		},
		RunObserver: func(
			_ context.Context,
			_ *RunReport,
			_ *objectstoragespool.TerminalPurgeReport,
			runErr error,
		) {
			if runErr != nil {
				select {
				case errCh <- runErr:
				default:
				}
			}
		},
	}, &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = route.Close() }()
	legacyBundle := filepath.Join(route.store.Root().ActiveDir(), "legacy-bundle")
	if err := os.Mkdir(legacyBundle, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyBundle, "keep.txt"), []byte("keep nested data"), 0o600); err != nil {
		t.Fatal(err)
	}
	valid, err := route.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, "app_20261004_1.log.gz"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case observed := <-warnCh:
		if !errors.Is(observed, objectstoragespool.ErrInvalidCompressedLogPath) {
			t.Fatalf("observer warning = %v, want ErrInvalidCompressedLogPath", observed)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("warning observer did not receive the active preservation warning")
	}
	select {
	case err := <-errCh:
		t.Fatalf("successful preservation must not fail replay: %v", err)
	default:
	}
	if _, err := os.Lstat(legacyBundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active debris was not moved: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(route.store.Root().DoneDir(), valid.FileName)); err != nil {
		t.Fatalf("valid file did not finish replay: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(route.store.Root().Path(), ".preserved"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("preserved entries = %v, %v", entries, err)
	}
	if body, err := os.ReadFile(filepath.Join(route.store.Root().Path(), ".preserved", entries[0].Name(), "keep.txt")); err != nil || string(body) != "keep nested data" {
		t.Fatalf("preserved contents = %q, %v", body, err)
	}
}

func TestManagedRoutePreservationFailureNotifiesErrorsAndRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval = time.Minute
		errCh := make(chan error, 1)
		warnCh := make(chan error, 1)
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		route, err := runtime.RegisterRoute(ManagedRouteConfig{
			FileName: replaytest.FileNamePattern("app_"),
			RunObserver: func(_ context.Context, _ *RunReport, _ *objectstoragespool.TerminalPurgeReport, err error) {
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			},
			OnWarn: func(_ context.Context, warning error) {
				if errors.Is(warning, objectstoragespool.ErrInvalidCompressedLogPath) {
					select {
					case warnCh <- warning:
					default:
					}
				}
			},
		}, &replaytest.Uploader{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = route.Close() }()
		valid, err := route.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, "app_20261004_1.log.gz"))
		if err != nil {
			t.Fatal(err)
		}
		invalidPath := filepath.Join(route.store.Root().ActiveDir(), "legacy-bundle")
		if err := os.Mkdir(invalidPath, 0o700); err != nil {
			t.Fatal(err)
		}
		preservedDirectory := filepath.Join(route.store.Root().Path(), ".preserved")
		if err := os.Remove(preservedDirectory); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(preservedDirectory, []byte("blocked"), 0o600); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			select {
			case <-errCh:
			case <-time.After(2 * time.Minute):
				t.Fatal("preservation failure was not reported in the next replay cycle")
			}
			if _, err := os.Lstat(invalidPath); err != nil {
				t.Fatalf("invalid entry must remain after preservation failure: %v", err)
			}
			if _, err := os.Lstat(valid.Path); err != nil {
				t.Fatalf("unclaimed file must remain after preservation failure: %v", err)
			}
		}
		if err := os.Remove(preservedDirectory); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(preservedDirectory, 0o750); err != nil {
			t.Fatal(err)
		}
		select {
		case <-warnCh:
		case <-time.After(2 * time.Minute):
			t.Fatal("preservation did not resume after the destination was repaired")
		}
		if _, err := os.Lstat(filepath.Join(route.store.Root().DoneDir(), valid.FileName)); err != nil {
			t.Fatalf("valid file did not finish replay after repair: %v", err)
		}
	})
}

func TestManagedRouteTerminalStateDebrisIsPreservedAndNotifiesWarning(t *testing.T) {
	spoolPath := t.TempDir()
	cfg := managedUploaderTestConfig(spoolPath)
	cfg.LifecycleInterval = 5 * time.Millisecond
	cfg.Done = objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
		Enabled:        true,
		MaxFileAgeDays: 1,
		MaxDirSize:     1,
	}
	root, err := objectstoragespool.NewRoot(
		spoolPath,
		cfg.Target,
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	if err != nil {
		t.Fatal(err)
	}
	root, err = root.ForDestination("s3")
	if err != nil {
		t.Fatal(err)
	}
	store, err := objectstoragespool.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureLayout(context.Background()); err != nil {
		t.Fatal(err)
	}
	legacyBundle := filepath.Join(root.DoneDir(), "legacy-bundle")
	if err := os.Mkdir(legacyBundle, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyBundle, "keep.txt"), []byte("keep nested data"), 0o600); err != nil {
		t.Fatal(err)
	}
	expiredPath := filepath.Join(root.DoneDir(), "app_20260901_1.log.gz")
	if err := os.WriteFile(expiredPath, []byte("expired"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().AddDate(0, 0, -10)
	if err := os.Chtimes(expiredPath, old, old); err != nil {
		t.Fatal(err)
	}

	warnCh := make(chan error, 1)
	errCh := make(chan error, 1)
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	route, err := runtime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
		OnWarn: func(_ context.Context, warning error) {
			if errors.Is(warning, objectstoragespool.ErrInvalidCompressedLogPath) {
				select {
				case warnCh <- warning:
				default:
				}
			}
		},
		RunObserver: func(
			_ context.Context,
			_ *RunReport,
			lifecycle *objectstoragespool.TerminalPurgeReport,
			runErr error,
		) {
			if lifecycle != nil && runErr != nil {
				select {
				case errCh <- runErr:
				default:
				}
			}
		},
	}, &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = route.Close() }()

	select {
	case observed := <-warnCh:
		if !errors.Is(observed, objectstoragespool.ErrInvalidCompressedLogPath) {
			t.Fatalf("observer warning = %v, want ErrInvalidCompressedLogPath", observed)
		}
	case <-time.After(hangDetectionTimeout):
		t.Fatal("warning observer did not receive the terminal preservation warning")
	}
	select {
	case err := <-errCh:
		t.Fatalf("successful preservation must not fail terminal purge: %v", err)
	default:
	}
	if _, err := os.Lstat(legacyBundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal debris was not moved: %v", err)
	}
	if _, err := os.Lstat(expiredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired terminal file was not deleted: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root.Path(), ".preserved"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("preserved entries = %v, %v", entries, err)
	}
	if body, err := os.ReadFile(filepath.Join(root.Path(), ".preserved", entries[0].Name(), "keep.txt")); err != nil || string(body) != "keep nested data" {
		t.Fatalf("preserved contents = %q, %v", body, err)
	}
}

func TestManagedRouteSeparatesCoordinatorsByObjectStorageService(t *testing.T) {
	spoolPath := resolvedTempDirForTest(t)
	s3Config := managedUploaderTestConfig(spoolPath)
	s3Runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(s3Config), objectstorage.UploadTargetS3))
	s3, err := s3Runtime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
	}, &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s3.Close() }()

	otherRuntime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(s3Config), objectstorage.UploadTarget("future-storage")))
	other, err := otherRuntime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
	}, &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	if s3.rootRuntime == other.rootRuntime {
		t.Fatal("different object storage services unexpectedly share one coordinator")
	}
	if got, want := s3.rootRuntime.store.Root().Path(), filepath.Join(
		spoolPath, "heterolog", "spool", "objectstorage", "s3", "logger",
	); got != want {
		t.Fatalf("S3 spool root = %q, want %q", got, want)
	}
	if got, want := other.rootRuntime.store.Root().Path(), filepath.Join(
		spoolPath, "heterolog", "spool", "objectstorage", "future-storage", "logger",
	); got != want {
		t.Fatalf("other service spool root = %q, want %q", got, want)
	}
}

func TestManagedRouteCleansStagingOnlyForNewSharedCoordinator(t *testing.T) {
	spoolPath := t.TempDir()
	root, err := objectstoragespool.NewRoot(
		spoolPath,
		"heterolog",
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	if err != nil {
		t.Fatal(err)
	}
	root, err = root.ForDestination("s3")
	if err != nil {
		t.Fatal(err)
	}
	store, err := objectstoragespool.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureLayout(context.Background()); err != nil {
		t.Fatal(err)
	}
	stagingDirectory := filepath.Join(root.Path(), ".staging")
	abandoned := filepath.Join(stagingDirectory, ".compressed-log-abandoned.tmp")
	if err := os.WriteFile(abandoned, []byte("partial"), 0o640); err != nil {
		t.Fatal(err)
	}

	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(managedUploaderTestConfig(spoolPath)), objectstorage.UploadTargetS3))
	managedConfig := ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
	}
	first, err := runtime.RegisterRoute(managedConfig, &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	if _, err := os.Stat(abandoned); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned staging file stat error = %v, want not exist", err)
	}

	inProgress := filepath.Join(stagingDirectory, ".compressed-log-in-progress.tmp")
	if err := os.WriteFile(inProgress, []byte("publishing"), 0o640); err != nil {
		t.Fatal(err)
	}
	second, err := runtime.RegisterRoute(managedConfig, &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	if _, err := os.Stat(inProgress); err != nil {
		t.Fatalf("existing coordinator owner removed in-progress staging file: %v", err)
	}
}

func TestManagedRouteConstructorValidation(t *testing.T) {
	cfg := managedUploaderTestConfig(t.TempDir())
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	if _, err := runtime.RegisterRoute(ManagedRouteConfig{}, &replaytest.Uploader{}); !errors.Is(err, compressedlogworker.ErrInvalidRouteFileName) {
		t.Fatalf("invalid file name error = %v", err)
	}
	if _, err := runtime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
	}, nil); !errors.Is(err, ErrNilDestinationUploader) {
		t.Fatalf("nil uploader error = %v", err)
	}
}

func newTestRootRuntime(t *testing.T, config ConfigProvider) *RootRuntime {
	t.Helper()
	runtime, err := NewRootRuntime(config)
	if err != nil {
		t.Fatalf("NewRootRuntime() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil && !(runtime.closeController.IsForced() && errors.Is(err, shutdown.ErrForcedClose)) {
			t.Errorf("root Close() error = %v", err)
		}
	})
	return runtime
}

func managedUploaderTestConfig(spoolPath string) objectstorage.CompressedLogFileUploadSpoolConfig {
	return objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath:                  spoolPath,
		Target:                     "heterolog",
		LoggerContract:             pkglogger.LoggerContractSync.String(),
		RunnerInterval:             5 * time.Millisecond,
		LifecycleInterval:          time.Hour,
		CircuitBreakerOpenCooldown: 5 * time.Minute,
	}
}

func waitForManagedState(
	t *testing.T,
	store interface {
		ListFiles(context.Context, pkgspool.State) ([]objectstoragespool.CompressedLogFile, error)
	},
	state pkgspool.State,
	want int,
) []objectstoragespool.CompressedLogFile {
	t.Helper()
	deadline := time.Now().Add(hangDetectionTimeout)
	for {
		files, err := store.ListFiles(context.Background(), state)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) == want {
			return files
		}
		if time.Now().After(deadline) {
			t.Fatalf("state %q file count = %d, want %d", state, len(files), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
