package compressedlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestRootRuntimeReportsPreservedRenamesAndCopiesAfterRestart(t *testing.T) {
	cfg := managedUploaderTestConfig(t.TempDir())
	cfg.RunnerInterval = time.Hour
	firstRuntime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	firstRoute, err := firstRuntime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
	}, &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	oldSource := replaytest.WriteSource(t, "app_20260929_9.log.gz")
	if _, err := firstRoute.SaveRetryableFailure(context.Background(), oldSource); err != nil {
		t.Fatal(err)
	}
	newSource := replaytest.WriteSource(t, "app_20260929_9.log.gz")
	if err := os.WriteFile(newSource, []byte("new bytes"), 0o640); err != nil {
		t.Fatal(err)
	}
	file, err := firstRoute.SaveRetryableFailure(context.Background(), newSource)
	if err != nil || len(file.ConflictRenames) != 1 {
		t.Fatalf("SaveRetryableFailure() = %+v, %v", file, err)
	}
	if _, err := firstRoute.PreserveFile(context.Background(), newSource); err != nil {
		t.Fatal(err)
	}
	if err := firstRoute.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstRuntime.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	var notification, reportedErr error
	restartedRoute, err := restarted.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
		RunObserver: func(_ context.Context, _ *RunReport, _ *objectstoragespool.TerminalPurgeReport, reported error) {
			reportedErr = reported
		},
		OnWarn: func(_ context.Context, warning error) { notification = warning },
	}, &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	defer restartedRoute.Close()
	restarted.ReportSpoolRunnerRun(context.Background(), RunReport{}, nil)
	var preserved *PreservedFilesWarning
	if !errors.Is(notification, ErrPreservedFilesRequireReview) || !errors.As(notification, &preserved) || preserved.Count != 2 {
		t.Fatalf("restart maintenance notification = %v, want two preserved files", notification)
	}
	if reportedErr != nil {
		t.Fatalf("preserved files must not turn successful maintenance into OnError: %v", reportedErr)
	}
}

func TestRootRuntimeInvalidFileWarningDoesNotFailSuccessfulReplay(t *testing.T) {
	cfg := managedUploaderTestConfig(t.TempDir())
	cfg.RunnerInterval = time.Hour
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	var warnings []error
	var reportedErr error
	route, err := runtime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
		RunObserver: func(_ context.Context, _ *RunReport, _ *objectstoragespool.TerminalPurgeReport, err error) {
			reportedErr = err
		},
		OnWarn: func(_ context.Context, warning error) { warnings = append(warnings, warning) },
	}, &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	defer route.Close()
	if _, err := route.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, "app_20261005_1.log.gz")); err != nil {
		t.Fatal(err)
	}
	invalidPath := filepath.Join(route.store.Root().ActiveDir(), "operator-note.txt")
	if err := os.WriteFile(invalidPath, []byte("keep these bytes"), 0o640); err != nil {
		t.Fatal(err)
	}
	report, err := runtime.runner.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.CompressedLogFiles) != 1 || !report.CompressedLogFiles[0].UploadSucceeded || report.CompressedLogFiles[0].State != pkgspool.StateDone {
		t.Fatalf("normal file must be uploaded despite invalid sibling: %+v", report)
	}
	runtime.ReportSpoolRunnerRun(context.Background(), report, nil)
	if reportedErr != nil {
		t.Fatalf("successful isolation must not become maintenance failure: %v", reportedErr)
	}
	var count *PreservedFilesWarning
	if len(warnings) != 2 || !errors.As(warnings[1], &count) || count.Count != 1 {
		t.Fatalf("preservation details and count warnings = %v", warnings)
	}
	// 管理者へ示す移動元と、root直下の.preservedにある保全先を確認します。
	var detail *objectstoragespool.InvalidEntryPreservationWarning
	if !errors.Is(warnings[0], objectstoragespool.ErrInvalidCompressedLogFileName) || !errors.As(warnings[0], &detail) ||
		detail.SourcePath != invalidPath || filepath.Dir(detail.PreservedPath) != filepath.Join(route.store.Root().Path(), ".preserved") {
		t.Fatalf("preservation detail warning = %v", warnings[0])
	}
	warnings = nil
	runtime.ReportSpoolRunnerRun(context.Background(), RunReport{}, nil)
	if len(warnings) != 1 || !errors.As(warnings[0], &count) || count.Count != 1 {
		t.Fatalf("completed isolation details must be drained once; retained count remains: %v", warnings)
	}
}

func TestRootRuntimeWarningObserverCanRequestClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval = time.Minute
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		closed := make(chan error, 1)
		routeReady := make(chan *ManagedRoute, 1)
		var once sync.Once
		route, err := runtime.RegisterRoute(ManagedRouteConfig{
			FileName: replaytest.FileNamePattern("app_"),
			OnWarn: func(_ context.Context, _ error) {
				once.Do(func() {
					registeredRoute := <-routeReady
					registeredRoute.RequestClose()
					runtime.RequestClose()
					closed <- nil
				})
			},
		}, &replaytest.Uploader{})
		if err != nil {
			t.Fatal(err)
		}
		routeReady <- route
		invalidPath := filepath.Join(route.store.Root().ActiveDir(), "operator-note.txt")
		if err := os.WriteFile(invalidPath, []byte("preserve these bytes"), 0o640); err != nil {
			t.Fatal(err)
		}
		// 初回の再送周期を待ちます。callbackは要求だけを送り、その外側で終了を待ちます。
		if err := <-closed; err != nil {
			t.Fatalf("warning observer must be able to close its runtime: %v", err)
		}
		synctest.Wait()
		select {
		case <-runtime.shutdownComplete:
		default:
			t.Fatal("runtime shutdown did not finish after warning callback")
		}
	})
}
