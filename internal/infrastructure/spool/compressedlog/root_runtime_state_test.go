package compressedlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestRootRuntimeStateConcurrentCloseBeforeAndAfterStart(t *testing.T) {
	for _, started := range []bool{false, true} {
		name := "before start"
		if started {
			name = "after start"
		}
		t.Run(name, func(t *testing.T) {
			cfg := managedUploaderTestConfig(t.TempDir())
			cfg.RunnerInterval = time.Hour
			runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
			requireRootRuntimeState(t, runtime, rootRuntimeNotStarted)
			if started {
				route, err := runtime.RegisterRoute(stateTestRouteConfig(), &replaytest.Uploader{})
				if err != nil {
					t.Fatal(err)
				}
				if err := route.Close(); err != nil {
					t.Fatal(err)
				}
				requireRootRuntimeState(t, runtime, rootRuntimeRunning)
			}

			// すべてのcallerは、二度目のcancelや、どちらかの完了channelの二度目のcloseを
			// 起こさずにretirementを観測しなければなりません。
			var callers sync.WaitGroup
			for i := 0; i < 4; i++ {
				callers.Add(1)
				go func() {
					defer callers.Done()
					if err := runtime.Close(); err != nil {
						t.Errorf("Close() error = %v", err)
					}
				}()
			}
			callers.Wait()
			requireRootRuntimeState(t, runtime, rootRuntimeShutdownStarted)
			if _, err := runtime.RegisterRoute(stateTestRouteConfig(), &replaytest.Uploader{}); !errors.Is(err, ErrRootRuntimeClosed) {
				t.Fatalf("RegisterRoute() after shutdown = %v, want ErrRootRuntimeClosed", err)
			}
			for name, completed := range map[string]<-chan struct{}{
				"runLoopStopped":   runtime.runLoopStopped,
				"shutdownComplete": runtime.shutdownComplete,
			} {
				select {
				case <-completed:
				default:
					t.Errorf("Close() returned before %s completed", name)
				}
			}
		})
	}
}

func TestRootRuntimeStateRootCloseWaitsForLastRoute(t *testing.T) {
	cfg := managedUploaderTestConfig(t.TempDir())
	cfg.RunnerInterval = time.Hour
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	route, err := runtime.RegisterRoute(stateTestRouteConfig(), &replaytest.Uploader{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = route.Close() }()
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	requireRootRuntimeState(t, runtime, rootRuntimeRegistrationClosed)
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	requireRootRuntimeState(t, runtime, rootRuntimeRegistrationClosed)
	select {
	case <-runtime.runLoopStopped:
		t.Fatal("root Close() stopped the loop while a route remained open")
	default:
	}
	select {
	case <-runtime.shutdownComplete:
		t.Fatal("root shutdown completed while its last route remained open")
	default:
	}
	if _, err := runtime.RegisterRoute(stateTestRouteConfig(), &replaytest.Uploader{}); !errors.Is(err, ErrRootRuntimeClosed) {
		t.Fatalf("RegisterRoute() after root Close() = %v, want ErrRootRuntimeClosed", err)
	}
	if _, err := route.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, "app_20260906_42.log.gz")); err != nil {
		t.Fatalf("existing route cannot save after root Close(): %v", err)
	}
	if err := route.Close(); err != nil {
		t.Fatal(err)
	}
	requireRootRuntimeState(t, runtime, rootRuntimeShutdownStarted)
	select {
	case <-runtime.shutdownComplete:
	default:
		t.Fatal("last route Close() returned before root retirement")
	}
}

func TestRootRuntimeStateInitializationFailureCanRetry(t *testing.T) {
	for _, stage := range []string{"layout", "staging cleanup"} {
		t.Run(stage, func(t *testing.T) {
			runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(managedUploaderTestConfig(t.TempDir())), objectstorage.UploadTargetS3))
			store, err := runtime.store.ForDestination("s3")
			if err != nil {
				t.Fatal(err)
			}
			blocker := store.Root().ActiveDir()
			wantError := ErrInitializeSpool
			if stage == "staging cleanup" {
				if err := store.EnsureLayout(context.Background()); err != nil {
					t.Fatal(err)
				}
				blocker = filepath.Join(store.Root().Path(), ".staging", "unexpected-entry")
				wantError = ErrRemoveAbandonedStagingFiles
			} else if err := os.MkdirAll(filepath.Dir(blocker), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(blocker, []byte("blocks initialization"), 0o600); err != nil {
				t.Fatal(err)
			}
			route, err := runtime.RegisterRoute(stateTestRouteConfig(), &replaytest.Uploader{})
			if route != nil {
				_ = route.Close()
				t.Fatal("failed initialization returned a registered route")
			}
			if !errors.Is(err, wantError) {
				t.Fatalf("RegisterRoute() error = %v, want %v", err, wantError)
			}
			requireRootRuntimeState(t, runtime, rootRuntimeNotStarted)
			select {
			case <-runtime.runLoopStopped:
				t.Fatal("failed initialization closed the reusable completion channel")
			default:
			}
			if err := os.Remove(blocker); err != nil {
				t.Fatal(err)
			}
			route, err = runtime.RegisterRoute(stateTestRouteConfig(), &replaytest.Uploader{})
			if err != nil {
				t.Fatalf("RegisterRoute() retry failed: %v", err)
			}
			defer func() { _ = route.Close() }()
			requireRootRuntimeState(t, runtime, rootRuntimeRunning)
		})
	}
}

func stateTestRouteConfig() ManagedRouteConfig {
	return ManagedRouteConfig{FileName: replaytest.FileNamePattern("app_")}
}

func requireRootRuntimeState(t *testing.T, runtime *RootRuntime, want rootRuntimeState) {
	t.Helper()
	runtime.mu.Lock()
	got := runtime.state
	runtime.mu.Unlock()
	if got != want {
		t.Fatalf("root runtime state = %v, want %v", got, want)
	}
}
