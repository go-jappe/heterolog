package compressedlog

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestRootRuntimeReplayAndPurgeObserversCanRequestClose(t *testing.T) {
	for _, operation := range []string{"replay", "purge"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := managedUploaderTestConfig(t.TempDir())
				cfg.RunnerInterval, cfg.LifecycleInterval = time.Hour, time.Hour
				if operation == "replay" {
					cfg.RunnerInterval = time.Second
				} else {
					cfg.LifecycleInterval = time.Second
					cfg.Dead = objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
						Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 1 << 20,
					}
				}
				runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
				type closeResult struct {
					replay *RunReport
					purge  *objectstoragespool.TerminalPurgeReport
					err    error
				}
				closed := make(chan closeResult, 1)
				ownerReady := make(chan struct{})
				var route *ManagedRoute
				route, err := runtime.RegisterRoute(ManagedRouteConfig{
					FileName: replaytest.FileNamePattern("app_"),
					RunObserver: func(_ context.Context, replay *RunReport, purge *objectstoragespool.TerminalPurgeReport, runErr error) {
						<-ownerReady
						runtime.RequestClose()
						route.RequestClose()
						closed <- closeResult{replay: replay, purge: purge, err: runErr}
					},
				}, &replaytest.Uploader{})
				if err != nil {
					t.Fatal(err)
				}
				close(ownerReady)
				defer route.Close()
				source := replaytest.WriteSource(t, "app_20260922_1.log.gz")
				if operation == "replay" {
					if _, err := route.SaveRetryableFailure(context.Background(), source); err != nil {
						t.Fatal(err)
					}
				} else {
					file, err := route.SavePermanentFailure(context.Background(), source)
					if err != nil {
						t.Fatal(err)
					}
					old := time.Now().Add(-48 * time.Hour)
					if err := os.Chtimes(file.Path, old, old); err != nil {
						t.Fatal(err)
					}
				}
				synctest.Wait()
				time.Sleep(time.Second)
				synctest.Wait()
				select {
				case result := <-closed:
					if result.err != nil {
						t.Fatal(result.err)
					}
					if operation == "replay" {
						if result.replay == nil || result.purge != nil || len(result.replay.CompressedLogFiles) != 1 || result.replay.CompressedLogFiles[0].State != pkgspool.StateDone {
							t.Fatalf("replay observer result = %+v", result)
						}
						_ = replaytest.OnlyFile(t, runtime.store, pkgspool.StateDone)
					} else if result.replay != nil || result.purge == nil || result.purge.ByState[pkgspool.StateDead].DeletedFileCount != 1 {
						t.Fatalf("purge observer result = %+v", result)
					}
				default:
					t.Fatal("observer did not complete both owner closes")
				}
				select {
				case <-runtime.shutdownComplete:
				default:
					t.Fatal("observer close returned before root retirement")
				}
			})
		})
	}
}

func TestRootRuntimeRetirementWaitsForAcceptedSave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval, cfg.LifecycleInterval = time.Hour, time.Hour
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		route, err := runtime.RegisterRoute(stateTestRouteConfig(), &replaytest.Uploader{})
		if err != nil {
			t.Fatal(err)
		}
		defer route.Close()
		synctest.Wait()

		runtime.saveOrClaimMu.Lock()
		unlock := sync.OnceFunc(runtime.saveOrClaimMu.Unlock)
		defer unlock()
		type saveResult struct {
			file objectstoragespool.CompressedLogFile
			err  error
		}
		saved := make(chan saveResult, 1)
		source := replaytest.WriteSource(t, "app_20260922_2.log.gz")
		go func() {
			file, err := route.SaveRetryableFailure(context.Background(), source)
			saved <- saveResult{file: file, err: err}
		}()
		// 保存はadmissionを通過し、claim lockを待つ間purgeのread lockを保持しています。
		// そのcontextは引き続きcallerが所有します。
		synctest.Wait()
		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}
		closed := make(chan error, 1)
		go func() { closed <- route.Close() }()
		synctest.Wait()
		requireRootRuntimeState(t, runtime, rootRuntimeShutdownStarted)

		type replacementResult struct {
			runtime *RootRuntime
			err     error
		}
		replaced := make(chan replacementResult, 1)
		go func() {
			replacement, err := NewRootRuntime(NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
			replaced <- replacementResult{runtime: replacement, err: err}
		}()
		synctest.Wait()
		if len(saved) != 0 || len(closed) != 0 || len(replaced) != 0 {
			t.Errorf("blocked save/close/replacement completed early: %d/%d/%d", len(saved), len(closed), len(replaced))
		}
		unlock()
		save := <-saved
		closeErr := <-closed
		replacement := <-replaced
		if replacement.runtime != nil {
			defer replacement.runtime.Close()
		}
		if err := errors.Join(save.err, closeErr, replacement.err); err != nil {
			t.Fatal(err)
		}
		if save.file.State != pkgspool.StateActive || !reflect.DeepEqual(save.file, replaytest.OnlyFile(t, replacement.runtime.store, pkgspool.StateActive)) {
			t.Fatalf("accepted save was not retained for the replacement root: %+v", save.file)
		}
	})
}

func TestRootRuntimeZeroRouteGapPreservesReplayFilesAndContinuesPurge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval, cfg.LifecycleInterval = time.Second, 2*time.Second
		cfg.Dead = objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
			Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 1 << 20,
		}
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		upload := &replaytest.Uploader{}
		route, err := runtime.RegisterRoute(stateTestRouteConfig(), upload)
		if err != nil {
			t.Fatal(err)
		}
		defer route.Close()
		synctest.Wait()
		if err := route.Close(); err != nil {
			t.Fatal(err)
		}
		replaytest.SaveFile(t, route.store, "app_20260922_3.log.gz")
		processing, claimed, err := route.store.ClaimOldestActive(context.Background())
		if err != nil || !claimed {
			t.Fatalf("claim processing fixture = %v, %v", claimed, err)
		}
		active := replaytest.SaveFile(t, route.store, "app_20260922_4.log.gz")
		dead, err := route.store.SavePermanentFailure(context.Background(), replaytest.WriteSource(t, "app_20260922_5.log.gz"))
		if err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-48 * time.Hour)
		for _, file := range []objectstoragespool.CompressedLogFile{processing, active, dead} {
			if err := os.Chtimes(file.Path, old, old); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		for _, file := range []objectstoragespool.CompressedLogFile{processing, active} {
			if got := replaytest.OnlyFile(t, runtime.store, file.State); !reflect.DeepEqual(got, file) {
				t.Fatalf("zero-route gap changed %s file: %+v, want %+v", file.State, got, file)
			}
			info, err := os.Stat(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			if !info.ModTime().Equal(old) {
				t.Fatalf("zero-route gap changed %s mtime: %v, want %v", file.State, info.ModTime(), old)
			}
		}
		if _, err := os.Stat(dead.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("terminal purge did not run during zero-route gap: %v", err)
		}
		if upload.CallCount() != 0 {
			t.Fatal("zero-route gap uploaded a file")
		}
		resumed, err := runtime.RegisterRoute(stateTestRouteConfig(), upload)
		if err != nil {
			t.Fatal(err)
		}
		defer resumed.Close()
		time.Sleep(time.Second)
		synctest.Wait()
		if upload.CallCount() != 1 || replaytest.OnlyFile(t, runtime.store, pkgspool.StateDone).FileName != active.FileName {
			t.Fatal("re-registration did not resume the oldest active file")
		}
		if replaytest.OnlyFile(t, runtime.store, pkgspool.StateActive).FileName != processing.FileName {
			t.Fatal("re-registration did not recover the processing file behind waiting active work")
		}
	})
}
