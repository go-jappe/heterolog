package compressedlog

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestManagedRouteSaveCancelsQueuedLocksAndCanRetry(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateActive, pkgspool.StateDead} {
		for _, lockName := range []string{"saveOrPurge", "saveOrClaim"} {
			t.Run(string(state)+"/"+lockName, func(t *testing.T) {
				cfg := managedUploaderTestConfig(t.TempDir())
				cfg.RunnerInterval = time.Hour
				runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
				route, err := runtime.RegisterRoute(ManagedRouteConfig{
					FileName: replaytest.FileNamePattern("app_"),
				}, &replaytest.Uploader{})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := route.Close(); err != nil {
						t.Error(err)
					}
				}()
				source := replaytest.WriteSource(t, "app_20260916_1.log.gz")
				save := route.SaveRetryableFailure
				if state == pkgspool.StateDead {
					save = route.SavePermanentFailure
				}
				var held sync.Locker = &route.rootRuntime.storeOrPurgeMu
				if lockName == "saveOrClaim" {
					held = &route.rootRuntime.saveOrClaimMu
				}
				held.Lock()
				unlock := sync.OnceFunc(held.Unlock)
				defer unlock()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				if _, err := save(ctx, source); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("save waiting for %s = %v, want deadline exceeded", lockName, err)
				}
				if _, err := os.Stat(source); err != nil {
					t.Fatalf("source must remain after canceled save: %v", err)
				}
				files, err := route.rootRuntime.store.ListFiles(context.Background(), state)
				if err != nil || len(files) != 0 {
					t.Fatalf("canceled save created files: %v, %v", files, err)
				}
				unlock()
				file, err := save(context.Background(), source)
				if err != nil || file.State != state {
					t.Fatalf("save retry = %+v, %v", file, err)
				}
			})
		}
	}
}
