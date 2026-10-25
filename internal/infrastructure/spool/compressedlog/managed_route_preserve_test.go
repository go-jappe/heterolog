package compressedlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestManagedRoutePreserveFileDoesNotAcquireAnotherPurgeLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval = time.Hour
		cfg.LifecycleInterval = time.Hour
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		route, err := runtime.RegisterRoute(ManagedRouteConfig{FileName: replaytest.FileNamePattern("app_")}, &replaytest.Uploader{})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, route.Close()) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		unlockPurge, err := route.BlockPurge(ctx)
		require.NoError(t, err)
		writerDone := make(chan struct{})
		go func() {
			runtime.storeOrPurgeMu.Lock()
			runtime.storeOrPurgeMu.Unlock()
			close(writerDone)
		}()
		synctest.Wait()
		source := replaytest.WriteSource(t, "app_20261003_1.log.gz")
		preserved, err := route.PreserveFile(ctx, source)
		unlockPurge()
		require.NoError(t, err, "a queued purge writer must not block preservation inside a handoff read lock")
		<-writerDone
		require.Equal(t, filepath.Join(route.Root().Path(), ".preserved"), filepath.Dir(preserved))
		original, err := os.ReadFile(source)
		require.NoError(t, err)
		copy, err := os.ReadFile(preserved)
		require.NoError(t, err)
		require.Equal(t, original, copy)
	})
}
