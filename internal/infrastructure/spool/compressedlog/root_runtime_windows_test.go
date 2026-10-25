package compressedlog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"

	"github.com/stretchr/testify/require"
)

func TestManagedRouteWindowsJoinPreservesOpenReaderAndLiveStaging(t *testing.T) {
	for _, alias := range []string{"same", "case", "extended"} {
		t.Run(alias, func(t *testing.T) {
			spoolConfig := managedUploaderTestConfig(t.TempDir())
			spoolConfig.RunnerInterval = time.Hour
			spoolConfig.LifecycleInterval = time.Hour
			runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(spoolConfig), objectstorage.UploadTargetS3))
			cfg := ManagedRouteConfig{
				FileName: replaytest.FileNamePattern("app_"),
			}
			first, err := runtime.RegisterRoute(cfg, &replaytest.Uploader{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, first.Close()) })
			unlockPurge, err := first.BlockPurge(context.Background())
			require.NoError(t, err)
			defer unlockPurge()
			root := first.store.Root()
			staging := filepath.Join(root.Path(), ".staging", ".compressed-log-in-progress.tmp")
			require.NoError(t, os.WriteFile(staging, []byte("live handoff"), 0o600))

			// os.OpenはFILE_SHARE_DELETEを持たない通常のdirectory readerです。
			// Root reservation rejection and a second registration must leave
			// the live directory reader and the in-progress staging file intact.
			reader, err := os.Open(root.ActiveDir())
			require.NoError(t, err)
			defer reader.Close()
			spoolConfig.SpoolPath = windowsCoordinatorRootAlias(spoolConfig.SpoolPath, alias)
			duplicate, err := NewRootRuntime(NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(spoolConfig), objectstorage.UploadTargetS3))
			if duplicate != nil {
				_ = duplicate.Close()
			}
			require.Nil(t, duplicate)
			require.ErrorIs(t, err, ErrSpoolRootAlreadyOwned)
			second, err := runtime.RegisterRoute(cfg, &replaytest.Uploader{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, second.Close()) })
			require.Same(t, first.rootRuntime, second.rootRuntime)
			require.Same(t, first.rootRuntime.store, second.rootRuntime.store)
			_, err = reader.Readdirnames(-1)
			require.NoError(t, err)
			contents, err := os.ReadFile(staging)
			require.NoError(t, err)
			require.Equal(t, "live handoff", string(contents))

			require.NoError(t, second.Close())
			rootRuntimeRegistry.Lock()
			registered := rootRuntimeRegistry.byRoot[pathidentity.ComparisonKey(runtime.Root().Path())]
			rootRuntimeRegistry.Unlock()
			require.Same(t, first.rootRuntime, registered)
		})
	}
}

func TestManagedRouteWindowsAliasReplacementWaitsForRetirement(t *testing.T) {
	for _, alias := range []string{"case", "extended"} {
		t.Run(alias, func(t *testing.T) {
			testManagedRouteReplacementWaitsForRetirement(t, func(path string) string {
				return windowsCoordinatorRootAlias(path, alias)
			})
		})
	}
}

func windowsCoordinatorRootAlias(path, alias string) string {
	switch alias {
	case "case":
		return strings.ToUpper(path)
	case "extended":
		return `\\?\` + strings.ToUpper(path)
	default:
		return path
	}
}
