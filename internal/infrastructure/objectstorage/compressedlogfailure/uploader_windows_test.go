package compressedlogfailure

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestManagedUploaderWindowsJoinPreservesOpenReaderAndLiveStaging(t *testing.T) {
	for _, alias := range []string{"same", "case", "extended"} {
		t.Run(alias, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				base := t.TempDir()
				failureStoreConfig := testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})
				cfg := ManagedUploaderConfig{
					CompressedLogFileUploadFailureStore: NewConfigAdapter(failureStoreConfig),
					FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
				}
				remote := &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}}
				first := newTestManagedUploader(t, cfg, remote)
				// 最初のcoordinatorは起動直後にscanします。次の周期を待つ状態まで
				// 時刻を進めずに待ち、長いscanを表すreaderを保持して別sourceを参加させます。
				synctest.Wait()
				unlockPurge, err := archiveRouteForTest(t, first, "destination-a").owner.coordinator.lockPurgeDuringSave(context.Background())
				require.NoError(t, err)
				defer unlockPurge()
				root := archiveRouteForTest(t, first, "destination-a").store.Root()
				staging := filepath.Join(root.RetryableDir(), ".compressed-log-in-progress.tmp")
				require.NoError(t, os.WriteFile(staging, []byte("live handoff"), 0o600))
				// 通常のdirectory readerはDELETEを共有しません。別sourceの参加では
				// 同じownerを使い、独立したStore lockで同じlayoutを初期化するための
				// MAXIMUM_ALLOWEDによる再openを行わないことを確認します。
				reader, err := os.Open(root.RetryableDir())
				require.NoError(t, err)
				defer reader.Close()
				archiveConfig := &failureStoreConfig.DurableStoreConfig.FileArchiveConfig
				archiveConfig.RootPath = windowsArchiveRootAlias(archiveConfig.RootPath, alias)
				cfg.CompressedLogFileUploadFailureStore = NewConfigAdapter(failureStoreConfig)
				second := newTestManagedUploader(t, cfg, remote)
				require.Same(t, archiveRouteForTest(t, first, "destination-a").owner.coordinator, archiveRouteForTest(t, second, "destination-a").owner.coordinator)
				require.Same(t, archiveRouteForTest(t, first, "destination-a").store, archiveRouteForTest(t, second, "destination-a").store)
				_, err = reader.Readdirnames(-1)
				require.NoError(t, err)
				contents, err := os.ReadFile(staging)
				require.NoError(t, err)
				require.Equal(t, "live handoff", string(contents))

				require.NoError(t, second.Close())
				archiveLifecycleRegistry.Lock()
				registered := archiveLifecycleRegistry.byRoot[pathidentity.ComparisonKey(root.Path())]
				archiveLifecycleRegistry.Unlock()
				require.Same(t, archiveRouteForTest(t, first, "destination-a").owner.coordinator, registered)
			})
		})
	}
}

func TestManagedUploaderWindowsAliasReplacementWaitsForRetirement(t *testing.T) {
	for _, alias := range []string{"case", "extended"} {
		t.Run(alias, func(t *testing.T) {
			testManagedUploaderReplacementWaitsForRetirement(t, func(path string) string {
				return windowsArchiveRootAlias(path, alias)
			})
		})
	}
}

func windowsArchiveRootAlias(path, alias string) string {
	switch alias {
	case "case":
		return strings.ToUpper(path)
	case "extended":
		return `\\?\` + strings.ToUpper(path)
	default:
		return path
	}
}
