package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFailureArchiveRotationDefersForSharedRawProtection(t *testing.T) {
	for _, failureCategory := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		for _, ownerRole := range []string{"active", "retained"} {
			t.Run(string(failureCategory)+"/"+ownerRole, func(t *testing.T) {
				now := time.Date(2026, 9, 14, 14, 0, 0, 0, time.UTC)
				var errorCalls atomic.Int32
				pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
					cfg.enableFileCompress = true
					cfg.compressFormat = pkgfsstore.CompressGzip
					cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader("primary"))
				}, InternalLogHandlers{OnError: func(
					context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter,
				) {
					errorCalls.Add(1)
				}})
				handler := pipeline.handler
				state := handler.directoryForFailureCategory(failureCategory)
				rawPath := filepath.Join(
					failureArchiveDirectoryForFailureCategory(handler, failureCategory),
					"test-app_20260913_1.jsonl",
				)
				gzipPath := rawPath + pkgfsstore.ExtensionCompressGzip
				unpublishedPeer := rawPath + pkgfsstore.ExtensionCompressZstd
				payload := []byte("retain this rotated failure archive unchanged")
				require.NoError(t, os.WriteFile(rawPath, payload, 0o600))
				require.NoFileExists(t, unpublishedPeer)
				require.NotContains(t, handler.archiveProtectedPaths(rawPath), unpublishedPeer)

				// 別suffixは未公開でも、そのarchive処理はrawの共通キーも保護します。
				// filesystem観測や今回の生成予定gzipにないsuffixも、このキーで競合します。
				peerOwner := appendlog.NewDirectoryGuardToken()
				t.Cleanup(func() {
					state.guard.UnprotectPaths(peerOwner, rawPath, unpublishedPeer)
					state.guard.RemoveToken(peerOwner)
				})
				state.guard.ProtectPaths(peerOwner, rawPath)
				if ownerRole == "active" {
					require.NoError(t, state.guard.TransitionActive(peerOwner, func() (string, error) {
						return unpublishedPeer, nil
					}))
				} else {
					state.guard.ProtectPaths(peerOwner, unpublishedPeer)
				}
				tracker := handler.directorySizeTracker(failureCategory)
				require.True(t, tracker.TryStartObservation(now, handler.Config, true))
				tracker.FinishObservation(uint64(len(payload)))
				require.False(t, tracker.TryStartObservation(now, handler.Config, false))

				handler.archiveTaskMu.Lock()
				previousTaskGeneration := handler.archiveTaskGeneration
				handler.archiveTaskMu.Unlock()
				activePath, err := pipeline.ActiveArchivePath(failureCategory)
				require.NoError(t, err)
				handler.dispatchArchiveRotatedFile(context.Background(), activePath, rawPath, failureCategory)

				handler.archiveTaskMu.Lock()
				taskCount := handler.archiveTaskCount
				taskGeneration := handler.archiveTaskGeneration
				handler.archiveTaskMu.Unlock()
				require.Zero(t, taskCount)
				require.Equal(t, previousTaskGeneration, taskGeneration, "no archive task was started, even briefly")
				require.Zero(t, errorCalls.Load(), "deferral must not be caused by a checkpoint error")
				contents, err := os.ReadFile(rawPath)
				require.NoError(t, err)
				require.Equal(t, payload, contents)
				require.NoFileExists(t, gzipPath)
				require.NoFileExists(t, unpublishedPeer)
				assertArchivePathProtected(t, state.guard, unpublishedPeer, true)
				assertArchivePathProtected(t, state.guard, rawPath, true)
				assertArchivePathProtected(t, state.guard, gzipPath, false)
				require.True(t, tracker.TryStartObservation(now, handler.Config, false), "deferred archive invalidates the estimate")
				tracker.FinishObservation(uint64(len(payload)))

				checkpoint, err := state.uploadCheckpoints.Load(context.Background(), failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, gzipPath))
				require.NoError(t, err)

				require.Empty(t, checkpoint.CompletedDestinationIDs, "deferral must not create upload success")
				state.guard.UnprotectPaths(peerOwner, rawPath, unpublishedPeer)
				state.guard.RemoveToken(peerOwner)
				assertArchivePathProtected(t, state.guard, rawPath, false)
			})
		}
	}
}
