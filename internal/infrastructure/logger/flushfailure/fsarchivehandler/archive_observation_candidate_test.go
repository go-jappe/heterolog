package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFailureArchiveFullLifecycleIgnoresNonCanonicalInspectionErrors(t *testing.T) {
	for _, phase := range []string{"recovery", "purge"} {
		t.Run(phase, func(t *testing.T) {
			now := time.Date(2026, 9, 14, 22, 30, 0, 0, time.UTC)
			codec := &failureArchiveRecoveryCodec{}
			failureCategory := failurearchive.FailureCategoryRetryable
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.maxFileAge = 30
				cfg.enableFileCompress = phase == "recovery"
				cfg.compressionCodec = codec
				cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader("primary"))
			})
			handler := pipeline.handler
			fileName := handler.Fsstore.FileNameRetryable
			// 構築時のtarget-only予約を消費し、今回はdirectory全体の観測を通します。
			require.True(t, handler.directoryForFailureCategory(failureCategory).uploadRetry.claimForAttempt(now, testUsageCacheTTL))
			invalidPath := filepath.Join(fileName.Dir, "test-app_notes_1.jsonl.gz")
			require.NoError(t, os.Mkdir(invalidPath, 0o700))
			rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260801", 1)
			old := now.AddDate(0, 0, -40)
			require.NoError(t, os.Chtimes(rawPath, old, old))
			if phase == "purge" {
				// 未圧縮fileは送信後も通常の保持・purgeへ残ります。送信済みの世代として記録し、
				// 送信待ちの保護ではなく保持期限のpurge対象にします。
				advanceArchiveUploadCursorForTest(t, handler, failureCategory, rawPath)
			}
			var scanModes []logpattern.GlobMode
			handler.globArchiveFiles = func(pattern logpattern.FileNamePattern, mode logpattern.GlobMode) ([]string, error) {
				scanModes = append(scanModes, mode)
				return logpattern.GlobLifecycleFileNames(pattern, mode)
			}
			observationCalls := 0
			handler.inspectArchiveFiles = func(paths []string) fsstore.FileInspection {
				observationCalls++
				observation := fsstore.InspectFilePaths(paths)
				require.Error(t, observation.PathErrors[invalidPath], "the unrelated directory must be rejected by Lstat")
				return observation
			}

			require.NoError(t, handler.runInlineArchivePurgeIfNeeded(context.Background(), failureCategory))
			require.NoFileExists(t, rawPath)
			require.DirExists(t, invalidPath)
			require.Equal(t, []logpattern.GlobMode{logpattern.GlobAllFiles}, scanModes)
			require.Equal(t, 1, observationCalls)
			if phase == "recovery" {
				require.FileExists(t, rawPath+".gz")
				require.EqualValues(t, 1, codec.compressCalls.Load())
			} else {
				require.NoFileExists(t, rawPath+".gz")
				require.Zero(t, codec.compressCalls.Load())
			}
		})
	}
}

func TestFailureArchiveTargetUploadIgnoresNonCanonicalInspectionErrors(t *testing.T) {
	now := time.Date(2026, 9, 14, 22, 45, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryRetryable
	const destination objectstorage.DestinationID = "primary"
	remote := newDestinationFailureArchiveUploader(destination)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, remote)
	})
	handler := pipeline.handler
	fileName := handler.Fsstore.FileNameRetryable
	invalidPath := filepath.Join(fileName.Dir, "test-app_20260913_01.jsonl.gz")
	require.NoError(t, os.Mkdir(invalidPath, 0o700))
	compressedPath := filepath.Join(fileName.Dir, "test-app_20260913_1.jsonl.gz")
	require.NoError(t, os.WriteFile(compressedPath, []byte("compressed archive"), 0o600))
	var scanModes []logpattern.GlobMode
	handler.globArchiveFiles = func(pattern logpattern.FileNamePattern, mode logpattern.GlobMode) ([]string, error) {
		scanModes = append(scanModes, mode)
		return logpattern.GlobFileNames(pattern, mode)
	}

	require.NoError(t, handler.runInlineArchivePurgeIfNeeded(context.Background(), failureCategory))
	require.NoFileExists(t, compressedPath)
	require.DirExists(t, invalidPath)
	require.Equal(t, 1, remote.attemptCount(compressedPath, destination))
	require.Equal(t, []logpattern.GlobMode{logpattern.GlobByTarget}, scanModes)
}
