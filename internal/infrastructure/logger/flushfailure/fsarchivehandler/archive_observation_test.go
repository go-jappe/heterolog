package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func archiveInfoPathsForTest(files []fsstore.FileInfo) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.Path)
	}
	return paths
}

func TestFailureArchiveFullLifecycleSharesOneFileInspection(t *testing.T) {
	for _, peerSuffix := range []string{"", ".unknown-codec"} {
		t.Run("peer="+peerSuffix, func(t *testing.T) {
			now := time.Date(2026, 9, 14, 21, 0, 0, 0, time.UTC)
			codec := &failureArchiveRecoveryCodec{}
			codec.failCompression.Store(true)
			failureCategory := failurearchive.FailureCategoryRetryable
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
				cfg.enableFileCompress = true
				cfg.compressFormat = codec.Format()
				cfg.compressionCodec = codec
				cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader("primary"))
			})
			handler := pipeline.handler
			// 構築時のtarget-only予約を消費し、今回はdirectory全体の観測を通します。
			require.True(t, handler.directoryForFailureCategory(failureCategory).uploadRetry.claimForAttempt(now, testUsageCacheTTL))
			rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260913", 1)
			if peerSuffix != "" {
				require.NoError(t, os.WriteFile(rawPath+peerSuffix, []byte("unverifiable existing peer"), 0o600))
			}
			var scanModes []logpattern.GlobMode
			handler.globArchiveFiles = func(pattern logpattern.FileNamePattern, mode logpattern.GlobMode) ([]string, error) {
				scanModes = append(scanModes, mode)
				return logpattern.GlobLifecycleFileNames(pattern, mode)
			}
			observationCalls := 0
			pathChecks := make(map[string]int)
			handler.inspectArchiveFiles = func(paths []string) fsstore.FileInspection {
				observationCalls++
				for _, path := range paths {
					pathChecks[path]++
				}
				return fsstore.InspectFilePaths(paths)
			}
			require.NoError(t, handler.runInlineArchivePurgeIfNeeded(context.Background(), failureCategory))
			require.Equal(t, []logpattern.GlobMode{logpattern.GlobAllFiles}, scanModes)
			require.Equal(t, 1, observationCalls, "checkpoint, recovery, pending upload and purge must reuse the observation")
			require.Equal(t, 1, pathChecks[rawPath])
			for path, checks := range pathChecks {
				require.Equal(t, 1, checks, "candidate %s", path)
			}
			require.FileExists(t, rawPath)
			if peerSuffix != "" {
				require.FileExists(t, rawPath+peerSuffix)
				require.Zero(t, codec.compressCalls.Load())
			} else {
				require.EqualValues(t, 1, codec.compressCalls.Load())
			}
		})
	}
}

func TestFailureArchiveUploadCursorSurvivesFileInspectionErrors(t *testing.T) {
	for _, route := range []string{"full", "target-only"} {
		for _, observationErr := range []error{os.ErrPermission, os.ErrNotExist, errors.New("not a regular file")} {
			t.Run(route+"/"+observationErr.Error(), func(t *testing.T) {
				now := time.Date(2026, 9, 14, 21, 30, 0, 0, time.UTC)
				failureCategory := failurearchive.FailureCategoryRetryable
				const required objectstorage.DestinationID = "original-required"
				pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
					cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader(required))
				})
				handler := pipeline.handler
				fileName := handler.Fsstore.FileNameRetryable
				path := filepath.Join(fileName.Dir, "test-app_20260913_1.jsonl.gz")
				store := handler.directoryForFailureCategory(failureCategory).uploadCheckpoints
				key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, path)
				_, err := store.AdvanceCompletionCursor(context.Background(), key, []objectstorage.DestinationID{required})
				require.NoError(t, err)
				observation := fsstore.FileInspection{Paths: []string{path}, PathErrors: map[string]error{path: observationErr}}
				handler.inspectArchiveFiles = func([]string) fsstore.FileInspection {
					t.Fatal("observed upload retention must not inspect paths again")
					return fsstore.FileInspection{}
				}
				var protection archivePathProtection
				if route == "full" {
					protection, err = handler.retainObservedArchivesForUpload(context.Background(), failureCategory, newArchiveDirectoryInspection(observation, fileName))
				} else {
					protection, err = handler.retainArchivePathsForUpload(context.Background(), failureCategory, fileName, fsstore.GroupFilesByCompressionSource(observation, fileName.Extension))
				}
				if errors.Is(observationErr, os.ErrNotExist) {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, observationErr)
				}
				require.Empty(t, protection.paths)
				checkpoint, err := store.Load(context.Background(), key)
				require.NoError(t, err)
				require.Equal(t, []objectstorage.DestinationID{required}, checkpoint.CompletedDestinationIDs)
			})
		}
	}
}

func TestFailureArchiveRotationObservesOnlyTheRequestedSource(t *testing.T) {
	pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 14, 22, 0, 0, 0, time.UTC), nil)
	handler := pipeline.handler
	directory := t.TempDir()
	source := filepath.Join(directory, "test-app_20260913_1.jsonl")
	peer := source + ".gz"
	for _, path := range []string{source, peer, source + ".gz.tmp", filepath.Join(directory, "test-app_20260913_2.jsonl.gz")} {
		require.NoError(t, os.WriteFile(path, nil, 0o600))
	}
	var observed []string
	handler.inspectArchiveFiles = func(paths []string) fsstore.FileInspection {
		observed = append(observed, paths...)
		return fsstore.InspectFilePaths(paths)
	}
	resolved, exists, err := handler.findExistingCompressedArchivePath(source)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, peer, resolved)
	require.ElementsMatch(t, []string{source, peer}, observed)
}

func TestFailureArchiveUploadUsesObservedSourceGroups(t *testing.T) {
	for _, route := range []string{"full", "target-only"} {
		t.Run(route, func(t *testing.T) {
			category := failurearchive.FailureCategoryRetryable
			pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 14, 22, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
				cfg.enableFileCompress = true
				cfg.setArchiveUploader(category, newDestinationFailureArchiveUploader("primary"))
			})
			handler := pipeline.handler
			fileName := handler.Fsstore.FileNameRetryable
			readySource := filepath.Join(fileName.Dir, "test-app_20260913_1.jsonl")
			unretiredSource := filepath.Join(fileName.Dir, "test-app_20260913_2.jsonl")
			otherTarget := filepath.Join(fileName.Dir, "test-other_20260913_1.jsonl.custom")
			invalidPath := filepath.Join(fileName.Dir, "test-app_20260913_01.jsonl.custom")
			paths := []string{readySource + ".custom", readySource + ".gz", unretiredSource, unretiredSource + ".gz", otherTarget}
			for _, path := range paths {
				require.NoError(t, os.WriteFile(path, []byte("observed file"), 0o600))
			}
			require.NoError(t, os.Mkdir(invalidPath, 0o700))
			files := fsstore.InspectFilePaths(append(paths, invalidPath))
			handler.inspectArchiveFiles = func([]string) fsstore.FileInspection {
				t.Fatal("upload retention must consume the existing source groups")
				return fsstore.FileInspection{}
			}
			var protection archivePathProtection
			var err error
			if route == "full" {
				observation := newArchiveDirectoryInspection(files, fileName)
				require.Contains(t, archiveInfoPathsForTest(observation.allFileInfos()), otherTarget)
				protection, err = handler.retainObservedArchivesForUpload(context.Background(), category, observation)
			} else {
				protection, err = handler.retainArchivePathsForUpload(context.Background(), category, fileName,
					fsstore.GroupFilesByCompressionSource(files, fileName.Extension))
			}
			require.NoError(t, err)
			defer handler.releaseArchivePathProtection(category, protection)
			require.Equal(t, []string{readySource + ".custom"}, protection.paths)
			guard := handler.directoryForFailureCategory(category).guard
			for _, path := range []string{readySource, readySource + ".custom", readySource + ".gz"} {
				assertArchivePathProtected(t, guard, path, true)
			}
			for _, path := range []string{unretiredSource, unretiredSource + ".gz", otherTarget, invalidPath} {
				assertArchivePathProtected(t, guard, path, false)
			}
		})
	}
}
