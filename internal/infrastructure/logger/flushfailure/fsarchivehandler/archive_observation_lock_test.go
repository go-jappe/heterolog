package fsarchivehandler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFailureArchiveObservationAllowsRotationAndKeepsFileNameSnapshot(t *testing.T) {
	for _, failureCategory := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		for _, route := range []string{"full", "target-only"} {
			for _, phase := range []string{"glob", "lstat"} {
				t.Run(string(failureCategory)+"/"+route+"/"+phase, func(t *testing.T) {
					now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
					pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
						cfg.enableRotate = true
						cfg.lifecycleMode = pkgfsstore.FileLifecycleModeDisable
					})
					handler := pipeline.handler
					oldActive, err := pipeline.ActiveArchivePath(failureCategory)
					require.NoError(t, err)

					// 観測のI/Oがblockされている間に、sizeによる実際のrotationが、writer lockの
					// 下でIndexとactive handleを同時に変更します。
					handler.Fsstore.Mu.Lock()
					originalPattern, patternErr := handler.archiveFileNamePattern(failureCategory)
					activeFile := handler.Fsstore.FileRetryable
					if failureCategory == failurearchive.FailureCategoryPermanent {
						activeFile = handler.Fsstore.FilePermanent
					}
					_, writeErr := activeFile.Write(make([]byte, handler.Config.MaxFileSize()+1))
					handler.Fsstore.Mu.Unlock()
					require.NoError(t, patternErr)
					require.NoError(t, writeErr)

					entered := make(chan struct{})
					release := make(chan struct{})
					var releaseOnce sync.Once
					unblock := func() { releaseOnce.Do(func() { close(release) }) }
					block := func() {
						close(entered)
						<-release
					}
					var globPattern logpattern.FileNamePattern
					var globMode logpattern.GlobMode
					handler.globArchiveFiles = func(pattern logpattern.FileNamePattern, mode logpattern.GlobMode) ([]string, error) {
						globPattern, globMode = pattern, mode
						if phase == "glob" {
							block()
						}
						return logpattern.GlobLifecycleFileNames(pattern, mode)
					}
					handler.inspectArchiveFiles = func(paths []string) fsstore.FileInspection {
						if phase == "lstat" {
							block()
						}
						return fsstore.InspectFilePaths(paths)
					}

					var observed fsstore.FileInspection
					var observedPattern logpattern.FileNamePattern
					var observationErr error
					observationDone := make(chan struct{})
					var writerDone chan struct{}
					t.Cleanup(func() {
						unblock()
						waitInlineArchivePurge(t, observationDone, "observation cleanup")
						if writerDone != nil {
							waitInlineArchivePurge(t, writerDone, "writer cleanup")
						}
					})
					go func() {
						defer close(observationDone)
						if route == "full" {
							var observation archiveDirectoryInspection
							observation, observationErr = handler.observeArchiveDirectoryForPurge(failureCategory)
							observed, observedPattern = observation.directoryFiles, observation.seriesPattern
						} else {
							observed, observedPattern, observationErr = handler.observeArchiveFileSeriesForUpload(failureCategory)
						}
					}()
					waitInlineArchivePurge(t, entered, "observation "+phase)

					result := retryableArchiveResult(now, "write-during-observation")
					if failureCategory == failurearchive.FailureCategoryPermanent {
						result.DeadRecords = result.RetryableRecords
						result.RetryableRecords = nil
					}
					var attempt failureArchiveWriteAttempt
					writerDone = make(chan struct{})
					go func() {
						defer close(writerDone)
						attempt = handler.rotateAndWriteFailureArchive(context.Background(), result)
					}()
					waitInlineArchivePurge(t, writerDone, "rotation and append while observation "+phase+" is blocked")
					require.NoError(t, attempt.rotationErr)
					require.NoError(t, attempt.archiveErr)
					require.False(t, attempt.overloaded)
					require.Empty(t, attempt.deferredRotations)
					newActive, err := pipeline.ActiveArchivePath(failureCategory)
					require.NoError(t, err)
					require.NotEqual(t, oldActive, newActive)
					require.Len(t, readArchiveRecords(t, newActive), 1)

					handler.Fsstore.Mu.Lock()
					currentPattern, err := handler.archiveFileNamePattern(failureCategory)
					handler.Fsstore.Mu.Unlock()
					require.NoError(t, err)
					require.Equal(t, originalPattern.Index+1, currentPattern.Index)

					unblock()
					waitInlineArchivePurge(t, observationDone, "observation completion")
					require.NoError(t, observationErr)
					require.Equal(t, originalPattern, globPattern)
					require.Equal(t, originalPattern, observedPattern)
					wantMode := logpattern.GlobAllFiles
					if route == "target-only" {
						wantMode = logpattern.GlobByTarget
					}
					require.Equal(t, wantMode, globMode)
					require.Contains(t, observed.Paths, oldActive)
					require.Empty(t, observed.PathErrors)
				})
			}
		}
	}
}
