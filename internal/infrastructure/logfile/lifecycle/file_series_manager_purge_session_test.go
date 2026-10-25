package lifecycle

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestFileSeriesManagerGenerationPurgeKeepsAgeProtectedPathsAfterRotation(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "purge-session-protection"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o700))
	now := time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC)
	paths := make([]string, 6)
	for index := range paths {
		paths[index] = filepath.Join(directory, fmt.Sprintf("%s_20260910_%d.log", target, index+1))
		require.NoError(t, os.WriteFile(paths[index], []byte("keep or purge"), 0o600))
		modified := now.Add(-72 * time.Hour).Add(time.Duration(index) * time.Hour)
		require.NoError(t, os.Chtimes(paths[index], modified, modified))
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAge := func() { releaseOnce.Do(func() { close(release) }) }
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		Config: testFileSeriesManagerConfig(root, target, 1, 36500, 0, false),
		FileName: logpattern.FileNamePattern{
			Dir: directory, Target: target, Extension: "log",
		},
		Clock: pkgclock.NewFixedClock(now),
		Tracer: purgeConcurrencyTracer{
			spanName: appendlog.SpanFilePurgeByAge,
			beforeStart: func() {
				close(entered)
				<-release
			},
		},
	})
	activeOwner := appendlog.NewDirectoryGuardToken()
	retainedOwner := appendlog.NewDirectoryGuardToken()
	require.NoError(t, manager.directoryGuard.TransitionActive(activeOwner, func() (string, error) {
		return paths[0], nil
	}))
	manager.directoryGuard.ProtectPaths(retainedOwner, paths[3])
	t.Cleanup(func() {
		manager.directoryGuard.RemoveToken(activeOwner)
		manager.directoryGuard.UnprotectPaths(retainedOwner, paths[3])
	})
	pending := map[string]struct{}{pathidentity.AbsoluteOrCleanPath(paths[4]): {}}
	done := runPurgeConcurrencyStep(manager, "age", pending)
	t.Cleanup(func() {
		releaseAge()
		select {
		case <-done:
		case <-time.After(boundedwait.HangTimeout):
			t.Error("age and generation purge did not finish during cleanup")
		}
	})
	select {
	case <-entered:
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("purge did not retain its age candidates")
	}

	rotated := make(chan error, 1)
	go func() {
		rotated <- manager.directoryGuard.TransitionActive(activeOwner, func() (string, error) {
			return paths[5], nil
		})
	}()
	select {
	case err := <-rotated:
		require.NoError(t, err)
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("rotation waited for age purge")
	}
	// age開始時のactiveは、後段のgeneration snapshotでは既にinactiveです。
	// ageが保護fileを含むRemainingを返しても、このpassでは再び候補にしません。
	releaseAge()
	var result purgeConcurrencyRunResult
	select {
	case result = <-done:
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("age and generation purge did not finish after rotation")
	}
	require.NoError(t, result.err)
	require.Nil(t, result.panicValue)
	require.True(t, result.report.ByAge.Ran)
	require.Empty(t, result.report.ByAge.RemovalResults)
	require.True(t, result.report.ByGeneration.Ran)
	require.Len(t, result.report.ByGeneration.RemovalResults, 1)
	require.Equal(t, paths[1], result.report.ByGeneration.RemovalResults[0].File.Path)
	require.NoError(t, result.report.ByGeneration.RemovalResults[0].Err)
	require.NoFileExists(t, paths[1])
	for _, path := range []string{paths[0], paths[2], paths[3], paths[4], paths[5]} {
		require.FileExists(t, path)
	}
	for _, path := range []string{paths[0], paths[1], paths[2], paths[4]} {
		require.True(t, claimCompressionConcurrencyPath(manager.directoryGuard, path), "purge leaked its own retention: %s", path)
	}
	for _, path := range []string{paths[3], paths[5]} {
		require.False(t, claimCompressionConcurrencyPath(manager.directoryGuard, path), "purge removed another owner's protection: %s", path)
	}
}
