package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestFileSeriesManagerPurgeRetainsCandidatesWithoutBlockingWriterTransitions(t *testing.T) {
	for _, step := range []string{"age", "generation", "directory size"} {
		t.Run(step, func(t *testing.T) {
			for _, outcome := range []string{"success", "remove error", "panic"} {
				t.Run(outcome, func(t *testing.T) {
					entered := make(chan struct{})
					release := make(chan struct{})
					var enteredOnce, releaseOnce sync.Once
					releasePurge := func() { releaseOnce.Do(func() { close(release) }) }
					const purgePanic = "purge span start panicked"
					tracer := purgeConcurrencyTracer{
						spanName: purgeConcurrencySpanName(step),
						beforeStart: func() {
							enteredOnce.Do(func() { close(entered) })
							<-release
							if outcome == "panic" {
								panic(purgePanic)
							}
						},
					}
					manager, paths, pending := newPurgeConcurrencyManager(t, step, tracer)
					candidate := paths[0]
					done := runPurgeConcurrencyStep(manager, step, pending)
					t.Cleanup(func() {
						releasePurge()
						select {
						case <-done:
						case <-time.After(boundedwait.HangTimeout):
							t.Error("purge did not finish during cleanup")
						}
					})
					select {
					case <-entered:
					case <-time.After(boundedwait.HangTimeout):
						t.Fatal("purge did not reach the deletion span gate")
					}
					assertPurgeConcurrentWriterTransition(t, manager.directoryGuard, candidate, releasePurge)
					require.False(t, claimCompressionConcurrencyPath(manager.directoryGuard, candidate), "purge candidate was not retained")
					if outcome == "remove error" {
						// 観測後のos.Removeだけを確実に失敗させるfault fixtureです。
						// 元fileは退避し、同じpathの非空directoryをRemoveへ渡します。
						require.NoError(t, os.Rename(candidate, filepath.Join(resolvedTempDirForTest(t), "saved-candidate")))
						require.NoError(t, os.Mkdir(candidate, 0o700))
						require.NoError(t, os.WriteFile(filepath.Join(candidate, "child"), []byte("x"), 0o600))
					}
					releasePurge()
					var result purgeConcurrencyRunResult
					select {
					case result = <-done:
					case <-time.After(boundedwait.HangTimeout):
						t.Fatal("purge did not finish after release")
					}
					switch outcome {
					case "success":
						require.NoError(t, result.err)
						require.Nil(t, result.panicValue)
						require.NoFileExists(t, candidate)
					case "remove error":
						require.Error(t, result.err)
						require.Nil(t, result.panicValue)
						require.DirExists(t, candidate)
					case "panic":
						require.Equal(t, purgePanic, result.panicValue)
						require.FileExists(t, candidate)
					}
					for _, path := range paths[:2] {
						require.True(t, claimCompressionConcurrencyPath(manager.directoryGuard, path), "purge leaked candidate retention: %s", path)
					}
					// active・他ownerのretain・今回pending・latestは削除されません。
					for _, path := range paths[2:] {
						require.FileExists(t, path)
					}
					for _, path := range paths[2:4] {
						require.False(t, claimCompressionConcurrencyPath(manager.directoryGuard, path), "purge removed another owner's protection")
					}
				})
			}
		})
	}
}

func TestFileSeriesManagerPurgePeerVerificationDoesNotBlockWriterTransitions(t *testing.T) {
	for _, step := range []string{"age", "generation"} {
		t.Run(step, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			var enteredOnce, releaseOnce sync.Once
			releaseVerification := func() { releaseOnce.Do(func() { close(release) }) }
			codec := &fileSeriesConcurrencyCodec{
				runnerTestCompressionCodec: &runnerTestCompressionCodec{},
				beforeVerify: func() error {
					enteredOnce.Do(func() { close(entered) })
					<-release
					return nil
				},
			}
			manager, sourcePath := newCompressionConcurrencyManager(t, codec, true)
			limits := &appendlog.FilePurgeConfigAdapter{}
			if step == "age" {
				limits.MaxFileAgeConfig = 1
			} else {
				limits.MaxFileBackupsConfig = 1
			}
			manager.Config = appendlog.WithPurgeConfig(manager.Config, limits)
			peerPath := sourcePath + ".br"
			old := time.Now().Add(-72 * time.Hour)
			for _, path := range []string{sourcePath, peerPath} {
				require.NoError(t, os.Chtimes(path, old, old))
			}
			// 前段の回復ではなくpurge候補の除外判定から、実codecの完全性検証へ入ります。
			done := runPurgeConcurrencyStep(manager, step, nil)
			t.Cleanup(func() {
				releaseVerification()
				select {
				case <-done:
				case <-time.After(boundedwait.HangTimeout):
					t.Error("peer verification did not finish during cleanup")
				}
			})
			select {
			case <-entered:
			case <-time.After(boundedwait.HangTimeout):
				t.Fatal("purge exclusions did not reach peer verification")
			}
			assertPurgeConcurrentWriterTransition(t, manager.directoryGuard, sourcePath, releaseVerification)
			for _, path := range []string{sourcePath, peerPath} {
				require.FileExists(t, path)
				require.False(t, claimCompressionConcurrencyPath(manager.directoryGuard, path), "verification candidate was not retained")
			}
			releaseVerification()
			select {
			case result := <-done:
				require.NoError(t, result.err)
				require.Nil(t, result.panicValue)
			case <-time.After(boundedwait.HangTimeout):
				t.Fatal("purge did not finish after peer verification")
			}
			compressCalls, verifyCalls := codec.calls()
			require.Zero(t, compressCalls, "purge must not recompress the raw candidate")
			require.Equal(t, 1, verifyCalls)
			for _, path := range []string{sourcePath, peerPath} {
				require.True(t, claimCompressionConcurrencyPath(manager.directoryGuard, path), "verification leaked retention")
			}
		})
	}
}

type purgeConcurrencyTracer struct {
	spanName    string
	beforeStart func()
}

func (t purgeConcurrencyTracer) StartSpan(
	ctx context.Context,
	name string,
	attrs ...observer.Attr,
) (context.Context, observer.Span) {
	if name == t.spanName {
		t.beforeStart()
	}
	return observer.NopTracer{}.StartSpan(ctx, name, attrs...)
}

func purgeConcurrencySpanName(step string) string {
	switch step {
	case "age":
		return appendlog.SpanFilePurgeByAge
	case "generation":
		return appendlog.SpanFilePurgeByGen
	default:
		return appendlog.SpanFilePurgeByDirSize
	}
}

func newPurgeConcurrencyManager(
	t *testing.T,
	step string,
	tracer observer.Tracer,
) (*FileSeriesManager, []string, map[string]struct{}) {
	t.Helper()
	root := resolvedTempDirForTest(t)
	const target = "purge-concurrency"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o700))
	now := time.Date(2026, time.September, 13, 12, 0, 0, 0, time.UTC)
	paths := make([]string, 6)
	for index := range paths {
		paths[index] = filepath.Join(directory, fmt.Sprintf("%s_20260910_%d.log", target, index+1))
		require.NoError(t, os.WriteFile(paths[index], []byte("x"), 0o600))
		modified := now.Add(-72 * time.Hour).Add(time.Duration(index) * time.Hour)
		require.NoError(t, os.Chtimes(paths[index], modified, modified))
	}
	var backups, age int
	var directoryBytes uint64
	switch step {
	case "age":
		age = 1
	case "generation":
		backups = 1
	case "directory size":
		// 全6fileで6B、削除可能fileだけでは2Bです。保護fileも容量へ
		// 含めなければ上限5Bを超えず、gateへ到達できません。
		directoryBytes = 5
	}
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		Config: testFileSeriesManagerConfig(root, target, backups, age, directoryBytes, false),
		FileName: logpattern.FileNamePattern{
			Dir: directory, Target: target, Extension: "log",
		},
		Clock:  pkgclock.NewFixedClock(now),
		Tracer: tracer,
	})
	activeOwner := appendlog.NewDirectoryGuardToken()
	retainedOwner := appendlog.NewDirectoryGuardToken()
	require.NoError(t, manager.directoryGuard.TransitionActive(activeOwner, func() (string, error) {
		return paths[2], nil
	}))
	manager.directoryGuard.ProtectPaths(retainedOwner, paths[3])
	t.Cleanup(func() {
		manager.directoryGuard.RemoveToken(activeOwner)
		manager.directoryGuard.UnprotectPaths(retainedOwner, paths[3])
	})
	return manager, paths, map[string]struct{}{pathidentity.AbsoluteOrCleanPath(paths[4]): {}}
}

type purgeConcurrencyRunResult struct {
	report     PurgeReport
	err        error
	panicValue any
}

// runPurgeConcurrencyStepは前段の圧縮を省き、production同様のlifecycle lock内で
// 指定purgeだけを実行します。panicはtestへ戻し、保持解除まで確認します。
func runPurgeConcurrencyStep(
	manager *FileSeriesManager,
	step string,
	pending map[string]struct{},
) <-chan purgeConcurrencyRunResult {
	done := make(chan purgeConcurrencyRunResult, 1)
	go func() {
		var result purgeConcurrencyRunResult
		defer func() {
			result.panicValue = recover()
			done <- result
			close(done)
		}()
		lifecycle, err := NewDirectoryLifecycle(manager)
		if err != nil {
			result.err = err
			return
		}
		result.err = manager.directoryGuard.RunWithLifecycleLock(func() error {
			if step == "directory size" {
				report, err := lifecycle.runDirectorySizePurge(context.Background(), pending)
				result.report.ByDirSize = report
				return err
			}
			report, err := manager.purgeFileSeriesByAgeAndGeneration(context.Background(), pending)
			result.report = report.report
			return err
		})
	}()
	return done
}

func assertPurgeConcurrentWriterTransition(
	t *testing.T,
	guard *appendlog.DirectoryGuard,
	candidate string,
	releaseOperation func(),
) {
	t.Helper()
	writer := appendlog.NewDirectoryGuardToken()
	transitioned := make(chan error, 1)
	go func() {
		transitioned <- guard.TransitionActive(writer, func() (string, error) {
			return filepath.Join(filepath.Dir(candidate), "unrelated-writer-next.log"), nil
		})
		close(transitioned)
	}()
	t.Cleanup(func() {
		releaseOperation()
		select {
		case <-transitioned:
		case <-time.After(boundedwait.HangTimeout):
			t.Error("writer transition did not finish during cleanup")
		}
		guard.RemoveToken(writer)
	})
	select {
	case err := <-transitioned:
		require.NoError(t, err)
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("unrelated writer transition waited for purge state lock")
	}
}
