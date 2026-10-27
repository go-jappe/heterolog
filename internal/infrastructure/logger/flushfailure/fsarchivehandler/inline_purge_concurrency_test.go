package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestInlineArchivePurgeAllowsRotationAndReleasesCandidates(t *testing.T) {
	for _, failureCategory := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		for _, phase := range []struct {
			name string
			span string
		}{
			{name: "age", span: SpanFilePurgeByAge},
			{name: "generation", span: SpanFilePurgeByGeneration},
			{name: "directory_size", span: SpanFilePurgeByDirectorySize},
		} {
			for _, outcome := range []string{"success", "remove_error", "panic"} {
				t.Run(string(failureCategory)+"/"+phase.name+"/"+outcome, func(t *testing.T) {
					now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
					pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
						cfg.enableRotate = true
						cfg.maxFileBackups = 2
						cfg.maxFileAge = 30
					})
					initializeInlineArchiveTrackers(t, pipeline)
					handler := pipeline.handler
					state := handler.directoryForFailureCategory(failureCategory)
					oldActive, err := pipeline.ActiveArchivePath(failureCategory)
					if err != nil {
						t.Fatal(err)
					}

					// 次の公開HandleFailureで実際に次世代をopenさせる。先行purgeの
					// snapshotには初めからactiveとして入り、削除候補にはならない。
					handler.Fsstore.Mu.Lock()
					activeFile := handler.Fsstore.FileRetryable
					if failureCategory == failurearchive.FailureCategoryPermanent {
						activeFile = handler.Fsstore.FilePermanent
					}
					// append用handleへの追記で準備し、Truncate権限には依存しません。
					_, err = activeFile.Write(make([]byte, handler.Config.MaxFileSize()+1))
					handler.Fsstore.Mu.Unlock()
					if err != nil {
						t.Fatal(err)
					}

					candidateCount := 1
					if phase.name == "generation" {
						candidateCount = 3
					}
					var candidates []string
					for index := 0; index < candidateCount; index++ {
						path := filepath.Join(filepath.Dir(oldActive), fmt.Sprintf("test-app_20000101_%d.jsonl", index+1))
						size := int64(1)
						if phase.name == "directory_size" {
							size = int64(handler.Config.MaxDirSize() + 1)
						}
						writeArchiveSizedFile(t, path, size, now.AddDate(0, 0, -40).Add(time.Duration(index)*time.Second))
						candidates = append(candidates, path)
					}
					if phase.name == "age" {
						state.lastAgePurgeAt = time.Time{}
					}
					state.directorySizeTracker.RecordGrowth(handler.Config.MaxDirSize() + 1)
					gate := newInlineArchivePurgeGate(phase.span, outcome == "panic")
					handler.Tracer = gate

					var cleanupErr error
					var cleanupPanic any
					cleanupDone := make(chan struct{})
					go func() {
						defer close(cleanupDone)
						defer func() { cleanupPanic = recover() }()
						cleanupErr = handler.triggerArchiveCleanup(context.Background(), failureCategory)
					}()
					var writeDone chan struct{}
					t.Cleanup(func() {
						gate.unblock()
						waitInlineArchivePurge(t, cleanupDone, "purge cleanup")
						if writeDone != nil {
							waitInlineArchivePurge(t, writeDone, "writer cleanup")
						}
					})
					waitInlineArchivePurge(t, gate.entered, "purge file operation")

					result := retryableArchiveResult(now, "write-during-purge")
					if failureCategory == failurearchive.FailureCategoryPermanent {
						result.DeadRecords = result.RetryableRecords
						result.RetryableRecords = nil
					}
					var writeErr error
					writeDone = make(chan struct{})
					go func() {
						defer close(writeDone)
						writeErr = pipeline.HandleFailure(context.Background(), result)
					}()
					waitInlineArchivePurge(t, writeDone, "rotation and write while purge is blocked")
					if writeErr != nil {
						t.Fatalf("HandleFailure: %v", writeErr)
					}
					newActive, err := pipeline.ActiveArchivePath(failureCategory)
					if err != nil || newActive == oldActive {
						t.Fatalf("active path = %q, err = %v, want a new generation", newActive, err)
					}
					if records := readArchiveRecords(t, newActive); len(records) != 1 {
						t.Fatalf("saved records = %d, want 1", len(records))
					}
					for _, path := range candidates {
						assertArchivePathProtected(t, state.guard, path, true)
					}
					// 別taskが削除中の候補を取得できない一方、states lock自体は空いている。
					claim := appendlog.NewDirectoryGuardToken()
					claimed := state.guard.ProtectPathsIfNoOtherOwnerConflict(
						claim, nil, candidates[:1],
					)
					state.guard.UnprotectPaths(claim, candidates[0])
					if claimed {
						t.Fatal("another task claimed a purge candidate")
					}

					if outcome == "remove_error" {
						// 実Removeの失敗だけを決定的に注入し、filesystem snapshotは使い回す。
						if err := os.Rename(candidates[0], filepath.Join(t.TempDir(), "original")); err != nil {
							t.Fatal(err)
						}
						if err := os.Mkdir(candidates[0], 0o700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(candidates[0], "child"), []byte("x"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					gate.unblock()
					waitInlineArchivePurge(t, cleanupDone, "purge completion")
					if (cleanupErr != nil) != (outcome == "remove_error") {
						t.Fatalf("cleanup error = %v, outcome = %s", cleanupErr, outcome)
					}
					if (cleanupPanic != nil) != (outcome == "panic") {
						t.Fatalf("cleanup panic = %v, outcome = %s", cleanupPanic, outcome)
					}
					for _, path := range candidates {
						assertArchivePathProtected(t, state.guard, path, false)
					}
					_, err = os.Stat(candidates[0])
					if outcome == "success" {
						if !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("purged candidate stat = %v, want absent", err)
						}
					} else if err != nil {
						t.Fatalf("failed/panicked purge lost candidate: %v", err)
					}
					if _, err := os.Stat(oldActive); err != nil {
						t.Fatalf("snapshot-active archive was purged after rotation: %v", err)
					}
					assertArchivePathProtected(t, state.guard, newActive, true)
					// panicでもlifecycle lockとcleanup受付が解放され、後続処理を受理できる。
					if state.cleanupRunning.Load() {
						t.Fatal("cleanup remained registered")
					}
					if err := state.guard.RunWithLifecycleLock(func() error { return nil }); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

type inlineArchivePurgeGate struct {
	span      string
	panic     bool
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
	exitOnce  sync.Once
}

func newInlineArchivePurgeGate(span string, shouldPanic bool) *inlineArchivePurgeGate {
	return &inlineArchivePurgeGate{
		span: span, panic: shouldPanic, entered: make(chan struct{}), release: make(chan struct{}),
	}
}

func (g *inlineArchivePurgeGate) StartSpan(ctx context.Context, name string, _ ...pkgobserver.Attr) (context.Context, pkgobserver.Span) {
	if name == g.span {
		first := false
		g.enterOnce.Do(func() { first = true; close(g.entered) })
		if first {
			<-g.release
			if g.panic {
				panic("purge tracer panic")
			}
		}
	}
	return ctx, observer.NopSpan{}
}

func (g *inlineArchivePurgeGate) unblock() {
	g.exitOnce.Do(func() { close(g.release) })
}

func waitInlineArchivePurge(t *testing.T, done <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(failureArchiveHangTimeout):
		t.Fatalf("timed out waiting for %s", operation)
	}
}
