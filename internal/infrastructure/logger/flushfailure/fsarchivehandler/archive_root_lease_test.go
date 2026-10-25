package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
)

func TestFailureArchivePipelineArchiveRootLeaseRejectsConcurrentOwners(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	first := newArchiveHandlerFromConfig(t, cfg)
	t.Cleanup(func() { _ = shutdownFailureArchiveForTest(first) })
	firstLeaseID := first.archiveRootLease.ID()
	wantScopeLabel := failureArchiveDirectoryScopeLabel()
	seenAttemptIDs := make(map[directorylease.AcquisitionID]struct{})

	tests := []struct {
		name string
		dir  string
	}{
		{name: "same root", dir: cfg.dir},
		{name: "nested root", dir: filepath.Join(cfg.dir, "nested")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conflictingCfg := cfg
			conflictingCfg.dir = tt.dir

			handler, err := createArchiveHandler(conflictingCfg)
			if !errors.Is(err, directorylease.ErrDirectoryLeaseConflict) {
				t.Fatalf("NewFailureArchivePipeline() error = %v, want ErrDirectoryLeaseConflict", err)
			}
			if handler != nil {
				t.Fatalf("NewFailureArchivePipeline() handler = %#v, want nil", handler)
			}
			var conflict *directorylease.ConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("NewFailureArchivePipeline() error type = %T, want *ConflictError", err)
			}
			if conflict.RequestedScopeLabel != wantScopeLabel ||
				conflict.ExistingScopeLabel != wantScopeLabel {
				t.Fatalf(
					"conflict scope labels = %q/%q, want %q",
					conflict.RequestedScopeLabel,
					conflict.ExistingScopeLabel,
					wantScopeLabel,
				)
			}
			if conflict.ExistingLeaseID != firstLeaseID {
				t.Fatalf(
					"existing lease ID = %d, want first handler lease ID %d",
					conflict.ExistingLeaseID,
					firstLeaseID,
				)
			}
			if conflict.RequestedAttemptID == 0 || conflict.RequestedAttemptID == firstLeaseID {
				t.Fatalf("requested attempt ID = %d, want a distinct non-zero ID", conflict.RequestedAttemptID)
			}
			if _, duplicated := seenAttemptIDs[conflict.RequestedAttemptID]; duplicated {
				t.Fatalf("requested attempt ID %d was reused", conflict.RequestedAttemptID)
			}
			seenAttemptIDs[conflict.RequestedAttemptID] = struct{}{}
		})
	}
}

func TestNewFailureArchivePipelineRejectsWriterDirectoriesOutsideArchiveRoot(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	cfg.dirCaseRetryable = "../retryable"
	cfg.dirCasePermanent = "../permanent"

	handler, err := createArchiveHandler(cfg)
	if err == nil {
		t.Fatal("NewFailureArchivePipeline() error = nil, want root containment error")
	}
	if handler != nil {
		t.Fatalf("NewFailureArchivePipeline() handler = %#v, want nil", handler)
	}
	if !errors.Is(err, failurearchive.ErrArchiveDirectoryOutsideRoot) {
		t.Fatalf("NewFailureArchivePipeline() error = %v, want ErrArchiveDirectoryOutsideRoot", err)
	}
}

func TestFailureArchivePipelineShutdownReleasesArchiveRootLease(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	first := newArchiveHandlerFromConfig(t, cfg)
	firstLeaseID := first.archiveRootLease.ID()
	if err := shutdownFailureArchiveForTest(first); err != nil {
		t.Fatalf("first Shutdown() error = %v", err)
	}
	if first.archiveRootLease != nil {
		t.Fatal("archive root lease remains after successful Shutdown")
	}

	second := newArchiveHandlerFromConfig(t, cfg)
	t.Cleanup(func() { _ = shutdownFailureArchiveForTest(second) })
	if second.archiveRootLease.ID() == firstLeaseID {
		t.Fatalf("replacement lease ID = %d, must differ from released lease ID", second.archiveRootLease.ID())
	}
}

func TestFailureArchivePipelineShutdownCloseErrorReleasesArchiveRootLease(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	first := newArchiveHandlerFromConfig(t, cfg)

	if err := first.handler.Fsstore.FileRetryable.Close(); err != nil {
		t.Fatalf("pre-close retryable file: %v", err)
	}
	if err := shutdownFailureArchiveForTest(first); err == nil {
		t.Fatal("first Shutdown() error = nil, want retryable close error")
	}
	if first.archiveRootLease != nil {
		t.Fatal("archive root lease remains after all file handles became unusable")
	}
	if first.shutdownState != shutdownStateCompleted {
		t.Fatalf("shutdownState = %d, want Completed after resource release", first.shutdownState)
	}
	if first.handler.Fsstore.FileRetryable != nil || first.handler.Fsstore.FilePermanent != nil {
		t.Fatal("active file pointer remains after Shutdown close attempt")
	}
	if err := shutdownFailureArchiveForTest(first); err != nil {
		t.Fatalf("retry Shutdown() error = %v, want completed Shutdown", err)
	}

	second := newArchiveHandlerFromConfig(t, cfg)
	t.Cleanup(func() { _ = shutdownFailureArchiveForTest(second) })
}

func TestNewFailureArchivePipelineReleasesArchiveRootLeaseAfterConstructionFailure(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	archiveRoot := failurearchive.ResolveDirectoryPath(
		cfg.ArchivePath(),
		cfg.Target(),
		cfg.Dir(),
		"",
	)
	if err := os.MkdirAll(archiveRoot, 0o750); err != nil {
		t.Fatalf("create archive root: %v", err)
	}
	blockingPath := filepath.Join(archiveRoot, cfg.DirCasePermanent())
	if err := os.WriteFile(blockingPath, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("create blocking file: %v", err)
	}

	handler, err := createArchiveHandler(cfg)
	if err == nil {
		t.Fatal("NewFailureArchivePipeline() error = nil, want directory construction error")
	}
	if handler != nil {
		t.Fatalf("NewFailureArchivePipeline() handler = %#v, want nil after successful cleanup", handler)
	}

	if err := os.Remove(blockingPath); err != nil {
		t.Fatalf("remove blocking file: %v", err)
	}
	retry := newArchiveHandlerFromConfig(t, cfg)
	t.Cleanup(func() { _ = shutdownFailureArchiveForTest(retry) })
}

func TestCleanupFailedConstructionReleasesResourcesAfterFileCloseError(t *testing.T) {
	cfg := newValidArchiveHandlerConfig(t)
	archiveRoot := failurearchive.ResolveDirectoryPath(
		cfg.ArchivePath(),
		cfg.Target(),
		cfg.Dir(),
		"",
	)
	lease, err := directorylease.Acquire(
		directorylease.ScopeLabel("failure_archive.constructor_cleanup_test"),
		archiveRoot,
	)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	t.Cleanup(lease.Release)

	file, err := os.CreateTemp(t.TempDir(), "preclosed-archive-*.jsonl")
	if err != nil {
		t.Fatalf("create archive file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("pre-close archive file: %v", err)
	}

	handler := &FailureArchivePipeline{
		handler: &FsArchiveHandler{
			Config:  cfg,
			Fsstore: &failurearchive.FileStore{FileRetryable: file},
		},
		archiveRootLease: lease,
	}
	initErr := errors.New("construction failed")
	partial, cleanupErr := handler.cleanupFailedConstruction(initErr)
	if partial != nil {
		t.Fatalf("cleanupFailedConstruction() handler = %#v, want nil after completed cleanup", partial)
	}
	if !errors.Is(cleanupErr, initErr) {
		t.Fatalf("cleanupFailedConstruction() error = %v, want original construction error", cleanupErr)
	}
	if handler.archiveRootLease != nil {
		t.Fatal("archive root lease remains after construction cleanup")
	}
	if handler.shutdownState != shutdownStateCompleted {
		t.Fatalf("shutdownState = %d, want Completed after construction cleanup", handler.shutdownState)
	}
	if handler.handler.Fsstore.FileRetryable != nil {
		t.Fatal("retryable file pointer remains after construction cleanup")
	}

	replacementLease, err := directorylease.Acquire(
		directorylease.ScopeLabel("failure_archive.constructor_cleanup_replacement"),
		archiveRoot,
	)
	if err != nil {
		t.Fatalf("Acquire() after construction cleanup error = %v", err)
	}
	replacementLease.Release()
}

func createArchiveHandler(cfg testArchiveConfig) (*FailureArchivePipeline, error) {
	return NewFailureArchivePipeline(
		context.Background(),
		archiveFixedClock{now: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)},
		cfg,
		observer.NopTracer{},
		observer.NopMeter{},
		InternalLogHandlers{},
	)
}

func newArchiveHandlerFromConfig(t *testing.T, cfg testArchiveConfig) *FailureArchivePipeline {
	t.Helper()
	handler, err := createArchiveHandler(cfg)
	if err != nil {
		t.Fatalf("NewFailureArchivePipeline() error = %v", err)
	}
	return handler
}
