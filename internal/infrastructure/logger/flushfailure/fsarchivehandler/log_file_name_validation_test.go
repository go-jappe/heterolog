package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFailureArchiveRejectsColonTargetBeforeFileCreation(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{
		pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner, pkgfsstore.FileLifecycleModeDisable,
	} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := newValidArchiveHandlerConfig(t)
			cfg.lifecycleMode = mode
			cfg.target = "app:stream"
			// targetをdirectory結合から相殺できても、file名の不正値は拒否します。
			cfg.dir = ".."
			pipeline, err := NewFailureArchivePipeline(
				context.Background(), archiveFixedClock{now: time.Now().UTC()}, cfg, nil, nil, InternalLogHandlers{},
			)
			if pipeline != nil {
				_ = shutdownFailureArchiveForTest(pipeline)
				t.Fatal("invalid target returned a pipeline")
			}
			var componentErr *pkgfsstore.LogFileNameComponentError
			if !errors.Is(err, pkgfsstore.ErrInvalidLogFileNameComponent) || !errors.As(err, &componentErr) ||
				componentErr.Field != "target" || componentErr.Value != cfg.target {
				t.Fatalf("error = %v, want colon rejection of target %q", err, cfg.target)
			}
			entries, readErr := os.ReadDir(cfg.archivePath)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("validation must not create files or directories: entries=%v error=%v", entries, readErr)
			}
		})
	}
}
