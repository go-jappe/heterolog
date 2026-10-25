package spoolstorehandler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestHandleFailurePreservesActiveObstructionBeforeOverloadScan(t *testing.T) {
	for _, obstruction := range []string{"regular file", "symlink", "large time partition obstruction"} {
		t.Run(obstruction, func(t *testing.T) {
			now := time.Date(2026, 10, 6, 9, 30, 0, 0, time.UTC)
			handler := newTestHandler(t, now, testOverloadPolicyConfig{maxDirSize: 1})
			active := handler.Store.RootDir.ActiveDir().Path()
			if err := os.MkdirAll(active, 0o700); err != nil {
				t.Fatal(err)
			}
			blockedPath := active
			if obstruction == "large time partition obstruction" {
				blockedPath = filepath.Join(active, now.Format("2006"))
			} else {
				if err := os.Remove(active); err != nil {
					t.Fatal(err)
				}
			}
			const original = "do not discard the directory obstruction"
			outside := filepath.Join(t.TempDir(), "outside.txt")
			if err := os.WriteFile(outside, []byte(original), 0o600); err != nil {
				t.Fatal(err)
			}
			if obstruction == "symlink" {
				testutil.Symlink(t, outside, blockedPath)
			} else if err := os.WriteFile(blockedPath, []byte(original), 0o600); err != nil {
				t.Fatal(err)
			}

			var warnings []*logging.LogEntry
			handler.OnWarn = func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				warnings = append(warnings, entry)
			}
			var reportedErrors []error
			handler.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				reportedErrors = append(reportedErrors, err)
			}
			scanned := false
			handler.dirStatsScanner = func(dir spooldir.StateDir) (DirStats, error) {
				scanned = true
				info, err := os.Lstat(dir.Path())
				if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
					t.Fatalf("overload scan ran before active was restored: info=%v err=%v", info, err)
				}
				return scanDirStats(dir)
			}
			result := loggerfailure.SendResult{RetryableRecords: []loggerfailure.FailedRecord{
				failedRecord("saved-after-preservation", "retryable", now),
			}}
			if err := handler.HandleFailure(context.Background(), result); err != nil {
				t.Fatalf("HandleFailure: %v", err)
			}
			if !scanned || len(reportedErrors) != 0 || len(warnings) != 1 {
				t.Fatalf("scanned=%v errors=%v warnings=%d", scanned, reportedErrors, len(warnings))
			}
			warning := warnings[0]
			if warning.Err == nil || warning.Fields["original_path"] != blockedPath {
				t.Fatalf("warning=%+v", warning)
			}
			preserved, ok := warning.Fields["retained_path"].(string)
			if !ok || filepath.Dir(preserved) != filepath.Join(handler.Store.RootDir.String(), ".preserved") {
				t.Fatalf("retained path=%v", warning.Fields["retained_path"])
			}
			info, err := os.Lstat(preserved)
			if err != nil {
				t.Fatal(err)
			}
			if obstruction == "symlink" && info.Mode()&os.ModeSymlink == 0 {
				t.Fatal("preserved entry is not the original symlink")
			}
			content, err := os.ReadFile(preserved)
			if err != nil || string(content) != original {
				t.Fatalf("preserved contents=%q err=%v", content, err)
			}
			content, err = os.ReadFile(outside)
			if err != nil || string(content) != original {
				t.Fatalf("outside target contents=%q err=%v", content, err)
			}
			assertRecordCount(t, readSpoolRecords(t, active), 1, "active")
		})
	}
}

func TestHandleFailureReportsUnrecoverableDirectoryObstruction(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 30, 0, 0, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{})
	active := handler.Store.RootDir.ActiveDir().Path()
	if err := os.MkdirAll(active, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(active); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(active, []byte("blocked active"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(handler.Store.RootDir.String(), ".preserved"), []byte("blocked preservation"), 0o600); err != nil {
		t.Fatal(err)
	}
	var reportedErrors []error
	handler.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		reportedErrors = append(reportedErrors, err)
	}
	handler.OnWarn = func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter) {
		t.Fatal("failed preservation must not notify success")
	}
	handler.dirStatsScanner = func(spooldir.StateDir) (DirStats, error) {
		t.Fatal("overload scan must not follow an unrecovered obstruction")
		return DirStats{}, nil
	}
	err := handler.HandleFailure(context.Background(), loggerfailure.SendResult{RetryableRecords: []loggerfailure.FailedRecord{
		failedRecord("not-saved", "retryable", now),
	}})
	if err == nil || len(reportedErrors) != 1 {
		t.Fatalf("HandleFailure error=%v OnError=%v", err, reportedErrors)
	}
	content, readErr := os.ReadFile(active)
	if readErr != nil || string(content) != "blocked active" {
		t.Fatalf("original obstruction changed: contents=%q err=%v", content, readErr)
	}
}
