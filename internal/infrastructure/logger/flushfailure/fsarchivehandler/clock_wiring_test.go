package fsarchivehandler

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewFailureArchivePipelineUsesClockTimezoneForInitialActiveFiles(t *testing.T) {
	location := time.FixedZone("JST", 9*60*60)
	// Clockが返すJSTの5月6日を、初期ファイル名にもそのまま使います。
	sourceAt := time.Date(2042, time.May, 6, 0, 30, 0, 0, location)

	pipeline := newTestArchiveHandler(t, sourceAt, nil)
	wantPeriod := sourceAt.Format("20060102")

	for failureCategory, path := range map[string]string{
		"retryable": pipeline.handler.Fsstore.ActiveFilePathRetryable,
		"permanent": pipeline.handler.Fsstore.ActiveFilePathPermanent,
	} {
		if got := filepath.Base(path); !strings.Contains(got, "_"+wantPeriod+"_") {
			t.Fatalf(
				"%s initial active file = %q, want configured-clock period %q",
				failureCategory,
				got,
				wantPeriod,
			)
		}
	}
}

func TestFailureArchiveRotationUsesClockTimezoneForDecisionAndTargetPaths(t *testing.T) {
	location := time.FixedZone("JST", 9*60*60)
	initialAt := time.Date(2042, time.May, 6, 23, 55, 0, 0, location)
	pipeline := newTestArchiveHandler(t, initialAt, func(cfg *testArchiveConfig) {
		cfg.enableRotate = true
	})

	oldRetryablePath := pipeline.handler.Fsstore.ActiveFilePathRetryable
	oldPermanentPath := pipeline.handler.Fsstore.ActiveFilePathPermanent
	rotationAt := initialAt.Add(10 * time.Minute)
	pipeline.handler.Clock = archiveFixedClock{now: rotationAt}

	attempt := pipeline.handler.rotateIfEnable(context.Background())
	if attempt.err != nil {
		t.Fatalf("rotateIfEnable() error = %v", attempt.err)
	}
	if len(attempt.deferredRotations) != 0 {
		t.Fatalf("deferred rotations = %v, want none", attempt.deferredRotations)
	}
	wantPeriod := rotationAt.Format("20060102")
	for failureCategory, paths := range map[string]struct {
		old string
		new string
	}{
		"retryable": {
			old: oldRetryablePath,
			new: pipeline.handler.Fsstore.ActiveFilePathRetryable,
		},
		"permanent": {
			old: oldPermanentPath,
			new: pipeline.handler.Fsstore.ActiveFilePathPermanent,
		},
	} {
		if paths.new == paths.old {
			t.Fatalf("%s active file did not rotate from %q", failureCategory, paths.old)
		}
		if got := filepath.Base(paths.new); !strings.Contains(got, "_"+wantPeriod+"_") {
			t.Fatalf(
				"%s rotation target = %q, want configured-clock period %q",
				failureCategory,
				got,
				wantPeriod,
			)
		}
	}
}
