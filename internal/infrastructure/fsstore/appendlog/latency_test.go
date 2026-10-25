package appendlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

func TestMeasureLatency(t *testing.T) {
	taskErr := errors.New("operation failed")
	for _, test := range []struct {
		name string
		err  error
		hook bool
	}{
		{name: "success", hook: true},
		{name: "failure", err: taskErr, hook: true},
		{name: "nil hook"},
	} {
		t.Run(test.name, func(t *testing.T) {
			// bubble内の時刻はtaskのsleep分だけ進むため、経過時間を厳密に比較できます。
			synctest.Test(t, func(t *testing.T) {
				const taskDuration = 3 * time.Second
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				meta := &struct{ path string }{path: "app.log"}
				taskCalls, hookCalls := 0, 0
				var hook LatencyHook
				if test.hook {
					hook = func(gotCtx context.Context, elapsed time.Duration, gotMeta any) {
						hookCalls++
						if taskCalls != 1 || gotCtx != ctx || gotMeta != meta || elapsed != taskDuration {
							t.Fatalf("callback = (task calls %d, same context %t, same meta %t, elapsed %v), want (1, true, true, %v)",
								taskCalls, gotCtx == ctx, gotMeta == meta, elapsed, taskDuration)
						}
					}
				}
				err := measureLatency(ctx, "write log to file", meta, func() error {
					taskCalls++
					time.Sleep(taskDuration)
					return test.err
				}, hook)
				if !errors.Is(err, test.err) {
					t.Fatalf("error = %v, want %v", err, test.err)
				}
				if test.err != nil {
					var measured *MeasuredOperationError
					if !errors.As(err, &measured) {
						t.Fatalf("error = %T %v, want *MeasuredOperationError", err, err)
					}
					if measured.Operation != "write log to file" || measured.Elapsed != taskDuration {
						t.Fatalf("measured failure = (%q, %v), want (%q, %v)",
							measured.Operation, measured.Elapsed, "write log to file", taskDuration)
					}
				}
				wantHookCalls := 0
				if test.hook && test.err == nil {
					wantHookCalls = 1
				}
				if taskCalls != 1 || hookCalls != wantHookCalls {
					t.Fatalf("task calls = %d, hook calls = %d; want 1, %d", taskCalls, hookCalls, wantHookCalls)
				}
			})
		})
	}
}

// 圧縮の遅延通知は成功時に一度だけ、元fileのpathをmetaとして同期的に呼ばれます。
// 経過時間の値はTestMeasureLatencyで確認します。hook呼び出し時点でsourceが残っているかは
// LatencyHookの契約に含まれないため、ここでは要求しません。
func TestCompressOldFileAndRemoveSourceReportsCompressionLatency(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "app_20261002_1.log")
	if err := os.WriteFile(sourcePath, []byte("compression latency\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := FileStore{Config: newCompressionTestConfig()}
	var reportedMeta []any
	compressedPath, err := store.CompressOldFileAndRemoveSource(context.Background(), sourcePath,
		func(_ context.Context, _ time.Duration, meta any) {
			reportedMeta = append(reportedMeta, meta)
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(reportedMeta) != 1 || reportedMeta[0] != sourcePath {
		t.Fatalf("latency callback metadata = %v, want one report for %q", reportedMeta, sourcePath)
	}
	if _, err := os.Stat(sourcePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source after compression: %v", err)
	}
	if err := store.VerifyCompressedFileIntegrity(compressedPath); err != nil {
		t.Fatal(err)
	}
}
