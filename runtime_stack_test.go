package heterolog_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog"
)

type stackCallSite struct {
	function string
	file     string
	line     int
}

// These helpers call the public API from the application goroutine. Their
// file/line/function must survive dispatch to a different goroutine.
func emitSyncStack(logger heterolog.Logger) (stackCallSite, error) {
	pc, file, line, _ := runtime.Caller(0)
	err := logger.ErrorWithStack(context.Background(), "stack origin", errors.New("application error"))
	return stackCallSite{runtime.FuncForPC(pc).Name(), file, line + 1}, err
}

func emitAsyncStack(logger heterolog.AsyncLogger) stackCallSite {
	pc, file, line, _ := runtime.Caller(0)
	logger.ErrorWithStack(context.Background(), "stack origin", errors.New("application error"))
	return stackCallSite{runtime.FuncForPC(pc).Name(), file, line + 1}
}

func emitDeliveryStack(logger heterolog.DeliveryLogger) (stackCallSite, error) {
	pc, file, line, _ := runtime.Caller(0)
	_, err := logger.ErrorWithStack(context.Background(), "stack origin", errors.New("application error"))
	return stackCallSite{runtime.FuncForPC(pc).Name(), file, line + 1}, err
}

func TestPublicErrorWithStackPreservesApplicationOriginAndDepth(t *testing.T) {
	for _, mode := range []string{"sync", "async", "delivery"} {
		for _, tc := range []struct {
			name  string
			depth int
			debug bool
		}{
			{"caller only", 0, false},
			{"caller and parents", 2, false},
			{"debug depth", 2, true},
		} {
			if mode == "delivery" && tc.debug {
				continue // Delivery stdout has no debug-mode setting.
			}
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				var site stackCallSite
				output := captureRuntimeStderr(t, func() {
					ctx := context.Background()
					cfg := heterolog.DefaultConfig()
					cfg.Stdout.LogBodyConfig.ErrorTraceDepth = tc.depth
					if tc.debug {
						cfg.Stdout.RuntimeConfig.IsDebug = true
						cfg.Stdout.LogBodyConfig.ErrorTraceDepth = 0
						cfg.Stdout.LogBodyConfig.ErrorTraceDepthDebugMode = tc.depth
					}
					switch mode {
					case "sync":
						logger, err := heterolog.New(ctx, cfg)
						if err != nil {
							t.Fatal(err)
						}
						defer logger.Close()
						site, err = emitSyncStack(logger)
						if err != nil {
							t.Fatal(err)
						}
						if err := logger.Close(); err != nil {
							t.Fatal(err)
						}
					case "async":
						asyncCfg := heterolog.DefaultAsyncConfig()
						asyncCfg.Stdout = cfg.Stdout
						logger, err := heterolog.NewAsync(ctx, asyncCfg)
						if err != nil {
							t.Fatal(err)
						}
						defer logger.Close()
						site = emitAsyncStack(logger)
						if err := logger.Close(); err != nil {
							t.Fatal(err)
						}
					case "delivery":
						deliveryCfg := heterolog.DefaultDeliveryConfig()
						deliveryCfg.Stdout.LogBodyConfig = cfg.Stdout.LogBodyConfig
						logger, err := heterolog.NewDelivery(ctx, deliveryCfg)
						if err != nil {
							t.Fatal(err)
						}
						defer logger.Close()
						site, err = emitDeliveryStack(logger)
						if err != nil {
							t.Fatal(err)
						}
						if err := logger.Close(); err != nil {
							t.Fatal(err)
						}
					}
				})
				// Verify the caller itself, including the actual call line, rather
				// than accepting any nonempty worker stack.
				firstFrame := fmt.Sprintf("(1) at: (File: %s | Line: %d | FuncName: %s)", site.file, site.line, site.function)
				if !strings.Contains(output, firstFrame) {
					t.Fatalf("missing application call site %q: %s", firstFrame, output)
				}
				if count := strings.Count(output, " | FuncName: "); count != tc.depth+1 {
					t.Fatalf("stack frames = %d, want %d: %s", count, tc.depth+1, output)
				}
			})
		}
	}
}

func captureRuntimeStderr(t *testing.T, fn func()) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	original := os.Stderr
	os.Stderr = file
	defer func() { os.Stderr = original }()
	fn()
	os.Stderr = original
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	return string(output)
}
