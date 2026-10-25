package stdout

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler/reentrytest"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	stdouterrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout/errors"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func reentryTestStdoutConfig() pkglogger.StdoutLoggerConfig {
	return pkglogger.StdoutLoggerConfig{
		TimeConfig:    pkglogger.TimeConfig{TimeZone: "UTC"},
		RuntimeConfig: pkglogger.RuntimeConfig{AppName: "stdout-reentry-test", LogLevel: pkglogger.LogLevelDebug},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
	}
}

// writeWithStdStreamsForTestは、stdoutとstderrをfileへ置き換えてfを実行し、元へ戻してから
// 各fileの内容を返します。failStdout・failStderrのfileは閉じておき、書き込みを
// os.ErrClosedで失敗させます。os.Stdout・os.Stderrを置き換えるため、t.Parallelと併用しません。
func writeWithStdStreamsForTest(t *testing.T, failStdout, failStderr bool, f func()) (stdoutText, stderrText string) {
	t.Helper()
	open := func(name string, fail bool) *os.File {
		file, err := os.CreateTemp(t.TempDir(), name)
		if err != nil {
			t.Fatal(err)
		}
		if fail {
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		}
		return file
	}
	stdoutFile, stderrFile := open("stdout-*", failStdout), open("stderr-*", failStderr)
	originalOut, originalErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = stdoutFile, stderrFile
	func() {
		defer func() { os.Stdout, os.Stderr = originalOut, originalErr }()
		f()
	}()
	read := func(file *os.File, fail bool) string {
		if !fail {
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		}
		content, err := os.ReadFile(file.Name())
		if err != nil {
			t.Fatal(err)
		}
		return string(content)
	}
	return read(stdoutFile, failStdout), read(stderrFile, failStderr)
}

func stdoutLogLines(text string) []string {
	text = strings.TrimSuffix(text, "\n")
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

func TestStdoutLoggerHandlerWriteFailureIsWrittenDirectlyInsteadOfRedispatched(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			logger, err := NewStdoutLogger(context.Background(), NewConfigAdapter(reentryTestStdoutConfig()), nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			probe := reentrytest.NewProbe(t)
			var mu sync.Mutex
			var nestedErrs []error
			logger.OnError = func(ctx context.Context, _ error, _ *infralogging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				depth, ok := probe.Enter()
				defer probe.Exit()
				if !ok {
					return
				}
				mode.Write(t, ctx, func(ctx context.Context) {
					err := logger.HandleLogEntry(ctx, &infralogging.LogEntry{
						Level: pkglogger.LogLevelInfo, Message: "write from OnError", RequestID: fmt.Sprintf("nested-%d", depth),
					})
					mu.Lock()
					nestedErrs = append(nestedErrs, err)
					mu.Unlock()
				})
			}

			// stdoutへの書き込みだけを失敗させ、ERRORの内部ログを書き込むstderrは読み戻します。
			var originalErr error
			_, stderrText := writeWithStdStreamsForTest(t, true, false, func() {
				originalErr = logger.HandleLogEntry(context.Background(), &infralogging.LogEntry{
					Level: pkglogger.LogLevelInfo, Message: "original write", RequestID: "original",
				})
			})

			var internalErr *stdouterrors.StdoutLoggerInternalError
			if !errors.Is(originalErr, os.ErrClosed) || !errors.As(originalErr, &internalErr) {
				t.Fatalf("original write error = %v, want a stdout internal error wrapping os.ErrClosed", originalErr)
			}
			probe.RequireCounts(t, mode, 1)
			mu.Lock()
			defer mu.Unlock()
			// 通知先は通知ごとに一回書き込みます（通知は元の書き込みの一回だけです）。
			if len(nestedErrs) != 1 {
				t.Fatalf("writes from OnError = %d, want 1", len(nestedErrs))
			}
			for _, nestedErr := range nestedErrs {
				if !errors.Is(nestedErr, os.ErrClosed) {
					t.Fatalf("write from OnError error = %v, want os.ErrClosed", nestedErr)
				}
			}
			// 再入した最も深い書き込みの失敗だけを、通知せずstderrへ直接書き込みます。
			lines := stdoutLogLines(stderrText)
			if len(lines) != 1 || !strings.HasPrefix(lines[0], "[INTERNAL][ERROR]") {
				t.Fatalf("directly written internal logs = %q, want one internal ERROR line", lines)
			}
		})
	}
}

func TestDeliveryStdoutLoggerHandlerWriteFailureIsNotRedispatched(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			logger, err := NewDeliveryStdoutLogger(context.Background(), NewDeliveryConfigAdapter(pkglogger.DeliveryStdoutLoggerConfig{
				TimeConfig:    pkglogger.TimeConfig{TimeZone: "UTC"},
				LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
			}), nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			probe := reentrytest.NewProbe(t)
			var mu sync.Mutex
			var nestedErrs []error
			logger.OnError = func(ctx context.Context, _ error, _ *infralogging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				depth, ok := probe.Enter()
				defer probe.Exit()
				if !ok {
					return
				}
				mode.Write(t, ctx, func(ctx context.Context) {
					_, nestedErr := logger.HandleLogEntry(ctx, &infralogging.LogEntry{
						Level: pkglogger.LogLevelInfo, Message: "write from OnError", RequestID: fmt.Sprintf("nested-%d", depth),
					})
					mu.Lock()
					nestedErrs = append(nestedErrs, nestedErr)
					mu.Unlock()
				})
			}

			var report pkglogger.DeliveryReport
			var originalErr error
			writeWithStdStreamsForTest(t, true, true, func() {
				report, originalErr = logger.HandleLogEntry(context.Background(), &infralogging.LogEntry{
					Level: pkglogger.LogLevelInfo, Message: "original write", RequestID: "original",
				})
			})

			if !errors.Is(originalErr, os.ErrClosed) {
				t.Fatalf("original write error = %v, want os.ErrClosed", originalErr)
			}
			if len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliveryFailed {
				t.Fatalf("delivery report = %+v, want one failed target", report)
			}
			probe.RequireCounts(t, mode, 1)
			mu.Lock()
			defer mu.Unlock()
			for _, nestedErr := range nestedErrs {
				if !errors.Is(nestedErr, os.ErrClosed) {
					t.Fatalf("write from OnError error = %v, want os.ErrClosed", nestedErr)
				}
			}
		})
	}
}

// TestDeliveryStdoutLoggerReentrantWriteErrorIsWrittenToItsOwnOutputは、OnErrorの中で
// 発生した書き込みerrorの内部ログが、OnErrorを再び呼ばずloggerの出力へ直接書き込まれることを
// 確認します。失敗が続く間は同じ出力へも書けないため、書き込める出力で再入時の報告を行います。
func TestDeliveryStdoutLoggerReentrantWriteErrorIsWrittenToItsOwnOutput(t *testing.T) {
	logger, err := NewDeliveryStdoutLogger(context.Background(), NewDeliveryConfigAdapter(pkglogger.DeliveryStdoutLoggerConfig{
		TimeConfig:    pkglogger.TimeConfig{TimeZone: "UTC"},
		LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
	}), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	logger.OnError = func(context.Context, error, *infralogging.LogEntry, observer.Tracer, observer.Meter) {
		calls.Add(1)
	}
	failure := errors.New("injected stdout write failure")
	stdoutText, stderrText := writeWithStdStreamsForTest(t, false, false, func() {
		logger.internalLogReentry.Dispatch(context.Background(), func(ctx context.Context) {
			logger.handleWriteError(ctx, failure, &infralogging.LogEntry{
				Level: pkglogger.LogLevelInfo, Message: "write from OnError", RequestID: "nested-1",
			})
		})
	})
	if got := calls.Load(); got != 0 {
		t.Fatalf("OnError calls = %d, want 0 for a write error raised inside OnError", got)
	}
	lines := stdoutLogLines(stdoutText)
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "[INTERNAL][INFO]") ||
		!strings.Contains(lines[0], "request_id: nested-1") || !strings.Contains(lines[0], failure.Error()) {
		// 内部ログの行（利用者が読む出力）にrequest idとerrorが残ることを確認します。
		t.Fatalf("directly written internal logs = %q", lines)
	}
	if stderrText != "" {
		t.Fatalf("stderr = %q, want the INFO internal log on stdout only", stderrText)
	}
}
