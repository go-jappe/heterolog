package loggerruntime_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type runtimeObserverTracer struct {
	mu    sync.Mutex
	spans []*runtimeObserverSpan
}

func (tr *runtimeObserverTracer) StartSpan(ctx context.Context, name string, _ ...pkgobserver.Attr) (context.Context, pkgobserver.Span) {
	sp := &runtimeObserverSpan{name: name}
	tr.mu.Lock()
	tr.spans = append(tr.spans, sp)
	tr.mu.Unlock()
	return ctx, sp
}

func (tr *runtimeObserverTracer) assertSpansEnded(t *testing.T) {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.spans) == 0 {
		t.Fatal("selected backend never notified the configured tracer")
	}
	for _, sp := range tr.spans {
		if got := sp.ends.Load(); got != 1 {
			t.Errorf("observer span %q ended %d times, want once", sp.name, got)
		}
	}
}

type runtimeObserverSpan struct {
	name string
	ends atomic.Int32
}

func (*runtimeObserverSpan) TraceID() pkgobserver.TraceID         { return "" }
func (sp *runtimeObserverSpan) End()                              { sp.ends.Add(1) }
func (*runtimeObserverSpan) AddEvent(string, ...pkgobserver.Attr) {}
func (*runtimeObserverSpan) SetAttributes(...pkgobserver.Attr)    {}
func (*runtimeObserverSpan) RecordError(error, ...pkgobserver.Attr) {
}

func TestObserversReachBackendAndEndStartedSpans(t *testing.T) {
	for _, kind := range []string{"sync", "async", "delivery"} {
		t.Run(kind, func(t *testing.T) {
			tracer := &runtimeObserverTracer{}
			const message = "entry with configured observer"
			output := testutil.CaptureStdout(t, func() {
				ctx := context.Background()
				option := loggerruntime.WithObservers(tracer, nil)
				var closeLogger func() error
				switch kind {
				case "sync":
					logger, err := loggerruntime.New(ctx, loggerruntime.DefaultConfig(), option)
					if err != nil {
						t.Fatal(err)
					}
					closeLogger = logger.Close
					defer closeLogger()
					if err := logger.Info(ctx, message); err != nil {
						t.Fatal(err)
					}
				case "async":
					logger, err := loggerruntime.NewAsync(ctx, loggerruntime.DefaultAsyncConfig(), option)
					if err != nil {
						t.Fatal(err)
					}
					closeLogger = logger.Close
					defer closeLogger()
					logger.Info(ctx, message)
				case "delivery":
					logger, err := loggerruntime.NewDelivery(ctx, loggerruntime.DefaultDeliveryConfig(), option)
					if err != nil {
						t.Fatal(err)
					}
					closeLogger = logger.Close
					defer closeLogger()
					report, err := logger.Info(ctx, message)
					if err != nil || !report.AllTargetsRegistered() {
						t.Fatalf("Info = (%+v, %v), want registered delivery", report, err)
					}
				}
				if err := closeLogger(); err != nil {
					t.Fatal(err)
				}
			})
			if got := strings.Count(output, message); got != 1 {
				t.Fatalf("logged %d entries with the configured observer, want one: %s", got, output)
			}
			// stdoutは、書き込みごとではなく作成を計測します。spanの数や名前を決めつけずに、
			// observerへの実際の通知を確かめます。
			tracer.assertSpansEnded(t)
		})
	}
}

type runtimeObserverMeter struct {
	mu     sync.Mutex
	totals map[string]int64
}

func (m *runtimeObserverMeter) Counter(name string) pkgobserver.Counter {
	return runtimeObserverCounter{meter: m, name: name}
}

func (*runtimeObserverMeter) Gauge(string) pkgobserver.Gauge { return runtimeObserverGauge{} }
func (*runtimeObserverMeter) Histogram(string) pkgobserver.Histogram {
	return runtimeObserverHistogram{}
}

type runtimeObserverCounter struct {
	meter *runtimeObserverMeter
	name  string
}

func (c runtimeObserverCounter) Add(_ context.Context, value int64, _ ...pkgobserver.Attr) {
	c.meter.mu.Lock()
	defer c.meter.mu.Unlock()
	if c.meter.totals == nil {
		c.meter.totals = make(map[string]int64)
	}
	c.meter.totals[c.name] += value
}

type runtimeObserverGauge struct{}

func (runtimeObserverGauge) Record(context.Context, float64, ...pkgobserver.Attr) {}

type runtimeObserverHistogram struct{}

func (runtimeObserverHistogram) Record(context.Context, float64, ...pkgobserver.Attr) {}

func TestObserverMeterReceivesFileDropNotification(t *testing.T) {
	for _, kind := range []string{"sync", "async"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			fileConfig := &pkglogger.FileLoggerConfig{
				LifecycleMode: fsstore.FileLifecycleModeDisable,
				RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelInfo},
				TimeConfig:    pkglogger.TimeConfig{TimeZone: "UTC"},
				LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
				FsstoreConfig: fsstore.Config{
					Layout: fsstore.FsstoreConfig{
						LogPath: root, Target: "observer-test", LogDir: "logs",
						FileNameFormat: fsstore.FileNameFormat, FileDateFormat: fsstore.FileDateFormat,
					},
					Lifecycle: fsstore.FileLifecycleConfig{
						MaxFileSize: 1 << 20, MaxFileBackups: 2, MaxFileAge: 1, MaxDirSize: 10 << 20,
					},
					Retry: fsstore.RetryConfig{MaxWriteRetries: 0},
					Overload: fsstore.FileArchiveOverloadPolicyConfig{
						OverloadPolicy: fsstore.OverloadPolicyDrop,
						// 達しない最小値を指定すると、filesystemを埋めたり空き容量を変えたりせずに、
						// 必ずdropを選びます。
						MinAvailableDiskSpace: ^uint64(0),
					},
				},
			}
			meter := &runtimeObserverMeter{}
			ctx := context.Background()
			option := loggerruntime.WithObservers(nil, meter)
			if kind == "sync" {
				cfg := loggerruntime.DefaultConfig()
				cfg.Stdout, cfg.File = nil, fileConfig
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				logger, err := loggerruntime.New(ctx, cfg, option)
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				// 同期のAPIは、nilを返す代わりに、捨てたlogを報告します。
				if err := logger.Info(ctx, "dropped entry"); !errors.Is(err, pkglogger.ErrLogDropped) {
					t.Fatalf("Info() error = %v, want ErrLogDropped", err)
				}
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				cfg := loggerruntime.DefaultAsyncConfig()
				cfg.Stdout, cfg.File = nil, fileConfig
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				logger, err := loggerruntime.NewAsync(ctx, cfg, option)
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				logger.Info(ctx, "dropped entry")
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
				// 非同期のAPIは、捨てたlogを呼出し元へ返さない代わりに、出力先の失敗として数えます。
				meter.mu.Lock()
				failures := meter.totals["heterolog_async_backend_failures_total"]
				meter.mu.Unlock()
				if failures != 1 {
					t.Fatalf("async backend failure total = %d, want 1", failures)
				}
			}
			meter.mu.Lock()
			total := meter.totals["heterolog_file_dropped_entries_total"]
			meter.mu.Unlock()
			if total != 1 {
				t.Fatalf("file drop notification total = %d, want 1", total)
			}
			files, err := filepath.Glob(filepath.Join(root, "observer-test", "logs", "*.log"))
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range files {
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(body), "dropped entry") {
					t.Fatalf("dropped entry reached file %s: %s", path, body)
				}
			}
		})
	}
}
