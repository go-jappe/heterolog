package heterolog_test

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

	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

type runtimeObserverTracer struct {
	mu    sync.Mutex
	spans []*runtimeObserverSpan
}

func (tr *runtimeObserverTracer) StartSpan(ctx context.Context, name string, _ ...heterolog.ObserverAttr) (context.Context, heterolog.Span) {
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

func (*runtimeObserverSpan) TraceID() heterolog.TraceID                 { return "" }
func (sp *runtimeObserverSpan) End()                                    { sp.ends.Add(1) }
func (*runtimeObserverSpan) AddEvent(string, ...heterolog.ObserverAttr) {}
func (*runtimeObserverSpan) SetAttributes(...heterolog.ObserverAttr)    {}
func (*runtimeObserverSpan) RecordError(error, ...heterolog.ObserverAttr) {
}

func TestPublicObserversReachBackendAndEndStartedSpans(t *testing.T) {
	for _, kind := range []string{"sync", "async", "delivery"} {
		t.Run(kind, func(t *testing.T) {
			tracer := &runtimeObserverTracer{}
			const message = "entry with configured observer"
			output := captureRuntimeStdout(t, func() {
				ctx := context.Background()
				option := heterolog.WithObservers(tracer, nil)
				var closeLogger func() error
				switch kind {
				case "sync":
					logger, err := heterolog.New(ctx, heterolog.DefaultConfig(), option)
					if err != nil {
						t.Fatal(err)
					}
					closeLogger = logger.Close
					defer closeLogger()
					if err := logger.Info(ctx, message); err != nil {
						t.Fatal(err)
					}
				case "async":
					logger, err := heterolog.NewAsync(ctx, heterolog.DefaultAsyncConfig(), option)
					if err != nil {
						t.Fatal(err)
					}
					closeLogger = logger.Close
					defer closeLogger()
					logger.Info(ctx, message)
				case "delivery":
					logger, err := heterolog.NewDelivery(ctx, heterolog.DefaultDeliveryConfig(), option)
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
			// stdout instruments construction, not each write. Assert actual observer
			// notifications without requiring a particular number or naming of spans.
			tracer.assertSpansEnded(t)
		})
	}
}

type runtimeObserverMeter struct {
	mu     sync.Mutex
	totals map[string]int64
}

func (m *runtimeObserverMeter) Counter(name string) heterolog.Counter {
	return runtimeObserverCounter{meter: m, name: name}
}

func (*runtimeObserverMeter) Gauge(string) heterolog.Gauge { return runtimeObserverGauge{} }

type runtimeObserverCounter struct {
	meter *runtimeObserverMeter
	name  string
}

func (c runtimeObserverCounter) Add(_ context.Context, value int64, _ ...heterolog.ObserverAttr) {
	c.meter.mu.Lock()
	defer c.meter.mu.Unlock()
	if c.meter.totals == nil {
		c.meter.totals = make(map[string]int64)
	}
	c.meter.totals[c.name] += value
}

type runtimeObserverGauge struct{}

func (runtimeObserverGauge) Record(context.Context, float64, ...heterolog.ObserverAttr) {}

func TestPublicObserverMeterReceivesFileDropNotification(t *testing.T) {
	for _, kind := range []string{"sync", "async"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			fileConfig := &pkglogger.FileLoggerConfig{
				LifecycleMode: fsstore.FileLifecycleModeDisable,
				RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: heterolog.LevelInfo},
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
					Retry: fsstore.RetryConfig{MaxWriteAttempts: 1},
					Overload: fsstore.FileArchiveOverloadPolicyConfig{
						OverloadPolicy: fsstore.OverloadPolicyDrop,
						// An unreachable minimum deterministically selects drop without
						// filling the filesystem or changing its available capacity.
						MinAvailableDiskSpace: ^uint64(0),
					},
				},
			}
			meter := &runtimeObserverMeter{}
			ctx := context.Background()
			option := heterolog.WithObservers(nil, meter)
			if kind == "sync" {
				cfg := heterolog.DefaultConfig()
				cfg.Stdout, cfg.File = nil, fileConfig
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				logger, err := heterolog.New(ctx, cfg, option)
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				// The sync API reports the dropped entry instead of returning nil.
				if err := logger.Info(ctx, "dropped entry"); !errors.Is(err, heterolog.ErrLogDropped) {
					t.Fatalf("Info() error = %v, want ErrLogDropped", err)
				}
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				cfg := heterolog.DefaultAsyncConfig()
				cfg.Stdout, cfg.File = nil, fileConfig
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				logger, err := heterolog.NewAsync(ctx, cfg, option)
				if err != nil {
					t.Fatal(err)
				}
				defer logger.Close()
				logger.Info(ctx, "dropped entry")
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
			}
			meter.mu.Lock()
			total := meter.totals["file_overload_drop_total"]
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
