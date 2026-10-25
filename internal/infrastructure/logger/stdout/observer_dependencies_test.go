package stdout

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

// 内部ログの通知専用instance（NotificationTarget）は失敗を通知しないため、ここでは
// 扱いません。その失敗の扱いはTestNotificationOnlyStdoutHandlersWriteEachFailureToStandardLogで確認します。
func TestStdoutConstructorsPassObserversToWriteErrorHandler(t *testing.T) {
	constructors := map[string]func(
		context.Context,
		StdoutLoggerConfigProvider,
		resource.OtelLogResourceProvider,
		observer.Tracer,
		observer.Meter,
	) (*StdoutLogger, error){
		"normal": func(ctx context.Context, cfg StdoutLoggerConfigProvider, resource resource.OtelLogResourceProvider, tracer observer.Tracer, meter observer.Meter) (*StdoutLogger, error) {
			return NewStdoutLogger(ctx, cfg, resource, tracer, meter)
		},
	}
	// configuredは試験ごとに新しいrecordingTracer・recordingMeterを作ります。大きさを持つ型
	// なので、require.Sameは構築時に渡したinstanceそのものかを判定できます。
	observerCases := []struct {
		name      string
		observers func() (observer.Tracer, observer.Meter)
	}{
		{name: "configured", observers: func() (observer.Tracer, observer.Meter) {
			return &recordingTracer{}, &recordingMeter{}
		}},
		{name: "nil", observers: func() (observer.Tracer, observer.Meter) { return nil, nil }},
		{name: "typed_nil", observers: func() (observer.Tracer, observer.Meter) {
			return (*observer.NopTracer)(nil), (*observer.NopMeter)(nil)
		}},
	}
	sinks := []struct {
		name   string
		stream **os.File
		level  pkglogger.LogLevel
	}{
		{name: "stdout", stream: &os.Stdout, level: pkglogger.LogLevelInfo},
		{name: "stderr", stream: &os.Stderr, level: pkglogger.LogLevelError},
	}

	for constructorName, constructor := range constructors {
		for _, observers := range observerCases {
			for _, sink := range sinks {
				t.Run(constructorName+"/"+observers.name+"/"+sink.name, func(t *testing.T) {
					// These tests replace process streams and must remain sequential.
					replaceStdoutTestStreamWithClosedFile(t, sink.stream)
					ctx := context.Background()
					cfg := NewConfigAdapter(pkglogger.StdoutLoggerConfig{
						TimeConfig: pkglogger.TimeConfig{TimeZone: "UTC"},
						LogBodyConfig: pkglogger.LogBodyConfig{
							LogLineDateFormat: time.RFC3339,
						},
					})
					configuredTracer, configuredMeter := observers.observers()
					logger, err := constructor(ctx, cfg, nil, configuredTracer, configuredMeter)
					require.NoError(t, err)
					calls := 0
					logger.OnError = func(
						ctx context.Context,
						err error,
						entry *logging.LogEntry,
						tracer observer.Tracer,
						meter observer.Meter,
					) {
						calls++
						require.ErrorIs(t, err, os.ErrClosed)
						require.ErrorIs(t, entry.Err, os.ErrClosed)
						require.False(t, observer.IsNilInterface(tracer), "OnError tracer must be usable")
						require.False(t, observer.IsNilInterface(meter), "OnError meter must be usable")
						if observers.name == "configured" {
							require.Same(t, configuredTracer, tracer)
							require.Same(t, configuredMeter, meter)
						}
						_, span := tracer.StartSpan(ctx, "write-error")
						span.End()
						meter.Counter("write-errors").Add(ctx, 1)
					}

					err = logger.HandleLogEntry(ctx, &logging.LogEntry{
						Level:   sink.level,
						Message: "write to closed stream",
					})
					require.ErrorIs(t, err, os.ErrClosed)
					require.Equal(t, 1, calls, "write failure must call OnError exactly once")
					if observers.name == "configured" {
						// OnErrorで使ったspanとcounterが、構築時に渡した観測先へ記録されます。
						require.Contains(t, configuredTracer.(*recordingTracer).spanNames(), "write-error")
						require.Equal(t, int64(1), configuredMeter.(*recordingMeter).count("write-errors"))
					}
				})
			}
		}
	}
}

func replaceStdoutTestStreamWithClosedFile(t *testing.T, stream **os.File) {
	t.Helper()

	readFile, writeFile, err := os.Pipe()
	require.NoError(t, err)
	require.NoError(t, readFile.Close())
	require.NoError(t, writeFile.Close())

	original := *stream
	*stream = writeFile
	t.Cleanup(func() { *stream = original })
}

// recordingTracerは開始したspan名を記録します。大きさを持つ型にし、異なるinstanceの
// pointerが同じaddressにならないようにしています。
type recordingTracer struct {
	mu    sync.Mutex
	spans []string
}

func (r *recordingTracer) StartSpan(ctx context.Context, name string, attrs ...observer.Attr) (context.Context, observer.Span) {
	r.mu.Lock()
	r.spans = append(r.spans, name)
	r.mu.Unlock()
	return observer.NopTracer{}.StartSpan(ctx, name, attrs...)
}

func (r *recordingTracer) spanNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.spans...)
}

// recordingMeterはcounterへ加算した値を名前ごとに記録します。
type recordingMeter struct {
	mu       sync.Mutex
	counters map[string]int64
}

func (m *recordingMeter) Counter(name string) observer.Counter {
	return recordingCounter{meter: m, name: name}
}

func (m *recordingMeter) Gauge(string) observer.Gauge {
	return observer.NopGauge{}
}

func (m *recordingMeter) count(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

type recordingCounter struct {
	meter *recordingMeter
	name  string
}

func (c recordingCounter) Add(_ context.Context, value int64, _ ...observer.Attr) {
	c.meter.mu.Lock()
	defer c.meter.mu.Unlock()
	if c.meter.counters == nil {
		c.meter.counters = make(map[string]int64)
	}
	c.meter.counters[c.name] += value
}
