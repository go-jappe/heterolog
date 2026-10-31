package loggerruntime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/configview"
	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/application/metrics"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// constructorは、使えないcontextを拒否し、loggerを組み立てません。出力先の設定値は
// Builderの実装が検証するため、ここでは検証しません。
func TestConstructorRejectsUnusableContext(t *testing.T) {
	builder := &recordingBuilder{}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	constructors := map[string]func(ctx context.Context) error{
		"logger": func(ctx context.Context) error {
			return closeConstructed(loggerruntime.New(ctx, builder, stdoutConfig()))
		},
		"async logger": func(ctx context.Context) error {
			return closeConstructed(loggerruntime.NewAsync(ctx, builder, stdoutAsyncConfig()))
		},
		"delivery logger": func(ctx context.Context) error {
			return closeConstructed(loggerruntime.NewDelivery(ctx, builder, stdoutDeliveryConfig()))
		},
	}
	for source, construct := range constructors {
		t.Run(source, func(t *testing.T) {
			if err := construct(nil); !errors.Is(err, loggerruntime.ErrNilContext) {
				t.Errorf("nil context error = %v, want ErrNilContext", err)
			}
			if err := construct(canceled); !errors.Is(err, context.Canceled) {
				t.Errorf("canceled context error = %v, want context.Canceled", err)
			}
		})
	}
	if builder.built != 0 {
		t.Errorf("builder built %d loggers from rejected arguments", builder.built)
	}
}

// Builderがなければ、loggerを組み立てられないため、ErrNilBuilderを返します。
func TestConstructorsRejectNilBuilder(t *testing.T) {
	ctx := context.Background()
	if err := closeConstructed(loggerruntime.New(ctx, nil, stdoutConfig())); !errors.Is(err, loggerruntime.ErrNilBuilder) {
		t.Errorf("New error = %v, want ErrNilBuilder", err)
	}
	if err := closeConstructed(loggerruntime.NewAsync(ctx, nil, stdoutAsyncConfig())); !errors.Is(err, loggerruntime.ErrNilBuilder) {
		t.Errorf("NewAsync error = %v, want ErrNilBuilder", err)
	}
	if err := closeConstructed(loggerruntime.NewDelivery(ctx, nil, stdoutDeliveryConfig())); !errors.Is(err, loggerruntime.ErrNilBuilder) {
		t.Errorf("NewDelivery error = %v, want ErrNilBuilder", err)
	}
}

// Delivery loggerは送信の結果を呼び出し元へ返し、再送のworkerを持たないため、
// NewDeliveryはWithSpoolを無視せずに拒否します。
func TestNewDeliveryRejectsWithSpool(t *testing.T) {
	err := closeConstructed(loggerruntime.NewDelivery(context.Background(), &recordingBuilder{}, stdoutDeliveryConfig(),
		loggerruntime.WithSpool(pkgspool.SpoolConfig{})))
	if !errors.Is(err, loggerruntime.ErrDeliverySpoolUnsupported) {
		t.Fatalf("NewDelivery(WithSpool) error = %v, want ErrDeliverySpoolUnsupported", err)
	}
}

// closeConstructedは、誤って作ったloggerを閉じ、constructorのerrorを返します。
func closeConstructed[R interface{ Close() error }](runtime R, err error) error {
	if err == nil {
		_ = runtime.Close()
	}
	return err
}

// 各runtimeは、自分を作った設定を、管理用APIの文書の種類とともに返します。
func TestRuntimesReturnTheirConfiguration(t *testing.T) {
	ctx := context.Background()
	builder := &recordingBuilder{}
	cfg := pkglogger.LoggerConfig{Timeout: 7 * time.Second, Stdout: &pkglogger.StdoutLoggerConfig{RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelInfo}}}
	runtime, err := loggerruntime.New(ctx, builder, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := runtime.Configuration(); got.Kind != configview.KindLogger || got.Value.(pkglogger.LoggerConfig).Timeout != 7*time.Second {
		t.Errorf("Runtime.Configuration() = %+v, want kind logger with the given timeout", got)
	}
	async, err := loggerruntime.NewAsync(ctx, builder, pkglogger.AsyncBestEffortMultiLoggerConfig{MaxParallel: 3, Stdout: &pkglogger.StdoutLoggerConfig{RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelInfo}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := async.Configuration(); got.Kind != configview.KindAsyncLogger || got.Value.(pkglogger.AsyncBestEffortMultiLoggerConfig).MaxParallel != 3 {
		t.Errorf("AsyncRuntime.Configuration() = %+v, want kind async_logger with max_parallel 3", got)
	}
	delivery, err := loggerruntime.NewDelivery(ctx, builder, pkglogger.DeliveryLoggerConfig{MaxLogEntryBytes: 99})
	if err != nil {
		t.Fatal(err)
	}
	if got := delivery.Configuration(); got.Kind != configview.KindDeliveryLogger || got.Value.(pkglogger.DeliveryLoggerConfig).MaxLogEntryBytes != 99 {
		t.Errorf("DeliveryRuntime.Configuration() = %+v, want kind delivery_logger with max_log_entry_bytes 99", got)
	}
}

// loggerは、利用者がMeterを渡さなくても内部のMeterへmetricsを記録し、Metricsで返します。
// 渡した場合は、両方へ記録します。
func TestRuntimesRecordMetricsInTheirInternalMeter(t *testing.T) {
	ctx := context.Background()
	builder := &recordingBuilder{}
	user := metrics.NewMeter(metrics.Config{}, nil)
	runtime, err := loggerruntime.New(ctx, builder, pkglogger.LoggerConfig{Stdout: &pkglogger.StdoutLoggerConfig{RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelInfo}}},
		loggerruntime.WithObservers(nil, user))
	if err != nil {
		t.Fatal(err)
	}
	builder.options.Meter.Counter("heterolog_backend_records_total").Add(ctx, 2)
	if got := runtime.Metrics(); len(got) != 1 || got[0].Points[0].Int != 2 {
		t.Errorf("Metrics() = %+v, want the counter at 2", got)
	}
	if got := user.Collect(); len(got) != 1 || got[0].Points[0].Int != 2 {
		t.Errorf("user meter = %+v, want the counter at 2 as well", got)
	}
	delivery, err := loggerruntime.NewDelivery(ctx, builder, pkglogger.DeliveryLoggerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	builder.options.Meter.Counter("heterolog_backend_records_total").Add(ctx, 1)
	if got := delivery.Metrics(); len(got) != 1 || got[0].Points[0].Int != 1 {
		t.Errorf("delivery Metrics() = %+v, want the counter at 1 without WithObservers", got)
	}
}
