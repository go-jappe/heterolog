package loggerruntime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// constructorは、拒否した引数をそれぞれ種類（errors.Is）で示し、設定の場合は
// ConfigFieldErrorの出所と項目でも示します。拒否する設定では、loggerを組み立てません。
func TestConstructorRejectsInvalidConfiguration(t *testing.T) {
	ctx := context.Background()
	builder := &recordingBuilder{}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	constructors := map[string]func(ctx context.Context, timeout time.Duration, opts ...loggerruntime.Option) error{
		"logger": func(ctx context.Context, timeout time.Duration, opts ...loggerruntime.Option) error {
			cfg := loggerruntime.DefaultConfig()
			cfg.Timeout = timeout
			return closeConstructed(loggerruntime.New(ctx, builder, cfg, opts...))
		},
		"async logger": func(ctx context.Context, timeout time.Duration, opts ...loggerruntime.Option) error {
			cfg := loggerruntime.DefaultAsyncConfig()
			cfg.Timeout = timeout
			return closeConstructed(loggerruntime.NewAsync(ctx, builder, cfg, opts...))
		},
		"delivery logger": func(ctx context.Context, timeout time.Duration, opts ...loggerruntime.Option) error {
			cfg := loggerruntime.DefaultDeliveryConfig()
			cfg.Timeout = timeout
			return closeConstructed(loggerruntime.NewDelivery(ctx, builder, cfg, opts...))
		},
	}
	for source, construct := range constructors {
		t.Run(source, func(t *testing.T) {
			if err := construct(nil, time.Second); !errors.Is(err, loggerruntime.ErrNilContext) {
				t.Errorf("nil context error = %v, want ErrNilContext", err)
			}
			if err := construct(canceled, time.Second); !errors.Is(err, context.Canceled) {
				t.Errorf("canceled context error = %v, want context.Canceled", err)
			}
			for _, timeout := range []time.Duration{0, -time.Second} {
				err := construct(ctx, timeout)
				requireConfigFieldError(t, err, source, loggerruntime.ConfigFieldTimeout, pkglogger.ErrNonPositiveSetting)
			}
			err := construct(ctx, time.Second, loggerruntime.WithCloudWatchLogsStreamRouting("invalid"))
			var routingErr *pkgcloudwatchlogs.InvalidStreamRoutingError
			if !errors.Is(err, pkgcloudwatchlogs.ErrInvalidStreamRouting) || !errors.As(err, &routingErr) {
				t.Errorf("invalid stream routing error = %v, want ErrInvalidStreamRouting", err)
			}
		})
	}
	// zero値の設定にも、正のTimeoutはありません。
	_, err := loggerruntime.New(ctx, builder, pkglogger.LoggerConfig{})
	requireConfigFieldError(t, err, "logger", loggerruntime.ConfigFieldTimeout, pkglogger.ErrNonPositiveSetting)

	for _, maxParallel := range []int{0, -1} {
		cfg := loggerruntime.DefaultAsyncConfig()
		cfg.MaxParallel = maxParallel
		err := closeConstructed(loggerruntime.NewAsync(ctx, builder, cfg))
		requireConfigFieldError(t, err, "async logger", loggerruntime.ConfigFieldMaxParallel, pkglogger.ErrNonPositiveSetting)
	}
	if builder.built != 0 {
		t.Errorf("builder built %d loggers from rejected configurations", builder.built)
	}
}

// Builderがなければ、loggerを組み立てられないため、ErrNilBuilderを返します。
func TestConstructorsRejectNilBuilder(t *testing.T) {
	ctx := context.Background()
	if err := closeConstructed(loggerruntime.New(ctx, nil, loggerruntime.DefaultConfig())); !errors.Is(err, loggerruntime.ErrNilBuilder) {
		t.Errorf("New error = %v, want ErrNilBuilder", err)
	}
	if err := closeConstructed(loggerruntime.NewAsync(ctx, nil, loggerruntime.DefaultAsyncConfig())); !errors.Is(err, loggerruntime.ErrNilBuilder) {
		t.Errorf("NewAsync error = %v, want ErrNilBuilder", err)
	}
	if err := closeConstructed(loggerruntime.NewDelivery(ctx, nil, loggerruntime.DefaultDeliveryConfig())); !errors.Is(err, loggerruntime.ErrNilBuilder) {
		t.Errorf("NewDelivery error = %v, want ErrNilBuilder", err)
	}
}

// Delivery loggerは送信の結果を呼び出し元へ返し、再送のworkerを持たないため、
// NewDeliveryはWithSpoolを無視せずに拒否します。
func TestNewDeliveryRejectsWithSpool(t *testing.T) {
	err := closeConstructed(loggerruntime.NewDelivery(context.Background(), &recordingBuilder{}, loggerruntime.DefaultDeliveryConfig(),
		loggerruntime.WithSpool(loggerruntime.DefaultSpoolConfig())))
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

// requireConfigFieldErrorは、errがsourceの設定fieldを、指定した種類の不正として
// 拒否していることを確かめます。
func requireConfigFieldError(t *testing.T, err error, source string, field pkglogger.ConfigField, kind error) {
	t.Helper()
	if !errors.Is(err, kind) {
		t.Fatalf("error = %v, want %v", err, kind)
	}
	var fieldErr *pkglogger.ConfigFieldError
	if !errors.As(err, &fieldErr) || fieldErr.Source != source || fieldErr.Field != field {
		t.Fatalf("error = %v (%+v), want a ConfigFieldError for %s %s", err, fieldErr, source, field)
	}
}
