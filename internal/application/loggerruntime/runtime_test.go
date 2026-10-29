package loggerruntime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
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
