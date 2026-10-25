package opensearch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestOpenSearchBufferedLoggerConstructorsRejectNonPositiveMaxBulkDocument(t *testing.T) {
	tests := []struct {
		name      string
		construct func(OpenSearchLoggerConfigProvider) error
	}{
		{
			name: "normal",
			construct: func(cfg OpenSearchLoggerConfigProvider) error {
				_, err := NewOpenSearchLogger(
					context.Background(),
					cfg,
					nil,
					nil,
					nil,
					nil,
					observer.NopTracer{},
					observer.NopMeter{},
					&LoggerBase{},
				)
				return err
			},
		},
		{
			name: "fallback",
			construct: func(cfg OpenSearchLoggerConfigProvider) error {
				_, err := newFallbackLogger(
					cfg,
					nil,
					nil,
					nil,
					nil,
					observer.NopTracer{},
					observer.NopMeter{},
				)
				return err
			},
		},
	}

	for _, constructor := range tests {
		t.Run(constructor.name, func(t *testing.T) {
			for _, maxBulkDocument := range []int{-1, 0} {
				cfg := NewOpenSearchLoggerConfigAdapter(pkglogger.OpenSearchLoggerBaseConfig{
					FlushInterval:   time.Hour,
					MaxBulkDocument: maxBulkDocument,
				})
				err := constructor.construct(cfg)
				requireOpenSearchConfigFieldError(
					t, err, pkglogger.ErrNonPositiveSetting, pkglogger.ConfigFieldMaxBulkDocument, maxBulkDocument,
				)
			}
		})
	}
}

func TestOpenSearchLoggerConstructorsRejectNonPositiveFlushInterval(t *testing.T) {
	cfg := NewOpenSearchLoggerConfigAdapter(pkglogger.OpenSearchLoggerBaseConfig{
		FlushInterval:   0,
		MaxBulkDocument: 1,
	})
	constructors := map[string]func() error{
		"normal": func() error {
			_, err := NewOpenSearchLogger(
				context.Background(), cfg, nil, nil, nil, nil,
				observer.NopTracer{}, observer.NopMeter{}, &LoggerBase{},
			)
			return err
		},
		"fallback": func() error {
			_, err := newFallbackLogger(
				cfg, nil, nil, nil, nil, observer.NopTracer{}, observer.NopMeter{},
			)
			return err
		},
	}

	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			err := construct()
			requireOpenSearchConfigFieldError(
				t, err, pkglogger.ErrNonPositiveSetting, pkglogger.ConfigFieldFlushInterval, time.Duration(0),
			)
		})
	}
}

func TestOpenSearchFallbackHandlerDoesNotRetryInvalidMaxBulkDocumentInitialization(t *testing.T) {
	controller := &fallbackHandlerController{
		adapter: NewOpenSearchLoggerConfigAdapter(pkglogger.OpenSearchLoggerBaseConfig{
			FlushInterval:   time.Hour,
			MaxBulkDocument: 0,
		}),
	}

	for attempt := 1; attempt <= 2; attempt++ {
		fallback, ok := controller.acquire(observer.NopTracer{}, observer.NopMeter{})
		if ok || fallback != nil {
			t.Fatalf("attempt %d acquire = (%T, %t), want (nil, false)", attempt, fallback, ok)
		}
	}
	if !controller.initializationAttempted {
		t.Fatal("invalid fallback logger initialization was not recorded")
	}
	if controller.logger != nil {
		t.Fatalf("invalid fallback logger = %T, want nil", controller.logger)
	}
}

func TestDeliveryOpenSearchPrimaryAndFallbackAdaptersKeepSeparateBatchContracts(t *testing.T) {
	primary := NewDeliveryOpenSearchLoggerConfigAdapter(pkglogger.DeliveryOpenSearchLoggerBaseConfig{})

	if got := primary.MaxBulkDocument(); got != 1 {
		t.Fatalf("delivery primary MaxBulkDocument() = %d, want 1", got)
	}
	if !primary.NoBulkFlushModeEnabled() {
		t.Fatal("delivery primary must use no-bulk mode")
	}

	fallback := NewDeliveryOpenSearchFallbackLoggerConfigAdapter(
		pkglogger.DeliveryOpenSearchLoggerBaseConfig{},
		pkglogger.DeliveryInternalLogOpenSearchBatchConfig{
			FlushInterval:       7 * time.Second,
			MaxBulkDocument:     9,
			BufferFlushBytes:    1234,
			MaxBulkRequestBytes: 5678,
		},
	)
	if got := fallback.FlushInterval(); got != 7*time.Second {
		t.Fatalf("delivery fallback FlushInterval() = %s, want 7s", got)
	}
	if got := fallback.MaxBulkDocument(); got != 9 {
		t.Fatalf("delivery fallback MaxBulkDocument() = %d, want 9", got)
	}
	if got := fallback.BufferFlushBytes(); got != 1234 {
		t.Fatalf("delivery fallback BufferFlushBytes() = %d, want 1234", got)
	}
	if got := fallback.MaxBulkRequestBytes(); got != 5678 {
		t.Fatalf("delivery fallback MaxBulkRequestBytes() = %d, want 5678", got)
	}
	if fallback.NoBulkFlushModeEnabled() {
		t.Fatal("delivery fallback must use batch mode")
	}
}

// requireOpenSearchConfigFieldErrorは、errがOpenSearch loggerのfield項目の不正を含み、
// 不正の種類と不正な値を保持することを検証して、その項目のerrorを返します。
func requireOpenSearchConfigFieldError(
	t *testing.T,
	err error,
	kind error,
	field pkglogger.ConfigField,
	value any,
) *pkglogger.ConfigFieldError {
	t.Helper()
	if !errors.Is(err, kind) {
		t.Fatalf("constructor error = %v, want %v", err, kind)
	}
	for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
		if fieldErr.Field != field {
			continue
		}
		if !errors.Is(fieldErr, kind) || fieldErr.Value != value {
			t.Fatalf("config field error = %+v, want %v with value %v", fieldErr, kind, value)
		}
		return fieldErr
	}
	t.Fatalf("config field errors = %+v, want %s", pkglogger.ConfigFieldErrors(err), field)
	return nil
}
