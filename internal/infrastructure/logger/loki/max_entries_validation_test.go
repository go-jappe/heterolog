package loki

import (
	"context"
	"errors"
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func TestLokiConcreteConstructorsRejectNonPositiveMaxEntries(t *testing.T) {
	for _, maxEntries := range []int{0, -1} {
		maxEntries := maxEntries
		constructors := map[string]func(LokiConfigProvider) error{
			"normal": func(cfg LokiConfigProvider) error {
				_, err := NewLokiLogger(
					context.Background(), cfg, nil, nil, nil, nil, nil, nil, nil,
				)
				return err
			},
			"delivery": func(cfg LokiConfigProvider) error {
				_, err := NewDeliveryLokiLogger(
					context.Background(), cfg, nil, nil, nil, nil, nil, nil, nil,
				)
				return err
			},
			"fallback": func(cfg LokiConfigProvider) error {
				_, err := newFallbackLogger(
					context.Background(), cfg, &pkgloki.Config{}, nil, nil, nil, nil, nil,
				)
				return err
			},
		}

		for name, construct := range constructors {
			t.Run(name+"/max_entries_"+maxEntriesTestName(maxEntries), func(t *testing.T) {
				cfg := NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
					FlushInterval:           time.Hour,
					MaxEntries:              maxEntries,
					BufferFlushBytes:        1,
					FallbackQueueMaxEntries: 1,
					MaxLineBytes:            1,
					MaxPushRequestBytes:     2,
					SamplingConfig:          validLokiMaxEntriesTestSamplingConfig(),
				})
				err := construct(cfg)
				requireLokiConfigFieldError(t, err, pkglogger.ErrNonPositiveSetting, pkglogger.ConfigFieldMaxEntries, maxEntries)
			})
		}
	}
}

func TestLokiLoggerConstructorsRejectNonPositiveFlushInterval(t *testing.T) {
	cfg := NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
		FlushInterval:           0,
		MaxEntries:              1,
		BufferFlushBytes:        1,
		FallbackQueueMaxEntries: 1,
		MaxLineBytes:            1,
		MaxPushRequestBytes:     2,
		SamplingConfig:          validLokiMaxEntriesTestSamplingConfig(),
	})
	constructors := map[string]func() error{
		"normal": func() error {
			_, err := NewLokiLogger(
				context.Background(), cfg, nil, nil, nil, nil, nil, nil, nil,
			)
			return err
		},
		"fallback": func() error {
			_, err := newFallbackLogger(
				context.Background(), cfg, &pkgloki.Config{}, nil, nil, nil, nil, nil,
			)
			return err
		},
	}

	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			err := construct()
			requireLokiConfigFieldError(t, err, pkglogger.ErrNonPositiveSetting, pkglogger.ConfigFieldFlushInterval, time.Duration(0))
		})
	}
}

func TestLokiConcreteConstructorsRejectNonPositiveBufferFlushBytes(t *testing.T) {
	for _, bufferFlushBytes := range []int{0, -1} {
		bufferFlushBytes := bufferFlushBytes
		constructors := map[string]func(LokiConfigProvider) error{
			"normal": func(cfg LokiConfigProvider) error {
				_, err := NewLokiLogger(
					context.Background(), cfg, nil, nil, nil, nil, nil, nil, nil,
				)
				return err
			},
			"delivery": func(cfg LokiConfigProvider) error {
				_, err := NewDeliveryLokiLogger(
					context.Background(), cfg, nil, nil, nil, nil, nil, nil, nil,
				)
				return err
			},
			"fallback": func(cfg LokiConfigProvider) error {
				_, err := newFallbackLogger(
					context.Background(), cfg, &pkgloki.Config{}, nil, nil, nil, nil, nil,
				)
				return err
			},
		}

		for name, construct := range constructors {
			t.Run(name+"/buffer_flush_bytes_"+maxEntriesTestName(bufferFlushBytes), func(t *testing.T) {
				cfg := NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
					FlushInterval:           time.Hour,
					MaxEntries:              1,
					BufferFlushBytes:        bufferFlushBytes,
					FallbackQueueMaxEntries: 1,
					MaxLineBytes:            1,
					MaxPushRequestBytes:     2,
					SamplingConfig:          validLokiMaxEntriesTestSamplingConfig(),
				})
				err := construct(cfg)
				requireLokiConfigFieldError(t, err, pkglogger.ErrNonPositiveSetting, pkglogger.ConfigFieldBufferFlushBytes, bufferFlushBytes)
			})
		}
	}
}

func TestLokiFallbackConstructorRejectsNonPositiveQueueMaxEntries(t *testing.T) {
	for _, queueMaxEntries := range []int{0, -1} {
		cfg := NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
			FlushInterval:           time.Hour,
			MaxEntries:              1,
			BufferFlushBytes:        1,
			FallbackQueueMaxEntries: queueMaxEntries,
			MaxLineBytes:            1,
			MaxPushRequestBytes:     2,
			SamplingConfig:          validLokiMaxEntriesTestSamplingConfig(),
		})
		_, err := newFallbackLogger(
			context.Background(), cfg, &pkgloki.Config{}, nil, nil, nil, nil, nil,
		)
		requireLokiConfigFieldError(
			t, err, pkglogger.ErrNonPositiveSetting, pkglogger.ConfigFieldFallbackQueueMaxEntries, queueMaxEntries,
		)
	}
}

func TestLokiFallbackConstructorRejectsInvalidPushLimits(t *testing.T) {
	cfg := NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
		FlushInterval:           time.Hour,
		MaxEntries:              1,
		BufferFlushBytes:        1,
		FallbackQueueMaxEntries: 1,
		MaxLineBytes:            2,
		MaxPushRequestBytes:     1,
		SamplingConfig:          validLokiMaxEntriesTestSamplingConfig(),
	})
	_, err := newFallbackLogger(
		context.Background(), cfg, &pkgloki.Config{}, nil, nil, nil, nil, nil,
	)
	// line上限はpush request上限より小さくなければなりません。
	fieldErr := requireLokiConfigFieldError(t, err, pkglogger.ErrInconsistentSetting, pkglogger.ConfigFieldMaxLineBytes, 2)
	if fieldErr.Limit != 1 {
		t.Fatalf("limit = %v, want the max push request bytes 1", fieldErr.Limit)
	}
}

func TestLokiBufferFlushBytesMayExceedHardPushRequestLimit(t *testing.T) {
	cfg := NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
		FlushInterval:           time.Hour,
		MaxEntries:              1,
		BufferFlushBytes:        4096,
		FallbackQueueMaxEntries: 1,
		MaxLineBytes:            1024,
		MaxPushRequestBytes:     2048,
		SamplingConfig:          validLokiMaxEntriesTestSamplingConfig(),
	})
	logger, err := NewLokiLogger(
		context.Background(), cfg, nil, nil, nil, nil, nil, nil, nil,
	)
	if err != nil {
		t.Fatalf("NewLokiLogger() rejected independent soft threshold: %v", err)
	}
	if err := logger.Close(context.Background()); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func validLokiMaxEntriesTestSamplingConfig() pkglogger.SamplingConfig {
	return pkglogger.SamplingConfig{
		SamplingDebugRate:      1,
		SamplingInfoRate:       1,
		SamplingWarnRate:       1,
		SamplingErrorRate:      1,
		LoggingByRequestIDRate: 1,
	}
}

func TestLokiFallbackHandlerDoesNotRetryInvalidMaxEntriesInitialization(t *testing.T) {
	controller := newFallbackHandlerController(
		NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
			FlushInterval:           time.Hour,
			MaxEntries:              0,
			BufferFlushBytes:        1,
			FallbackQueueMaxEntries: 1,
			MaxLineBytes:            1,
			MaxPushRequestBytes:     2,
			SamplingConfig:          validLokiMaxEntriesTestSamplingConfig(),
		}),
		nil,
		nil,
		nil,
		nil,
	)

	for attempt := 1; attempt <= 2; attempt++ {
		fallback, ok := controller.acquire(context.Background(), nil, nil)
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

func maxEntriesTestName(value int) string {
	if value == 0 {
		return "zero"
	}
	return "negative"
}

// requireLokiConfigFieldErrorは、errがLoki loggerの一つの設定項目の不正を表し、不正の種類、
// 項目、不正な値を保持することを検証して、その項目のerrorを返します。
func requireLokiConfigFieldError(
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
	fieldErrs := pkglogger.ConfigFieldErrors(err)
	if len(fieldErrs) != 1 || fieldErrs[0].Field != field || fieldErrs[0].Value != value {
		t.Fatalf("config field errors = %+v, want %s with value %v", fieldErrs, field, value)
	}
	return fieldErrs[0]
}
