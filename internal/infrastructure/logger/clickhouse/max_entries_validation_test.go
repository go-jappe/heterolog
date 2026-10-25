package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestClickHouseConcreteConstructorsRejectNonPositiveMaxEntries(t *testing.T) {
	for _, maxEntries := range []int{0, -1} {
		maxEntries := maxEntries
		constructors := map[string]func(ClickHouseConfigProvider) error{
			"normal": func(cfg ClickHouseConfigProvider) error {
				_, err := NewClickHouseLogger(
					context.Background(), cfg, nil, nil, nil, nil, nil, nil, nil,
				)
				return err
			},
			"delivery": func(cfg ClickHouseConfigProvider) error {
				_, err := NewDeliveryClickHouseLogger(
					context.Background(), cfg, nil, nil, nil, nil, nil, nil, nil,
				)
				return err
			},
			"fallback": func(cfg ClickHouseConfigProvider) error {
				_, err := newFallbackLogger(
					context.Background(), cfg, &pkgclickhouse.Config{}, nil, nil, nil, nil, nil,
				)
				return err
			},
		}

		for name, construct := range constructors {
			t.Run(name+"/max_entries_"+clickHouseMaxEntriesTestName(maxEntries), func(t *testing.T) {
				cfg := NewClickHouseLoggerConfigAdapter(pkglogger.ClickHouseLoggerBaseConfig{
					FlushInterval:  time.Hour,
					MaxEntries:     maxEntries,
					SamplingConfig: validClickHouseMaxEntriesTestSamplingConfig(),
				})
				err := construct(cfg)
				requireClickHouseConfigFieldError(t, err, pkglogger.ErrNonPositiveSetting, pkglogger.ConfigFieldMaxEntries, maxEntries)
			})
		}
	}
}

func TestClickHouseLoggerConstructorsRejectNonPositiveFlushInterval(t *testing.T) {
	cfg := NewClickHouseLoggerConfigAdapter(pkglogger.ClickHouseLoggerBaseConfig{
		FlushInterval:           0,
		MaxEntries:              1,
		BufferFlushBytes:        1,
		FallbackQueueMaxEntries: 1,
		MaxInsertRequestBytes:   1,
		SamplingConfig:          validClickHouseMaxEntriesTestSamplingConfig(),
	})
	constructors := map[string]func() error{
		"normal": func() error {
			_, err := NewClickHouseLogger(
				context.Background(), cfg, nil, nil, nil, nil, nil, nil, nil,
			)
			return err
		},
		"fallback": func() error {
			_, err := newFallbackLogger(
				context.Background(), cfg, &pkgclickhouse.Config{}, nil, nil, nil, nil, nil,
			)
			return err
		},
	}

	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			err := construct()
			requireClickHouseConfigFieldError(t, err, pkglogger.ErrNonPositiveSetting, pkglogger.ConfigFieldFlushInterval, time.Duration(0))
		})
	}
}

func TestClickHouseLoggerConstructorRejectsInvalidFallbackQueueLimit(t *testing.T) {
	for _, tt := range []struct {
		queueMaxEntries int
		wantKind        error
	}{
		{queueMaxEntries: 0, wantKind: pkglogger.ErrNonPositiveSetting},
		// fallback queueはbuffer flushの件数閾値（MaxEntries=2）以上でなければなりません。
		{queueMaxEntries: 1, wantKind: pkglogger.ErrInconsistentSetting},
	} {
		queueMaxEntries := tt.queueMaxEntries
		cfg := newTestClickHouseLoggerBaseConfig()
		cfg.MaxEntries = 2
		cfg.FallbackQueueMaxEntries = queueMaxEntries
		_, err := NewClickHouseLogger(
			context.Background(),
			NewClickHouseLoggerConfigAdapter(cfg),
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
		fieldErr := requireClickHouseConfigFieldError(
			t, err, tt.wantKind, pkglogger.ConfigFieldFallbackQueueMaxEntries, queueMaxEntries,
		)
		if tt.wantKind == pkglogger.ErrInconsistentSetting && fieldErr.Limit != cfg.MaxEntries {
			t.Fatalf("limit = %v, want MaxEntries %d", fieldErr.Limit, cfg.MaxEntries)
		}
	}
}

func validClickHouseMaxEntriesTestSamplingConfig() pkglogger.SamplingConfig {
	return pkglogger.SamplingConfig{
		SamplingDebugRate:      1,
		SamplingInfoRate:       1,
		SamplingWarnRate:       1,
		SamplingErrorRate:      1,
		LoggingByRequestIDRate: 1,
	}
}

func TestClickHouseFallbackHandlerDoesNotRetryInvalidMaxEntriesInitialization(t *testing.T) {
	controller := newFallbackHandlerController(
		NewClickHouseLoggerConfigAdapter(pkglogger.ClickHouseLoggerBaseConfig{
			FlushInterval:  time.Hour,
			MaxEntries:     0,
			SamplingConfig: validClickHouseMaxEntriesTestSamplingConfig(),
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

func clickHouseMaxEntriesTestName(value int) string {
	if value == 0 {
		return "zero"
	}
	return "negative"
}

// requireClickHouseConfigFieldErrorは、errがClickHouse loggerのfield項目の不正を含み、
// 不正の種類と不正な値を保持することを検証して、その項目のerrorを返します。
// 他の項目も同時に不正な場合があるため、対象の項目だけを確認します。
func requireClickHouseConfigFieldError(
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
