package logger

import (
	"testing"
	"time"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
)

func TestValidateDeliveryLoggerBackendConfigsRejectsNonPositiveBufferedClickHouseFlushInterval(
	t *testing.T,
) {
	cfg := DeliveryLoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputClickHouse},
		ClickHouse: &DeliveryClickHouseConfig{
			DeliveryClickHouseLoggerBaseConfig: DeliveryClickHouseLoggerBaseConfig{
				FlushInterval:           -time.Nanosecond,
				MaxEntries:              1,
				BufferFlushBytes:        1,
				FallbackQueueMaxEntries: 1,
				MaxInsertRequestBytes:   1,
			},
		},
		ClickHouseClient: &pkgclickhouse.Config{
			Endpoint: "http://localhost:8123",
			Table:    "logs",
		},
	}

	err := ValidateDeliveryLoggerBackendConfigs("delivery logger", cfg)
	fieldErr := findFieldError(err, ConfigFieldFlushInterval, ErrNonPositiveSetting)
	if fieldErr == nil || fieldErr.Source != "delivery logger" || fieldErr.Backend != LogOutputClickHouse ||
		fieldErr.Value != -time.Nanosecond {
		t.Fatalf(
			"ValidateDeliveryLoggerBackendConfigs() error = %v, want non-positive flush interval",
			err,
		)
	}
}

func TestValidateDeliveryLoggerBackendConfigsRejectsNonPositiveClickHouseMaxEntries(t *testing.T) {
	for _, maxEntries := range []int{-1, 0} {
		cfg := DeliveryLoggerConfig{
			LogOutputDestinations: LogOutputDestinations{LogOutputClickHouse},
			ClickHouse: &DeliveryClickHouseConfig{
				DeliveryClickHouseLoggerBaseConfig: DeliveryClickHouseLoggerBaseConfig{
					MaxEntries:              maxEntries,
					BufferFlushBytes:        1,
					FallbackQueueMaxEntries: 1,
					MaxInsertRequestBytes:   1,
				},
			},
			ClickHouseClient: &pkgclickhouse.Config{
				Endpoint: "http://localhost:8123",
				Table:    "logs",
			},
		}

		err := ValidateDeliveryLoggerBackendConfigs("delivery logger", cfg)
		fieldErr := findFieldError(err, ConfigFieldMaxEntries, ErrNonPositiveSetting)
		if fieldErr == nil || fieldErr.Backend != LogOutputClickHouse || fieldErr.Value != maxEntries {
			t.Fatalf(
				"maxEntries=%d ValidateDeliveryLoggerBackendConfigs() error = %v, want non-positive max entries",
				maxEntries,
				err,
			)
		}
	}
}

func TestValidateClickHouseBatchLimitsRejectsInvalidFallbackQueue(t *testing.T) {
	for _, tt := range []struct {
		queueMaxEntries int
		// wantは、fallback queueのhard上限として報告される不正です。
		want ConfigFieldError
	}{
		{queueMaxEntries: 0, want: ConfigFieldError{Value: 0, Err: ErrNonPositiveSetting}},
		{queueMaxEntries: 1, want: ConfigFieldError{Value: 1, Limit: 2, Err: ErrInconsistentSetting}},
	} {
		err := ValidateClickHouseBatchLimits(2, 1, tt.queueMaxEntries, 1)
		fieldErr := findFieldError(err, ConfigFieldFallbackQueueMaxEntries, tt.want.Err)
		if fieldErr == nil || fieldErr.Value != tt.want.Value || fieldErr.Limit != tt.want.Limit {
			t.Fatalf(
				"queueMaxEntries=%d ValidateClickHouseBatchLimits() error = %v, want %v value=%v limit=%v",
				tt.queueMaxEntries,
				err,
				tt.want.Err,
				tt.want.Value,
				tt.want.Limit,
			)
		}
	}
}
