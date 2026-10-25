package logger

import (
	"testing"
	"time"

	loggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

func TestValidateDeliveryInternalLogBatchConfigOnlyValidatesUsedTargets(t *testing.T) {
	valid := DeliveryInternalLogBatchConfig{
		OpenSearch: DeliveryInternalLogOpenSearchBatchConfig{
			FlushInterval:       time.Second,
			MaxBulkDocument:     1,
			BufferFlushBytes:    512,
			MaxBulkRequestBytes: 1024,
		},
		Loki: DeliveryInternalLogLokiBatchConfig{
			FlushInterval:           time.Second,
			MaxEntries:              1,
			BufferFlushBytes:        1,
			FallbackQueueMaxEntries: 1,
		},
		CloudWatchLogs: DeliveryInternalLogCloudWatchLogsBatchConfig{
			FlushInterval:          time.Second,
			BufferFlushMaxEvents:   1,
			BufferFlushBytes:       1,
			FallbackQueueMaxEvents: 1,
		},
		Slack: DeliveryInternalLogSlackBatchConfig{
			Intervals:              []time.Duration{time.Minute},
			MaxLogsPerNotification: 1,
			MaxRestThreshold:       time.Minute,
		},
	}

	unused := DeliveryLoggerConfig{
		LogOutputDestinations:  LogOutputDestinations{LogOutputStdout},
		Stdout:                 &DeliveryStdoutLoggerConfig{},
		InternalLogBatchConfig: DeliveryInternalLogBatchConfig{},
	}
	if err := validateDeliveryInternalLogBatchConfig("delivery logger", unused); err != nil {
		t.Fatalf("unused zero-value batch configs produced error: %v", err)
	}

	tests := []struct {
		name      string
		target    LogOutputDestination
		configure func(*DeliveryLoggerConfig)
		mutate    func(*DeliveryInternalLogBatchConfig)
		// wantは、fallback batch設定の不正として報告される項目です。
		want ConfigFieldError
	}{
		{
			name:   "opensearch",
			target: LogOutputOpenSearch,
			configure: func(cfg *DeliveryLoggerConfig) {
				cfg.OpenSearch = &DeliveryOpenSearchConfig{}
			},
			mutate: func(cfg *DeliveryInternalLogBatchConfig) {
				cfg.OpenSearch.MaxBulkDocument = 0
			},
			want: ConfigFieldError{Field: ConfigFieldMaxBulkDocument, Value: 0, Err: ErrNonPositiveSetting},
		},
		{
			name:   "loki",
			target: LogOutputLoki,
			configure: func(cfg *DeliveryLoggerConfig) {
				cfg.Loki = &DeliveryLokiConfig{}
			},
			mutate: func(cfg *DeliveryInternalLogBatchConfig) {
				cfg.Loki.FlushInterval = 0
			},
			want: ConfigFieldError{Field: ConfigFieldFlushInterval, Value: time.Duration(0), Err: ErrNonPositiveSetting},
		},
		{
			name:   "cloudwatch logs",
			target: LogOutputCloudWatchLogs,
			configure: func(cfg *DeliveryLoggerConfig) {
				cfg.CloudWatchLogs = &DeliveryCloudWatchLogsConfig{}
			},
			mutate: func(cfg *DeliveryInternalLogBatchConfig) {
				cfg.CloudWatchLogs.BufferFlushBytes = 0
			},
			want: ConfigFieldError{Field: ConfigFieldBufferFlushBytes, Value: 0, Err: ErrNonPositiveSetting},
		},
		{
			name:   "slack",
			target: LogOutputSlack,
			configure: func(cfg *DeliveryLoggerConfig) {
				cfg.Slack = &DeliverySlackLoggerConfig{}
			},
			mutate: func(cfg *DeliveryInternalLogBatchConfig) {
				cfg.Slack.Intervals = nil
			},
			want: ConfigFieldError{Field: ConfigFieldIntervals, Err: ErrEmptySetting},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batchCfg := valid
			batchCfg.Slack.Intervals = append([]time.Duration(nil), valid.Slack.Intervals...)
			test.mutate(&batchCfg)
			cfg := DeliveryLoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
				Stdout: &DeliveryStdoutLoggerConfig{HandlerConfig: loggerhandler.HandlerConfig{
					OnErrorConfig: []string{test.target.String()},
				}},
				InternalLogBatchConfig: batchCfg,
			}
			test.configure(&cfg)
			err := validateDeliveryInternalLogBatchConfig("delivery logger", cfg)
			fieldErr := findFieldError(err, test.want.Field, test.want.Err)
			if fieldErr == nil || fieldErr.Source != "delivery logger internal log fallback" ||
				fieldErr.Backend != test.target || fieldErr.Value != test.want.Value {
				t.Fatalf("validation error = %v, want internal log fallback %s %s %v of kind %v",
					err, test.target, test.want.Field, test.want.Value, test.want.Err)
			}
		})
	}
}

func TestValidateDeliveryInternalLogBatchConfigValidatesDedicatedTarget(t *testing.T) {
	cfg := DeliveryLoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
		Stdout: &DeliveryStdoutLoggerConfig{HandlerConfig: loggerhandler.HandlerConfig{
			OnErrorConfig: []string{LogOutputOpenSearch.String()},
		}},
		InternalLogDestinations: &DeliveryInternalLogDestinationConfig{
			OpenSearch: &DeliveryOpenSearchConfig{},
		},
		InternalLogBatchConfig: DeliveryInternalLogBatchConfig{
			OpenSearch: DeliveryInternalLogOpenSearchBatchConfig{
				FlushInterval:       time.Second,
				BufferFlushBytes:    512,
				MaxBulkRequestBytes: 1024,
			},
		},
	}

	err := validateDeliveryInternalLogBatchConfig("delivery logger", cfg)
	fieldErr := findFieldError(err, ConfigFieldMaxBulkDocument, ErrNonPositiveSetting)
	if fieldErr == nil || fieldErr.Source != "delivery logger internal log fallback" ||
		fieldErr.Backend != LogOutputOpenSearch || fieldErr.Value != 0 {
		t.Fatalf("validation error = %v, want internal log fallback opensearch non-positive max bulk document", err)
	}
}
