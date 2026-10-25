package logger

import (
	"errors"
	"testing"
	"time"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

func TestValidateLoggerBackendConfigsRejectsCloudWatchLogGroupMismatch(t *testing.T) {
	client := validBackendValidationCloudWatchLogsClient()
	cfg := LoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputCloudWatchLogs},
		CloudWatchLogs: &CloudWatchLogsConfig{
			CloudWatchLogsFlushManagerConfig: CloudWatchLogsFlushManagerConfig{
				LogGroupName:  "logger-group",
				LogStreamName: "logger-stream",
			},
		},
		CloudWatchLogsClient: client,
	}

	err := ValidateLoggerBackendConfigs("logger", cfg)
	fieldErr := findFieldError(err, ConfigFieldLogGroupName, ErrLogGroupMismatch)
	if fieldErr == nil || fieldErr.Source != "logger" || fieldErr.Backend != LogOutputCloudWatchLogs ||
		fieldErr.Value != "logger-group" || fieldErr.Limit != client.LogGroupName {
		t.Fatalf("ValidateLoggerBackendConfigs() error = %v, want logger-group mismatching %s", err, client.LogGroupName)
	}
}

func TestValidateLoggerBackendConfigsRejectsInvalidCloudWatchLogStreamName(t *testing.T) {
	client := validBackendValidationCloudWatchLogsClient()
	cfg := LoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputCloudWatchLogs},
		CloudWatchLogs: &CloudWatchLogsConfig{
			CloudWatchLogsFlushManagerConfig: CloudWatchLogsFlushManagerConfig{
				LogGroupName:  client.LogGroupName,
				LogStreamName: "invalid:stream",
			},
		},
		CloudWatchLogsClient: client,
	}

	err := ValidateLoggerBackendConfigs("logger", cfg)
	fieldErr := findFieldError(err, ConfigFieldLogStreamName, ErrInvalidSetting)
	if fieldErr == nil || fieldErr.Value != "invalid:stream" ||
		!errors.Is(fieldErr, pkgcloudwatchlogs.ErrLogStreamNameInvalidCharacter) {
		t.Fatalf("ValidateLoggerBackendConfigs() error = %v, want invalid log stream name", err)
	}
}

func TestValidateLoggerBackendConfigsValidatesCloudWatchLogsBufferConfig(t *testing.T) {
	tests := []struct {
		name      string
		maxEvents int
		wantErr   bool
	}{
		{
			name:      "negative",
			maxEvents: -1,
			wantErr:   true,
		},
		{
			name:      "zero",
			maxEvents: 0,
			wantErr:   true,
		},
		{
			name:      "request limit",
			maxEvents: pkgcloudwatchlogs.MaxPutLogEventsCount,
		},
		{
			name:      "above request limit",
			maxEvents: pkgcloudwatchlogs.MaxPutLogEventsCount + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := validBackendValidationCloudWatchLogsClient()
			cfg := LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputCloudWatchLogs},
				CloudWatchLogs: &CloudWatchLogsConfig{
					CloudWatchLogsLoggerBaseConfig: CloudWatchLogsLoggerBaseConfig{
						FlushInterval:          time.Second,
						BufferFlushMaxEvents:   tt.maxEvents,
						BufferFlushBytes:       1,
						FallbackQueueMaxEvents: max(tt.maxEvents, 1),
					},
					CloudWatchLogsFlushManagerConfig: CloudWatchLogsFlushManagerConfig{
						LogGroupName:  client.LogGroupName,
						LogStreamName: "logger-stream",
					},
				},
				CloudWatchLogsClient: client,
			}

			err := ValidateLoggerBackendConfigs("logger", cfg)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("ValidateLoggerBackendConfigs() error = %v, want nil", err)
				}
				return
			}
			fieldErr := findFieldError(err, ConfigFieldBufferFlushMaxEvents, ErrNonPositiveSetting)
			if fieldErr == nil || fieldErr.Value != tt.maxEvents {
				t.Fatalf("ValidateLoggerBackendConfigs() error = %v, want non-positive buffer flush max events %d",
					err, tt.maxEvents)
			}
		})
	}
}

func TestValidateDeliveryLoggerBackendConfigsRejectsInternalCloudWatchDestinationMismatch(
	t *testing.T,
) {
	cfg := DeliveryLoggerConfig{
		InternalLogDestinations: &DeliveryInternalLogDestinationConfig{
			CloudWatchLogs: &DeliveryCloudWatchLogsConfig{
				DeliveryCloudWatchLogsFlushManagerConfig: DeliveryCloudWatchLogsFlushManagerConfig{
					LogGroupName:  "logger-group",
					LogStreamName: "logger-stream",
				},
			},
			CloudWatchLogsClient: validBackendValidationCloudWatchLogsClient(),
		},
	}

	err := ValidateDeliveryLoggerBackendConfigs("delivery logger", cfg)
	fieldErr := findFieldError(err, ConfigFieldLogGroupName, ErrLogGroupMismatch)
	if fieldErr == nil || fieldErr.Source != "delivery logger internal log destination" ||
		fieldErr.Backend != LogOutputCloudWatchLogs {
		t.Fatalf(
			"ValidateDeliveryLoggerBackendConfigs() error = %v, want internal destination mismatch",
			err,
		)
	}
}
