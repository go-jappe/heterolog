package logger

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/handler"
)

func TestValidateLoggerCloudWatchLogsStreamRoutingRejectsUnsupportedGranularity(t *testing.T) {
	t.Parallel()

	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs},
		CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
			CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{
				LogStreamName: "app/task-1",
			},
		},
	}

	err := validateLoggerCloudWatchLogsStreamRouting(
		"logger",
		cfg,
		streamrouting.Granularity("weekly"),
	)
	if !errors.Is(err, ErrUnsupportedStreamRoutingGranularity) {
		t.Fatalf("validation error = %v, want %v", err, ErrUnsupportedStreamRoutingGranularity)
	}
	requireCloudWatchLogsStreamRoutingError(t, err, "logger", streamrouting.Granularity("weekly"), "")
}

func TestValidateLoggerCloudWatchLogsStreamRoutingChecksInternalLogResolvedName(t *testing.T) {
	t.Parallel()

	cfg := pkglogger.LoggerConfig{
		InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
			CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
				CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{
					LogStreamName: strings.Repeat("a", 505),
				},
			},
		},
	}

	err := validateLoggerCloudWatchLogsStreamRouting("logger", cfg, streamrouting.Daily)
	if !errors.Is(err, pkgcloudwatchlogs.ErrLogStreamNameTooLong) {
		t.Fatalf("validation error = %v, want resolved stream name length error", err)
	}
	requireCloudWatchLogsStreamRoutingError(t, err, "logger", streamrouting.Daily, strings.Repeat("a", 505))
}

func TestValidateDeliveryLoggerCloudWatchLogsStreamRoutingChecksResolvedName(t *testing.T) {
	t.Parallel()

	cfg := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs},
		CloudWatchLogs: &pkglogger.DeliveryCloudWatchLogsConfig{
			DeliveryCloudWatchLogsFlushManagerConfig: pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{
				LogStreamName: strings.Repeat("b", 505),
			},
		},
	}

	err := validateDeliveryLoggerCloudWatchLogsStreamRouting(
		"delivery logger",
		cfg,
		streamrouting.Hourly,
	)
	if !errors.Is(err, pkgcloudwatchlogs.ErrLogStreamNameTooLong) {
		t.Fatalf("validation error = %v, want resolved stream name length error", err)
	}
	requireCloudWatchLogsStreamRoutingError(t, err, "delivery logger", streamrouting.Hourly, strings.Repeat("b", 505))
}

// requireCloudWatchLogsStreamRoutingErrorは、検証errorの出所・分割粒度・失敗した基準
// stream名を確認します。粒度が未対応の場合、streamNameは空です。
func requireCloudWatchLogsStreamRoutingError(
	t *testing.T,
	err error,
	source string,
	granularity streamrouting.Granularity,
	streamName string,
) {
	t.Helper()
	var routingErr *CloudWatchLogsStreamRoutingError
	if !errors.As(err, &routingErr) {
		t.Fatalf("validation error = %v, want CloudWatchLogsStreamRoutingError", err)
	}
	if routingErr.Source != source || routingErr.Granularity != granularity || routingErr.StreamName != streamName {
		t.Fatalf("stream routing error = %+v, want source=%q granularity=%q stream=%q", routingErr, source, granularity, streamName)
	}
}

func TestCloudWatchLogsStreamRoutingValidatesOnlyRequiredBaseDestination(t *testing.T) {
	t.Parallel()
	longName := strings.Repeat("a", 505) // Valid alone; the daily suffix exceeds the stream limit.
	for _, test := range []struct {
		name         string
		destinations pkglogger.LogOutputDestinations
		handler      bool
		baseName     string
		internalName string
		wantError    bool
	}{
		{name: "inactive configured base", destinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}, baseName: longName},
		{name: "selected base", destinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs}, baseName: longName, wantError: true},
		{name: "implicit base", baseName: longName, wantError: true},
		{name: "handler only base", destinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}, handler: true, baseName: longName, wantError: true},
		{name: "dedicated target replaces handler only base", destinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}, handler: true, baseName: longName, internalName: "internal-events"},
		{name: "dedicated target does not replace normal output", destinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs}, baseName: longName, internalName: "internal-events", wantError: true},
		{name: "dedicated target still validated", destinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}, handler: true, baseName: "base-events", internalName: longName, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var routing handler.HandlerConfig
			if test.handler {
				routing.OnErrorConfig = []string{"cloudwatchlogs"}
			}
			cfg := pkglogger.LoggerConfig{
				LogOutputDestinations: test.destinations,
				Stdout:                &pkglogger.StdoutLoggerConfig{HandlerConfig: routing},
				CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
					CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{LogStreamName: test.baseName},
				},
			}
			delivery := pkglogger.DeliveryLoggerConfig{
				LogOutputDestinations: test.destinations,
				Stdout:                &pkglogger.DeliveryStdoutLoggerConfig{HandlerConfig: routing},
				CloudWatchLogs: &pkglogger.DeliveryCloudWatchLogsConfig{
					DeliveryCloudWatchLogsFlushManagerConfig: pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{LogStreamName: test.baseName},
				},
			}
			if test.internalName != "" {
				cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
					CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{LogStreamName: test.internalName},
				}}
				delivery.InternalLogDestinations = &pkglogger.DeliveryInternalLogDestinationConfig{CloudWatchLogs: &pkglogger.DeliveryCloudWatchLogsConfig{
					DeliveryCloudWatchLogsFlushManagerConfig: pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{LogStreamName: test.internalName},
				}}
			}
			for _, result := range []struct {
				name string
				err  error
			}{
				{"sync and async", validateLoggerCloudWatchLogsStreamRouting("logger", cfg, streamrouting.Daily)},
				{"delivery", validateDeliveryLoggerCloudWatchLogsStreamRouting("delivery logger", delivery, streamrouting.Daily)},
			} {
				if test.wantError {
					// 長い名前を持つ方の保存先が、検証で拒否された基準stream名です。
					var routingErr *CloudWatchLogsStreamRoutingError
					if !errors.Is(result.err, pkgcloudwatchlogs.ErrLogStreamNameTooLong) ||
						!errors.As(result.err, &routingErr) || routingErr.StreamName != longName {
						t.Errorf("%s validation = %v, want stream name error for the long base name", result.name, result.err)
					}
				} else if result.err != nil {
					t.Errorf("%s rejected an unused base destination: %v", result.name, result.err)
				}
			}
		})
	}
}

func TestValidateLoggerCloudWatchLogsStreamRoutingIgnoresUnusedGranularity(t *testing.T) {
	t.Parallel()

	err := validateLoggerCloudWatchLogsStreamRouting(
		"logger",
		pkglogger.LoggerConfig{},
		streamrouting.Granularity("weekly"),
	)
	if err != nil {
		t.Fatalf("unused stream routing validation error = %v, want nil", err)
	}
}
