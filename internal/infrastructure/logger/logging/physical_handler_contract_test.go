package logging_test

import (
	"reflect"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/loki"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/slack"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
)

func TestPhysicalHandlersDoNotExposeApplicationLoggerMethods(t *testing.T) {
	physicalHandlerTypes := []reflect.Type{
		reflect.TypeOf((*file.FileLogger)(nil)),
		reflect.TypeOf((*file.DeliveryFileLogger)(nil)),
		reflect.TypeOf((*stdout.StdoutLogger)(nil)),
		reflect.TypeOf((*stdout.DeliveryStdoutLogger)(nil)),
		reflect.TypeOf((*slack.SlackLogger)(nil)),
		reflect.TypeOf((*slack.DeliverySlackLogger)(nil)),
		reflect.TypeOf((*opensearch.OpenSearchLogger)(nil)),
		reflect.TypeOf((*opensearch.DeliveryOpenSearchLogger)(nil)),
		reflect.TypeOf((*loki.LokiLogger)(nil)),
		reflect.TypeOf((*loki.DeliveryLokiLogger)(nil)),
		reflect.TypeOf((*cloudwatchlogs.CloudWatchLogsLogger)(nil)),
		reflect.TypeOf((*cloudwatchlogs.DeliveryCloudWatchLogsLogger)(nil)),
		reflect.TypeOf((*clickhouse.ClickHouseLogger)(nil)),
		reflect.TypeOf((*clickhouse.DeliveryClickHouseLogger)(nil)),
	}
	applicationMethodNames := []string{
		"Debug",
		"Info",
		"Warn",
		"Error",
		"ErrorWithStack",
		"Log",
		"LogWithStack",
	}

	for _, handlerType := range physicalHandlerTypes {
		for _, methodName := range applicationMethodNames {
			if _, exists := handlerType.MethodByName(methodName); exists {
				t.Errorf(
					"physical handler %s must not expose application logger method %s",
					handlerType,
					methodName,
				)
			}
		}
	}
}
