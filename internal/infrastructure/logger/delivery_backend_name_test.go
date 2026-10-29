package logger

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/require"
)

// deliveryBackendNameUnreachableEndpointは、閉じたlocal portです。この試験は送信を始める前に
// 拒否される結果だけを確かめるため、どの宛先へもrequestを送りません。
const deliveryBackendNameUnreachableEndpoint = "http://127.0.0.1:1"

// 7つの出力先それぞれを一つだけ通常出力に選んだDelivery loggerで、Close開始後の要求に
// multi loggerが作る送達結果のBackendが、その出力先の論理出力名
// （pkg/logger.LogOutputDestinationの名前）であることを確かめます。構築時に各childへ付ける名前を
// 出力先ごとに確認し、別の出力先の名前を付ける誤りを検出します。
func TestDeliveryLoggerSynthesizedResultsNameEveryOutputByLogicalName(t *testing.T) {
	for _, output := range pkglogger.AllLogOutputDestinations() {
		t.Run(output.String(), func(t *testing.T) {
			cfg := deliveryBackendNameConfig(t, output)
			logger, err := NewDeliveryLogger(context.Background(), cfg, pkgcloudwatchlogs.StreamRoutingNone, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			require.NoError(t, boundedwait.Close(t, "Delivery logger Close", logger.Close, boundedwait.ForceCloserOf(logger)))

			report, err := logger.Info(context.Background(), "after close")
			require.ErrorIs(t, err, pkglogger.ErrLoggerClosed)
			require.Len(t, report.Targets, 1)
			require.Equal(t, output.String(), report.Targets[0].Backend)
			require.Equal(t, pkglogger.DeliveryRejected, report.Targets[0].Status)
		})
	}
}

// deliveryBackendNameConfigは、outputだけを通常出力に選ぶDelivery設定です。network backendの
// 接続先は閉じたlocal portで、構築とCloseはrequestを送りません。
func deliveryBackendNameConfig(t *testing.T, output pkglogger.LogOutputDestination) pkglogger.DeliveryLoggerConfig {
	t.Helper()
	noFailureStore := durable.DeliveryFailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeNone}}
	body := pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano}
	cfg := pkglogger.DeliveryLoggerConfig{
		Timeout:               time.Second,
		MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
		LogOutputDestinations: pkglogger.LogOutputDestinations{output},
	}
	switch output {
	case pkglogger.LogOutputFile:
		cfg.File = newCloseOwnershipDeliveryFileConfig(t.TempDir(), "delivery-backend-name")
	case pkglogger.LogOutputStdout:
		cfg.Stdout = newActivationDeliveryStdoutConfig()
	case pkglogger.LogOutputSlack:
		cfg.Slack = &pkglogger.DeliverySlackLoggerConfig{
			Token: "test-token", Channel: "test-channel", Timeout: time.Second, LogBodyConfig: body,
			RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelDebug},
		}
	case pkglogger.LogOutputOpenSearch:
		cfg.OpenSearchClient = &pkgopensearch.Config{
			Endpoint: deliveryBackendNameUnreachableEndpoint, Timeout: time.Second,
			AuthConfig: pkgopensearch.AuthConfig{Type: pkgopensearch.AuthTypeNone},
		}
		cfg.OpenSearch = &pkglogger.DeliveryOpenSearchConfig{
			DeliveryOpenSearchLoggerBaseConfig: pkglogger.DeliveryOpenSearchLoggerBaseConfig{LogBodyConfig: body},
			DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
				Index: "logs", FailureStoreConfig: noFailureStore, Resource: activationResource(),
			},
		}
	case pkglogger.LogOutputLoki:
		cfg.LokiClient = &pkgloki.Config{Endpoint: deliveryBackendNameUnreachableEndpoint, Timeout: time.Second}
		cfg.Loki = &pkglogger.DeliveryLokiConfig{
			DeliveryLokiLoggerBaseConfig: pkglogger.DeliveryLokiLoggerBaseConfig{
				LogBodyConfig: body, MaxLineBytes: 1 << 16, MaxPushRequestBytes: 1 << 20,
			},
			DeliveryLokiFlushManagerConfig: pkglogger.DeliveryLokiFlushManagerConfig{
				FailureStoreConfig: noFailureStore, Resource: activationResource(),
			},
		}
	case pkglogger.LogOutputCloudWatchLogs:
		client := validActivationCloudWatchLogsClient()
		client.Endpoint = deliveryBackendNameUnreachableEndpoint
		client.Timeout = time.Second
		cfg.CloudWatchLogsClient = client
		cfg.CloudWatchLogs = &pkglogger.DeliveryCloudWatchLogsConfig{
			DeliveryCloudWatchLogsLoggerBaseConfig: pkglogger.DeliveryCloudWatchLogsLoggerBaseConfig{LogBodyConfig: body},
			DeliveryCloudWatchLogsFlushManagerConfig: pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{
				LogGroupName: "test-group", LogStreamName: "test-stream",
				FailureStoreConfig: noFailureStore, Resource: activationResource(),
			},
		}
	case pkglogger.LogOutputClickHouse:
		cfg.ClickHouseClient = &pkgclickhouse.Config{
			Endpoint: deliveryBackendNameUnreachableEndpoint, Database: "default", Table: "logs", Timeout: time.Second,
		}
		cfg.ClickHouse = &pkglogger.DeliveryClickHouseConfig{
			DeliveryClickHouseLoggerBaseConfig: pkglogger.DeliveryClickHouseLoggerBaseConfig{
				LogBodyConfig:           body,
				FlushInterval:           time.Hour,
				MaxEntries:              1,
				BufferFlushBytes:        directRetryClickHouseBufferFlushBytes,
				FallbackQueueMaxEntries: 1,
				MaxInsertRequestBytes:   directRetryClickHouseMaxInsertRequestBytes,
			},
			DeliveryClickHouseFlushManagerConfig: pkglogger.DeliveryClickHouseFlushManagerConfig{
				FailureStoreConfig: noFailureStore, Resource: activationResource(),
			},
		}
	default:
		t.Fatalf("no Delivery configuration for output %q", output)
	}
	return cfg
}
