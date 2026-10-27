package logger

import (
	"testing"

	clickhouselogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	cloudwatchlogslogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs"
	lokilogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki"
	opensearchlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 通常loggerとfallbackが使用する各backendの設定変換を通し、Cloudの省略と指定を
// 同じ規則で扱うことを確認します。外部clientの生成や送信は行いません。
func TestBackendResourceAdaptersUseCommonOptionalCloudHandling(t *testing.T) {
	for _, backend := range []struct {
		name  string
		adapt func(pkglogger.Resource) resource.ResourceProvider
	}{
		{
			name: "opensearch",
			adapt: func(input pkglogger.Resource) resource.ResourceProvider {
				_, _, _, result, _ := opensearchlogger.NewOpenSearchLoggerAdapter(pkglogger.LoggerConfig{
					OpenSearch: &pkglogger.OpenSearchConfig{
						OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{Resource: input},
					},
				})
				return result
			},
		},
		{
			name: "loki",
			adapt: func(input pkglogger.Resource) resource.ResourceProvider {
				_, _, _, result, _ := lokilogger.NewLokiLoggerAdapter(pkglogger.LoggerConfig{
					Loki: &pkglogger.LokiConfig{
						LokiFlushManagerConfig: pkglogger.LokiFlushManagerConfig{Resource: input},
					},
				})
				return result
			},
		},
		{
			name: "cloudwatchlogs",
			adapt: func(input pkglogger.Resource) resource.ResourceProvider {
				_, _, _, result, _ := cloudwatchlogslogger.NewCloudWatchLogsLoggerAdapter(pkglogger.LoggerConfig{
					CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
						CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{Resource: input},
					},
				}, pkgcloudwatchlogs.StreamRoutingNone)
				return result
			},
		},
		{
			name: "clickhouse",
			adapt: func(input pkglogger.Resource) resource.ResourceProvider {
				_, _, _, result, _ := clickhouselogger.NewClickHouseLoggerAdapter(pkglogger.LoggerConfig{
					ClickHouse: &pkglogger.ClickHouseConfig{
						ClickHouseFlushManagerConfig: pkglogger.ClickHouseFlushManagerConfig{Resource: input},
					},
				})
				return result
			},
		},
	} {
		for _, configuredCloud := range []bool{false, true} {
			name := "omitted"
			if configuredCloud {
				name = "configured"
			}
			t.Run(backend.name+"/"+name, func(t *testing.T) {
				input := pkglogger.Resource{ServiceName: "service"}
				var want pkglogger.CloudResourceInfo
				if configuredCloud {
					want = pkglogger.CloudResourceInfo{Provider: "aws", Region: "ap-northeast-1", RuntimeInstanceID: "instance-1"}
					input.Cloud = &want
				}
				provider := backend.adapt(input)
				if provider.ServiceName() != input.ServiceName || provider.CloudProvider() != want.Provider ||
					provider.CloudProviderRegion() != want.Region || provider.RuntimeInstanceID() != want.RuntimeInstanceID {
					t.Fatalf("resource fields = %q/%q/%q/%q, want %q/%+v",
						provider.ServiceName(), provider.CloudProvider(), provider.CloudProviderRegion(),
						provider.RuntimeInstanceID(), input.ServiceName, want)
				}
			})
		}
	}
}
