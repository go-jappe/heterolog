package loggerruntime_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	cw "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestConstructorsValidateOnlyEffectiveCloudWatchStreamNames(t *testing.T) {
	for _, mode := range []string{"sync", "async", "delivery"} {
		for _, test := range []struct {
			name         string
			routing      cw.StreamRouting
			selected     bool
			internalName string
			wantError    bool
		}{
			{name: "unused base without suffix", routing: cw.StreamRoutingNone},
			{name: "unused base with daily routing", routing: cw.StreamRoutingDaily},
			{name: "selected base with daily routing", routing: cw.StreamRoutingDaily, selected: true, wantError: true},
			{name: "dedicated fallback replaces base", routing: cw.StreamRoutingDaily, internalName: "internal-events"},
			{name: "dedicated fallback still validates suffix", routing: cw.StreamRoutingDaily, internalName: strings.Repeat("b", 505), wantError: true},
		} {
			t.Run(mode+"/"+test.name, func(t *testing.T) {
				transport := &cloudWatchRoutingSelectionTransport{}
				client := &cw.Config{
					Region: "ap-northeast-1", Endpoint: "http://cloudwatch.invalid", LogGroupName: "logs",
					AccessKeyID: "test", SecretAccessKey: "test", Timeout: time.Second,
					HTTPClient: &http.Client{Transport: transport},
				}
				base := &pkglogger.CloudWatchLogsConfig{
					CloudWatchLogsLoggerBaseConfig: pkglogger.CloudWatchLogsLoggerBaseConfig{
						RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelInfo},
						FlushInterval: time.Hour, BufferFlushMaxEvents: 1, BufferFlushBytes: 1, FallbackQueueMaxEvents: 1,
					},
					CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{
						LogGroupName: "logs", LogStreamName: strings.Repeat("a", 505),
						FailureStoreConfig: durable.FailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeNone}},
					},
				}
				deliveryBase := &pkglogger.DeliveryCloudWatchLogsConfig{
					DeliveryCloudWatchLogsFlushManagerConfig: pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{
						LogGroupName: "logs", LogStreamName: base.LogStreamName,
						FailureStoreConfig: durable.DeliveryFailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeNone}},
					},
				}
				// 保存した元の名前は有効で、日ごとの接尾辞を付けた場合だけ上限を超えます。
				// 設定を残しただけでbackendを有効にしてはいけません。
				configure := func(cfg *pkglogger.LoggerConfig) {
					cfg.CloudWatchLogs, cfg.CloudWatchLogsClient = base, client
					if test.selected {
						cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs}
					}
					if test.internalName != "" {
						internal := *base
						internal.LogStreamName = test.internalName
						cfg.Stdout.HandlerConfig.OnErrorConfig = []string{"cloudwatchlogs"}
						cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{CloudWatchLogs: &internal, CloudWatchLogsClient: client}
					}
				}
				var created io.Closer
				var err error
				switch mode {
				case "sync":
					cfg := loggerruntime.DefaultConfig()
					configure(&cfg)
					created, err = loggerruntime.New(context.Background(), cfg, loggerruntime.WithCloudWatchLogsStreamRouting(test.routing))
				case "async":
					cfg := loggerruntime.DefaultAsyncConfig()
					baseCfg := cfg.ToLoggerConfig()
					configure(&baseCfg)
					cfg.LogOutputDestinations = baseCfg.LogOutputDestinations
					cfg.CloudWatchLogs, cfg.CloudWatchLogsClient = baseCfg.CloudWatchLogs, baseCfg.CloudWatchLogsClient
					cfg.InternalLogDestinations = baseCfg.InternalLogDestinations
					created, err = loggerruntime.NewAsync(context.Background(), cfg, loggerruntime.WithCloudWatchLogsStreamRouting(test.routing))
				case "delivery":
					cfg := loggerruntime.DefaultDeliveryConfig()
					cfg.CloudWatchLogs, cfg.CloudWatchLogsClient = deliveryBase, client
					if test.selected {
						cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs}
					}
					if test.internalName != "" {
						internal := *deliveryBase
						internal.LogStreamName = test.internalName
						cfg.Stdout.HandlerConfig.OnErrorConfig = []string{"cloudwatchlogs"}
						cfg.InternalLogDestinations = &pkglogger.DeliveryInternalLogDestinationConfig{CloudWatchLogs: &internal, CloudWatchLogsClient: client}
						cfg.InternalLogBatchConfig.CloudWatchLogs = pkglogger.DeliveryInternalLogCloudWatchLogsBatchConfig{
							FlushInterval: time.Hour, BufferFlushMaxEvents: 1, BufferFlushBytes: 1, FallbackQueueMaxEvents: 1,
						}
					}
					created, err = loggerruntime.NewDelivery(context.Background(), cfg, loggerruntime.WithCloudWatchLogsStreamRouting(test.routing))
				}
				if err == nil {
					if closeErr := created.Close(); closeErr != nil {
						t.Fatal(closeErr)
					}
				}
				if test.wantError {
					if !errors.Is(err, cw.ErrLogStreamNameTooLong) {
						t.Fatalf("constructor error = %v, want effective stream name length error", err)
					}
				} else if err != nil {
					t.Fatalf("constructor rejected unused base stream settings: %v", err)
				}
				if calls := transport.calls.Load(); calls != 0 {
					t.Fatalf("constructor made %d unexpected CloudWatch requests", calls)
				}
			})
		}
	}
}

type cloudWatchRoutingSelectionTransport struct{ calls atomic.Int64 }

func (tr *cloudWatchRoutingSelectionTransport) RoundTrip(*http.Request) (*http.Response, error) {
	tr.calls.Add(1)
	return nil, errors.New("unexpected CloudWatch request during constructor test")
}
