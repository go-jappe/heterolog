package heterolog_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog"
	cw "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestPublicConstructorsValidateOnlyEffectiveCloudWatchStreamNames(t *testing.T) {
	for _, mode := range []string{"sync", "async", "delivery"} {
		for _, test := range []struct {
			name         string
			routing      heterolog.StreamRouting
			selected     bool
			internalName string
			wantError    bool
		}{
			{name: "unused base without suffix", routing: heterolog.StreamRoutingNone},
			{name: "unused base with daily routing", routing: heterolog.StreamRoutingDaily},
			{name: "selected base with daily routing", routing: heterolog.StreamRoutingDaily, selected: true, wantError: true},
			{name: "dedicated fallback replaces base", routing: heterolog.StreamRoutingDaily, internalName: "internal-events"},
			{name: "dedicated fallback still validates suffix", routing: heterolog.StreamRoutingDaily, internalName: strings.Repeat("b", 505), wantError: true},
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
						RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: heterolog.LevelInfo},
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
				// The stored base name is valid; only an applied daily suffix would
				// exceed the limit. Keeping its config must not activate the backend.
				configure := func(cfg *heterolog.Config) {
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
					cfg := heterolog.DefaultConfig()
					configure(&cfg)
					created, err = heterolog.New(context.Background(), cfg, heterolog.WithCloudWatchLogsStreamRouting(test.routing))
				case "async":
					cfg := heterolog.DefaultAsyncConfig()
					baseCfg := cfg.ToLoggerConfig()
					configure(&baseCfg)
					cfg.LogOutputDestinations = baseCfg.LogOutputDestinations
					cfg.CloudWatchLogs, cfg.CloudWatchLogsClient = baseCfg.CloudWatchLogs, baseCfg.CloudWatchLogsClient
					cfg.InternalLogDestinations = baseCfg.InternalLogDestinations
					created, err = heterolog.NewAsync(context.Background(), cfg, heterolog.WithCloudWatchLogsStreamRouting(test.routing))
				case "delivery":
					cfg := heterolog.DefaultDeliveryConfig()
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
					created, err = heterolog.NewDelivery(context.Background(), cfg, heterolog.WithCloudWatchLogsStreamRouting(test.routing))
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
