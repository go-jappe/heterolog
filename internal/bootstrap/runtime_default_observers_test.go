package bootstrap_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

func TestOpenSearchDefaultObservers(t *testing.T) {
	for _, kind := range []string{"sync", "async", "delivery"} {
		for _, observers := range []string{"omitted", "explicit nil", "typed nil", "provided control"} {
			t.Run(kind+"/"+observers, func(t *testing.T) {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("valid constructor panicked: %v", r)
					}
				}()
				var opts []loggerruntime.Option
				if observers == "explicit nil" {
					opts = []loggerruntime.Option{loggerruntime.WithObservers(nil, nil)}
				}
				if observers == "typed nil" {
					var tracer *runtimeObserverTracer
					var meter *runtimeObserverMeter
					opts = []loggerruntime.Option{loggerruntime.WithObservers(tracer, meter)}
				}
				if observers == "provided control" {
					opts = []loggerruntime.Option{loggerruntime.WithObservers(&runtimeObserverTracer{}, &runtimeObserverMeter{})}
				}
				client := &pkgopensearch.Config{Endpoint: "http://opensearch.invalid", Timeout: time.Second, AuthConfig: pkgopensearch.AuthConfig{Type: pkgopensearch.AuthTypeNone}}
				backend := &pkglogger.OpenSearchConfig{
					OpenSearchLoggerBaseConfig:   pkglogger.OpenSearchLoggerBaseConfig{RuntimeConfig: pkglogger.RuntimeConfig{AppName: "default-observers", LogLevel: pkglogger.LogLevelInfo}, LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano}, FlushInterval: time.Hour, MaxBulkDocument: 2, BufferFlushBytes: 1 << 20, MaxBulkRequestBytes: 2 << 20},
					OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{Index: "logs", MaxItemRetries: 0, FailureStoreConfig: durable.FailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeNone}}},
				}
				var closeLogger func() error
				switch kind {
				case "sync":
					cfg := loggerruntime.DefaultConfig()
					cfg.Stdout = nil
					cfg.OpenSearch = backend
					cfg.OpenSearchClient = client
					cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
					logger, err := bootstrap.New(context.Background(), cfg, opts...)
					if err != nil {
						t.Fatal(err)
					}
					closeLogger = logger.Close
				case "async":
					cfg := loggerruntime.DefaultAsyncConfig()
					cfg.Stdout = nil
					cfg.OpenSearch = backend
					cfg.OpenSearchClient = client
					cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
					logger, err := bootstrap.NewAsync(context.Background(), cfg, opts...)
					if err != nil {
						t.Fatal(err)
					}
					closeLogger = logger.Close
				case "delivery":
					cfg := loggerruntime.DefaultDeliveryConfig()
					cfg.Stdout = nil
					cfg.OpenSearchClient = client
					cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
					cfg.OpenSearch = &pkglogger.DeliveryOpenSearchConfig{
						DeliveryOpenSearchLoggerBaseConfig:   pkglogger.DeliveryOpenSearchLoggerBaseConfig{RuntimeConfig: pkglogger.DeliveryRuntimeConfig{AppName: "default-observers"}, LogBodyConfig: backend.LogBodyConfig},
						DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{Index: "logs", FailureStoreConfig: durable.DeliveryFailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeNone}}},
					}
					logger, err := bootstrap.NewDelivery(context.Background(), cfg, opts...)
					if err != nil {
						t.Fatal(err)
					}
					closeLogger = logger.Close
				}
				if err := closeLogger(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})
		}
	}
}
