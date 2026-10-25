package heterolog_test

import (
	"context"
	"github.com/go-jappe/heterolog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	"testing"
	"time"
)

func TestOpenSearchDefaultObservers(t *testing.T) {
	for _, kind := range []string{"sync", "async", "delivery"} {
		for _, observers := range []string{"omitted", "explicit nil", "typed nil", "provided control"} {
			t.Run(kind+"/"+observers, func(t *testing.T) {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("valid public constructor panicked: %v", r)
					}
				}()
				var opts []heterolog.Option
				if observers == "explicit nil" {
					opts = []heterolog.Option{heterolog.WithObservers(nil, nil)}
				}
				if observers == "typed nil" {
					var tracer *runtimeObserverTracer
					var meter *runtimeObserverMeter
					opts = []heterolog.Option{heterolog.WithObservers(tracer, meter)}
				}
				if observers == "provided control" {
					opts = []heterolog.Option{heterolog.WithObservers(&runtimeObserverTracer{}, &runtimeObserverMeter{})}
				}
				client := &pkgopensearch.Config{Endpoint: "http://opensearch.invalid", Timeout: time.Second, AuthConfig: pkgopensearch.AuthConfig{Type: pkgopensearch.AuthTypeNone}}
				backend := &pkglogger.OpenSearchConfig{
					OpenSearchLoggerBaseConfig:   pkglogger.OpenSearchLoggerBaseConfig{RuntimeConfig: pkglogger.RuntimeConfig{AppName: "default-observers", LogLevel: heterolog.LevelInfo}, LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano}, FlushInterval: time.Hour, MaxBulkDocument: 2, BufferFlushBytes: 1 << 20, MaxBulkRequestBytes: 2 << 20},
					OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{Index: "logs", MaxRetryAttempts: 1, FailureStoreConfig: durable.FailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeNone}}},
				}
				var closeLogger func() error
				switch kind {
				case "sync":
					cfg := heterolog.DefaultConfig()
					cfg.Stdout = nil
					cfg.OpenSearch = backend
					cfg.OpenSearchClient = client
					cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
					logger, err := heterolog.New(context.Background(), cfg, opts...)
					if err != nil {
						t.Fatal(err)
					}
					closeLogger = logger.Close
				case "async":
					cfg := heterolog.DefaultAsyncConfig()
					cfg.Stdout = nil
					cfg.OpenSearch = backend
					cfg.OpenSearchClient = client
					cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
					logger, err := heterolog.NewAsync(context.Background(), cfg, opts...)
					if err != nil {
						t.Fatal(err)
					}
					closeLogger = logger.Close
				case "delivery":
					cfg := heterolog.DefaultDeliveryConfig()
					cfg.Stdout = nil
					cfg.OpenSearchClient = client
					cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
					cfg.OpenSearch = &pkglogger.DeliveryOpenSearchConfig{
						DeliveryOpenSearchLoggerBaseConfig:   pkglogger.DeliveryOpenSearchLoggerBaseConfig{RuntimeConfig: pkglogger.DeliveryRuntimeConfig{AppName: "default-observers"}, LogBodyConfig: backend.LogBodyConfig},
						DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{Index: "logs", FailureStoreConfig: durable.DeliveryFailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeNone}}},
					}
					logger, err := heterolog.NewDelivery(context.Background(), cfg, opts...)
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
