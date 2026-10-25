package logger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	loggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

func TestLoggerConstructorsRejectUnknownHandlerSelectorBeforeRuntimeBuild(t *testing.T) {
	routing := loggerhandler.HandlerConfig{OnErrorConfig: []string{"unknown"}}
	tests := []struct {
		name      string
		construct func() error
	}{
		{
			name: "sync",
			construct: func() error {
				_, err := NewLogger(
					context.Background(),
					pkglogger.LoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputStdout,
						},
						Timeout:          time.Second,
						MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
						Stdout:           &pkglogger.StdoutLoggerConfig{HandlerConfig: routing},
					}, streamrouting.None, nil,

					nil,
					observer.NopTracer{},
					observer.NopMeter{})

				return err
			},
		},
		{
			name: "async",
			construct: func() error {
				_, err := NewAsyncLogger(
					context.Background(),
					pkglogger.AsyncBestEffortMultiLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputStdout,
						},
						Timeout:          time.Second,
						MaxParallel:      1,
						MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
						Stdout:           &pkglogger.StdoutLoggerConfig{HandlerConfig: routing},
					}, streamrouting.None, nil,

					nil,
					observer.NopTracer{},
					observer.NopMeter{})

				return err
			},
		},
		{
			name: "delivery",
			construct: func() error {
				_, err := NewDeliveryLogger(
					context.Background(),
					pkglogger.DeliveryLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputStdout,
						},
						Timeout:          time.Second,
						MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
						Stdout:           &pkglogger.DeliveryStdoutLoggerConfig{HandlerConfig: routing},
					},
					streamrouting.None,
					observer.NopTracer{},
					observer.NopMeter{},
				)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.construct()
			var selectorErr *pkglogger.HandlerSelectorError
			if !errors.As(err, &selectorErr) || selectorErr.Selector != "unknown" || selectorErr.Level != "error" {
				t.Fatalf("constructor error = %v, want unknown handler selector error", err)
			}
			if !errors.Is(err, pkglogger.ErrHandlerSelectorNotConfigured) {
				t.Fatalf("constructor error = %v, want ErrHandlerSelectorNotConfigured", err)
			}
		})
	}
}

func TestNewLoggerRejectsMissingProfileAndRemoteClient(t *testing.T) {
	tests := []struct {
		name  string
		cfg   pkglogger.LoggerConfig
		check func(t *testing.T, err error)
	}{
		{
			name: "missing profile",
			cfg: pkglogger.LoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{
					pkglogger.LogOutputStdout,
				},
				Timeout:          time.Second,
				MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
				Stdout: &pkglogger.StdoutLoggerConfig{
					HandlerConfig: loggerhandler.HandlerConfig{
						OnErrorConfig: []string{"file:missing"},
					},
				},
			},
			check: func(t *testing.T, err error) {
				var selectorErr *pkglogger.HandlerSelectorError
				if !errors.As(err, &selectorErr) || selectorErr.Selector != "file:missing" {
					t.Fatalf("NewLogger() error = %v, want unresolved file:missing selector", err)
				}
			},
		},
		{
			name: "remote client missing",
			cfg: pkglogger.LoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{
					pkglogger.LogOutputStdout,
				},
				Timeout:          time.Second,
				MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
				Stdout: &pkglogger.StdoutLoggerConfig{
					HandlerConfig: loggerhandler.HandlerConfig{
						OnErrorConfig: []string{"opensearch"},
					},
				},
				OpenSearch: &pkglogger.OpenSearchConfig{
					OpenSearchLoggerBaseConfig: pkglogger.OpenSearchLoggerBaseConfig{
						BufferFlushBytes:    testOpenSearchBufferFlushBytes,
						MaxBulkRequestBytes: testOpenSearchMaxBulkRequestBytes,
					},
					OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
						Index: "logs",
					},
				},
			},
			check: func(t *testing.T, err error) {
				if !errors.Is(err, pkglogger.ErrClientConfigRequired) {
					t.Fatalf("NewLogger() error = %v, want ErrClientConfigRequired", err)
				}
				for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
					if errors.Is(fieldErr, pkglogger.ErrClientConfigRequired) &&
						fieldErr.Backend == pkglogger.LogOutputOpenSearch && fieldErr.Field == pkglogger.ConfigFieldClient {
						return
					}
				}
				t.Fatalf("NewLogger() error = %v, want missing OpenSearch client", err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewLogger(
				context.Background(),
				tt.cfg,
				streamrouting.None,
				nil,
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
			)

			tt.check(t, err)
		})
	}
}
