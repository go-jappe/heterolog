package logger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgslack "github.com/go-jappe/heterolog/pkg/slack"
)

func TestBufferedLoggerConstructorsDoNotDefaultZeroOpenSearchBulkByteLimits(t *testing.T) {
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	client := validActivationOpenSearchClient()

	tests := []struct {
		name       string
		construct  func() error
		wantSource string
		wantField  pkglogger.ConfigField
	}{
		{
			name: "sync buffer flush bytes",
			construct: func() error {
				_, err := NewLogger(
					context.Background(),
					pkglogger.LoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
						Timeout:               time.Second,
						MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
						OpenSearch: &pkglogger.OpenSearchConfig{
							OpenSearchLoggerBaseConfig: pkglogger.OpenSearchLoggerBaseConfig{
								FlushInterval:       time.Hour,
								MaxBulkDocument:     1,
								MaxBulkRequestBytes: testOpenSearchMaxBulkRequestBytes,
							},
							OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
								Index:    "logs",
								Resource: activationResource(),
							},
						},
						OpenSearchClient: client,
					},
					pkgcloudwatchlogs.StreamRoutingNone,
					nil,
					nil,
					tracer,
					meter,
				)
				return err
			},
			wantSource: pkglogger.LoggerContractSync.DisplayName(),
			wantField:  pkglogger.ConfigFieldBufferFlushBytes,
		},
		{
			name: "sync max bulk request bytes",
			construct: func() error {
				_, err := NewLogger(
					context.Background(),
					pkglogger.LoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
						Timeout:               time.Second,
						MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
						OpenSearch: &pkglogger.OpenSearchConfig{
							OpenSearchLoggerBaseConfig: pkglogger.OpenSearchLoggerBaseConfig{
								FlushInterval:    time.Hour,
								MaxBulkDocument:  1,
								BufferFlushBytes: testOpenSearchBufferFlushBytes,
							},
							OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
								Index:    "logs",
								Resource: activationResource(),
							},
						},
						OpenSearchClient: client,
					},
					pkgcloudwatchlogs.StreamRoutingNone,
					nil,
					nil,
					tracer,
					meter,
				)
				return err
			},
			wantSource: pkglogger.LoggerContractSync.DisplayName(),
			wantField:  pkglogger.ConfigFieldMaxBulkRequestBytes,
		},
		{
			name: "async buffer flush bytes",
			construct: func() error {
				_, err := NewAsyncLogger(
					context.Background(),
					pkglogger.AsyncBestEffortMultiLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
						Timeout:               time.Second,
						MaxParallel:           1,
						MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
						OpenSearch: &pkglogger.OpenSearchConfig{
							OpenSearchLoggerBaseConfig: pkglogger.OpenSearchLoggerBaseConfig{
								FlushInterval:       time.Hour,
								MaxBulkDocument:     1,
								MaxBulkRequestBytes: testOpenSearchMaxBulkRequestBytes,
							},
							OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
								Index:    "logs",
								Resource: activationResource(),
							},
						},
						OpenSearchClient: client,
					},
					pkgcloudwatchlogs.StreamRoutingNone,
					nil,
					nil,
					tracer,
					meter,
				)
				return err
			},
			wantSource: pkglogger.LoggerContractAsyncBestEffort.DisplayName(),
			wantField:  pkglogger.ConfigFieldBufferFlushBytes,
		},
		{
			name: "async max bulk request bytes",
			construct: func() error {
				_, err := NewAsyncLogger(
					context.Background(),
					pkglogger.AsyncBestEffortMultiLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
						Timeout:               time.Second,
						MaxParallel:           1,
						MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
						OpenSearch: &pkglogger.OpenSearchConfig{
							OpenSearchLoggerBaseConfig: pkglogger.OpenSearchLoggerBaseConfig{
								FlushInterval:    time.Hour,
								MaxBulkDocument:  1,
								BufferFlushBytes: testOpenSearchBufferFlushBytes,
							},
							OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
								Index:    "logs",
								Resource: activationResource(),
							},
						},
						OpenSearchClient: client,
					},
					pkgcloudwatchlogs.StreamRoutingNone,
					nil,
					nil,
					tracer,
					meter,
				)
				return err
			},
			wantSource: pkglogger.LoggerContractAsyncBestEffort.DisplayName(),
			wantField:  pkglogger.ConfigFieldMaxBulkRequestBytes,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.construct()
			requireConstructorConfigFieldError(t, err, wantConfigFieldError{
				source:  tt.wantSource,
				backend: pkglogger.LogOutputOpenSearch,
				field:   tt.wantField,
				value:   0,
				kind:    pkglogger.ErrNonPositiveSetting,
			})
		})
	}
}

// wantConfigFieldErrorは、constructorが返すpkg/logger.ConfigFieldErrorの期待値です。
type wantConfigFieldError struct {
	source  string
	backend pkglogger.LogOutputDestination
	field   pkglogger.ConfigField
	value   any
	limit   any
	kind    error
}

// requireConstructorConfigFieldErrorは、errが期待した種類・出所・backend・項目・値の
// pkg/logger.ConfigFieldErrorを含むことを確認します。
func requireConstructorConfigFieldError(t *testing.T, err error, want wantConfigFieldError) {
	t.Helper()
	if !errors.Is(err, want.kind) {
		t.Fatalf("constructor error = %v, want %v", err, want.kind)
	}
	for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
		if errors.Is(fieldErr, want.kind) &&
			fieldErr.Source == want.source &&
			fieldErr.Backend == want.backend &&
			fieldErr.Field == want.field &&
			fieldErr.Value == want.value &&
			fieldErr.Limit == want.limit {
			return
		}
	}
	t.Fatalf("constructor error = %v, want config field error %+v", err, want)
}

func TestDeliveryLoggerDirectOpenSearchDoesNotRequireBulkByteLimits(t *testing.T) {
	created, err := NewDeliveryLogger(
		context.Background(),
		pkglogger.DeliveryLoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
			Timeout:               time.Second,
			MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
			OpenSearch: &pkglogger.DeliveryOpenSearchConfig{
				DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
					Index: "logs",
					FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
						DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
					},
					Resource: activationResource(),
				},
			},
			OpenSearchClient: validActivationOpenSearchClient(),
		},
		pkgcloudwatchlogs.StreamRoutingNone,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("NewDeliveryLogger() error = %v, want nil", err)
	}
	closeable, ok := created.(interface{ Close() error })
	if !ok {
		t.Fatalf("NewDeliveryLogger() type = %T, want Close() error", created)
	}
	if err := closeable.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestLoggerConstructorsRejectInvalidBackendValuesBeforeRuntimeBuild(t *testing.T) {
	tests := []struct {
		name      string
		construct func() error
		check     func(t *testing.T, err error)
	}{
		{
			name: "sync active slack",
			construct: func() error {
				_, err := NewLogger(
					context.Background(),
					pkglogger.LoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputSlack,
						},
						Timeout:          time.Second,
						MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
						Slack:            &pkglogger.SlackLoggerConfig{Channel: "logs"},
					}, pkgcloudwatchlogs.StreamRoutingNone, nil,

					nil,
					observer.NopTracer{},
					observer.NopMeter{})

				return err
			},
			check: func(t *testing.T, err error) {
				requireConstructorConfigFieldError(t, err, wantConfigFieldError{
					source:  pkglogger.LoggerContractSync.DisplayName(),
					backend: pkglogger.LogOutputSlack,
					field:   pkglogger.ConfigFieldClient,
					kind:    pkglogger.ErrInvalidClientConfig,
				})
				if !errors.Is(err, pkgslack.ErrTokenRequired) {
					t.Fatalf("constructor error = %v, want %v", err, pkgslack.ErrTokenRequired)
				}
			},
		},
		{
			name: "async handler-only opensearch",
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
						Stdout: &pkglogger.StdoutLoggerConfig{
							HandlerConfig: pkgloggerhandler.HandlerConfig{
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
						OpenSearchClient: &pkgopensearch.Config{},
					}, pkgcloudwatchlogs.StreamRoutingNone, nil,

					nil,
					observer.NopTracer{},
					observer.NopMeter{})

				return err
			},
			check: func(t *testing.T, err error) {
				requireConstructorConfigFieldError(t, err, wantConfigFieldError{
					source:  pkglogger.LoggerContractAsyncBestEffort.DisplayName(),
					backend: pkglogger.LogOutputOpenSearch,
					field:   pkglogger.ConfigFieldClient,
					kind:    pkglogger.ErrInvalidClientConfig,
				})
				if !errors.Is(err, pkgopensearch.ErrEndpointRequired) {
					t.Fatalf("constructor error = %v, want %v", err, pkgopensearch.ErrEndpointRequired)
				}
			},
		},
		{
			name: "delivery internal log destination clickhouse",
			construct: func() error {
				_, err := NewDeliveryLogger(
					context.Background(),
					pkglogger.DeliveryLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputStdout,
						},
						Timeout:          time.Second,
						MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
						Stdout: &pkglogger.DeliveryStdoutLoggerConfig{
							HandlerConfig: pkgloggerhandler.HandlerConfig{
								OnErrorConfig: []string{pkglogger.LogOutputClickHouse.String()},
							},
						},
						InternalLogDestinations: &pkglogger.DeliveryInternalLogDestinationConfig{
							ClickHouse:       &pkglogger.DeliveryClickHouseConfig{},
							ClickHouseClient: &pkgclickhouse.Config{},
						},
					},
					pkgcloudwatchlogs.StreamRoutingNone,
					observer.NopTracer{},
					observer.NopMeter{},
				)
				return err
			},
			check: func(t *testing.T, err error) {
				requireConstructorConfigFieldError(t, err, wantConfigFieldError{
					source:  pkglogger.LoggerContractDelivery.DisplayName() + " internal log destination",
					backend: pkglogger.LogOutputClickHouse,
					field:   pkglogger.ConfigFieldClient,
					kind:    pkglogger.ErrInvalidClientConfig,
				})
			},
		},
		{
			name: "sync cloudwatch destination mismatch",
			construct: func() error {
				_, err := NewLogger(
					context.Background(),
					pkglogger.LoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputCloudWatchLogs,
						},
						Timeout:          time.Second,
						MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
						CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
							CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{
								LogGroupName:  "logger-group",
								LogStreamName: "logger-stream",
							},
						},
						CloudWatchLogsClient: &pkgcloudwatchlogs.Config{
							Region:          "ap-northeast-1",
							LogGroupName:    "client-group",
							AccessKeyID:     "test-access-key",
							SecretAccessKey: "test-secret-key",
						},
					}, pkgcloudwatchlogs.StreamRoutingNone, nil,

					nil,
					observer.NopTracer{},
					observer.NopMeter{})

				return err
			},
			check: func(t *testing.T, err error) {
				requireConstructorConfigFieldError(t, err, wantConfigFieldError{
					source:  "logger",
					backend: pkglogger.LogOutputCloudWatchLogs,
					field:   pkglogger.ConfigFieldLogGroupName,
					value:   "logger-group",
					limit:   "client-group",
					kind:    pkglogger.ErrLogGroupMismatch,
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, tt.construct())
		})
	}
}

func TestLoggerConstructorsRejectMissingExplicitBackendConfig(t *testing.T) {
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	tests := []struct {
		name       string
		construct  func() error
		wantSource string
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
					}, pkgcloudwatchlogs.StreamRoutingNone, nil,

					nil,
					tracer,
					meter)

				return err
			},
			wantSource: "logger",
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
					}, pkgcloudwatchlogs.StreamRoutingNone, nil,

					nil,
					tracer,
					meter)

				return err
			},
			wantSource: "async logger",
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
					},
					pkgcloudwatchlogs.StreamRoutingNone,
					tracer,
					meter,
				)
				return err
			},
			wantSource: "delivery logger",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.construct()
			requireConstructorConfigFieldError(t, err, wantConfigFieldError{
				source:  tt.wantSource,
				backend: pkglogger.LogOutputStdout,
				field:   pkglogger.ConfigFieldBackendConfig,
				kind:    pkglogger.ErrBackendConfigRequired,
			})
		})
	}
}
