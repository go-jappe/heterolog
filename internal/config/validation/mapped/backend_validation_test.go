package mappedvalidation

import (
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestMappedLoggerConfigsValidateRetryConfigForEveryBackend(t *testing.T) {
	tests := []struct {
		name        string
		validate    func() error
		wantSource  string
		wantBackend pkglogger.LogOutputDestination
	}{
		{
			name: "sync opensearch",
			validate: func() error {
				return ValidateSyncLoggerConfig("logger", pkglogger.LoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{
						pkglogger.LogOutputOpenSearch,
					},
					OpenSearch: &pkglogger.OpenSearchConfig{
						OpenSearchRetryableClientCaseTransportErrorConfig: invalidOpenSearchRetryConfig(),
					},
				})
			},
			wantSource:  "logger",
			wantBackend: pkglogger.LogOutputOpenSearch,
		},
		{
			name: "sync loki",
			validate: func() error {
				return ValidateSyncLoggerConfig("logger", pkglogger.LoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{
						pkglogger.LogOutputLoki,
					},
					Loki: &pkglogger.LokiConfig{
						LokiRetryableClientCaseTransportErrorConfig: invalidLokiRetryConfig(),
					},
				})
			},
			wantSource:  "logger",
			wantBackend: pkglogger.LogOutputLoki,
		},
		{
			name: "sync cloudwatch logs",
			validate: func() error {
				return ValidateSyncLoggerConfig("logger", pkglogger.LoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{
						pkglogger.LogOutputCloudWatchLogs,
					},
					CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
						CloudWatchLogsRetryableClientCaseTransportErrorConfig: invalidCloudWatchLogsRetryConfig(),
					},
				})
			},
			wantSource:  "logger",
			wantBackend: pkglogger.LogOutputCloudWatchLogs,
		},
		{
			name: "sync clickhouse",
			validate: func() error {
				return ValidateSyncLoggerConfig("logger", pkglogger.LoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{
						pkglogger.LogOutputClickHouse,
					},
					ClickHouse: &pkglogger.ClickHouseConfig{
						ClickHouseRetryableClientCaseTransportErrorConfig: invalidClickHouseRetryConfig(),
					},
				})
			},
			wantSource:  "logger",
			wantBackend: pkglogger.LogOutputClickHouse,
		},
		{
			name: "async opensearch",
			validate: func() error {
				return ValidateSyncLoggerConfig(
					"async logger",
					pkglogger.AsyncBestEffortMultiLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputOpenSearch,
						},
						OpenSearch: &pkglogger.OpenSearchConfig{
							OpenSearchRetryableClientCaseTransportErrorConfig: invalidOpenSearchRetryConfig(),
						},
					}.ToLoggerConfig(),
				)
			},
			wantSource:  "async logger",
			wantBackend: pkglogger.LogOutputOpenSearch,
		},
		{
			name: "async loki",
			validate: func() error {
				return ValidateSyncLoggerConfig(
					"async logger",
					pkglogger.AsyncBestEffortMultiLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputLoki,
						},
						Loki: &pkglogger.LokiConfig{
							LokiRetryableClientCaseTransportErrorConfig: invalidLokiRetryConfig(),
						},
					}.ToLoggerConfig(),
				)
			},
			wantSource:  "async logger",
			wantBackend: pkglogger.LogOutputLoki,
		},
		{
			name: "async cloudwatch logs",
			validate: func() error {
				return ValidateSyncLoggerConfig(
					"async logger",
					pkglogger.AsyncBestEffortMultiLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputCloudWatchLogs,
						},
						CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
							CloudWatchLogsRetryableClientCaseTransportErrorConfig: invalidCloudWatchLogsRetryConfig(),
						},
					}.ToLoggerConfig(),
				)
			},
			wantSource:  "async logger",
			wantBackend: pkglogger.LogOutputCloudWatchLogs,
		},
		{
			name: "async clickhouse",
			validate: func() error {
				return ValidateSyncLoggerConfig(
					"async logger",
					pkglogger.AsyncBestEffortMultiLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputClickHouse,
						},
						ClickHouse: &pkglogger.ClickHouseConfig{
							ClickHouseRetryableClientCaseTransportErrorConfig: invalidClickHouseRetryConfig(),
						},
					}.ToLoggerConfig(),
				)
			},
			wantSource:  "async logger",
			wantBackend: pkglogger.LogOutputClickHouse,
		},
		{
			name: "delivery opensearch",
			validate: func() error {
				return ValidateDeliveryLoggerConfig(
					"delivery logger",
					pkglogger.DeliveryLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputOpenSearch,
						},
						OpenSearch: &pkglogger.DeliveryOpenSearchConfig{
							OpenSearchRetryableClientCaseTransportErrorConfig: invalidOpenSearchRetryConfig(),
						},
					},
				)
			},
			wantSource:  "delivery logger",
			wantBackend: pkglogger.LogOutputOpenSearch,
		},
		{
			name: "delivery loki",
			validate: func() error {
				return ValidateDeliveryLoggerConfig(
					"delivery logger",
					pkglogger.DeliveryLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputLoki,
						},
						Loki: &pkglogger.DeliveryLokiConfig{
							LokiRetryableClientCaseTransportErrorConfig: invalidLokiRetryConfig(),
						},
					},
				)
			},
			wantSource:  "delivery logger",
			wantBackend: pkglogger.LogOutputLoki,
		},
		{
			name: "delivery cloudwatch logs",
			validate: func() error {
				return ValidateDeliveryLoggerConfig(
					"delivery logger",
					pkglogger.DeliveryLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputCloudWatchLogs,
						},
						CloudWatchLogs: &pkglogger.DeliveryCloudWatchLogsConfig{
							CloudWatchLogsRetryableClientCaseTransportErrorConfig: invalidCloudWatchLogsRetryConfig(),
						},
					},
				)
			},
			wantSource:  "delivery logger",
			wantBackend: pkglogger.LogOutputCloudWatchLogs,
		},
		{
			// Delivery ClickHouseはtimeout hierarchyには含めないが、client
			// wrapperの設定は起動時に検証しなければならない。
			name: "delivery clickhouse",
			validate: func() error {
				return ValidateDeliveryLoggerConfig(
					"delivery logger",
					pkglogger.DeliveryLoggerConfig{
						LogOutputDestinations: pkglogger.LogOutputDestinations{
							pkglogger.LogOutputClickHouse,
						},
						ClickHouse: &pkglogger.DeliveryClickHouseConfig{
							ClickHouseRetryableClientCaseTransportErrorConfig: invalidClickHouseRetryConfig(),
						},
					},
				)
			},
			wantSource:  "delivery logger",
			wantBackend: pkglogger.LogOutputClickHouse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.validate()
			if err == nil {
				t.Fatal("mapped logger retry config validation error = nil, want error")
			}
			fieldErr := requireConfigFieldError(
				t,
				err,
				tt.wantSource,
				tt.wantBackend,
				pkglogger.ConfigFieldRetryMaxRetries,
				pkglogger.ErrNegativeSetting,
			)
			if fieldErr.Value != -1 {
				t.Fatalf("retry max retries error value = %v, want -1", fieldErr.Value)
			}
		})
	}
}

// handler-only backendはfallback candidateとしてlogger本体とclientだけを使い、
// そのbackend自身のfailure storeは構築しない。したがって、未使用failure storeの
// object storage設定は検証せず、通常出力先として選択した場合だけ検証する。
func TestMappedFailureStoreValidationRequiresNormalDestination(t *testing.T) {
	invalidUpload := objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
	}
	syncFailure := durable.FailureStoreConfig{
		DurableType: []durable.DurableType{durable.DurableTypeFileArchive},
		DurableStoreConfig: durable.DurableStoreConfig{
			FileArchiveConfig: durable.FileArchiveConfig{
				ObjectStorageUploadConfig: invalidUpload,
			},
		},
	}
	deliveryFailure := durable.DeliveryFailureStoreConfig{
		DurableType: []durable.DurableType{durable.DurableTypeFileArchive},
		FileArchiveConfig: durable.DeliveryFileArchiveConfig{
			ObjectStorageUploadConfig: invalidUpload,
		},
	}

	syncCfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				FailureStoreConfig: syncFailure,
			},
		},
	}
	if err := validateMappedFailureStores("logger", syncCfg); err != nil {
		t.Fatalf("handler-only sync failure store validation error = %v, want nil", err)
	}
	syncCfg.LogOutputDestinations = append(
		syncCfg.LogOutputDestinations,
		pkglogger.LogOutputOpenSearch,
	)
	if err := validateMappedFailureStores("logger", syncCfg); err == nil {
		t.Fatal("normal sync destination failure store validation error = nil, want error")
	}

	deliveryCfg := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		OpenSearch: &pkglogger.DeliveryOpenSearchConfig{
			DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
				FailureStoreConfig: deliveryFailure,
			},
		},
	}
	if err := validateMappedDeliveryFailureStores("delivery logger", deliveryCfg); err != nil {
		t.Fatalf("handler-only delivery failure store validation error = %v, want nil", err)
	}
	deliveryCfg.LogOutputDestinations = append(
		deliveryCfg.LogOutputDestinations,
		pkglogger.LogOutputOpenSearch,
	)
	if err := validateMappedDeliveryFailureStores("delivery logger", deliveryCfg); err == nil {
		t.Fatal("normal delivery destination failure store validation error = nil, want error")
	}
}

func invalidOpenSearchRetryConfig() *pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig {
	return &pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicy,
		MaxRetries:      -1,
		BaseDelay:       time.Millisecond,
		MaxDelay:        time.Second,
	}
}

func invalidLokiRetryConfig() *pkglogger.LokiRetryableClientCaseTransportErrorConfig {
	return &pkglogger.LokiRetryableClientCaseTransportErrorConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicy,
		MaxRetries:      -1,
		BaseDelay:       time.Millisecond,
		MaxDelay:        time.Second,
	}
}

func invalidCloudWatchLogsRetryConfig() *pkglogger.CloudWatchLogsRetryableClientCaseTransportErrorConfig {
	return &pkglogger.CloudWatchLogsRetryableClientCaseTransportErrorConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicy,
		MaxRetries:      -1,
		BaseDelay:       time.Millisecond,
		MaxDelay:        time.Second,
	}
}

func invalidClickHouseRetryConfig() *pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig {
	return &pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicy,
		MaxRetries:      -1,
		BaseDelay:       time.Millisecond,
		MaxDelay:        time.Second,
	}
}
