package mappedvalidation

import (
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestMappedUsageCacheTTLValidationCoversActiveFileLifecycleConfigs(t *testing.T) {
	tests := []struct {
		name       string
		validate   func() error
		wantSource string
	}{
		{
			name: "sync file",
			validate: func() error {
				return ValidateSyncLoggerConfig("logger", pkglogger.LoggerConfig{
					File: &pkglogger.FileLoggerConfig{
						LifecycleMode: pkgfsstore.FileLifecycleModeInline,
					},
				})
			},
			wantSource: "logger file",
		},
		{
			name: "async file",
			validate: func() error {
				return ValidateSyncLoggerConfig("async logger", pkglogger.LoggerConfig{
					File: &pkglogger.FileLoggerConfig{
						LifecycleMode: pkgfsstore.FileLifecycleModeInline,
					},
				})
			},
			wantSource: "async logger file",
		},
		{
			name: "sync file runner",
			validate: func() error {
				return ValidateSyncLoggerConfig("logger", pkglogger.LoggerConfig{
					File: &pkglogger.FileLoggerConfig{
						LifecycleMode: pkgfsstore.FileLifecycleModeRunner,
						LifecycleRunnerSettings: pkgfsstore.AppendLogRunnerConfig{
							Interval: time.Minute,
						},
					},
				})
			},
			wantSource: "logger file",
		},
		{
			name: "delivery file runner",
			validate: func() error {
				return ValidateDeliveryLoggerConfig(
					"delivery logger",
					pkglogger.DeliveryLoggerConfig{
						File: &pkglogger.DeliveryFileLoggerConfig{
							LifecycleMode: pkgfsstore.FileLifecycleModeRunner,
							LifecycleRunnerSettings: pkglogger.DeliveryFileLifecycleRunnerSettings{
								RunnerConfig: pkgfsstore.AppendLogRunnerConfig{Interval: time.Minute},
							},
						},
					},
				)
			},
			wantSource: "delivery logger file",
		},
		{
			name: "sync internal log file",
			validate: func() error {
				return validateMappedInternalLogFileConfig(
					"logger",
					&pkglogger.InternalLogDestinationConfig{
						File: &pkglogger.FileLoggerConfig{
							LifecycleMode: pkgfsstore.FileLifecycleModeInline,
						},
					},
				)
			},
			wantSource: "logger internal log file",
		},
		{
			name: "async internal log file",
			validate: func() error {
				return validateMappedInternalLogFileConfig(
					"async logger",
					&pkglogger.InternalLogDestinationConfig{
						File: &pkglogger.FileLoggerConfig{
							LifecycleMode: pkgfsstore.FileLifecycleModeInline,
						},
					},
				)
			},
			wantSource: "async logger internal log file",
		},
		{
			name: "delivery internal log file runner",
			validate: func() error {
				return validateMappedDeliveryInternalLogFileConfig(
					"delivery logger",
					&pkglogger.DeliveryInternalLogDestinationConfig{
						File: &pkglogger.DeliveryFileLoggerConfig{
							LifecycleMode: pkgfsstore.FileLifecycleModeRunner,
							LifecycleRunnerSettings: pkglogger.DeliveryFileLifecycleRunnerSettings{
								RunnerConfig: pkgfsstore.AppendLogRunnerConfig{Interval: time.Minute},
							},
						},
					},
				)
			},
			wantSource: "delivery logger internal log file",
		},
		{
			name: "sync failure archive",
			validate: func() error {
				cfg := failureArchiveLifecycleValidationLoggerConfig(
					pkgfsstore.FileLifecycleModeInline,
					validArchiveCheckpointUploadTargetConfig(),
				)
				cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.
					DurableStoreConfig.FileArchiveConfig.UsageCacheTTL = 0
				return validateMappedFailureStores(
					"logger",
					cfg,
				)
			},
			wantSource: "logger opensearch failure file archive",
		},
		{
			name: "sync failure archive runner",
			validate: func() error {
				cfg := failureArchiveLifecycleValidationLoggerConfig(
					pkgfsstore.FileLifecycleModeRunner,
					validArchiveCheckpointUploadTargetConfig(),
				)
				cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.
					DurableStoreConfig.FileArchiveConfig.UsageCacheTTL = 0
				return validateMappedFailureStores("logger", cfg)
			},
			wantSource: "logger opensearch failure file archive",
		},
		{
			name: "delivery failure archive",
			validate: func() error {
				cfg := failureArchiveLifecycleValidationDeliveryLoggerConfig(
					pkgfsstore.FileLifecycleModeInline,
					validArchiveCheckpointUploadTargetConfig(),
				)
				cfg.OpenSearch.DeliveryOpenSearchFlushManagerConfig.FailureStoreConfig.
					FileArchiveConfig.UsageCacheTTL = 0
				return validateMappedDeliveryFailureStores(
					"delivery logger",
					cfg,
				)
			},
			wantSource: "delivery logger opensearch failure file archive",
		},
		{
			name: "delivery failure archive runner",
			validate: func() error {
				cfg := failureArchiveLifecycleValidationDeliveryLoggerConfig(
					pkgfsstore.FileLifecycleModeRunner,
					validArchiveCheckpointUploadTargetConfig(),
				)
				cfg.OpenSearch.DeliveryOpenSearchFlushManagerConfig.FailureStoreConfig.
					FileArchiveConfig.UsageCacheTTL = 0
				return validateMappedDeliveryFailureStores("delivery logger", cfg)
			},
			wantSource: "delivery logger opensearch failure file archive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.validate()
			// 各caseで対象のTTLは0（未設定）なので、設定元と種類で拒否したTTLを特定できます。
			requireConfigError(t, err, tt.wantSource, ConfigSectionLifecycle, pkgfsstore.ErrInvalidUsageCacheTTL)
		})
	}
}

func TestMappedUsageCacheTTLValidationIgnoresUnusedSettings(t *testing.T) {
	t.Run("sync file lifecycle disabled", func(t *testing.T) {
		err := ValidateSyncLoggerConfig(
			"logger",
			pkglogger.LoggerConfig{
				File: &pkglogger.FileLoggerConfig{
					LifecycleMode: pkgfsstore.FileLifecycleModeDisable,
					FsstoreConfig: pkgfsstore.Config{
						Layout: pkgfsstore.FsstoreConfig{LogDir: "."},
						Lifecycle: pkgfsstore.FileLifecycleConfig{
							UsageCacheTTL: -time.Second,
						},
					},
					ArchiveCheckpointConfig: validArchiveCheckpointConfig(),
				},
			},
		)
		if err != nil {
			t.Fatalf("disabled sync file lifecycle validation error = %v, want nil", err)
		}
	})

	t.Run("delivery file without runner", func(t *testing.T) {
		err := ValidateDeliveryLoggerConfig(
			"delivery logger",
			pkglogger.DeliveryLoggerConfig{
				File: &pkglogger.DeliveryFileLoggerConfig{
					LifecycleMode: pkgfsstore.FileLifecycleModeDisable,
					FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{Layout: pkgfsstore.FsstoreConfig{LogDir: "."}},
					LifecycleRunnerSettings: pkglogger.DeliveryFileLifecycleRunnerSettings{
						ArchiveCheckpointConfig: validArchiveCheckpointConfig(),
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("delivery file without runner validation error = %v, want nil", err)
		}
	})

	t.Run("sync internal log file lifecycle disabled", func(t *testing.T) {
		err := validateMappedInternalLogFileConfig(
			"logger",
			&pkglogger.InternalLogDestinationConfig{
				File: &pkglogger.FileLoggerConfig{
					LifecycleMode: pkgfsstore.FileLifecycleModeDisable,
					FsstoreConfig: pkgfsstore.Config{
						Layout: pkgfsstore.FsstoreConfig{LogDir: "."},
						Lifecycle: pkgfsstore.FileLifecycleConfig{
							UsageCacheTTL: -time.Second,
						},
					},
					ArchiveCheckpointConfig: validArchiveCheckpointConfig(),
				},
			},
		)
		if err != nil {
			t.Fatalf("disabled sync internal file lifecycle validation error = %v, want nil", err)
		}
	})

	t.Run("selected sync failure archive lifecycle disabled", func(t *testing.T) {
		cfg := failureArchiveLifecycleValidationLoggerConfig(
			pkgfsstore.FileLifecycleModeDisable,
			validArchiveCheckpointUploadTargetConfig(),
		)
		cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.
			DurableStoreConfig.FileArchiveConfig.UsageCacheTTL = -time.Second

		if err := validateMappedFailureStores("logger", cfg); err != nil {
			t.Fatalf("disabled sync failure archive lifecycle validation error = %v, want nil", err)
		}
	})

	t.Run("selected delivery failure archive lifecycle disabled", func(t *testing.T) {
		cfg := failureArchiveLifecycleValidationDeliveryLoggerConfig(
			pkgfsstore.FileLifecycleModeDisable,
			validArchiveCheckpointUploadTargetConfig(),
		)
		cfg.OpenSearch.DeliveryOpenSearchFlushManagerConfig.FailureStoreConfig.
			FileArchiveConfig.UsageCacheTTL = -time.Second

		if err := validateMappedDeliveryFailureStores("delivery logger", cfg); err != nil {
			t.Fatalf("disabled delivery failure archive lifecycle validation error = %v, want nil", err)
		}
	})

	t.Run("sync failure archive not selected", func(t *testing.T) {
		err := validateMappedFailureStores("logger", pkglogger.LoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
			OpenSearch: &pkglogger.OpenSearchConfig{
				OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
					FailureStoreConfig: durable.FailureStoreConfig{
						DurableType: []durable.DurableType{durable.DurableTypeNone},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("unselected sync failure archive validation error = %v, want nil", err)
		}
	})

	t.Run("delivery failure archive not selected", func(t *testing.T) {
		err := validateMappedDeliveryFailureStores(
			"delivery logger",
			pkglogger.DeliveryLoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
				OpenSearch: &pkglogger.DeliveryOpenSearchConfig{
					DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
						FailureStoreConfig: durable.DeliveryFailureStoreConfig{
							DurableType: []durable.DurableType{durable.DurableTypeNone},
						},
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("unselected delivery failure archive validation error = %v, want nil", err)
		}
	})
}
