package mappedvalidation

import (
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestMappedFailureArchiveLifecycleModeValidation(t *testing.T) {
	tests := []struct {
		name string
		mode pkgfsstore.FileLifecycleMode
	}{
		{name: "disable", mode: pkgfsstore.FileLifecycleModeDisable},
		{name: "inline", mode: pkgfsstore.FileLifecycleModeInline},
		{name: "runner", mode: pkgfsstore.FileLifecycleModeRunner},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateMappedFailureStores(
				"logger",
				failureArchiveLifecycleValidationLoggerConfig(tt.mode, objectstorage.UploadTargetConfig{}),
			); err != nil {
				t.Fatalf("validateMappedFailureStores() error = %v, want nil", err)
			}
		})
	}

	err := validateMappedFailureStores(
		"logger",
		failureArchiveLifecycleValidationLoggerConfig(
			pkgfsstore.FileLifecycleMode("unknown"),
			objectstorage.UploadTargetConfig{},
		),
	)
	requireConfigError(
		t,
		err,
		"logger opensearch failure file archive",
		ConfigSectionLifecycle,
		pkgfsstore.ErrUnsupportedFileLifecycleMode,
	)

	deliveryErr := validateMappedDeliveryFailureStores(
		"delivery logger",
		failureArchiveLifecycleValidationDeliveryLoggerConfig(
			pkgfsstore.FileLifecycleMode("unknown"),
			objectstorage.UploadTargetConfig{},
		),
	)
	// Deliveryの送信失敗レコード用file archiveは、Delivery file loggerではなく通常のmode検証を使います。
	requireConfigError(
		t,
		deliveryErr,
		"delivery logger opensearch failure file archive",
		ConfigSectionLifecycle,
		pkgfsstore.ErrUnsupportedFileLifecycleMode,
	)
}

func TestDisabledFailureArchiveLifecycleIgnoresUnusedUploadConfig(t *testing.T) {
	invalidUpload := objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
	}

	if err := validateMappedFailureStores(
		"logger",
		failureArchiveLifecycleValidationLoggerConfig(
			pkgfsstore.FileLifecycleModeDisable,
			invalidUpload,
		),
	); err != nil {
		t.Fatalf("disabled failure archive lifecycle validation error = %v, want nil", err)
	}

	if err := validateMappedDeliveryFailureStores(
		"delivery logger",
		failureArchiveLifecycleValidationDeliveryLoggerConfig(
			pkgfsstore.FileLifecycleModeDisable,
			invalidUpload,
		),
	); err != nil {
		t.Fatalf("disabled delivery failure archive lifecycle validation error = %v, want nil", err)
	}
}

func failureArchiveLifecycleValidationLoggerConfig(
	mode pkgfsstore.FileLifecycleMode,
	upload objectstorage.UploadTargetConfig,
) pkglogger.LoggerConfig {
	return pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				FailureStoreConfig: durable.FailureStoreConfig{
					MaxConcurrent: 1,
					DurableType:   []durable.DurableType{durable.DurableTypeFileArchive},
					DurableStoreConfig: durable.DurableStoreConfig{
						FileArchiveConfig: durable.FileArchiveConfig{
							LifecycleMode:             mode,
							UsageCacheTTL:             time.Minute,
							LifecycleRunnerConfig:     pkgfsstore.AppendLogRunnerConfig{Interval: time.Minute},
							ObjectStorageUploadConfig: upload,
							ArchiveCheckpointConfig: pkgfsstore.ArchiveCheckpointConfig{
								Directory:    "/var/lib/heterolog/archive-checkpoints",
								SourcePrefix: "failure_archive_lifecycle_validation",
							},
						},
					},
				},
			},
		},
	}
}

func failureArchiveLifecycleValidationDeliveryLoggerConfig(
	mode pkgfsstore.FileLifecycleMode,
	upload objectstorage.UploadTargetConfig,
) pkglogger.DeliveryLoggerConfig {
	return pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
		OpenSearch: &pkglogger.DeliveryOpenSearchConfig{
			DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
				FailureStoreConfig: durable.DeliveryFailureStoreConfig{
					DurableType: []durable.DurableType{durable.DurableTypeFileArchive},
					FileArchiveConfig: durable.DeliveryFileArchiveConfig{
						LifecycleMode:             mode,
						UsageCacheTTL:             time.Minute,
						LifecycleRunnerConfig:     pkgfsstore.AppendLogRunnerConfig{Interval: time.Minute},
						ObjectStorageUploadConfig: upload,
						ArchiveCheckpointConfig: pkgfsstore.ArchiveCheckpointConfig{
							Directory:    "/var/lib/heterolog/archive-checkpoints",
							SourcePrefix: "delivery_failure_archive_lifecycle_validation",
						},
					},
				},
			},
		},
	}
}
