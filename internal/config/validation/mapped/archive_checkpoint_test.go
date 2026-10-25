package mappedvalidation

import (
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestMappedFileArchiveCheckpointValidationCoversEveryLoggerContract(t *testing.T) {
	upload := validArchiveCheckpointLogFileUploadConfig()

	syncConfig := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		File: &pkglogger.FileLoggerConfig{
			LifecycleMode:             pkgfsstore.FileLifecycleModeRunner,
			LifecycleRunnerSettings:   pkgfsstore.AppendLogRunnerConfig{Interval: time.Minute},
			ObjectStorageUploadConfig: upload,
			FsstoreConfig: pkgfsstore.Config{
				Layout:    pkgfsstore.FsstoreConfig{LogDir: "."},
				Lifecycle: pkgfsstore.FileLifecycleConfig{UsageCacheTTL: time.Minute},
			},
		},
	}
	asyncConfig := syncConfig
	deliveryConfig := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		File: &pkglogger.DeliveryFileLoggerConfig{
			LifecycleMode: pkgfsstore.FileLifecycleModeRunner,
			FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{Layout: pkgfsstore.FsstoreConfig{LogDir: "."}},
			LifecycleRunnerSettings: pkglogger.DeliveryFileLifecycleRunnerSettings{
				UsageCacheTTL:             time.Minute,
				RunnerConfig:              pkgfsstore.AppendLogRunnerConfig{Interval: time.Minute},
				ObjectStorageUploadConfig: upload,
			},
		},
	}

	tests := []struct {
		name       string
		wantSource string
		validate   func() error
	}{
		{
			name:       "sync",
			wantSource: "logger file",
			validate: func() error {
				return ValidateSyncLoggerConfig("logger", syncConfig)
			},
		},
		{
			name:       "async",
			wantSource: "async logger file",
			validate: func() error {
				return ValidateSyncLoggerConfig("async logger", asyncConfig)
			},
		},
		{
			name:       "delivery",
			wantSource: "delivery logger file",
			validate: func() error {
				return ValidateDeliveryLoggerConfig("delivery logger", deliveryConfig)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertArchiveCheckpointRequiredError(t, test.validate(), test.wantSource)
		})
	}

	checkpoint := validArchiveCheckpointConfig()
	syncConfig.File.ArchiveCheckpointConfig = checkpoint
	if err := ValidateSyncLoggerConfig("logger", syncConfig); err != nil {
		t.Fatalf("complete sync archive checkpoint config error = %v, want nil", err)
	}
	deliveryConfig.File.LifecycleRunnerSettings.ArchiveCheckpointConfig = checkpoint
	if err := ValidateDeliveryLoggerConfig("delivery logger", deliveryConfig); err != nil {
		t.Fatalf("complete delivery archive checkpoint config error = %v, want nil", err)
	}
}

func TestMappedArchiveCheckpointValidationAppliesToEveryFileLifecycleMode(t *testing.T) {
	tests := []struct {
		name string
		mode pkgfsstore.FileLifecycleMode
	}{
		{
			name: "inline",
			mode: pkgfsstore.FileLifecycleModeInline,
		},
		{
			name: "runner without upload",
			mode: pkgfsstore.FileLifecycleModeRunner,
		},
		{
			name: "disable",
			mode: pkgfsstore.FileLifecycleModeDisable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := pkglogger.LoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
				File: &pkglogger.FileLoggerConfig{
					LifecycleMode: test.mode,
				},
			}
			assertArchiveCheckpointRequiredError(
				t,
				ValidateSyncLoggerConfig("logger", cfg),
				"logger file",
			)
		})
	}
}

func TestMappedFailureArchiveCheckpointValidationRequiresSelectedArchiveState(t *testing.T) {
	upload := validArchiveCheckpointUploadTargetConfig()

	syncConfig := failureArchiveLifecycleValidationLoggerConfig(
		pkgfsstore.FileLifecycleModeRunner,
		upload,
	)
	syncConfig.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.
		DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig =
		pkgfsstore.ArchiveCheckpointConfig{}
	assertArchiveCheckpointRequiredError(
		t,
		validateMappedFailureStores("logger", syncConfig),
		"logger opensearch failure file archive",
	)
	syncConfig.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.
		DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig =
		validArchiveCheckpointConfig()
	if err := validateMappedFailureStores("logger", syncConfig); err != nil {
		t.Fatalf("complete sync failure archive checkpoint config error = %v, want nil", err)
	}

	deliveryConfig := failureArchiveLifecycleValidationDeliveryLoggerConfig(
		pkgfsstore.FileLifecycleModeRunner,
		upload,
	)
	deliveryConfig.OpenSearch.DeliveryOpenSearchFlushManagerConfig.FailureStoreConfig.
		FileArchiveConfig.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{}
	assertArchiveCheckpointRequiredError(
		t,
		validateMappedDeliveryFailureStores("delivery logger", deliveryConfig),
		"delivery logger opensearch failure file archive",
	)
	deliveryConfig.OpenSearch.DeliveryOpenSearchFlushManagerConfig.FailureStoreConfig.
		FileArchiveConfig.ArchiveCheckpointConfig = validArchiveCheckpointConfig()
	if err := validateMappedDeliveryFailureStores("delivery logger", deliveryConfig); err != nil {
		t.Fatalf("complete delivery failure archive checkpoint config error = %v, want nil", err)
	}

	// handler-only candidateのfailure storeはruntimeで構築しないため、checkpointも不要です。
	syncConfig.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
	if err := validateMappedFailureStores("logger", syncConfig); err != nil {
		t.Fatalf("unselected failure archive checkpoint validation error = %v, want nil", err)
	}
}

func TestMappedInternalLogFileArchiveCheckpointValidation(t *testing.T) {
	destinations := &pkglogger.InternalLogDestinationConfig{
		File: &pkglogger.FileLoggerConfig{
			LifecycleMode:             pkgfsstore.FileLifecycleModeRunner,
			ObjectStorageUploadConfig: validArchiveCheckpointLogFileUploadConfig(),
			FsstoreConfig: pkgfsstore.Config{
				Lifecycle: pkgfsstore.FileLifecycleConfig{UsageCacheTTL: time.Minute},
			},
		},
	}
	assertArchiveCheckpointRequiredError(
		t,
		validateMappedInternalLogFileConfig("logger", destinations),
		"logger internal log file",
	)
}

func validArchiveCheckpointLogFileUploadConfig() objectstorage.LogFileUploadConfig {
	return objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: validArchiveCheckpointUploadTargetConfig(),
	}
}

func validArchiveCheckpointConfig() pkgfsstore.ArchiveCheckpointConfig {
	return pkgfsstore.ArchiveCheckpointConfig{
		Directory:    "/var/lib/heterolog/archive-checkpoints",
		SourcePrefix: "logger_file",
	}
}

func validArchiveCheckpointUploadTargetConfig() objectstorage.UploadTargetConfig {
	return objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{
			Region:          "ap-northeast-1",
			AccessKeyID:     "access-key",
			SecretAccessKey: "secret-key",
			Bucket:          "archive-bucket",
		},
	}
}

// assertArchiveCheckpointRequiredErrorは、errがsourceのarchive checkpoint設定について、
// 保存directoryとSourcePrefixの両方の不足を報告していることを確認します。
func assertArchiveCheckpointRequiredError(t *testing.T, err error, source string) {
	t.Helper()
	if err == nil {
		t.Fatal("archive checkpoint validation error = nil, want error")
	}
	for _, want := range []error{
		ErrArchiveCheckpointDirectoryRequired,
		ErrArchiveCheckpointSourcePrefixRequired,
	} {
		if _, ok := errortree.Find(err, func(checkpointErr *ArchiveCheckpointConfigError) bool {
			return checkpointErr.Source == source && errors.Is(checkpointErr, want)
		}); !ok {
			t.Fatalf("archive checkpoint validation error = %v, want %s %v", err, source, want)
		}
	}
}
