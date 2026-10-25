package mappedvalidation

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestMappedSyncLoggerRejectsBaseAndInternalCompressedLogFileNameCollision(
	t *testing.T,
) {
	base := compressedLogUploadCollisionTestSyncFile("app")
	base.FsstoreConfig.Layout.LogDir = "application"
	internal := compressedLogUploadCollisionTestSyncFile("app")
	internal.FsstoreConfig.Layout.LogDir = "internal"
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		File:                  base,
		InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
			File: internal,
		},
	}

	err := ValidateSyncLoggerConfig("async logger", cfg)
	assertCompressedLogUploadNameCollision(t, err, "async_logger.file.default", "async_logger.internal_log.file.default")
}

func TestMappedSyncLoggerRejectsCrossRoleCompressedLogFileNameCollision(t *testing.T) {
	base := compressedLogUploadCollisionTestSyncFile("app")
	base.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	base.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error_"
	base.ObjectStorageUploadConfig.Default.Enabled = false
	base.ObjectStorageUploadConfig.Error = compressedLogUploadCollisionTestTarget()

	internal := compressedLogUploadCollisionTestSyncFile("error_app")
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		File:                  base,
		InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
			File: internal,
		},
	}

	err := ValidateSyncLoggerConfig("logger", cfg)
	assertCompressedLogUploadNameCollision(t, err, "logger.file.error", "logger.internal_log.file.default")
}

func TestMappedDeliveryLoggerRejectsBaseAndInternalCompressedLogFileNameCollision(
	t *testing.T,
) {
	base := compressedLogUploadCollisionTestDeliveryFile("delivery")
	base.FsstoreConfig.Layout.LogDir = "application"
	internal := compressedLogUploadCollisionTestDeliveryFile("delivery")
	internal.FsstoreConfig.Layout.LogDir = "internal"
	cfg := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		File:                  base,
		InternalLogDestinations: &pkglogger.DeliveryInternalLogDestinationConfig{
			File: internal,
		},
	}

	err := ValidateDeliveryLoggerConfig("delivery logger", cfg)
	assertCompressedLogUploadNameCollision(t, err, "delivery_logger.file.default", "delivery_logger.internal_log.file.default")
}

func TestMappedCompressedLogFileUploadNameCollisionIgnoresInactiveSeries(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*pkglogger.LoggerConfig)
	}{
		{
			name: "base file backend is not active",
			configure: func(cfg *pkglogger.LoggerConfig) {
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
				cfg.Stdout = &pkglogger.StdoutLoggerConfig{}
			},
		},
		{
			name: "internal lifecycle is disabled",
			configure: func(cfg *pkglogger.LoggerConfig) {
				cfg.InternalLogDestinations.File.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
			},
		},
		{
			name: "internal upload is disabled",
			configure: func(cfg *pkglogger.LoggerConfig) {
				cfg.InternalLogDestinations.File.ObjectStorageUploadConfig.Enabled = false
			},
		},
		{
			name: "unseparated error series is not physical",
			configure: func(cfg *pkglogger.LoggerConfig) {
				cfg.File.ObjectStorageUploadConfig.Default.Enabled = false
				cfg.File.ObjectStorageUploadConfig.Error = compressedLogUploadCollisionTestTarget()
				cfg.File.FsstoreConfig.Layout.SeparateErrorLogsPrefix = ""
				cfg.InternalLogDestinations.File.ObjectStorageUploadConfig.Default.Enabled = false
				cfg.InternalLogDestinations.File.ObjectStorageUploadConfig.Error = compressedLogUploadCollisionTestTarget()
				cfg.InternalLogDestinations.File.FsstoreConfig.Layout.SeparateErrorLogsPrefix = ""
			},
		},
		{
			name: "different filename identity",
			configure: func(cfg *pkglogger.LoggerConfig) {
				cfg.InternalLogDestinations.File.FsstoreConfig.Layout.Target = "internal"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := pkglogger.LoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
				File:                  compressedLogUploadCollisionTestSyncFile("app"),
				InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
					File: compressedLogUploadCollisionTestSyncFile("app"),
				},
			}
			tt.configure(&cfg)

			if err := ValidateSyncLoggerConfig("logger", cfg); err != nil {
				t.Fatalf("inactive or distinct series validation error = %v, want nil", err)
			}
		})
	}
}

func TestMappedCompressedLogFileUploadNameCollisionIsScopedByService(t *testing.T) {
	base := []mappedCompressedLogFileUploadSeries{{
		source:    "logger.file.default",
		service:   objectstorage.UploadTargetS3,
		target:    "app",
		extension: mappedAppendLogFileExtension,
	}}
	internal := []mappedCompressedLogFileUploadSeries{{
		source:    "logger.internal_log.file.default",
		service:   objectstorage.UploadTarget("another-service"),
		target:    "app",
		extension: mappedAppendLogFileExtension,
	}}

	if err := validateMappedCompressedLogFileUploadSeriesCollisions(base, internal, pathidentity.DirectoryNameCase); err != nil {
		t.Fatalf("different service collision validation error = %v, want nil", err)
	}
}

func compressedLogUploadCollisionTestSyncFile(target string) *pkglogger.FileLoggerConfig {
	return &pkglogger.FileLoggerConfig{
		LifecycleMode: pkgfsstore.FileLifecycleModeInline,
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:         target,
				LogDir:         ".",
				FileNameFormat: pkgfsstore.FileNameFormat,
				FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL:      time.Minute,
				EnableFileCompress: true,
				CompressFormat:     pkgfsstore.CompressGzip,
			},
		},
		ObjectStorageUploadConfig: compressedLogUploadCollisionTestConfig(),
		ArchiveCheckpointConfig:   validArchiveCheckpointConfig(),
	}
}

func compressedLogUploadCollisionTestDeliveryFile(
	target string,
) *pkglogger.DeliveryFileLoggerConfig {
	return &pkglogger.DeliveryFileLoggerConfig{
		LifecycleMode: pkgfsstore.FileLifecycleModeRunner,
		FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{
			Layout: pkgfsstore.FsstoreConfig{
				Target:         target,
				LogDir:         ".",
				FileNameFormat: pkgfsstore.FileNameFormat,
				FileDateFormat: pkgfsstore.FileDateFormat,
			},
		},
		LifecycleRunnerSettings: pkglogger.DeliveryFileLifecycleRunnerSettings{
			UsageCacheTTL:             time.Minute,
			EnableFileCompress:        true,
			CompressFormat:            pkgfsstore.CompressGzip,
			ObjectStorageUploadConfig: compressedLogUploadCollisionTestConfig(),
			ArchiveCheckpointConfig:   validArchiveCheckpointConfig(),
			RunnerConfig: pkgfsstore.AppendLogRunnerConfig{
				Interval: time.Minute,
			},
		},
	}
}

func compressedLogUploadCollisionTestConfig() objectstorage.LogFileUploadConfig {
	return objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: compressedLogUploadCollisionTestTarget(),
	}
}

func compressedLogUploadCollisionTestTarget() objectstorage.UploadTargetConfig {
	return validArchiveCheckpointUploadTargetConfig()
}

// assertCompressedLogUploadNameCollisionは、errが通常出力baseSourceと内部ログ出力internalSourceの
// s3への圧縮ログ名の衝突を報告していることを確認し、そのerrorを返します。
func assertCompressedLogUploadNameCollision(
	t *testing.T,
	err error,
	baseSource string,
	internalSource string,
) *CompressedLogFileUploadNameCollisionError {
	t.Helper()
	collision := requireCompressedLogUploadNameCollision(t, err, baseSource, internalSource)
	if collision.Base.Extension != mappedAppendLogFileExtension ||
		collision.Internal.Extension != mappedAppendLogFileExtension {
		t.Fatalf("compressed log file upload name collision = %+v, want %s files", collision, mappedAppendLogFileExtension)
	}
	return collision
}

// requireCompressedLogUploadNameCollisionは、errが通常出力baseSourceと内部ログ出力internalSourceの
// s3への圧縮ログ名の衝突を含むことを確認し、そのerrorを返します。
func requireCompressedLogUploadNameCollision(
	t *testing.T,
	err error,
	baseSource string,
	internalSource string,
) *CompressedLogFileUploadNameCollisionError {
	t.Helper()
	if err == nil {
		t.Fatal("compressed log file upload name collision error = nil, want error")
	}
	collision, ok := errortree.Find(err, func(collision *CompressedLogFileUploadNameCollisionError) bool {
		return collision.Base.Source == baseSource && collision.Internal.Source == internalSource
	})
	if !ok {
		t.Fatalf("compressed log file upload name collision error = %v, want %s and %s", err, baseSource, internalSource)
	}
	if collision.Service != objectstorage.UploadTargetS3 {
		t.Fatalf("compressed log file upload name collision service = %q, want %q", collision.Service, objectstorage.UploadTargetS3)
	}
	return collision
}
