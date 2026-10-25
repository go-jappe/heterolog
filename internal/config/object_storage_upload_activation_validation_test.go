package config

import (
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestLogFileUploadActivationIsValidatedAtApplicationBoundary(t *testing.T) {
	cfg := mapperValidationTestConfig()
	cfg.Logger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.Logger.FileConfig.FileSystemStoreConfig.LifecycleMode =
		pkgfsstore.FileLifecycleModeInline
	cfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig =
		objectstorage.LogFileUploadConfig{Enabled: true}

	// mapperは個々のupload targetを受け取り先packageの契約として検証しますが、
	// lifecycleとfile classを横断する有効化policyは扱いません。
	if _, err := MapLoggerConfig(cfg); err != nil {
		t.Fatalf("MapLoggerConfig() error = %v, want application policy to be handled elsewhere", err)
	}

	err := ValidateApplicationConfig(cfg)
	requireFileUploadActivationError(t, err, "logger file")
}

func TestLogFileUploadActivationCoversEveryLoggerContract(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*AppConfig)
		wantScope string
	}{
		{
			name: "sync logger",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				store := &cfg.Logger.FileConfig.FileSystemStoreConfig
				store.LifecycleMode = pkgfsstore.FileLifecycleModeInline
				store.ObjectStorageUploadConfig.Enabled = true
			},
			wantScope: "logger file",
		},
		{
			name: "async logger",
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				store := &cfg.AsyncBestEffortMultiLoggerConfig.
					FileConfig.
					FileSystemStoreConfig
				store.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
				store.ObjectStorageUploadConfig.Enabled = true
			},
			wantScope: "async logger file",
		},
		{
			name: "delivery logger",
			configure: func(cfg *AppConfig) {
				cfg.DeliveryLogger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				store := &cfg.DeliveryLogger.
					DeliveryFileConfig.
					FileSystemStoreConfig
				store.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
				store.ObjectStorageUploadConfig.Enabled = true
			},
			wantScope: "delivery logger file",
		},
		{
			name: "handler-only file logger",
			configure: func(cfg *AppConfig) {
				cfg.Logger.HandlerTargetDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				store := &cfg.Logger.FileConfig.FileSystemStoreConfig
				store.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
				store.ObjectStorageUploadConfig.Enabled = true
			},
			wantScope: "logger file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg AppConfig
			tt.configure(&cfg)

			err := validateLogFileObjectStorageUploadActivation(cfg)
			requireFileUploadActivationError(t, err, tt.wantScope)
		})
	}
}

func TestLogFileUploadActivationIgnoresLifecycleWithoutUploadOwner(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*AppConfig)
	}{
		{
			name: "unselected sync file logger",
			configure: func(cfg *AppConfig) {
				store := &cfg.Logger.FileConfig.FileSystemStoreConfig
				store.LifecycleMode = pkgfsstore.FileLifecycleModeInline
				store.ObjectStorageUploadConfig.Enabled = true
			},
		},
		{
			name: "disabled sync lifecycle",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				store := &cfg.Logger.FileConfig.FileSystemStoreConfig
				store.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
				store.ObjectStorageUploadConfig.Enabled = true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg AppConfig
			tt.configure(&cfg)
			if err := validateLogFileObjectStorageUploadActivation(cfg); err != nil {
				t.Fatalf("unused object storage upload config error = %v, want nil", err)
			}
		})
	}
}

func TestLogFileUploadActivationAcceptsEnabledPhysicalFileClass(t *testing.T) {
	tests := []struct {
		name          string
		upload        objectstorage.LogFileUploadConfig
		separateError bool
		separateDebug bool
	}{
		{
			name: "default file",
			upload: objectstorage.LogFileUploadConfig{
				Enabled: true,
				Default: objectstorage.UploadTargetConfig{
					Enabled: true,
				},
			},
		},
		{
			name: "separate error file",
			upload: objectstorage.LogFileUploadConfig{
				Enabled: true,
				Error: objectstorage.UploadTargetConfig{
					Enabled: true,
				},
			},
			separateError: true,
		},
		{
			name: "separate debug file",
			upload: objectstorage.LogFileUploadConfig{
				Enabled: true,
				Debug: objectstorage.UploadTargetConfig{
					Enabled: true,
				},
			},
			separateDebug: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateLogFileUploadActivation(
				"logger file",
				true,
				tt.upload,
				tt.separateError,
				tt.separateDebug,
			); err != nil {
				t.Fatalf("validateLogFileUploadActivation() error = %v, want nil", err)
			}
		})
	}
}

func TestMapLoggerConfigIgnoresUploadConfigWhenLifecycleIsDisabled(t *testing.T) {
	cfg := mapperValidationTestConfig()
	cfg.Logger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.Logger.FileConfig.FileSystemStoreConfig.LifecycleMode =
		pkgfsstore.FileLifecycleModeDisable
	cfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig =
		objectstorage.LogFileUploadConfig{
			Enabled: true,
			Default: objectstorage.UploadTargetConfig{
				Enabled: true,
				Targets: []objectstorage.UploadTarget{"unsupported"},
			},
		}

	if _, err := MapLoggerConfig(cfg); err != nil {
		t.Fatalf("MapLoggerConfig() disabled lifecycle error = %v, want nil", err)
	}
}

// requireFileUploadActivationErrorは、errが設定元sourceのfile出力について、適用できるfile upload
// がないことを報告していることを確認します。
func requireFileUploadActivationError(t *testing.T, err error, source string) {
	t.Helper()
	if _, ok := errortree.Find(err, func(activationErr *FileUploadActivationError) bool {
		return activationErr.Source == source
	}); !ok || !errors.Is(err, ErrNoApplicableFileUpload) {
		t.Fatalf("upload activation error = %v, want %s %v", err, source, ErrNoApplicableFileUpload)
	}
}
