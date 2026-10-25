package mappedvalidation

import (
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestMappedFileLifecycleModeRejectsEmptySelectedConfig(t *testing.T) {
	for _, source := range []string{"logger", "async logger"} {
		t.Run(source, func(t *testing.T) {
			cfg := pkglogger.LoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
				File: &pkglogger.FileLoggerConfig{
					ArchiveCheckpointConfig: validArchiveCheckpointConfig(),
				},
			}
			assertMappedEmptyLifecycleModeError(t, ValidateSyncLoggerConfig(source, cfg), source+" file", ErrEmptyFileLifecycleMode)

			// 通常file出力がなくても、handlerから参照されるbase fileは使用対象です。
			cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
			cfg.Stdout = &pkglogger.StdoutLoggerConfig{
				HandlerConfig: pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{"file"}},
			}
			assertMappedEmptyLifecycleModeError(t, ValidateSyncLoggerConfig(source, cfg), source+" file", ErrEmptyFileLifecycleMode)

			// 専用internal-log fileを指定した場合は、その設定のmodeを検証します。
			cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{File: cfg.File}
			assertMappedEmptyLifecycleModeError(t, ValidateSyncLoggerConfig(source, cfg), source+" internal log file", ErrEmptyFileLifecycleMode)
		})
	}

	t.Run("delivery logger", func(t *testing.T) {
		cfg := pkglogger.DeliveryLoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
			File: &pkglogger.DeliveryFileLoggerConfig{
				LifecycleRunnerSettings: pkglogger.DeliveryFileLifecycleRunnerSettings{
					ArchiveCheckpointConfig: validArchiveCheckpointConfig(),
				},
			},
		}
		assertMappedEmptyLifecycleModeError(t, ValidateDeliveryLoggerConfig("delivery logger", cfg), "delivery logger file", ErrEmptyDeliveryFileLifecycleMode)

		cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
		cfg.Stdout = &pkglogger.DeliveryStdoutLoggerConfig{
			HandlerConfig: pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{"file"}},
		}
		assertMappedEmptyLifecycleModeError(t, ValidateDeliveryLoggerConfig("delivery logger", cfg), "delivery logger file", ErrEmptyDeliveryFileLifecycleMode)

		cfg.InternalLogDestinations = &pkglogger.DeliveryInternalLogDestinationConfig{File: cfg.File}
		assertMappedEmptyLifecycleModeError(t, ValidateDeliveryLoggerConfig("delivery logger", cfg), "delivery logger internal log file", ErrEmptyDeliveryFileLifecycleMode)
	})
}

func TestMappedFileLifecycleModeIgnoresUnusedBaseFile(t *testing.T) {
	for _, source := range []string{"logger", "async logger"} {
		t.Run(source, func(t *testing.T) {
			cfg := pkglogger.LoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
				Stdout:                &pkglogger.StdoutLoggerConfig{},
				File:                  &pkglogger.FileLoggerConfig{},
			}
			if err := ValidateSyncLoggerConfig(source, cfg); err != nil {
				t.Fatalf("unused base file config error = %v, want nil", err)
			}

			cfg.Stdout.HandlerConfig.OnErrorConfig = []string{"file"}
			cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{
				File: &pkglogger.FileLoggerConfig{
					LifecycleMode:           pkgfsstore.FileLifecycleModeDisable,
					FsstoreConfig:           pkgfsstore.Config{Layout: pkgfsstore.FsstoreConfig{LogDir: "."}},
					ArchiveCheckpointConfig: validArchiveCheckpointConfig(),
				},
			}
			if err := ValidateSyncLoggerConfig(source, cfg); err != nil {
				t.Fatalf("unused base file with internal-log override error = %v, want nil", err)
			}
		})
	}

	t.Run("delivery logger", func(t *testing.T) {
		cfg := pkglogger.DeliveryLoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
			Stdout:                &pkglogger.DeliveryStdoutLoggerConfig{},
			File:                  &pkglogger.DeliveryFileLoggerConfig{},
		}
		if err := ValidateDeliveryLoggerConfig("delivery logger", cfg); err != nil {
			t.Fatalf("unused delivery base file config error = %v, want nil", err)
		}

		cfg.Stdout.HandlerConfig.OnErrorConfig = []string{"file"}
		cfg.InternalLogDestinations = &pkglogger.DeliveryInternalLogDestinationConfig{
			File: &pkglogger.DeliveryFileLoggerConfig{
				LifecycleMode: pkgfsstore.FileLifecycleModeDisable,
				FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{Layout: pkgfsstore.FsstoreConfig{LogDir: "."}},
				LifecycleRunnerSettings: pkglogger.DeliveryFileLifecycleRunnerSettings{
					ArchiveCheckpointConfig: validArchiveCheckpointConfig(),
				},
			},
		}
		if err := ValidateDeliveryLoggerConfig("delivery logger", cfg); err != nil {
			t.Fatalf("unused delivery base file with internal-log override error = %v, want nil", err)
		}
	})
}

func TestMappedFailureArchiveLifecycleModeRejectsEmptySelectedArchive(t *testing.T) {
	for _, source := range []string{"logger", "async logger"} {
		t.Run(source, func(t *testing.T) {
			cfg := failureArchiveLifecycleValidationLoggerConfig("", objectstorage.UploadTargetConfig{})
			assertMappedEmptyLifecycleModeError(t, validateMappedFailureStores(source, cfg), source+" opensearch failure file archive", ErrEmptyFileLifecycleMode)

			cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
			if err := validateMappedFailureStores(source, cfg); err != nil {
				t.Fatalf("unselected failure archive with empty mode error = %v, want nil", err)
			}
		})
	}

	t.Run("delivery logger", func(t *testing.T) {
		cfg := failureArchiveLifecycleValidationDeliveryLoggerConfig("", objectstorage.UploadTargetConfig{})
		assertMappedEmptyLifecycleModeError(t, validateMappedDeliveryFailureStores("delivery logger", cfg), "delivery logger opensearch failure file archive", ErrEmptyFileLifecycleMode)

		cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
		if err := validateMappedDeliveryFailureStores("delivery logger", cfg); err != nil {
			t.Fatalf("unselected delivery failure archive with empty mode error = %v, want nil", err)
		}
	})
}

// assertMappedEmptyLifecycleModeErrorは、errがsourceのlifecycle設定について、config mapping後に
// 空のmodeをkindの理由で拒否したerrorを含むことを確認します。
func assertMappedEmptyLifecycleModeError(t *testing.T, err error, source string, kind error) {
	t.Helper()
	requireConfigError(t, err, source, ConfigSectionLifecycle, kind)
}
