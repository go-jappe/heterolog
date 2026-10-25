package config

import (
	"errors"
	"testing"

	mappedvalidation "github.com/go-jappe/heterolog/internal/config/validation/mapped"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestMappedLogFileNameInternalTargetFromEnv(t *testing.T) {
	contracts := []struct {
		name      string
		prefix    string
		mapConfig func(AppConfig) error
	}{
		{name: "logger", mapConfig: func(cfg AppConfig) error {
			_, err := MapLoggerConfig(cfg)
			return err
		}},
		{name: "async logger", prefix: "ASYNC_", mapConfig: func(cfg AppConfig) error {
			_, err := MapAsyncLoggerConfig(cfg)
			return err
		}},
		{name: "delivery logger", prefix: "DELIVERY_", mapConfig: func(cfg AppConfig) error {
			_, err := MapDeliveryLoggerConfig(cfg)
			return err
		}},
	}
	for _, contract := range contracts {
		for _, tc := range []struct {
			name    string
			target  string
			handler string
			wantErr bool
		}{
			{name: "alternate stream target", target: "app:stream", handler: "file", wantErr: true},
			{name: "literal brackets", target: "app[1]", handler: "file"},
			{name: "unused internal file", target: "app:stream"},
		} {
			t.Run(contract.name+"/"+tc.name, func(t *testing.T) {
				cfg := loadConfigFromTempEnv(t, []string{
					"HETEROLOG_ENABLE_LOGGING=true",
					"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
					"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
					"HETEROLOG_LOG_TYPE=stdout",
					"HETEROLOG_ASYNC_LOG_TYPE=stdout",
					"HETEROLOG_DELIVERY_LOG_TYPE=stdout",
					"HETEROLOG_" + contract.prefix + "STDOUT_LOGGER_ERROR_HANDLER=" + tc.handler,
					"HETEROLOG_" + contract.prefix + "LOG_INTERNAL_LOG_FILE_TARGET=" + tc.target,
					"HETEROLOG_" + contract.prefix + `LOG_INTERNAL_LOG_FILE_LOG_PATH='C:\internal-logs'`,
					"HETEROLOG_" + contract.prefix + "LOG_INTERNAL_LOG_FILE_LOG_DIR=..",
					"HETEROLOG_" + contract.prefix + "LOG_FILE_LIFECYCLE_MODE=disable",
					"HETEROLOG_ENABLE_" + contract.prefix + "LOG_FILE_SEPARATE_ERROR_LOGS=false",
					"HETEROLOG_ENABLE_" + contract.prefix + "LOG_FILE_SEPARATE_DEBUG_LOGS=false",
				})
				err := contract.mapConfig(cfg)
				if !tc.wantErr {
					if err != nil {
						t.Fatalf("mapped logger config error = %v, want nil", err)
					}
					return
				}
				configErr := requireMappedConfigError(
					t,
					err,
					contract.name+" internal log file",
					mappedvalidation.ConfigSectionFileName,
					pkgfsstore.ErrInvalidLogFileNameComponent,
				)
				var nameErr *pkgfsstore.LogFileNameComponentError
				if !errors.As(configErr, &nameErr) || nameErr.Field != "target" || nameErr.Value != "app:stream" {
					t.Fatalf("file name error = %v, want rejected target %q", configErr, "app:stream")
				}
			})
		}
	}
}
