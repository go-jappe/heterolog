package mappedvalidation

import (
	"errors"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestMappedFileRolePurgeValidation(t *testing.T) {
	cases := []struct {
		name string
		edit func(*pkglogger.FileLoggerConfig, *pkgfsstore.FilePurgeConfig)
		want error
	}{
		{name: "valid"},
		{name: "zero", edit: func(_ *pkglogger.FileLoggerConfig, purge *pkgfsstore.FilePurgeConfig) {
			*purge = pkgfsstore.FilePurgeConfig{}
		}, want: infrafsstore.ErrInvalidMaxFileBackups},
		{name: "negative backups", edit: func(_ *pkglogger.FileLoggerConfig, purge *pkgfsstore.FilePurgeConfig) {
			purge.MaxFileBackups = -1
		}, want: infrafsstore.ErrInvalidMaxFileBackups},
		{name: "negative age", edit: func(_ *pkglogger.FileLoggerConfig, purge *pkgfsstore.FilePurgeConfig) {
			purge.MaxFileAge = -1
		}, want: infrafsstore.ErrInvalidMaxFileAge},
		{name: "zero capacity", edit: func(_ *pkglogger.FileLoggerConfig, purge *pkgfsstore.FilePurgeConfig) {
			purge.MaxDirSize = 0
		}, want: infrafsstore.ErrInvalidMaxDirSize},
		{name: "small capacity", edit: func(_ *pkglogger.FileLoggerConfig, purge *pkgfsstore.FilePurgeConfig) {
			purge.MaxDirSize = 1
		}, want: infrafsstore.ErrInvalidMaxDirSize},
		{name: "capacity below file size", edit: func(cfg *pkglogger.FileLoggerConfig, purge *pkgfsstore.FilePurgeConfig) {
			cfg.FsstoreConfig.Lifecycle.MaxFileSize = 20 << 20
			purge.MaxDirSize = 10 << 20
		}, want: infrafsstore.ErrMaxDirSizeBelowMaxFileSize},
		{name: "shared zero capacity", edit: func(cfg *pkglogger.FileLoggerConfig, purge *pkgfsstore.FilePurgeConfig) {
			cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
			cfg.FsstoreConfig.Layout.DebugLogDir = cfg.FsstoreConfig.Layout.LogDir
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = cfg.FsstoreConfig.Layout.LogDir != ""
			purge.MaxDirSize = 0
		}},
		{name: "shared small capacity", edit: func(cfg *pkglogger.FileLoggerConfig, purge *pkgfsstore.FilePurgeConfig) {
			cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
			cfg.FsstoreConfig.Layout.DebugLogDir = cfg.FsstoreConfig.Layout.LogDir
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = cfg.FsstoreConfig.Layout.LogDir != ""
			purge.MaxDirSize = 1
		}},
		{name: "shared invalid age", edit: func(cfg *pkglogger.FileLoggerConfig, purge *pkgfsstore.FilePurgeConfig) {
			cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
			cfg.FsstoreConfig.Layout.DebugLogDir = cfg.FsstoreConfig.Layout.LogDir
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = cfg.FsstoreConfig.Layout.LogDir != ""
			purge.MaxFileAge = 0
		}, want: infrafsstore.ErrInvalidMaxFileAge},
		{name: "disabled lifecycle", edit: func(cfg *pkglogger.FileLoggerConfig, purge *pkgfsstore.FilePurgeConfig) {
			cfg.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
			*purge = pkgfsstore.FilePurgeConfig{}
		}},
		{name: "disabled roles", edit: func(cfg *pkglogger.FileLoggerConfig, purge *pkgfsstore.FilePurgeConfig) {
			cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
			*purge = pkgfsstore.FilePurgeConfig{}
		}},
		{name: "nil overrides inherit valid common settings", edit: func(cfg *pkglogger.FileLoggerConfig, _ *pkgfsstore.FilePurgeConfig) {
			cfg.FsstoreConfig.Lifecycle.ErrorPurge = nil
			cfg.FsstoreConfig.Lifecycle.DebugPurge = nil
		}},
		{name: "nil overrides do not bypass invalid common settings", edit: func(cfg *pkglogger.FileLoggerConfig, _ *pkgfsstore.FilePurgeConfig) {
			cfg.FsstoreConfig.Lifecycle.ErrorPurge = nil
			cfg.FsstoreConfig.Lifecycle.DebugPurge = nil
			cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 0
			cfg.FsstoreConfig.Lifecycle.MaxFileAge = 0
			cfg.FsstoreConfig.Lifecycle.MaxFileSize = 0
			cfg.FsstoreConfig.Lifecycle.MaxDirSize = 0
		}, want: infrafsstore.ErrInvalidMaxFileBackups},
	}
	for _, source := range []string{"logger", "async logger", "delivery logger"} {
		for _, internal := range []bool{false, true} {
			label := source + " file"
			if internal {
				label = source + " internal log file"
			}
			for _, role := range []string{"error", "debug"} {
				for _, tc := range cases {
					t.Run(label+"/"+role+"/"+tc.name, func(t *testing.T) {
						cfg := mappedRolePurgeFileConfig()
						purge := cfg.FsstoreConfig.Lifecycle.ErrorPurge
						if role == "debug" {
							purge = cfg.FsstoreConfig.Lifecycle.DebugPurge
						}
						if tc.edit != nil {
							tc.edit(cfg, purge)
						}
						err := validateMappedRolePurgeDestination(source, internal, cfg)
						if tc.want == nil {
							if err != nil {
								t.Fatalf("validation error = %v, want nil", err)
							}
							return
						}
						requirePurgeConfigError(t, err, label, appendlog.PurgeRole(role), tc.want)
					})
				}
			}
		}
	}
}

func TestMappedFileRolePurgeUsesInternalDestinationLayout(t *testing.T) {
	for _, source := range []string{"logger", "async logger", "delivery logger"} {
		t.Run(source, func(t *testing.T) {
			base := mappedRolePurgeFileConfig()
			base.FsstoreConfig.Layout.ErrorLogDir = base.FsstoreConfig.Layout.LogDir
			base.FsstoreConfig.Lifecycle.ErrorPurge.MaxDirSize = 0
			if err := validateMappedRolePurgeDestination(source, false, base); err != nil {
				t.Fatalf("shared base capacity validation error = %v", err)
			}
			internal := *base
			internal.FsstoreConfig.Layout.LogDir = "internal"
			internal.FsstoreConfig.Layout.ErrorLogDir = "internal-error"
			err := validateMappedRolePurgeDestination(source, true, &internal)
			requirePurgeConfigError(
				t,
				err,
				source+" internal log file",
				appendlog.PurgeRoleError,
				infrafsstore.ErrInvalidMaxDirSize,
			)
			if base.FsstoreConfig.Lifecycle.ErrorPurge.MaxDirSize != 0 {
				t.Fatal("validation mutated the inherited raw role capacity")
			}
		})
	}
}

func TestMappedFileRolePurgeIgnoresUnusedBase(t *testing.T) {
	file := mappedRolePurgeFileConfig()
	file.FsstoreConfig.Lifecycle.ErrorPurge.MaxFileBackups = 0
	if err := ValidateSyncLoggerConfig("logger", pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		Stdout:                &pkglogger.StdoutLoggerConfig{}, File: file,
	}); err != nil {
		t.Fatalf("unused sync base validation error = %v", err)
	}
	if err := ValidateDeliveryLoggerConfig("delivery logger", pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		Stdout:                &pkglogger.DeliveryStdoutLoggerConfig{}, File: mappedRolePurgeDeliveryConfig(file),
	}); err != nil {
		t.Fatalf("unused delivery base validation error = %v", err)
	}
}

// requirePurgeConfigErrorは、errがsourceのlifecycle設定について、roleの保持設定をkindの理由で
// 拒否したerrorを含むことを確認します。
func requirePurgeConfigError(t *testing.T, err error, source string, role appendlog.PurgeRole, kind error) {
	t.Helper()
	configErr := requireConfigError(t, err, source, ConfigSectionLifecycle, nil)
	if _, ok := errortree.Find(configErr, func(purgeErr *appendlog.PurgeConfigError) bool {
		return purgeErr.Role == role && errors.Is(purgeErr, kind)
	}); !ok {
		t.Fatalf("%s lifecycle error = %v, want %s log purge config error %v", source, configErr, role, kind)
	}
}

func mappedRolePurgeFileConfig() *pkglogger.FileLoggerConfig {
	return &pkglogger.FileLoggerConfig{
		LifecycleMode: pkgfsstore.FileLifecycleModeInline,
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				LogPath: "/logs", Target: "app", LogDir: "default", ErrorLogDir: "error", DebugLogDir: "debug",
				IsSeparateDebugLogDirEnabled: true,
				IsSeparateErrorLogsEnabled:   true, IsSeparateDebugLogsEnabled: true,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				UsageCacheTTL: time.Minute, MaxFileBackups: 5, MaxFileAge: 5, MaxFileSize: 1 << 20, MaxDirSize: 50 << 20,
				ErrorPurge: &pkgfsstore.FilePurgeConfig{MaxFileBackups: 4, MaxFileAge: 4, MaxDirSize: 40 << 20},
				DebugPurge: &pkgfsstore.FilePurgeConfig{MaxFileBackups: 3, MaxFileAge: 3, MaxDirSize: 30 << 20},
			},
		},
		ArchiveCheckpointConfig: validArchiveCheckpointConfig(),
	}
}

func mappedRolePurgeDeliveryConfig(cfg *pkglogger.FileLoggerConfig) *pkglogger.DeliveryFileLoggerConfig {
	lifecycle := cfg.FsstoreConfig.Lifecycle
	mode := cfg.LifecycleMode
	if mode == pkgfsstore.FileLifecycleModeInline {
		mode = pkgfsstore.FileLifecycleModeRunner
	}
	return &pkglogger.DeliveryFileLoggerConfig{
		LifecycleMode: mode,
		FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{
			Layout:   cfg.FsstoreConfig.Layout,
			Rotation: pkgfsstore.FileRotationConfig{MaxFileSize: lifecycle.MaxFileSize},
		},
		LifecycleRunnerSettings: pkglogger.DeliveryFileLifecycleRunnerSettings{
			UsageCacheTTL: lifecycle.UsageCacheTTL, MaxFileBackups: lifecycle.MaxFileBackups,
			MaxFileAge: lifecycle.MaxFileAge, MaxDirSize: lifecycle.MaxDirSize,
			ErrorPurge: lifecycle.ErrorPurge, DebugPurge: lifecycle.DebugPurge,
			RunnerConfig:            pkgfsstore.AppendLogRunnerConfig{Interval: time.Minute},
			ArchiveCheckpointConfig: cfg.ArchiveCheckpointConfig,
		},
	}
}

func validateMappedRolePurgeDestination(source string, internal bool, file *pkglogger.FileLoggerConfig) error {
	if source == "delivery logger" {
		cfg := pkglogger.DeliveryLoggerConfig{File: mappedRolePurgeDeliveryConfig(file)}
		if internal {
			cfg.InternalLogDestinations = &pkglogger.DeliveryInternalLogDestinationConfig{File: cfg.File}
			cfg.File = nil
			// 専用internal log fileは通常出力に数えないため、通常出力としてstdoutを持たせます。
			cfg.Stdout = &pkglogger.DeliveryStdoutLoggerConfig{}
		}
		return ValidateDeliveryLoggerConfig(source, cfg)
	}
	cfg := pkglogger.LoggerConfig{File: file}
	if internal {
		cfg.InternalLogDestinations = &pkglogger.InternalLogDestinationConfig{File: cfg.File}
		cfg.File = nil
		// 専用internal log fileは通常出力に数えないため、通常出力としてstdoutを持たせます。
		cfg.Stdout = &pkglogger.StdoutLoggerConfig{}
	}
	return ValidateSyncLoggerConfig(source, cfg)
}
