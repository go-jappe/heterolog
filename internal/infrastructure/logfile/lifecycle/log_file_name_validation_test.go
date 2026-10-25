package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"

	"github.com/stretchr/testify/require"
)

func TestLifecycleFactoriesRejectColonInLogFileNamesBeforeCreatingFiles(t *testing.T) {
	// 検証errorは、拒否した設定を検証側の設定名で示します。
	validatedFields := map[string]string{
		"target": "target", "error prefix": "error log prefix", "debug prefix": "debug log prefix",
		"file name format": "file name format", "file date format": "file date format",
	}
	for _, field := range []string{"target", "error prefix", "debug prefix", "file name format", "file date format"} {
		for _, factory := range []string{
			"log lifecycles",
			"delivery lifecycles",
		} {
			t.Run(field+"/"+factory, func(t *testing.T) {
				cfg := configuredCheckpointFactoryTestConfig(t)
				cfg.RunnerConfig.Interval = time.Minute
				switch field {
				case "target":
					cfg.FsstoreConfig.Layout.Target = "app:stream"
				case "error prefix":
					cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
					cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error:"
				case "debug prefix":
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
					cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug:"
				case "file name format":
					cfg.FsstoreConfig.Layout.FileNameFormat = "/%s%s:%s_%d.log"
				case "file date format":
					cfg.FsstoreConfig.Layout.FileDateFormat = "20060102:150405"
				}
				err := invokeLogFileNameValidationFactory(factory, cfg)
				require.ErrorIs(t, err, ErrInvalidFileNameConfig)
				var nameErr *pkgfsstore.LogFileNameComponentError
				require.ErrorAs(t, err, &nameErr)
				require.Equal(t, validatedFields[field], nameErr.Field)
				entries, err := os.ReadDir(cfg.FsstoreConfig.Layout.LogPath)
				require.NoError(t, err)
				require.Empty(t, entries, "invalid names must not create log or checkpoint directories")
			})
		}
	}
}

func TestLifecycleFactoriesAllowUnusedColonPrefixes(t *testing.T) {
	for _, factory := range []string{
		"log lifecycles",
		"delivery lifecycles",
	} {
		t.Run(factory, func(t *testing.T) {
			cfg := configuredCheckpointFactoryTestConfig(t)
			cfg.RunnerConfig.Interval = time.Minute
			cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
			cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "unused:error:"
			cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "unused:debug:"
			cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}
			cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{}
			require.NoError(t, invokeLogFileNameValidationFactory(factory, cfg))
		})
	}
}

func invokeLogFileNameValidationFactory(factory string, cfg pkgfsstore.LogFileLifecycleRunnerConfig) error {
	deliveryCfg := pkgfsstore.DeliveryFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.DeliveryFileLifecycleStoreConfig{
			Layout: cfg.FsstoreConfig.Layout,
			Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{
				UsageCacheTTL:  cfg.FsstoreConfig.Lifecycle.UsageCacheTTL,
				MaxFileBackups: cfg.FsstoreConfig.Lifecycle.MaxFileBackups,
				MaxFileAge:     cfg.FsstoreConfig.Lifecycle.MaxFileAge,
				MaxFileSize:    cfg.FsstoreConfig.Lifecycle.MaxFileSize,
				MaxDirSize:     cfg.FsstoreConfig.Lifecycle.MaxDirSize,
			},
		},
		RunnerConfig:              cfg.RunnerConfig,
		ArchiveCheckpointConfig:   cfg.ArchiveCheckpointConfig,
		ObjectStorageUploadConfig: cfg.ObjectStorageUploadConfig,
	}
	var err error
	switch factory {
	case "log lifecycles":
		_, err = NewLogFileLifecycles(LogFileOptions{Config: cfg})
	case "delivery lifecycles":
		_, err = NewDeliveryFileLifecycles(DeliveryFileOptions{Config: deliveryCfg})
	default:
		panic("unknown test factory: " + factory)
	}
	return err
}

func TestFileSeriesManagerRejectsColonInEffectiveFileNameBeforeCreatingDirectory(t *testing.T) {
	for _, field := range []string{"target", "prefix", "extension"} {
		t.Run(field, func(t *testing.T) {
			root := resolvedTempDirForTest(t)
			pattern := logpattern.FileNamePattern{Dir: filepath.Join(root, "explicit"), Target: "app", Extension: "log"}
			switch field {
			case "target":
				pattern.Target = "app:stream"
			case "prefix":
				pattern.Prefix = "error:"
			case "extension":
				pattern.Extension = "log:stream"
			}
			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				Config:   testFileSeriesManagerConfig(root, "app", 10, 36500, 1<<20, false),
				FileName: pattern,
			})
			_, err := NewDirectoryLifecycle(manager)
			requireInvalidLogFileNameComponent(t, err, "file "+field)
			_, err = manager.RunOnce(context.Background())
			requireInvalidLogFileNameComponent(t, err, "file "+field)
			entries, err := os.ReadDir(root)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestFileSeriesManagerAllowsUnusedColonPrefixes(t *testing.T) {
	cfg := configuredCheckpointFactoryTestConfig(t)
	cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "unused:error:"
	cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "unused:debug:"
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		Config:   appendlog.NewConfigAdapter(cfg.FsstoreConfig),
		FileName: logpattern.FileNamePattern{Dir: cfg.FsstoreConfig.Layout.LogPath, Target: "app", Extension: "log"},
	})
	_, err := NewDirectoryLifecycle(manager)
	require.NoError(t, err)
}

func TestFileSeriesManagerRejectsColonInLayoutBeforeCreatingDirectory(t *testing.T) {
	for _, field := range []string{"target", "file name format", "file date format"} {
		t.Run(field, func(t *testing.T) {
			cfg := configuredCheckpointFactoryTestConfig(t)
			switch field {
			case "target":
				cfg.FsstoreConfig.Layout.Target = "app:stream"
			case "file name format":
				cfg.FsstoreConfig.Layout.FileNameFormat = "/%s%s_%s_%d.log:stream"
			case "file date format":
				cfg.FsstoreConfig.Layout.FileDateFormat = "20060102:150405"
			}
			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				Config:   appendlog.NewConfigAdapter(cfg.FsstoreConfig),
				FileName: logpattern.FileNamePattern{Dir: filepath.Join(cfg.FsstoreConfig.Layout.LogPath, "explicit"), Target: "app", Extension: "log"},
			})
			_, err := NewDirectoryLifecycle(manager)
			requireInvalidLogFileNameComponent(t, err, field)
			entries, err := os.ReadDir(cfg.FsstoreConfig.Layout.LogPath)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

// requireInvalidLogFileNameComponentは、file名に使えない値をfieldの設定として拒否したことを確認します。
func requireInvalidLogFileNameComponent(t *testing.T, err error, field string) {
	t.Helper()
	require.ErrorIs(t, err, ErrInvalidFileName)
	require.ErrorIs(t, err, pkgfsstore.ErrInvalidLogFileNameComponent)
	var nameErr *pkgfsstore.LogFileNameComponentError
	require.ErrorAs(t, err, &nameErr)
	require.Equal(t, field, nameErr.Field)
}
