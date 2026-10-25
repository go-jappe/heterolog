package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"

	"github.com/stretchr/testify/require"
)

func TestLifecyclesUseEachRoleRetentionInSharedDirectory(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		for _, operation := range []string{"age", "generation"} {
			t.Run(fmt.Sprintf("delivery=%t/%s", delivery, operation), func(t *testing.T) {
				cfg := rolePurgeRunnerTestConfig(t)
				cfg.FsstoreConfig.Layout.ErrorLogDir = "unused/../logs"
				cfg.FsstoreConfig.Layout.DebugLogDir = "./logs"
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
				cfg.FsstoreConfig.Lifecycle.ErrorPurge = &pkgfsstore.FilePurgeConfig{MaxFileAge: 3, MaxFileBackups: 2, MaxDirSize: 20 << 20}
				cfg.FsstoreConfig.Lifecycle.DebugPurge = &pkgfsstore.FilePurgeConfig{MaxFileAge: 10, MaxFileBackups: 4, MaxDirSize: 30 << 20}
				errorOverride := *cfg.FsstoreConfig.Lifecycle.ErrorPurge
				debugOverride := *cfg.FsstoreConfig.Lifecycle.DebugPurge
				now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
				lifecycles := newRolePurgeTestLifecycles(t, cfg, now, delivery)
				require.Len(t, lifecycles, 1)
				managers := lifecycles[0].fileSeriesManagers
				require.Len(t, managers, 3)
				paths := make([][]string, len(managers))
				activePaths := make([]string, len(managers))
				for role, manager := range managers {
					require.Equal(t, uint64(100<<20), manager.Config.Purge().MaxDirSize())
					pattern := manager.FileNamePattern()
					activePaths[role] = writeRunnerRolePurgeFile(t, pattern, now, 1, 1)
					ages := []int{7, 20}
					if operation == "generation" {
						ages = []int{1, 1, 1, 1, 1, 1}
					}
					for index, age := range ages {
						modified := now.AddDate(0, 0, -age).Add(time.Duration(index) * time.Minute)
						paths[role] = append(paths[role], writeRunnerRolePurgeFile(t, pattern, modified, index+1, 1))
					}
				}
				report, err := lifecycles[0].RunOnce(context.Background())
				require.NoError(t, err)
				require.Empty(t, report.Purge.ByDirSize.RemovalResults)
				for role := range managers {
					for index, path := range paths[role] {
						removed := role == 1 || (role == 2 && index == 1)
						if operation == "generation" {
							removed = index < len(paths[role])-[]int{6, 2, 4}[role]
						}
						if removed {
							require.NoFileExists(t, path)
						} else {
							require.FileExists(t, path)
						}
					}
					require.FileExists(t, activePaths[role])
				}
				require.Equal(t, errorOverride, *cfg.FsstoreConfig.Lifecycle.ErrorPurge)
				require.Equal(t, debugOverride, *cfg.FsstoreConfig.Lifecycle.DebugPurge)
			})
		}
	}
}

func TestLifecyclesUseRolePhysicalDirectoryCapacity(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		for _, role := range []fileClass{fileClassError, fileClassDebug} {
			for _, shared := range []bool{false, true} {
				for _, override := range []bool{false, true} {
					t.Run(fmt.Sprintf("delivery=%t/%s/shared=%t/override=%t", delivery, role, shared, override), func(t *testing.T) {
						cfg := rolePurgeRunnerTestConfig(t)
						directory := "independent"
						if shared {
							directory = "unused/../logs"
						}
						var purge *pkgfsstore.FilePurgeConfig
						if override {
							purge = &pkgfsstore.FilePurgeConfig{MaxFileAge: 30, MaxFileBackups: 6, MaxDirSize: 10 << 20}
						}
						cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = role == fileClassError
						cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = role == fileClassDebug
						if role == fileClassError {
							cfg.FsstoreConfig.Layout.ErrorLogDir = directory
							cfg.FsstoreConfig.Lifecycle.ErrorPurge = purge
						} else {
							cfg.FsstoreConfig.Layout.DebugLogDir = directory
							cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = directory != ""
							cfg.FsstoreConfig.Lifecycle.DebugPurge = purge
						}
						now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
						lifecycles := newRolePurgeTestLifecycles(t, cfg, now, delivery)
						var archivePath string
						for _, runner := range lifecycles {
							for _, manager := range runner.fileSeriesManagers {
								pattern := manager.FileNamePattern()
								writeRunnerRolePurgeFile(t, pattern, now, 1, 1)
								if pattern.Prefix == string(role)+"_" {
									archivePath = writeRunnerRolePurgeFile(t, pattern, now.AddDate(0, 0, -1), 1, 12<<20)
								}
							}
						}
						require.NotEmpty(t, archivePath)
						for _, runner := range lifecycles {
							_, err := runner.RunOnce(context.Background())
							require.NoError(t, err)
						}
						if override && !shared {
							require.NoFileExists(t, archivePath)
						} else {
							require.FileExists(t, archivePath)
						}
					})
				}
			}
		}
	}
}

func TestLifecyclesKeepAdapterResolvedRolePurgeProviders(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		for _, override := range []string{"none", "error", "debug"} {
			t.Run(fmt.Sprintf("delivery=%t/override=%s", delivery, override), func(t *testing.T) {
				cfg := rolePurgeRunnerTestConfig(t)
				cfg.FsstoreConfig.Lifecycle.ErrorPurge = nil
				cfg.FsstoreConfig.Lifecycle.DebugPurge = nil
				purge := &pkgfsstore.FilePurgeConfig{MaxFileAge: 3, MaxFileBackups: 2, MaxDirSize: 20 << 20}
				if override == "error" {
					cfg.FsstoreConfig.Lifecycle.ErrorPurge = purge
				} else if override == "debug" {
					cfg.FsstoreConfig.Lifecycle.DebugPurge = purge
				}
				now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
				lifecycles := newRolePurgeTestLifecycles(t, cfg, now, delivery)
				var managers []*FileSeriesManager
				var base *FileSeriesManager
				for _, runner := range lifecycles {
					for _, manager := range runner.fileSeriesManagers {
						managers = append(managers, manager)
						if manager.FileNamePattern().Prefix == "" {
							base = manager
						}
					}
				}
				require.Len(t, managers, 3)
				require.NotNil(t, base)
				paths := make(map[string]string)
				for _, manager := range managers {
					pattern := manager.FileNamePattern()
					resolved := base.Config.Purge()
					if pattern.Prefix == "error_" {
						resolved = manager.Config.ErrorPurge()
					} else if pattern.Prefix == "debug_" {
						resolved = manager.Config.DebugPurge()
					}
					// factoryの系列wrapperにもadapterの確定済みproviderがそのまま届きます。
					require.NotNil(t, resolved)
					require.Same(t, resolved, manager.Config.Purge())
					if pattern.Prefix != override+"_" {
						require.Same(t, base.Config.Purge(), manager.Config.Purge())
					}
					writeRunnerRolePurgeFile(t, pattern, now, 1, 1)
					paths[pattern.Prefix] = writeRunnerRolePurgeFile(t, pattern, now.AddDate(0, 0, -7), 1, 1)
				}
				for _, runner := range lifecycles {
					_, err := runner.RunOnce(context.Background())
					require.NoError(t, err)
				}
				for prefix, path := range paths {
					if prefix == override+"_" {
						require.NoFileExists(t, path)
					} else {
						require.FileExists(t, path)
					}
				}
			})
		}
	}
}

func TestLifecycleFactoriesRejectInvalidRolePurgeBeforeSideEffects(t *testing.T) {
	for _, factory := range []string{"log lifecycles", "delivery lifecycles"} {
		for _, role := range []fileClass{fileClassError, fileClassDebug} {
			for _, invalid := range []string{"age", "backups", "capacity"} {
				t.Run(factory+"/"+string(role)+"/"+invalid, func(t *testing.T) {
					cfg := rolePurgeRunnerTestConfig(t)
					purge := &pkgfsstore.FilePurgeConfig{MaxFileAge: 30, MaxFileBackups: 6, MaxDirSize: 20 << 20}
					switch invalid {
					case "age":
						purge.MaxFileAge = 0
					case "backups":
						purge.MaxFileBackups = 1
					case "capacity":
						purge.MaxDirSize = 0
					}
					if role == fileClassError {
						cfg.FsstoreConfig.Lifecycle.ErrorPurge = purge
					} else {
						cfg.FsstoreConfig.Lifecycle.DebugPurge = purge
					}
					var err error
					switch factory {
					case "log lifecycles":
						_, err = NewLogFileLifecycles(LogFileOptions{Config: cfg})
					case "delivery lifecycles":
						_, err = NewDeliveryFileLifecycles(DeliveryFileOptions{Config: deliveryRolePurgeTestConfig(cfg)})
					}
					require.ErrorIs(t, err, ErrInvalidRoleLogPurgeConfiguration)
					require.NotErrorIs(t, err, ErrInvalidDefaultLogPurgeConfiguration)
					// 拒否した系列と保持設定の項目を、型付きerrorと項目ごとのsentinelで確認します。
					var purgeErr *appendlog.PurgeConfigError
					require.ErrorAs(t, err, &purgeErr)
					require.Equal(t, appendlog.PurgeRole(role), purgeErr.Role)
					requireOnlyPurgeFieldRejected(t, purgeErr, invalid)
					entries, readErr := os.ReadDir(cfg.FsstoreConfig.Layout.LogPath)
					require.NoError(t, readErr)
					require.Empty(t, entries, "invalid retention must not create logs or checkpoint stores")
				})
			}
		}
	}
}

func rolePurgeRunnerTestConfig(t *testing.T) pkgfsstore.LogFileLifecycleRunnerConfig {
	t.Helper()
	cfg := configuredCheckpointFactoryTestConfig(t)
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{}
	cfg.RunnerConfig.Interval = time.Minute
	cfg.FsstoreConfig.Layout.ErrorLogDir = "errors"
	cfg.FsstoreConfig.Layout.DebugLogDir = "debug"
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
	cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error_"
	cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug_"
	cfg.FsstoreConfig.Lifecycle.MaxFileAge = 30
	cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 6
	cfg.FsstoreConfig.Lifecycle.MaxFileSize = 1 << 20
	cfg.FsstoreConfig.Lifecycle.MaxDirSize = 100 << 20
	return cfg
}

func deliveryRolePurgeTestConfig(cfg pkgfsstore.LogFileLifecycleRunnerConfig) pkgfsstore.DeliveryFileLifecycleRunnerConfig {
	lifecycle := cfg.FsstoreConfig.Lifecycle
	return pkgfsstore.DeliveryFileLifecycleRunnerConfig{
		FsstoreConfig: pkgfsstore.DeliveryFileLifecycleStoreConfig{
			Layout: cfg.FsstoreConfig.Layout,
			Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{
				RotationType:   lifecycle.RotationType,
				UsageCacheTTL:  lifecycle.UsageCacheTTL,
				MaxFileSize:    lifecycle.MaxFileSize,
				MaxFileAge:     lifecycle.MaxFileAge,
				MaxFileBackups: lifecycle.MaxFileBackups,
				MaxDirSize:     lifecycle.MaxDirSize,
				ErrorPurge:     lifecycle.ErrorPurge,
				DebugPurge:     lifecycle.DebugPurge,
			},
		},
		RunnerConfig: cfg.RunnerConfig,
	}
}

func newRolePurgeTestLifecycles(t *testing.T, cfg pkgfsstore.LogFileLifecycleRunnerConfig, now time.Time, delivery bool) []*DirectoryLifecycle {
	t.Helper()
	var lifecycles []*DirectoryLifecycle
	var err error
	if delivery {
		lifecycles, err = NewDeliveryFileLifecycles(DeliveryFileOptions{
			Config: deliveryRolePurgeTestConfig(cfg),
			Clock:  pkgclock.NewFixedClock(now),
		})
	} else {
		lifecycles, err = NewLogFileLifecycles(LogFileOptions{
			Config: cfg,
			Clock:  pkgclock.NewFixedClock(now),
		})
	}
	require.NoError(t, err)
	return lifecycles
}

func writeRunnerRolePurgeFile(t *testing.T, pattern logpattern.FileNamePattern, modified time.Time, index int, size int64) string {
	t.Helper()
	path := filepath.Join(pattern.Dir, fmt.Sprintf("%s%s_%s_%d.log", pattern.Prefix, pattern.Target, modified.Format("20060102"), index))
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	require.NoError(t, os.Truncate(path, size))
	require.NoError(t, os.Chtimes(path, modified, modified))
	return path
}

// purgeFieldSentinelsは、保持設定の項目ごとの検証errorです。試験の不正値の種類から引きます。
var purgeFieldSentinels = map[string]error{
	"age":      infrafsstore.ErrInvalidMaxFileAge,
	"backups":  infrafsstore.ErrInvalidMaxFileBackups,
	"capacity": infrafsstore.ErrInvalidMaxDirSize,
}

// requireOnlyPurgeFieldRejectedは、errがinvalidの項目を拒否し、他の項目を拒否していないことを確認します。
func requireOnlyPurgeFieldRejected(t *testing.T, err error, invalid string) {
	t.Helper()
	want, ok := purgeFieldSentinels[invalid]
	require.True(t, ok, "unknown purge field %q", invalid)
	require.ErrorIs(t, err, want)
	for field, sentinel := range purgeFieldSentinels {
		if field != invalid {
			require.NotErrorIs(t, err, sentinel, "only the %s setting is invalid", invalid)
		}
	}
}
