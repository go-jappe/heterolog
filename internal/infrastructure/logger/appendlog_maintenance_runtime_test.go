package logger

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	appconfig "github.com/go-jappe/heterolog/internal/config"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestLoggerConstructorsOwnRunnerModeAppendLogMaintenanceRuntime(t *testing.T) {
	appCfg := loadLoggerConfigFromConfigFixtureCopy(t, map[string]string{
		"HETEROLOG_ENABLE_LOGGING":                                 "true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING":               "true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING":                        "true",
		"HETEROLOG_LOG_TYPE":                                       "file",
		"HETEROLOG_ASYNC_LOG_TYPE":                                 "file",
		"HETEROLOG_DELIVERY_LOG_TYPE":                              "file",
		"HETEROLOG_LOG_FILE_LIFECYCLE_MODE":                        "runner",
		"HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_MODE":                  "runner",
		"HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_MODE":               "runner",
		"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD":          "false",
		"HETEROLOG_ENABLE_ASYNC_LOG_FILE_OBJECT_STORAGE_UPLOAD":    "false",
		"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_OBJECT_STORAGE_UPLOAD": "false",
	})
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	syncCfg := appconfig.ToLoggerConfig(appCfg)
	syncCfg.File.FsstoreConfig.Layout.LogPath = t.TempDir()
	syncLogger, err := NewLogger(
		context.Background(),
		syncCfg,
		appCfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity,
		nil,
		nil,
		tracer,
		meter,
	)
	require.NoError(t, err)
	syncOwned := syncLogger.(*topLevelLoggerRuntime)
	require.NotNil(t, syncOwned.appendLogMaintenanceRuntime)
	require.NoError(t, syncOwned.Close())
	require.True(t, syncOwned.appendLogMaintenanceRuntime.closed)

	asyncCfg := appconfig.ToAsyncBestEffortMultiLoggerConfig(appCfg)
	asyncCfg.File.FsstoreConfig.Layout.LogPath = t.TempDir()
	asyncLogger, err := NewAsyncLogger(
		context.Background(),
		asyncCfg,
		appCfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogStreamRoutingGranularity,
		nil,
		nil,
		tracer,
		meter,
	)
	require.NoError(t, err)
	asyncOwned := asyncLogger.(*topLevelAsyncLoggerRuntime)
	require.NotNil(t, asyncOwned.appendLogMaintenanceRuntime)
	require.NoError(t, asyncOwned.Close())
	require.True(t, asyncOwned.appendLogMaintenanceRuntime.closed)

	deliveryCfg := appconfig.ToDeliveryLoggerConfig(appCfg)
	deliveryCfg.File.FsstoreConfig.Layout.LogPath = t.TempDir()
	deliveryLogger, err := NewDeliveryLogger(
		context.Background(),
		deliveryCfg,
		appCfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogStreamRoutingGranularity,
		tracer,
		meter,
	)
	require.NoError(t, err)
	deliveryOwned := deliveryLogger.(*topLevelDeliveryLoggerRuntime)
	require.NotNil(t, deliveryOwned.appendLogMaintenanceRuntime)
	require.NoError(t, deliveryOwned.Close())
	require.True(t, deliveryOwned.appendLogMaintenanceRuntime.closed)

	var _ applogging.CloseableLogger = syncOwned
	var _ applogging.AsyncCloseableLogger = asyncOwned
	var _ applogging.DeliveryClosableLogger = deliveryOwned
}

// loadLoggerConfigFromConfigFixtureCopyは`.env.example`から生成した試験用`.env`を丸ごと複製し、
// 対象testの値だけをcopy上で置き換えます。process environmentによる上書きではなく、
// HETEROLOG_ENV_PATHが指す`.env` fileの内容だけからLoadConfigする境界を検証します。
func loadLoggerConfigFromConfigFixtureCopy(t *testing.T, overrides map[string]string) appconfig.AppConfig {
	t.Helper()

	repoEnvPath := filepath.Join(configFixtureRootForLoggerTest(t), ".env")
	envFile, err := godotenv.Read(repoEnvPath)
	require.NoError(t, err)
	// 起動時のcheckpoint初期化を、設定例にある運用向けstate directoryから分離します。
	envFile["HETEROLOG_APP_STATE_DIR"] = t.TempDir()
	for key, value := range overrides {
		envFile[key] = value
	}

	envPath := filepath.Join(t.TempDir(), ".env")
	require.NoError(t, godotenv.Write(envFile, envPath))
	// 設定loaderが読む環境変数だけを外し、実行環境の変数は残します。
	testutil.UnsetConfigEnvironment(t)
	require.NoError(t, os.Setenv("HETEROLOG_ENV_PATH", envPath))

	cfg, err := appconfig.LoadConfig()
	require.NoError(t, err)
	return cfg
}

func TestNewLoggerRoutesAppendLogMaintenanceEventToInternalLogFileFromEnv(t *testing.T) {
	baseRoot := t.TempDir()
	internalRoot := t.TempDir()
	appCfg := loadLoggerConfigFromConfigFixtureCopy(t, map[string]string{
		"HETEROLOG_ENABLE_LOGGING":                        "true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING":      "false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING":               "false",
		"HETEROLOG_LOG_TYPE":                              "file",
		"HETEROLOG_SYSTEM_LOG_DIR":                        baseRoot,
		"HETEROLOG_LOG_FILE_LIFECYCLE_MODE":               "runner",
		"HETEROLOG_LOG_FILE_LIFECYCLE_INFO_HANDLER":       "file",
		"HETEROLOG_LOG_INTERNAL_LOG_FILE_LOG_PATH":        internalRoot,
		"HETEROLOG_LOG_INTERNAL_LOG_FILE_LOG_DIR":         "internal-logs",
		"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD": "false",
		// 通知のroutingを確認するため、運用設定のsamplingへ依存しません。
		"HETEROLOG_ENABLE_LOG_FILE_SAMPLING": "false",
	})
	requireConfigFixtureDiskSpaceForLoggerTest(t, appCfg)

	mapped, err := appconfig.MapLoggerConfig(appCfg)
	require.NoError(t, err)
	require.NotNil(t, mapped.InternalLogDestinations)
	require.NotNil(t, mapped.InternalLogDestinations.File)

	created, err := NewLogger(
		context.Background(),
		mapped,
		appCfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.NoError(t, err)
	owned := created.(*topLevelLoggerRuntime)
	require.Nil(t, owned.internalLogDestinations.parentOwnedFileLoggerSet)
	require.NotNil(t, owned.internalLogDestinations.runtimeOwnedFileLoggerSet)
	require.Same(
		t,
		owned.internalLogDestinations.runtimeOwnedFileLoggerSet,
		owned.internalLogDestinations.fileLoggerSetForHandlers(),
	)
	require.Len(
		t,
		owned.appendLogMaintenanceRuntime.entries,
		lifecycleRunnerDirectoryCount(mapped.File.FsstoreConfig.Layout)+
			lifecycleRunnerDirectoryCount(
				mapped.InternalLogDestinations.File.FsstoreConfig.Layout,
			),
	)

	// 先頭は通常file directoryのrunnerです。そのinternal logは同じLoggerContractKindで共有する
	// bare `file` handlerへ委譲されます。
	owned.appendLogMaintenanceRuntime.entries[0].runner.Reporter.ReportRun(
		context.Background(),
		logfilelifecycle.RunReport{
			ObjectStorageUploads: []logfilelifecycle.ObjectStorageUploadReport{{Path: "rotated.log.gz"}},
		},
		nil,
	)
	require.NoError(t, owned.Close())

	internalLayout := mapped.InternalLogDestinations.File.FsstoreConfig.Layout
	require.Contains(
		t,
		readLifecycleLogBody(t, internalLayout),
		"appendlog maintenance runner completed",
	)
	require.NotContains(
		t,
		readLifecycleLogBodyIfPresent(t, mapped.File.FsstoreConfig.Layout),
		"appendlog maintenance runner completed",
	)
}

func TestLoggerConstructorsCreateOneAdditionalRunnerForDedicatedInternalLogFile(t *testing.T) {
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	t.Run("sync", func(t *testing.T) {
		base := runnerFileConfigForInternalLogRuntime(t.TempDir(), "sync-internal-runtime")
		internalFile := dedicatedInternalLogFileConfig(base, t.TempDir())
		cfg := pkglogger.LoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
			Timeout:               time.Second,
			MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
			File:                  base,
			InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
				File: internalFile,
			},
		}

		created, err := NewLogger(context.Background(), cfg, streamrouting.None, nil, nil, tracer, meter)
		require.NoError(t, err)
		owned := created.(*topLevelLoggerRuntime)
		require.Len(t, owned.appendLogMaintenanceRuntime.entries, 2)
		require.NotSame(t, internalFile, owned.internalLogDestinations.dedicatedFileConfig())
		require.Equal(t, internalFile, owned.internalLogDestinations.dedicatedFileConfig())
		assertInternalLogLifecycleCompresses(
			t,
			owned.appendLogMaintenanceRuntime,
			internalFile.FsstoreConfig.Layout,
		)
		require.NoError(t, owned.Close())
	})

	t.Run("async", func(t *testing.T) {
		base := runnerFileConfigForInternalLogRuntime(t.TempDir(), "async-internal-runtime")
		internalFile := dedicatedInternalLogFileConfig(base, t.TempDir())
		cfg := pkglogger.AsyncBestEffortMultiLoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
			Timeout:               time.Second,
			MaxParallel:           1,
			MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
			File:                  base,
			InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
				File: internalFile,
			},
		}

		created, err := NewAsyncLogger(context.Background(), cfg, streamrouting.None, nil, nil, tracer, meter)
		require.NoError(t, err)
		owned := created.(*topLevelAsyncLoggerRuntime)
		require.Len(t, owned.appendLogMaintenanceRuntime.entries, 2)
		require.NotSame(t, internalFile, owned.internalLogDestinations.dedicatedFileConfig())
		require.Equal(t, internalFile, owned.internalLogDestinations.dedicatedFileConfig())
		require.NoError(t, owned.Close())
	})

	t.Run("delivery", func(t *testing.T) {
		base := runnerDeliveryFileConfigForInternalLogRuntime(
			t.TempDir(),
			"delivery-internal-runtime",
		)
		internalFile := dedicatedDeliveryInternalLogFileConfig(base, t.TempDir())
		cfg := pkglogger.DeliveryLoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
			Timeout:               time.Second,
			MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
			File:                  base,
			InternalLogDestinations: &pkglogger.DeliveryInternalLogDestinationConfig{
				File: internalFile,
			},
		}

		created, err := NewDeliveryLogger(context.Background(), cfg, streamrouting.None, tracer, meter)
		require.NoError(t, err)
		owned := created.(*topLevelDeliveryLoggerRuntime)
		require.Len(t, owned.appendLogMaintenanceRuntime.entries, 2)
		require.NotSame(t, internalFile, owned.internalLogDestinations.dedicatedFileConfig())
		require.Equal(t, internalFile, owned.internalLogDestinations.dedicatedFileConfig())
		require.NoError(t, owned.Close())
	})
}

func TestInternalLogFileWithoutOverrideSharesBaseLoggerSetAndRunner(t *testing.T) {
	base := runnerFileConfigForInternalLogRuntime(t.TempDir(), "shared-internal-runtime")
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
		Timeout:               time.Second,
		MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
		File:                  base,
	}

	created, err := NewLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.NoError(t, err)
	owned := created.(*topLevelLoggerRuntime)
	require.Same(t, owned.fileLoggerSet, owned.internalLogDestinations.parentOwnedFileLoggerSet)
	require.Same(t, owned.fileLoggerSet, owned.internalLogDestinations.fileLoggerSetForHandlers())
	require.Nil(t, owned.internalLogDestinations.runtimeOwnedFileLoggerSet)
	require.Len(t, owned.appendLogMaintenanceRuntime.entries, 1)
	require.NoError(t, owned.Close())
}

func TestHandlerOnlyDedicatedInternalLogFileDoesNotStartUnusedBaseFileRuntime(t *testing.T) {
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}

	t.Run("sync", func(t *testing.T) {
		base := runnerFileConfigForInternalLogRuntime(t.TempDir(), "sync-handler-only-base")
		internalFile := dedicatedInternalLogFileConfig(base, t.TempDir())
		stdoutCfg := newActivationStdoutConfig()
		stdoutCfg.HandlerConfig.OnErrorConfig = []string{"file"}
		cfg := pkglogger.LoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
			Timeout:               time.Second,
			MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
			File:                  base,
			Stdout:                stdoutCfg,
			InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
				File: internalFile,
			},
		}

		created, err := NewLogger(context.Background(), cfg, streamrouting.None, nil, nil, tracer, meter)
		require.NoError(t, err)
		owned := created.(*topLevelLoggerRuntime)
		require.Nil(t, owned.fileLoggerSet)
		require.Nil(t, owned.internalLogDestinations.parentOwnedFileLoggerSet)
		require.NotNil(t, owned.internalLogDestinations.runtimeOwnedFileLoggerSet)
		require.Same(
			t,
			owned.internalLogDestinations.runtimeOwnedFileLoggerSet,
			owned.internalLogDestinations.fileLoggerSetForHandlers(),
		)
		require.Len(t, owned.appendLogMaintenanceRuntime.entries, 1)
		require.NoError(t, owned.Close())
		require.Empty(t, logFilesUnder(t, base.FsstoreConfig.Layout.LogPath))
	})

	t.Run("async", func(t *testing.T) {
		base := runnerFileConfigForInternalLogRuntime(t.TempDir(), "async-handler-only-base")
		internalFile := dedicatedInternalLogFileConfig(base, t.TempDir())
		stdoutCfg := newActivationStdoutConfig()
		stdoutCfg.HandlerConfig.OnErrorConfig = []string{"file"}
		cfg := pkglogger.AsyncBestEffortMultiLoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
			Timeout:               time.Second,
			MaxParallel:           1,
			MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
			File:                  base,
			Stdout:                stdoutCfg,
			InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
				File: internalFile,
			},
		}

		created, err := NewAsyncLogger(context.Background(), cfg, streamrouting.None, nil, nil, tracer, meter)
		require.NoError(t, err)
		owned := created.(*topLevelAsyncLoggerRuntime)
		require.Nil(t, owned.fileLoggerSet)
		require.Nil(t, owned.internalLogDestinations.parentOwnedFileLoggerSet)
		require.NotNil(t, owned.internalLogDestinations.runtimeOwnedFileLoggerSet)
		require.Same(
			t,
			owned.internalLogDestinations.runtimeOwnedFileLoggerSet,
			owned.internalLogDestinations.fileLoggerSetForHandlers(),
		)
		require.Len(t, owned.appendLogMaintenanceRuntime.entries, 1)
		require.NoError(t, owned.Close())
		require.Empty(t, logFilesUnder(t, base.FsstoreConfig.Layout.LogPath))
	})

	t.Run("delivery", func(t *testing.T) {
		base := runnerDeliveryFileConfigForInternalLogRuntime(
			t.TempDir(),
			"delivery-handler-only-base",
		)
		internalFile := dedicatedDeliveryInternalLogFileConfig(base, t.TempDir())
		stdoutCfg := newActivationDeliveryStdoutConfig()
		stdoutCfg.HandlerConfig.OnErrorConfig = []string{"file"}
		cfg := pkglogger.DeliveryLoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
			Timeout:               time.Second,
			MaxLogEntryBytes:      pkglogger.DefaultMaxLogEntryBytes,
			File:                  base,
			Stdout:                stdoutCfg,
			InternalLogDestinations: &pkglogger.DeliveryInternalLogDestinationConfig{
				File: internalFile,
			},
		}

		created, err := NewDeliveryLogger(context.Background(), cfg, streamrouting.None, tracer, meter)
		require.NoError(t, err)
		owned := created.(*topLevelDeliveryLoggerRuntime)
		require.Nil(t, owned.fileLoggerSet)
		require.Nil(t, owned.internalLogDestinations.parentOwnedFileLoggerSet)
		require.NotNil(t, owned.internalLogDestinations.runtimeOwnedFileLoggerSet)
		require.Same(
			t,
			owned.internalLogDestinations.runtimeOwnedFileLoggerSet,
			owned.internalLogDestinations.fileLoggerSetForHandlers(),
		)
		require.Len(t, owned.appendLogMaintenanceRuntime.entries, 1)
		require.NoError(t, owned.Close())
		require.Empty(t, logFilesUnder(t, base.FsstoreConfig.Layout.LogPath))
	})
}

func TestInternalLogFileLifecycleRejectsOverlappingDirectoryOwnership(t *testing.T) {
	t.Run("sync debug directory", func(t *testing.T) {
		root := t.TempDir()
		base := runnerFileConfigForInternalLogRuntime(root, "debug-sync-runtime")
		base.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
		base.FsstoreConfig.Layout.DebugLogDir = "shared-debug"
		base.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
		internalFile := dedicatedInternalLogFileConfig(base, root)
		internalFile.FsstoreConfig.Layout.LogDir = "internal-logs"

		_, err := logFileMaintenanceRunnerSpecs(base, internalFile)
		requireLifecycleDirectoryOverlap(t, err, "base file", "internal log file")
	})

	t.Run("delivery debug directory", func(t *testing.T) {
		root := t.TempDir()
		base := runnerDeliveryFileConfigForInternalLogRuntime(root, "debug-delivery-runtime")
		base.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
		base.FsstoreConfig.Layout.DebugLogDir = "shared-debug"
		base.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
		internalFile := dedicatedDeliveryInternalLogFileConfig(base, root)
		internalFile.FsstoreConfig.Layout.LogDir = "internal-logs"

		_, err := deliveryFileMaintenanceRunnerSpecs(base, internalFile)
		requireLifecycleDirectoryOverlap(t, err, "base delivery file", "delivery internal log file")
	})

	t.Run("sync same normalized directory", func(t *testing.T) {
		root := t.TempDir()
		base := runnerFileConfigForInternalLogRuntime(root, "same-sync-runtime")
		internalFile := dedicatedInternalLogFileConfig(
			base,
			filepath.Join(root, "unused", ".."),
		)

		_, err := logFileMaintenanceRunnerSpecs(base, internalFile)
		requireLifecycleDirectoryOverlap(t, err, "base file", "internal log file")
	})

	t.Run("sync parent and child directory", func(t *testing.T) {
		root := t.TempDir()
		base := runnerFileConfigForInternalLogRuntime(root, "overlap-sync-runtime")
		internalFile := dedicatedInternalLogFileConfig(base, root)
		internalFile.FsstoreConfig.Layout.Target = base.FsstoreConfig.Layout.Target
		internalFile.FsstoreConfig.Layout.LogDir =
			filepath.Join(base.FsstoreConfig.Layout.LogDir, "internal")

		_, err := logFileMaintenanceRunnerSpecs(base, internalFile)
		requireLifecycleDirectoryOverlap(t, err, "base file", "internal log file")
	})

	t.Run("delivery same normalized directory", func(t *testing.T) {
		root := t.TempDir()
		base := runnerDeliveryFileConfigForInternalLogRuntime(
			root,
			"same-delivery-runtime",
		)
		internalFile := dedicatedDeliveryInternalLogFileConfig(
			base,
			filepath.Join(root, "unused", ".."),
		)

		_, err := deliveryFileMaintenanceRunnerSpecs(base, internalFile)
		requireLifecycleDirectoryOverlap(t, err, "base delivery file", "delivery internal log file")
	})
}

// requireLifecycleDirectoryOverlapは、errがownerとcandidateのdirectory重複を表すことを確認します。
func requireLifecycleDirectoryOverlap(t *testing.T, err error, owner, candidate string) {
	t.Helper()
	require.ErrorIs(t, err, ErrLifecycleDirectoryOverlap)
	var overlapErr *LifecycleDirectoryOverlapError
	require.ErrorAs(t, err, &overlapErr)
	require.Equal(t, owner, overlapErr.Owner)
	require.Equal(t, candidate, overlapErr.Candidate)
}

func TestLogFileMaintenanceRunnerSpecsUseUTCClockIndependentOfDisplayTimezone(t *testing.T) {
	const layout = "2006/01/02 15:04:05 -07:00"

	base := runnerFileConfigForInternalLogRuntime(t.TempDir(), "clock-base")
	base.TimeConfig = pkglogger.TimeConfig{
		IsLocalTimeEnabled: true,
		TimeZone:           "Asia/Tokyo",
	}
	base.LogBodyConfig.LogLineDateFormat = layout

	internalFile := dedicatedInternalLogFileConfig(base, t.TempDir())
	internalFile.TimeConfig.TimeZone = "America/New_York"

	specs, err := logFileMaintenanceRunnerSpecs(base, internalFile)
	require.NoError(t, err)
	require.Len(t, specs, 2)
	require.Equal(t, time.UTC, specs[0].timeSource.Now().Location())
	require.Equal(t, time.UTC, specs[1].timeSource.Now().Location())
}

func TestBuildLogFileMaintenanceRuntimeUsesUTCOccurrenceClockForReporters(t *testing.T) {
	base := runnerFileConfigForInternalLogRuntime(t.TempDir(), "reporter-clock-base")
	base.TimeConfig = pkglogger.TimeConfig{
		IsLocalTimeEnabled: true,
		TimeZone:           "Asia/Tokyo",
	}
	internalFile := dedicatedInternalLogFileConfig(base, t.TempDir())
	internalFile.TimeConfig.TimeZone = "America/New_York"
	internalFile.LifecycleHandlerConfig = base.LifecycleHandlerConfig

	runtime, err := buildLogFileMaintenanceRuntime(
		context.Background(),
		base,
		internalFile,
		func() handlerStruct {
			return handlerStruct{OnInfo: []logging.NamedLoggerHandler{{
				Name: "file",
				Handler: func(
					context.Context,
					*logging.LogEntry,
					observer.Tracer,
					observer.Meter,
				) {
				},
			}}}
		},
		newFallbackHandlerCloserOwner(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	require.Len(t, runtime.entries, 2)

	baseNotifier, ok := runtime.entries[0].runner.Reporter.(*appendLogMaintenanceResultNotifier)
	require.True(t, ok)
	internalNotifier, ok := runtime.entries[1].runner.Reporter.(*appendLogMaintenanceResultNotifier)
	require.True(t, ok)
	require.Equal(t, time.UTC, baseNotifier.clock.Now().Location())
	require.Equal(t, time.UTC, internalNotifier.clock.Now().Location())
}

func TestDeliveryFileMaintenanceRunnerSpecsUseUTCClockIndependentOfDisplayTimezone(t *testing.T) {
	const layout = "02-01-2006 15:04 -07:00"

	base := runnerDeliveryFileConfigForInternalLogRuntime(t.TempDir(), "delivery-clock-base")
	base.TimeConfig = pkglogger.TimeConfig{
		IsLocalTimeEnabled: true,
		TimeZone:           "Europe/London",
	}
	base.LogBodyConfig.LogLineDateFormat = layout

	internalFile := dedicatedDeliveryInternalLogFileConfig(base, t.TempDir())
	internalFile.TimeConfig.TimeZone = "Asia/Tokyo"

	specs, err := deliveryFileMaintenanceRunnerSpecs(base, internalFile)
	require.NoError(t, err)
	require.Len(t, specs, 2)
	require.Equal(t, time.UTC, specs[0].timeSource.Now().Location())
	require.Equal(t, time.UTC, specs[1].timeSource.Now().Location())
}

func TestLogFileMaintenanceRunnerSpecsDoNotResolveDisplayTimezone(t *testing.T) {
	base := runnerFileConfigForInternalLogRuntime(t.TempDir(), "invalid-clock")
	base.TimeConfig = pkglogger.TimeConfig{
		IsLocalTimeEnabled: true,
		TimeZone:           "invalid/timezone",
	}

	// 表示用timezoneの検証はlogger設定側の責務で、runner用Clockの生成とは独立です。
	specs, err := logFileMaintenanceRunnerSpecs(base, nil)
	require.NoError(t, err)
	require.Len(t, specs, 1)
	require.Equal(t, time.UTC, specs[0].timeSource.Now().Location())
	require.Equal(t, "invalid/timezone", base.TimeConfig.TimeZone)
}

func TestAppendLogMaintenanceRuntimeErrorIdentifiesEachDirectoryOwner(t *testing.T) {
	// ownerはproductionが解決した物理pathを含むため、期待値も解決済みpathから作ります。
	base := runnerFileConfigForInternalLogRuntime(
		resolvedTempDirForTest(t),
		"runtime-error-owner",
	)
	base.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	base.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
	base.FsstoreConfig.Layout.DebugLogDir = "debug"
	base.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true

	specs, err := logFileMaintenanceRunnerSpecs(base, nil)
	require.NoError(t, err)
	runtime, err := newLogFileMaintenanceRuntime(
		specs,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	require.Len(t, runtime.entries, 3)

	layout := base.FsstoreConfig.Layout
	defaultDirectory := pathidentity.AbsoluteOrCleanPath(filepath.Join(
		layout.LogPath,
		layout.Target,
		layout.LogDir,
	))
	errorDirectory := pathidentity.AbsoluteOrCleanPath(filepath.Join(
		layout.LogPath,
		layout.Target,
		layout.ErrorLogDir,
	))
	debugDirectory := pathidentity.AbsoluteOrCleanPath(filepath.Join(
		layout.LogPath,
		layout.Target,
		layout.DebugLogDir,
	))
	require.Equal(
		t,
		"base file:"+defaultDirectory,
		runtime.entries[0].owner,
	)
	require.Equal(
		t,
		"base file:"+errorDirectory,
		runtime.entries[1].owner,
	)
	require.Equal(t, "base file:"+debugDirectory, runtime.entries[2].owner)

	// 同じconcrete typeのrunnerをすべて失敗させ、起動前reconcileの集約errorだけで
	// default/error/debug directoryを区別できることを検証します。
	for index := range runtime.entries {
		runtime.entries[index].runner = nil
	}
	startErr := runtime.Start(context.Background())
	require.ErrorIs(t, startErr, logfilerunner.ErrNilRunner)
	var owners []string
	for _, runnerErr := range joinedErrorsOfType[*AppendLogMaintenanceRunnerError](startErr) {
		require.Equal(t, "reconcile object storage upload destinations", runnerErr.Op)
		require.ErrorIs(t, runnerErr, logfilerunner.ErrNilRunner)
		owners = append(owners, runnerErr.Owner)
	}
	require.Equal(
		t,
		[]string{runtime.entries[0].owner, runtime.entries[1].owner, runtime.entries[2].owner},
		owners,
	)
	require.NoError(t, runtime.Close())
}

// joinedErrorsOfTypeはJoinされたerrorを辿り、T型のerrorを出現順にすべて返します。
// errors.Asは最初の一件だけを返すため、同じ型の複数のerrorの識別に使います。
// T型のerrorの内側は辿りません。
func joinedErrorsOfType[T error](err error) []T {
	if typed, ok := err.(T); ok {
		return []T{typed}
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		var found []T
		for _, cause := range wrapped.Unwrap() {
			found = append(found, joinedErrorsOfType[T](cause)...)
		}
		return found
	case interface{ Unwrap() error }:
		return joinedErrorsOfType[T](wrapped.Unwrap())
	default:
		return nil
	}
}

func readLifecycleLogBody(t *testing.T, layout pkgfsstore.FsstoreConfig) string {
	t.Helper()
	body := readLifecycleLogBodyIfPresent(t, layout)
	require.NotEmpty(t, body)
	return body
}

func readLifecycleLogBodyIfPresent(t *testing.T, layout pkgfsstore.FsstoreConfig) string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(
		layout.LogPath,
		layout.Target,
		layout.LogDir,
		layout.Target+"_*.log",
	))
	require.NoError(t, err)
	if len(files) == 0 {
		return ""
	}
	require.Len(t, files, 1)
	body, err := os.ReadFile(files[0])
	require.NoError(t, err)
	return string(body)
}

func lifecycleRunnerDirectoryCount(layout pkgfsstore.FsstoreConfig) int {
	directories := map[string]struct{}{
		filepath.Clean(filepath.Join(layout.LogPath, layout.Target, layout.LogDir)): {},
	}
	if layout.IsSeparateErrorLogsEnabled {
		directories[filepath.Clean(
			filepath.Join(layout.LogPath, layout.Target, layout.ErrorLogDir),
		)] = struct{}{}
	}
	if layout.IsSeparateDebugLogsEnabled {
		directories[filepath.Clean(filepath.Join(
			layout.LogPath,
			layout.Target,
			appendlog.NewFsstoreConfigAdapter(layout).DebugLogDir(),
		))] = struct{}{}
	}
	return len(directories)
}

func runnerFileConfigForInternalLogRuntime(root, target string) *pkglogger.FileLoggerConfig {
	return &pkglogger.FileLoggerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:         target,
				LogPath:        root,
				LogDir:         "logs",
				ErrorLogDir:    "errors",
				FileNameFormat: pkgfsstore.FileNameFormat,
				FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				EnableRotate:       true,
				RotationType:       pkgfsstore.FileRotationDaily,
				MaxFileBackups:     infrafsstore.MinAllowedMaxFileBackups,
				MaxFileAge:         infrafsstore.MinAllowedMaxFileAge,
				MaxFileSize:        infrafsstore.MinAllowedFileSizeBytes,
				MaxDirSize:         infrafsstore.MinAllowedMaxDirSize,
				UsageCacheTTL:      time.Hour,
				EnableFileCompress: true,
				CompressFormat:     pkgfsstore.CompressGzip,
			},
			Duration: pkgfsstore.DurationConfig{
				CompressWarnThresholdDuration:   time.Millisecond,
				CompressErrorThresholdDuration:  time.Millisecond,
				WriteFileWarnThresholdDuration:  time.Millisecond,
				WriteFileErrorThresholdDuration: time.Millisecond,
			},
			Retry: pkgfsstore.RetryConfig{
				MaxWriteAttempts:           1,
				FileOperationRetryWaitTime: time.Millisecond,
			},
		},
		LifecycleMode: pkgfsstore.FileLifecycleModeRunner,
		LifecycleRunnerSettings: pkgfsstore.AppendLogRunnerConfig{
			Interval: time.Minute,
		},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "internal-log-runtime-test",
			Target:   target,
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		SamplingConfig: pkglogger.SamplingConfig{
			LoggingByRequestIDRate: 1,
			SamplingDebugRate:      1,
			SamplingInfoRate:       1,
			SamplingWarnRate:       1,
			SamplingErrorRate:      1,
		},
		LifecycleHandlerConfig: pkgloggerhandler.HandlerConfig{
			OnInfoConfig: []string{"file"},
		},
	}
}

func dedicatedInternalLogFileConfig(
	base *pkglogger.FileLoggerConfig,
	root string,
) *pkglogger.FileLoggerConfig {
	internalFile := *base
	internalFile.FsstoreConfig.Layout.LogPath = root
	internalFile.LifecycleHandlerConfig = pkgloggerhandler.HandlerConfig{}
	return &internalFile
}

func runnerDeliveryFileConfigForInternalLogRuntime(
	root string,
	target string,
) *pkglogger.DeliveryFileLoggerConfig {
	return &pkglogger.DeliveryFileLoggerConfig{
		FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{
			Layout: pkgfsstore.FsstoreConfig{
				Target:         target,
				LogPath:        root,
				LogDir:         "logs",
				ErrorLogDir:    "errors",
				FileNameFormat: pkgfsstore.FileNameFormat,
				FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Rotation: pkgfsstore.FileRotationConfig{
				EnableRotate: true,
				RotationType: pkgfsstore.FileRotationDaily,
				MaxFileSize:  infrafsstore.MinAllowedFileSizeBytes,
			},
			Duration: pkgfsstore.DurationConfig{
				WriteFileWarnThresholdDuration:  time.Millisecond,
				WriteFileErrorThresholdDuration: time.Millisecond,
			},
			Retry: pkgfsstore.RetryConfig{
				MaxWriteAttempts:           1,
				FileOperationRetryWaitTime: time.Millisecond,
			},
		},
		LifecycleMode: pkgfsstore.FileLifecycleModeRunner,
		LifecycleRunnerSettings: pkglogger.DeliveryFileLifecycleRunnerSettings{
			MaxFileBackups:                 infrafsstore.MinAllowedMaxFileBackups,
			MaxFileAge:                     infrafsstore.MinAllowedMaxFileAge,
			MaxDirSize:                     infrafsstore.MinAllowedMaxDirSize,
			UsageCacheTTL:                  time.Hour,
			EnableFileCompress:             true,
			CompressFormat:                 pkgfsstore.CompressGzip,
			CompressWarnThresholdDuration:  time.Millisecond,
			CompressErrorThresholdDuration: time.Millisecond,
			RunnerConfig: pkgfsstore.AppendLogRunnerConfig{
				Interval: time.Minute,
			},
			LifecycleHandlerConfig: pkgloggerhandler.HandlerConfig{
				OnInfoConfig: []string{"file"},
			},
		},
		RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
			AppName: "internal-log-runtime-test",
			Target:  target,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
	}
}

func dedicatedDeliveryInternalLogFileConfig(
	base *pkglogger.DeliveryFileLoggerConfig,
	root string,
) *pkglogger.DeliveryFileLoggerConfig {
	internalFile := *base
	internalFile.FsstoreConfig.Layout.LogPath = root
	internalFile.LifecycleRunnerSettings.LifecycleHandlerConfig =
		pkgloggerhandler.HandlerConfig{}
	return &internalFile
}

func assertInternalLogLifecycleCompresses(
	t *testing.T,
	runtime *appendLogMaintenanceRuntime,
	layout pkgfsstore.FsstoreConfig,
) {
	t.Helper()

	directory := filepath.Join(layout.LogPath, layout.Target, layout.LogDir)
	require.NoError(t, os.MkdirAll(directory, 0o750))
	date := time.Now().Format(pkgfsstore.FileDateFormat)
	oldPath := filepath.Join(directory, layout.Target+"_"+date+"_1.log")
	latestPath := filepath.Join(directory, layout.Target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(oldPath, []byte("old"), 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("latest"), 0o600))

	for _, entry := range runtime.entries {
		_, err := entry.runner.RunOnce(context.Background())
		require.NoError(t, err)
	}
	require.NoFileExists(t, oldPath)
	require.FileExists(t, oldPath+pkgfsstore.ExtensionCompressGzip)
}
