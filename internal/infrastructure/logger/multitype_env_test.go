package logger

import (
	"context"
	"github.com/go-jappe/heterolog/internal/testutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	appconfig "github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/pkg/disk"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiTypeLoggersUseConfigFixture(t *testing.T) {
	appCfg, envFile := loadConfigFixtureForLoggerTest(t)
	requireConfigFixtureDiskSpaceForLoggerTest(t, appCfg)
	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	runID := time.Now().Format("20060102T150405.000000000")
	syncMessage := "env sync multi logger file and stdout send " + runID
	asyncMessage := "env async multi logger file and stdout send " + runID
	deliveryMessage := "env delivery multi logger file and stdout send " + runID

	syncCfg := appconfig.ToLoggerConfig(appCfg)
	expectedSyncTimeout := time.Duration(requiredDotEnvIntForLoggerTest(t, envFile, "HETEROLOG_LOG_TIMEOUT_SECOND")) * time.Second
	require.Equal(t, expectedSyncTimeout, syncCfg.Timeout)

	syncLogPath := filepath.Join(t.TempDir(), "sync")
	syncCfg = fileSlackAndStdoutLoggerConfig(t, appCfg, syncCfg, syncLogPath)
	requireSlackLoggerConfigForTest(t, syncCfg.Slack)
	// Slack's actual-.env mapping is verified above and its HTTP boundary has a
	// dedicated mock-transport test. Keep this multi-logger I/O test hermetic.
	syncCfg.Slack = nil
	syncCfg.LogOutputDestinations = pkglogger.LogOutputDestinations{
		pkglogger.LogOutputFile,
		pkglogger.LogOutputStdout,
	}
	require.Equal(t, expectedSyncTimeout, NewMultiLoggerAdapter(syncCfg).Timeout())

	syncLogger, err := NewLogger(
		ctx,
		syncCfg,
		appCfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity,
		nil,
		nil,
		tracer,
		meter,
	)
	require.NoError(t, err)
	require.Implements(t, (*applogging.CloseableLogger)(nil), syncLogger)
	require.NoError(t, syncLogger.Error(ctx, syncMessage))
	closeLoggerForTest(t, syncLogger)
	waitForFileContentForLoggerTest(t, syncLogPath, syncMessage)

	asyncCfg := appconfig.ToAsyncBestEffortMultiLoggerConfig(appCfg)
	expectedAsyncTimeout := time.Duration(requiredDotEnvIntForLoggerTest(t, envFile, "HETEROLOG_ASYNC_LOG_TIMEOUT_SECOND")) * time.Second
	expectedAsyncMaxParallel := requiredDotEnvIntForLoggerTest(t, envFile, "HETEROLOG_MAX_ASYNC_LOG_PARALLEL")
	require.Equal(t, expectedAsyncTimeout, asyncCfg.Timeout)
	require.Equal(t, expectedAsyncMaxParallel, asyncCfg.MaxParallel)

	asyncLogPath := filepath.Join(t.TempDir(), "async")
	asyncCfg = fileSlackAndStdoutAsyncLoggerConfig(t, appCfg, asyncCfg, asyncLogPath)
	requireSlackLoggerConfigForTest(t, asyncCfg.Slack)
	asyncCfg.Slack = nil
	asyncCfg.LogOutputDestinations = pkglogger.LogOutputDestinations{
		pkglogger.LogOutputFile,
		pkglogger.LogOutputStdout,
	}
	asyncAdapter := NewAsyncBestEffortMultiLoggerAdapter(asyncCfg)
	require.Equal(t, expectedAsyncTimeout, asyncAdapter.Timeout())
	require.Equal(t, expectedAsyncMaxParallel, asyncAdapter.MaxParallel())

	asyncLogger, err := NewAsyncLogger(
		ctx,
		asyncCfg,
		appCfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogStreamRoutingGranularity,
		nil,
		nil,
		tracer,
		meter,
	)
	require.NoError(t, err)
	require.Implements(t, (*applogging.AsyncCloseableLogger)(nil), asyncLogger)
	asyncLogger.Error(ctx, asyncMessage)
	// Closeは受理済みの非同期処理を待ってからfileを閉じます。閉じずに終えると
	// t.TempDir配下のfileが開いたまま残り、Windowsでは後片付けにも失敗します。
	closeLoggerForTest(t, asyncLogger)
	waitForFileContentForLoggerTest(t, asyncLogPath, asyncMessage)

	deliveryCfg := appconfig.ToDeliveryLoggerConfig(appCfg)
	expectedDeliveryTimeout := time.Duration(requiredDotEnvIntForLoggerTest(t, envFile, "HETEROLOG_DELIVERY_LOG_TIMEOUT_SECOND")) * time.Second
	require.Equal(t, expectedDeliveryTimeout, deliveryCfg.Timeout)

	deliveryLogPath := filepath.Join(t.TempDir(), "delivery")
	deliveryCfg = fileSlackAndStdoutDeliveryLoggerConfig(t, appCfg, deliveryCfg, deliveryLogPath)
	requireDeliverySlackLoggerConfigForTest(t, deliveryCfg.Slack)
	deliveryCfg.Slack = nil
	deliveryCfg.LogOutputDestinations = pkglogger.LogOutputDestinations{
		pkglogger.LogOutputFile,
		pkglogger.LogOutputStdout,
	}
	require.Equal(t, expectedDeliveryTimeout, NewDeliveryMultiLoggerAdapter(deliveryCfg).Timeout())

	deliveryLogger, err := NewDeliveryLogger(
		ctx,
		deliveryCfg,
		appCfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogStreamRoutingGranularity,
		tracer,
		meter,
	)
	require.NoError(t, err)
	require.Implements(t, (*applogging.DeliveryClosableLogger)(nil), deliveryLogger)

	report, err := deliveryLogger.Error(ctx, deliveryMessage)
	require.NoError(t, err)
	assert.True(t, report.HasRegisteredTarget())
	closeLoggerForTest(t, deliveryLogger)
	waitForFileContentForLoggerTest(t, deliveryLogPath, deliveryMessage)
}

func fileSlackAndStdoutLoggerConfig(t *testing.T, appCfg appconfig.AppConfig, cfg pkglogger.LoggerConfig, logPath string) pkglogger.LoggerConfig {
	t.Helper()

	require.NotNil(t, cfg.File)
	cfg.File.FsstoreConfig.Layout.LogPath = logPath
	cfg.File.FsstoreConfig.Lifecycle.EnableRotate = false
	cfg.File.FsstoreConfig.Lifecycle.EnableFileCompress = false
	cfg.File.SamplingConfig.IsSamplingEnabled = false
	cfg.File.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
	cfg.File.HandlerConfig = pkgloggerhandler.HandlerConfig{}
	cfg.File.LifecycleHandlerConfig = pkgloggerhandler.HandlerConfig{}
	cfg.OpenSearch = nil
	cfg.OpenSearchClient = nil
	cfg.Loki = nil
	cfg.LokiClient = nil
	cfg.Stdout = appconfig.NewStdoutLoggerConfig(&appCfg)
	cfg.Stdout.HandlerConfig = pkgloggerhandler.HandlerConfig{}
	cfg.Resource = appconfig.NewOtelResourceConfig(&appCfg)
	return cfg
}

func fileSlackAndStdoutAsyncLoggerConfig(t *testing.T, appCfg appconfig.AppConfig, cfg pkglogger.AsyncBestEffortMultiLoggerConfig, logPath string) pkglogger.AsyncBestEffortMultiLoggerConfig {
	t.Helper()

	require.NotNil(t, cfg.File)
	cfg.File.FsstoreConfig.Layout.LogPath = logPath
	cfg.File.FsstoreConfig.Lifecycle.EnableRotate = false
	cfg.File.FsstoreConfig.Lifecycle.EnableFileCompress = false
	cfg.File.SamplingConfig.IsSamplingEnabled = false
	cfg.File.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
	cfg.File.HandlerConfig = pkgloggerhandler.HandlerConfig{}
	cfg.File.LifecycleHandlerConfig = pkgloggerhandler.HandlerConfig{}
	cfg.OpenSearch = nil
	cfg.OpenSearchClient = nil
	cfg.Loki = nil
	cfg.LokiClient = nil
	cfg.Stdout = appconfig.NewAsyncStdoutLoggerConfig(&appCfg)
	cfg.Stdout.HandlerConfig = pkgloggerhandler.HandlerConfig{}
	cfg.Resource = appconfig.NewOtelResourceConfig(&appCfg)
	return cfg
}

func fileSlackAndStdoutDeliveryLoggerConfig(t *testing.T, appCfg appconfig.AppConfig, cfg pkglogger.DeliveryLoggerConfig, logPath string) pkglogger.DeliveryLoggerConfig {
	t.Helper()

	require.NotNil(t, cfg.File)
	cfg.File.FsstoreConfig.Layout.LogPath = logPath
	cfg.File.FsstoreConfig.Rotation.EnableRotate = false
	cfg.File.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
	cfg.File.HandlerConfig = pkgloggerhandler.HandlerConfig{}
	cfg.File.LifecycleRunnerSettings.LifecycleHandlerConfig = pkgloggerhandler.HandlerConfig{}
	cfg.OpenSearch = nil
	cfg.OpenSearchClient = nil
	cfg.Loki = nil
	cfg.LokiClient = nil
	cfg.Stdout = appconfig.NewDeliveryStdoutLoggerConfig(&appCfg)
	cfg.Stdout.HandlerConfig = pkgloggerhandler.HandlerConfig{}
	cfg.Resource = appconfig.NewOtelResourceConfig(&appCfg)
	return cfg
}

func requireSlackLoggerConfigForTest(t *testing.T, cfg *pkglogger.SlackLoggerConfig) {
	t.Helper()

	require.NotNil(t, cfg)
	require.NotEmpty(t, cfg.Token)
	require.NotEmpty(t, cfg.Channel)
	require.NotEmpty(t, cfg.ErrorChannel)
}

func requireDeliverySlackLoggerConfigForTest(t *testing.T, cfg *pkglogger.DeliverySlackLoggerConfig) {
	t.Helper()

	require.NotNil(t, cfg)
	require.NotEmpty(t, cfg.Token)
	require.NotEmpty(t, cfg.Channel)
	require.NotEmpty(t, cfg.ErrorChannel)
}

func waitForFileContentForLoggerTest(t *testing.T, root string, expected string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		found, err := fileTreeContainsForLoggerTest(root, expected)
		require.NoError(t, err)
		if found {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("expected log content %q under %s, but it was not written", expected, root)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func fileTreeContainsForLoggerTest(root string, expected string) (bool, error) {
	found := false
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".log" {
			return nil
		}

		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), expected) {
			found = true
			return filepath.SkipAll
		}
		return nil
	})
	return found, err
}

func loadConfigFixtureForLoggerTest(t *testing.T) (appconfig.AppConfig, map[string]string) {
	t.Helper()

	return loadConfigFixtureForLoggerTestWithOverrides(t, nil)
}

func loadConfigFixtureForLoggerTestWithOverrides(t *testing.T, overrides map[string]string) (appconfig.AppConfig, map[string]string) {
	t.Helper()

	repoRoot := configFixtureRootForLoggerTest(t)
	envPath := filepath.Join(repoRoot, ".env")
	envFile, err := godotenv.Read(envPath)
	require.NoError(t, err)

	// 設定loaderが読む環境変数だけを外し、PATH・HOME・TMP・SystemRootなどの実行環境は
	// 残します。終了時に環境全体を元へ戻します。
	testutil.UnsetConfigEnvironment(t)
	require.NoError(t, os.Setenv("HETEROLOG_ENV_PATH", envPath))
	// logger構築時のcheckpoint保存先は、明示指定がなければ試験ごとに分離します。
	// 設定例にある運用向けの既定directoryへ書き込みません。
	require.NoError(t, os.Setenv("HETEROLOG_APP_STATE_DIR", t.TempDir()))
	for key, value := range overrides {
		require.NoError(t, os.Setenv(key, value))
	}

	cfg, err := appconfig.LoadConfig()
	require.NoError(t, err)

	return cfg, envFile
}

// requireConfigFixtureDiskSpaceForLoggerTestは、設定例の空き容量の閾値（HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB）を
// 試験の一時directoryがあるfilesystemが満たすことを確かめます。下回るとfile loggerは仕様どおり
// 書込みをdropし、fileが空という原因の分からない形で試験が失敗するため、先に不足を示して止めます。
// 設定例のままfile loggerの出力を確かめる試験だけが呼び出します。
func requireConfigFixtureDiskSpaceForLoggerTest(t *testing.T, cfg appconfig.AppConfig) {
	t.Helper()
	minimum := cfg.FsstoreConfig.MinAvailableDiskSpace
	if minimum == 0 {
		return
	}
	directory := t.TempDir()
	free, _, err := disk.FreeAndTotal(directory)
	require.NoError(t, err)
	if free < minimum {
		t.Fatalf("the filesystem of %q has %d bytes available, below HETEROLOG_MIN_AVAILABLE_DISK_SPACE_GB of the config fixture (%d bytes), "+
			"so the file logger would drop writes; run the tests with TMPDIR (or GOTMPDIR, which t.TempDir prefers) "+
			"on a filesystem with more free space", directory, free, minimum)
	}
}

func configFixtureRootForLoggerTest(t *testing.T) string {
	t.Helper()
	return testutil.ConfigFixtureRoot(t)
}

func requiredDotEnvIntForLoggerTest(t *testing.T, envFile map[string]string, key string) int {
	t.Helper()

	raw, ok := envFile[key]
	if !ok {
		t.Fatalf("environment file is missing %s", key)
	}

	value, err := strconv.Atoi(raw)
	require.NoError(t, err)
	return value
}

func requiredDotEnvStringForLoggerTest(t *testing.T, envFile map[string]string, key string) string {
	t.Helper()

	raw, ok := envFile[key]
	if !ok {
		t.Fatalf("environment file is missing %s", key)
	}
	return raw
}

func closeLoggerForTest(t *testing.T, logger any) {
	t.Helper()

	closeable, ok := logger.(interface {
		Close() error
	})
	if !ok {
		return
	}

	require.NoError(t, closeable.Close())
}
