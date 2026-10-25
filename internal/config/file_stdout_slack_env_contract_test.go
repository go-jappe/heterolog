package config

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/config/internalerror"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	configslack "github.com/go-jappe/heterolog/internal/config/slack"
	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	infralogger "github.com/go-jappe/heterolog/internal/infrastructure/logger"
	filelogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	slacklogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/slack"
	stdoutlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgslack "github.com/go-jappe/heterolog/pkg/slack"
)

// TestLoadConfigKeepsFileStdoutSlackContractsSeparated gives each LoggerContractKind
// deliberately conflicting values. This prevents a wrong env key or mapper
// source from passing only because the normal .env happens to reuse values.
func TestLoadConfigKeepsFileStdoutSlackContractsSeparated(t *testing.T) {
	root := t.TempDir()
	setup := []string{
		"HETEROLOG_APP_NAME=file-stdout-slack-contract-test",
		"HETEROLOG_TARGET=file-stdout-slack-contract-target",
		"HETEROLOG_SYSTEM_LOG_DIR=" + filepath.Join(root, "log"),
		"HETEROLOG_SYSTEM_LIB_DIR=" + filepath.Join(root, "lib"),
		"HETEROLOG_APP_STATE_DIR=" + filepath.Join(root, "state"),
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		// object storage uploadは既定どおり無効のまま、この試験の対象から外します。
		"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD=false",
		"HETEROLOG_ENABLE_ASYNC_LOG_FILE_OBJECT_STORAGE_UPLOAD=false",
		"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_OBJECT_STORAGE_UPLOAD=false",
	}
	// 各行は既定値と異なる値にし、全てのkeyが読込結果に現れるようにします。
	contractLines := []string{
		"HETEROLOG_LOG_TYPE=file,stdout,slack",
		"HETEROLOG_ASYNC_LOG_TYPE=file,stdout,slack",
		"HETEROLOG_DELIVERY_LOG_TYPE=file,stdout,slack",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_LOG_TIMEOUT_SECOND=7",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_SECOND=8",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_SECOND=9",
		"HETEROLOG_MAX_ASYNC_LOG_PARALLEL=13",

		"HETEROLOG_FILE_LOG_LEVEL=DEBUG",
		"HETEROLOG_MAX_LOG_SIZE_MB=11",
		"HETEROLOG_MAX_LOG_FILE_DIR_SIZE_MB=111",
		"HETEROLOG_LOG_DIR=sync-log",
		"HETEROLOG_ERROR_LOG_DIR=sync-error-log",
		"HETEROLOG_WRITE_LOG_FILE_LATENCY_ENABLED=false",
		"HETEROLOG_FILE_LOGGER_ERROR_HANDLER=stdout",
		"HETEROLOG_FILE_LOGGER_WARN_HANDLER=slack",
		"HETEROLOG_FILE_LOGGER_INFO_HANDLER=file",
		"HETEROLOG_FILE_LOGGER_DEBUG_HANDLER=stdout,slack",

		"HETEROLOG_ASYNC_FILE_LOG_LEVEL=ERROR",
		"HETEROLOG_MAX_ASYNC_LOG_SIZE_MB=22",
		"HETEROLOG_MAX_ASYNC_LOG_FILE_DIR_SIZE_MB=222",
		"HETEROLOG_ASYNC_LOG_DIR=async-log",
		"HETEROLOG_ASYNC_ERROR_LOG_DIR=async-error-log",
		"HETEROLOG_WRITE_ASYNC_LOG_FILE_LATENCY_ENABLED=false",
		"HETEROLOG_ASYNC_FILE_LOGGER_ERROR_HANDLER=slack",
		"HETEROLOG_ASYNC_FILE_LOGGER_WARN_HANDLER=file",
		"HETEROLOG_ASYNC_FILE_LOGGER_INFO_HANDLER=stdout",
		"HETEROLOG_ASYNC_FILE_LOGGER_DEBUG_HANDLER=file,slack",

		"HETEROLOG_MAX_DELIVERY_LOG_SIZE_MB=33",
		"HETEROLOG_MAX_DELIVERY_LOG_FILE_DIR_SIZE_MB=333",
		"HETEROLOG_DELIVERY_LOG_DIR=delivery-log",
		"HETEROLOG_DELIVERY_ERROR_LOG_DIR=delivery-error-log",
		"HETEROLOG_WRITE_DELIVERY_LOG_FILE_LATENCY_ENABLED=false",
		"HETEROLOG_DELIVERY_FILE_LOGGER_ERROR_HANDLER=stdout",
		"HETEROLOG_DELIVERY_FILE_LOGGER_WARN_HANDLER=slack",
		"HETEROLOG_DELIVERY_FILE_LOGGER_INFO_HANDLER=file",
		"HETEROLOG_DELIVERY_FILE_LOGGER_DEBUG_HANDLER=stdout,slack",

		"HETEROLOG_STDOUT_LOG_LEVEL=WARN",
		"HETEROLOG_ENABLE_LOG_STDOUT_SAMPLING=false",
		"HETEROLOG_ENABLE_LOGGING_STDOUT_BY_REQUEST_ID=false",
		"HETEROLOG_LOG_STDOUT_REQUEST_ID_RATE=17",
		"HETEROLOG_LOG_STDOUT_DEBUG_RATE=18",
		"HETEROLOG_LOG_STDOUT_INFO_RATE=19",
		"HETEROLOG_LOG_STDOUT_WARN_RATE=20",
		"HETEROLOG_LOG_STDOUT_ERROR_RATE=21",
		"HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER=file",
		"HETEROLOG_STDOUT_LOGGER_WARN_HANDLER=slack",
		"HETEROLOG_STDOUT_LOGGER_INFO_HANDLER=stdout",
		"HETEROLOG_STDOUT_LOGGER_DEBUG_HANDLER=file,stdout",
		"HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER=slack",
		"HETEROLOG_ASYNC_STDOUT_LOGGER_WARN_HANDLER=file",
		"HETEROLOG_ASYNC_STDOUT_LOGGER_INFO_HANDLER=stdout",
		"HETEROLOG_ASYNC_STDOUT_LOGGER_DEBUG_HANDLER=file,slack",
		"HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER=file,slack",
		"HETEROLOG_DELIVERY_STDOUT_LOGGER_WARN_HANDLER=stdout",
		"HETEROLOG_DELIVERY_STDOUT_LOGGER_INFO_HANDLER=file",
		"HETEROLOG_DELIVERY_STDOUT_LOGGER_DEBUG_HANDLER=slack",
		"HETEROLOG_ENABLE_STDOUT_OTEL_FORMAT=false",

		"HETEROLOG_SLACK_LOG_LEVEL=DEBUG",
		"HETEROLOG_SLACK_API_KEY=sync-token",
		"HETEROLOG_SLACK_TIMEOUT_SECOND=4",
		"HETEROLOG_SLACK_NORMAL_CHANNEL=sync-channel",
		"HETEROLOG_SLACK_ERROR_CHANNEL=sync-error-channel",
		"HETEROLOG_SLACK_WARN_CHANNEL=sync-warn-channel",
		"HETEROLOG_SLACK_INFO_CHANNEL=sync-info-channel",
		"HETEROLOG_SLACK_DEBUG_CHANNEL=sync-debug-channel",
		"HETEROLOG_MAX_LOGS_PER_SLACK_NOTIFICATION=23",
		"HETEROLOG_MAX_SLACK_REST_THRESHOLD_MINUTE=7",
		"HETEROLOG_SLACK_INTERVALS=1,2,3",
		"HETEROLOG_ENABLE_LOG_SLACK_SAMPLING=false",
		"HETEROLOG_ENABLE_LOGGING_SLACK_BY_REQUEST_ID=false",
		"HETEROLOG_LOG_SLACK_REQUEST_ID_RATE=24",
		"HETEROLOG_SLACK_LOGGER_ERROR_HANDLER=stdout",
		"HETEROLOG_SLACK_LOGGER_WARN_HANDLER=file",
		"HETEROLOG_SLACK_LOGGER_INFO_HANDLER=slack",
		"HETEROLOG_SLACK_LOGGER_DEBUG_HANDLER=file,stdout",

		"HETEROLOG_ASYNC_SLACK_LOG_LEVEL=WARN",
		"HETEROLOG_ASYNC_SLACK_API_KEY=async-token",
		"HETEROLOG_ASYNC_SLACK_TIMEOUT_SECOND=5",
		"HETEROLOG_ASYNC_SLACK_NORMAL_CHANNEL=async-channel",
		"HETEROLOG_ASYNC_SLACK_ERROR_CHANNEL=async-error-channel",
		"HETEROLOG_ASYNC_SLACK_WARN_CHANNEL=async-warn-channel",
		"HETEROLOG_ASYNC_SLACK_INFO_CHANNEL=async-info-channel",
		"HETEROLOG_ASYNC_SLACK_DEBUG_CHANNEL=async-debug-channel",
		"HETEROLOG_MAX_ASYNC_LOGS_PER_SLACK_NOTIFICATION=25",
		"HETEROLOG_MAX_ASYNC_SLACK_REST_THRESHOLD_MINUTE=11",
		"HETEROLOG_ASYNC_SLACK_INTERVALS=4,5,6",
		"HETEROLOG_ENABLE_ASYNC_LOG_SLACK_SAMPLING=false",
		"HETEROLOG_ENABLE_ASYNC_LOGGING_SLACK_BY_REQUEST_ID=false",
		"HETEROLOG_ASYNC_LOG_SLACK_REQUEST_ID_RATE=26",
		"HETEROLOG_ASYNC_SLACK_LOGGER_ERROR_HANDLER=file",
		"HETEROLOG_ASYNC_SLACK_LOGGER_WARN_HANDLER=stdout",
		"HETEROLOG_ASYNC_SLACK_LOGGER_INFO_HANDLER=slack",
		"HETEROLOG_ASYNC_SLACK_LOGGER_DEBUG_HANDLER=stdout,slack",

		"HETEROLOG_DELIVERY_SLACK_LOG_LEVEL=INFO",
		"HETEROLOG_DELIVERY_SLACK_API_KEY=delivery-token",
		"HETEROLOG_DELIVERY_SLACK_TIMEOUT_SECOND=6",
		"HETEROLOG_DELIVERY_SLACK_NORMAL_CHANNEL=delivery-channel",
		"HETEROLOG_DELIVERY_SLACK_ERROR_CHANNEL=delivery-error-channel",
		"HETEROLOG_DELIVERY_SLACK_WARN_CHANNEL=delivery-warn-channel",
		"HETEROLOG_DELIVERY_SLACK_INFO_CHANNEL=delivery-info-channel",
		"HETEROLOG_DELIVERY_SLACK_DEBUG_CHANNEL=delivery-debug-channel",
		"HETEROLOG_DELIVERY_SLACK_LOGGER_ERROR_HANDLER=stdout",
		"HETEROLOG_DELIVERY_SLACK_LOGGER_WARN_HANDLER=file",
		"HETEROLOG_DELIVERY_SLACK_LOGGER_INFO_HANDLER=slack",
		"HETEROLOG_DELIVERY_SLACK_LOGGER_DEBUG_HANDLER=file,stdout",
	}
	lines := append(append([]string(nil), setup...), contractLines...)
	cfg := loadConfigFromTempEnv(t, lines)

	assertFileStdoutSlackLoadedContracts(t, cfg)
	assertFileStdoutSlackLoadedEnvLines(t, cfg, contractLines)
	// 全ての行が読み込まれ、その行のLoggerContractKindの設定だけに効くことを確認します。
	assertEveryContractEnvLineChangesItsContract(t, lines, contractLines, fileStdoutSlackSharedEnvKeys)

	syncMapped := ToLoggerConfig(cfg)
	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	deliveryMapped := ToDeliveryLoggerConfig(cfg)
	assertFileStdoutSlackMappedContracts(t, syncMapped, asyncMapped, deliveryMapped)
	assertFileStdoutSlackInfrastructureAdapters(t, syncMapped, asyncMapped, deliveryMapped)
}

// TestDefaultEnvironmentConfigLogsToStdoutWithoutExternalServicesは、環境変数も.envも
// 与えない状態から既定構成を生成し、検証付きmapperを通したsync・async・deliveryの
// 各loggerが標準出力へログを出力できることを検証します。出力先の既定値はstdoutだけで、
// fileやSlackなどの設定・認証情報を暗黙に要求しません。
func TestDefaultEnvironmentConfigLogsToStdoutWithoutExternalServices(t *testing.T) {
	// Windowsでは一時directoryの決定に環境変数を使うため、消去前に作成します。
	workDirectory := t.TempDir()
	testutil.PreserveEnvironment(t)
	clearProcessEnvForTest(t)
	// 作業directoryの既定.envも存在しない状態で読み込みます。
	t.Chdir(workDirectory)

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with empty environment: %v", err)
	}
	if err := ValidateApplicationConfig(cfg); err != nil {
		t.Fatalf("ValidateApplicationConfig() with empty environment: %v", err)
	}
	assertStdoutOnlyLoadedDestinations(t, cfg)
	// 時刻もREADME・DefaultConfigと同じくUTCで記録し、ローカル時刻は明示した場合だけ使います。
	wantClock := logconfig.ClockConfig{IsLocalTimeEnabled: false, TimeZone: "UTC"}
	for name, got := range map[string]logconfig.ClockConfig{
		"sync":     cfg.Logger.ClockConfig,
		"async":    cfg.AsyncBestEffortMultiLoggerConfig.ClockConfig,
		"delivery": cfg.DeliveryLogger.ClockConfig,
	} {
		if got != wantClock {
			t.Fatalf("loaded %s ClockConfig = %+v, want %+v", name, got, wantClock)
		}
	}

	syncMapped, err := MapLoggerConfig(cfg)
	if err != nil {
		t.Fatalf("MapLoggerConfig() with empty environment: %v", err)
	}
	asyncMapped, err := MapAsyncLoggerConfig(cfg)
	if err != nil {
		t.Fatalf("MapAsyncLoggerConfig() with empty environment: %v", err)
	}
	deliveryMapped, err := MapDeliveryLoggerConfig(cfg)
	if err != nil {
		t.Fatalf("MapDeliveryLoggerConfig() with empty environment: %v", err)
	}
	assertStdoutOnlyMappedBackends(t, syncMapped, asyncMapped, deliveryMapped)
	wantTime := pkglogger.TimeConfig{IsLocalTimeEnabled: false, TimeZone: "UTC"}
	for name, got := range map[string]pkglogger.TimeConfig{
		"sync":     syncMapped.Stdout.TimeConfig,
		"async":    asyncMapped.Stdout.TimeConfig,
		"delivery": deliveryMapped.Stdout.TimeConfig,
	} {
		if got != wantTime {
			t.Fatalf("mapped %s stdout TimeConfig = %+v, want %+v", name, got, wantTime)
		}
	}

	const (
		syncMessage     = "default environment sync stdout entry"
		asyncMessage    = "default environment async stdout entry"
		deliveryMessage = "default environment delivery stdout entry"
	)
	ctx := context.Background()
	output := captureDefaultLoggerStdout(t, func() {
		syncLogger, err := infralogger.NewLogger(
			ctx, syncMapped, streamrouting.None, nil, nil, observer.NopTracer{}, observer.NopMeter{},
		)
		if err != nil {
			t.Fatalf("NewLogger() with default config: %v", err)
		}
		if err := syncLogger.Info(ctx, syncMessage); err != nil {
			t.Errorf("sync Info() with default config: %v", err)
		}
		if err := syncLogger.Close(); err != nil {
			t.Errorf("sync Close(): %v", err)
		}

		asyncLogger, err := infralogger.NewAsyncLogger(
			ctx, asyncMapped, streamrouting.None, nil, nil, observer.NopTracer{}, observer.NopMeter{},
		)
		if err != nil {
			t.Fatalf("NewAsyncLogger() with default config: %v", err)
		}
		asyncLogger.Info(ctx, asyncMessage)
		// Closeは受理済みのasync処理が完了するまで待ちます。
		if err := asyncLogger.Close(); err != nil {
			t.Errorf("async Close(): %v", err)
		}

		deliveryLogger, err := infralogger.NewDeliveryLogger(
			ctx, deliveryMapped, streamrouting.None, observer.NopTracer{}, observer.NopMeter{},
		)
		if err != nil {
			t.Fatalf("NewDeliveryLogger() with default config: %v", err)
		}
		report, err := deliveryLogger.Info(ctx, deliveryMessage)
		if err != nil {
			t.Errorf("delivery Info() with default config: %v", err)
		}
		if len(report.Targets) != 1 || !report.AllTargetsRegistered() {
			t.Errorf("delivery report = %+v, want one registered stdout target", report)
		}
		if err := deliveryLogger.Close(); err != nil {
			t.Errorf("delivery Close(): %v", err)
		}
	})
	for _, message := range []string{syncMessage, asyncMessage, deliveryMessage} {
		if !strings.Contains(output, message) {
			t.Errorf("stdout does not contain %q:\n%s", message, output)
		}
	}
}

// TestLoadConfigUsesStdoutForUnsetOrBlankLoggerDestinationsは、通常・非同期・Deliveryの
// 出力先が未設定・空の場合を同じ規則で扱い、stdoutだけを選択することを検証します。
func TestLoadConfigUsesStdoutForUnsetOrBlankLoggerDestinations(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value string
		set   bool
	}{
		{name: "unset"},
		{name: "empty", set: true},
		{name: "blank", value: `"   "`, set: true},
		{name: "separators only", value: `" , "`, set: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lines := []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
			}
			if tt.set {
				for _, key := range []string{"HETEROLOG_LOG_TYPE", "HETEROLOG_ASYNC_LOG_TYPE", "HETEROLOG_DELIVERY_LOG_TYPE"} {
					lines = append(lines, key+"="+tt.value)
				}
			}
			cfg, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, lines...))
			if err != nil {
				t.Fatalf("LoadConfig() with %s logger destinations: %v", tt.name, err)
			}
			assertStdoutOnlyLoadedDestinations(t, cfg)

			syncMapped, err := MapLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapLoggerConfig(): %v", err)
			}
			asyncMapped, err := MapAsyncLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapAsyncLoggerConfig(): %v", err)
			}
			deliveryMapped, err := MapDeliveryLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapDeliveryLoggerConfig(): %v", err)
			}
			assertStdoutOnlyMappedBackends(t, syncMapped, asyncMapped, deliveryMapped)
		})
	}
}

// TestLoadConfigRejectsUnknownLoggerDestinationWithItsKeyは、未知の出力先を既定値へ
// 置き換えず、通常・非同期・Deliveryそれぞれのkey名を含む設定エラーにすることを検証します。
func TestLoadConfigRejectsUnknownLoggerDestinationWithItsKey(t *testing.T) {
	for _, tt := range []struct{ key, enableKey string }{
		{key: "HETEROLOG_LOG_TYPE", enableKey: "HETEROLOG_ENABLE_LOGGING"},
		{key: "HETEROLOG_ASYNC_LOG_TYPE", enableKey: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING"},
		{key: "HETEROLOG_DELIVERY_LOG_TYPE", enableKey: "HETEROLOG_ENABLE_DELIVERY_LOGGING"},
	} {
		t.Run(tt.key, func(t *testing.T) {
			_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t,
				tt.enableKey+"=true",
				tt.key+"=stdout,smtp",
			))
			var invalid *InvalidEnvironmentValueError
			if !errors.As(err, &invalid) || invalid.Key != tt.key {
				t.Fatalf("LoadConfig() error = %v, want an invalid %s value error", err, tt.key)
			}
		})
	}
}

// TestExplicitSlackDestinationRequiresTokenは、Slackを出力先として明示した場合だけ
// Slack loggerを有効にし、tokenがなければmapper後のvalidationで設定エラーにすることを
// 通常・非同期・Deliveryの各LoggerContractKindで検証します。
func TestExplicitSlackDestinationRequiresToken(t *testing.T) {
	contracts := []struct {
		name, prefix, enableKey string
		mapSlack                func(AppConfig) (bool, error)
	}{
		{
			name: "sync", prefix: "", enableKey: "HETEROLOG_ENABLE_LOGGING",
			mapSlack: func(cfg AppConfig) (bool, error) {
				mapped, err := MapLoggerConfig(cfg)
				return mapped.Slack != nil, err
			},
		},
		{
			name: "async", prefix: "ASYNC_", enableKey: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING",
			mapSlack: func(cfg AppConfig) (bool, error) {
				mapped, err := MapAsyncLoggerConfig(cfg)
				return mapped.Slack != nil, err
			},
		},
		{
			name: "delivery", prefix: "DELIVERY_", enableKey: "HETEROLOG_ENABLE_DELIVERY_LOGGING",
			mapSlack: func(cfg AppConfig) (bool, error) {
				mapped, err := MapDeliveryLoggerConfig(cfg)
				return mapped.Slack != nil, err
			},
		},
	}
	for _, contract := range contracts {
		for _, tt := range []struct {
			name    string
			token   string
			wantErr error
		}{
			{name: "missing token", wantErr: pkgslack.ErrTokenRequired},
			{name: "blank token", token: `"   "`, wantErr: pkgslack.ErrTokenRequired},
			{name: "explicit token", token: contract.name + "-token"},
		} {
			t.Run(contract.name+"/"+tt.name, func(t *testing.T) {
				lines := []string{
					contract.enableKey + "=true",
					"HETEROLOG_" + contract.prefix + "LOG_TYPE=slack",
					"HETEROLOG_" + contract.prefix + "SLACK_NORMAL_CHANNEL=" + contract.name + "-channel",
				}
				if tt.token != "" {
					lines = append(lines, "HETEROLOG_"+contract.prefix+"SLACK_API_KEY="+tt.token)
				}
				cfg, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, lines...))
				if err != nil {
					t.Fatalf("LoadConfig() with explicit Slack destination: %v", err)
				}

				slackMapped, err := contract.mapSlack(cfg)
				if tt.wantErr != nil {
					if !errors.Is(err, tt.wantErr) {
						t.Fatalf("mapper error = %v, want %v", err, tt.wantErr)
					}
					return
				}
				if err != nil {
					t.Fatalf("mapper error with Slack token = %v, want nil", err)
				}
				if !slackMapped {
					t.Fatal("explicit Slack destination was not mapped")
				}
			})
		}
	}
}

func assertStdoutOnlyLoadedDestinations(t *testing.T, cfg AppConfig) {
	t.Helper()

	want := pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
	for name, got := range map[string]pkglogger.LogOutputDestinations{
		"sync":     cfg.Logger.LogOutputDestinations,
		"async":    cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations,
		"delivery": cfg.DeliveryLogger.LogOutputDestinations,
	} {
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("loaded %s destinations = %v, want %v", name, got, want)
		}
	}
	for name, got := range map[string]pkglogger.LogOutputDestinations{
		"sync":     cfg.Logger.HandlerTargetDestinations,
		"async":    cfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations,
		"delivery": cfg.DeliveryLogger.HandlerTargetDestinations,
	} {
		if len(got) != 0 {
			t.Fatalf("loaded %s handler target destinations = %v, want none", name, got)
		}
	}
}

func assertStdoutOnlyMappedBackends(
	t *testing.T,
	syncMapped pkglogger.LoggerConfig,
	asyncMapped pkglogger.AsyncBestEffortMultiLoggerConfig,
	deliveryMapped pkglogger.DeliveryLoggerConfig,
) {
	t.Helper()

	for name, cfg := range map[string]pkglogger.LoggerConfig{
		"sync":  syncMapped,
		"async": asyncMapped.ToLoggerConfig(),
	} {
		if cfg.Stdout == nil {
			t.Fatalf("mapped %s stdout config is nil", name)
		}
		if cfg.File != nil || cfg.Slack != nil || cfg.OpenSearch != nil || cfg.Loki != nil ||
			cfg.CloudWatchLogs != nil || cfg.ClickHouse != nil {
			t.Fatalf("mapped %s config enables a backend other than stdout: %+v", name, cfg)
		}
	}
	if deliveryMapped.Stdout == nil {
		t.Fatal("mapped delivery stdout config is nil")
	}
	if deliveryMapped.File != nil || deliveryMapped.Slack != nil || deliveryMapped.OpenSearch != nil ||
		deliveryMapped.Loki != nil || deliveryMapped.CloudWatchLogs != nil || deliveryMapped.ClickHouse != nil {
		t.Fatalf("mapped delivery config enables a backend other than stdout: %+v", deliveryMapped)
	}
}

// captureDefaultLoggerStdoutはfnの実行中だけos.Stdoutを一時fileへ差し替え、
// stdout loggerが書き込んだ内容を返します。
func captureDefaultLoggerStdout(t *testing.T, fn func()) string {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatalf("create stdout capture file: %v", err)
	}
	defer file.Close()
	original := os.Stdout
	os.Stdout = file
	defer func() { os.Stdout = original }()
	fn()
	os.Stdout = original

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("rewind stdout capture file: %v", err)
	}
	output, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("read stdout capture file: %v", err)
	}
	return string(output)
}

func assertFileStdoutSlackLoadedContracts(t *testing.T, cfg AppConfig) {
	t.Helper()

	if got, want := cfg.Logger.FileConfig.FileSystemStoreConfig.MaxFileSize, uint64(11)*BytesPerMegabyte; got != want {
		t.Fatalf("loaded sync file MaxFileSize = %d, want %d", got, want)
	}
	if got, want := cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.MaxFileSize, uint64(22)*BytesPerMegabyte; got != want {
		t.Fatalf("loaded async file MaxFileSize = %d, want %d", got, want)
	}
	if got, want := cfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.MaxFileSize, uint64(33)*BytesPerMegabyte; got != want {
		t.Fatalf("loaded delivery file MaxFileSize = %d, want %d", got, want)
	}
	assertFileStdoutSlackStrings(t, "loaded sync file handlers", cfg.Logger.FileConfig.HandlerConfig.OnErrorConfig, []string{"stdout"})
	assertFileStdoutSlackStrings(t, "loaded async file handlers", cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.HandlerConfig.OnErrorConfig, []string{"slack"})
	assertFileStdoutSlackStrings(t, "loaded delivery file handlers", cfg.DeliveryLogger.DeliveryFileConfig.HandlerConfig.OnErrorConfig, []string{"stdout"})
	assertFileStdoutSlackStrings(t, "loaded stdout handlers", cfg.Logger.StdoutConfig.HandlerConfig.OnErrorConfig, []string{"file"})
	assertFileStdoutSlackStrings(t, "loaded async stdout handlers", cfg.AsyncBestEffortMultiLoggerConfig.StdoutConfig.HandlerConfig.OnErrorConfig, []string{"slack"})
	assertFileStdoutSlackStrings(t, "loaded delivery stdout handlers", cfg.DeliveryLogger.DeliveryStdoutConfig.HandlerConfig.OnErrorConfig, []string{"file", "slack"})
	assertFileStdoutSlackStrings(t, "loaded sync slack handlers", cfg.Logger.SlackConfig.HandlerConfig.OnErrorConfig, []string{"stdout"})
	assertFileStdoutSlackStrings(t, "loaded async slack handlers", cfg.AsyncBestEffortMultiLoggerConfig.SlackConfig.HandlerConfig.OnErrorConfig, []string{"file"})
	assertFileStdoutSlackStrings(t, "loaded delivery slack handlers", cfg.DeliveryLogger.DeliverySlackConfig.HandlerConfig.OnErrorConfig, []string{"stdout"})
	if got, want := cfg.Logger.SlackConfig.Timeout, 4*time.Second; got != want {
		t.Fatalf("loaded sync Slack Timeout = %s, want %s", got, want)
	}
	if got, want := cfg.AsyncBestEffortMultiLoggerConfig.SlackConfig.Timeout, 5*time.Second; got != want {
		t.Fatalf("loaded async Slack Timeout = %s, want %s", got, want)
	}
	if got, want := cfg.DeliveryLogger.DeliverySlackConfig.Timeout, 6*time.Second; got != want {
		t.Fatalf("loaded delivery Slack Timeout = %s, want %s", got, want)
	}
	if cfg.Logger.SlackConfig.SamplingConfig.EnableLoggingByRequestID {
		t.Fatal("loaded sync Slack EnableLoggingByRequestID = true, want false")
	}
	if cfg.AsyncBestEffortMultiLoggerConfig.SlackConfig.SamplingConfig.EnableLoggingByRequestID {
		t.Fatal("loaded async Slack EnableLoggingByRequestID = true, want false")
	}
}

func assertFileStdoutSlackMappedContracts(
	t *testing.T,
	syncMapped pkglogger.LoggerConfig,
	asyncMapped pkglogger.AsyncBestEffortMultiLoggerConfig,
	deliveryMapped pkglogger.DeliveryLoggerConfig,
) {
	t.Helper()

	if got, want := syncMapped.Timeout, 7*time.Second; got != want {
		t.Fatalf("mapped sync Timeout = %s, want %s", got, want)
	}
	if got, want := asyncMapped.Timeout, 8*time.Second; got != want {
		t.Fatalf("mapped async Timeout = %s, want %s", got, want)
	}
	if got, want := deliveryMapped.Timeout, 9*time.Second; got != want {
		t.Fatalf("mapped delivery Timeout = %s, want %s", got, want)
	}
	if got, want := asyncMapped.MaxParallel, 13; got != want {
		t.Fatalf("mapped async MaxParallel = %d, want %d", got, want)
	}

	if syncMapped.File == nil || asyncMapped.File == nil || deliveryMapped.File == nil {
		t.Fatal("mapped file config is nil")
	}
	if got, want := syncMapped.File.FsstoreConfig.Lifecycle.MaxFileSize, uint64(11)*BytesPerMegabyte; got != want {
		t.Fatalf("mapped sync file MaxFileSize = %d, want %d", got, want)
	}
	if got, want := asyncMapped.File.FsstoreConfig.Lifecycle.MaxFileSize, uint64(22)*BytesPerMegabyte; got != want {
		t.Fatalf("mapped async file MaxFileSize = %d, want %d", got, want)
	}
	if got, want := asyncMapped.File.FsstoreConfig.Lifecycle.MaxDirSize, uint64(222)*BytesPerMegabyte; got != want {
		t.Fatalf("mapped async file MaxDirSize = %d, want %d", got, want)
	}
	if got, want := deliveryMapped.File.FsstoreConfig.Rotation.MaxFileSize, uint64(33)*BytesPerMegabyte; got != want {
		t.Fatalf("mapped delivery file MaxFileSize = %d, want %d", got, want)
	}
	assertFileStdoutSlackStrings(t, "mapped sync file handlers", syncMapped.File.HandlerConfig.OnErrorConfig, []string{"stdout"})
	assertFileStdoutSlackStrings(t, "mapped async file handlers", asyncMapped.File.HandlerConfig.OnErrorConfig, []string{"slack"})
	assertFileStdoutSlackStrings(t, "mapped delivery file handlers", deliveryMapped.File.HandlerConfig.OnErrorConfig, []string{"stdout"})

	if syncMapped.Stdout == nil || asyncMapped.Stdout == nil || deliveryMapped.Stdout == nil {
		t.Fatal("mapped stdout config is nil")
	}
	if got, want := asyncMapped.Stdout.RuntimeConfig.LogLevel, pkglogger.LogLevelWarn; got != want {
		t.Fatalf("mapped async stdout LogLevel = %s, want %s", got, want)
	}
	if got, want := asyncMapped.Stdout.SamplingConfig.LoggingByRequestIDRate, 17; got != want {
		t.Fatalf("mapped async stdout request ID rate = %d, want %d", got, want)
	}
	assertFileStdoutSlackStrings(t, "mapped sync stdout handlers", syncMapped.Stdout.HandlerConfig.OnErrorConfig, []string{"file"})
	assertFileStdoutSlackStrings(t, "mapped async stdout handlers", asyncMapped.Stdout.HandlerConfig.OnErrorConfig, []string{"slack"})
	assertFileStdoutSlackStrings(t, "mapped delivery stdout handlers", deliveryMapped.Stdout.HandlerConfig.OnErrorConfig, []string{"file", "slack"})
	if syncMapped.Stdout.UseOpenTelemetryFormat || asyncMapped.Stdout.UseOpenTelemetryFormat || deliveryMapped.Stdout.UseOpenTelemetryFormat {
		t.Fatal("mapped stdout OpenTelemetry format = true, want false for every LoggerContractKind")
	}

	if syncMapped.Slack == nil || asyncMapped.Slack == nil || deliveryMapped.Slack == nil {
		t.Fatal("mapped Slack config is nil")
	}
	if got, want := syncMapped.Slack.Token, "sync-token"; got != want {
		t.Fatalf("mapped sync Slack token = %q, want %q", got, want)
	}
	if got, want := asyncMapped.Slack.Token, "async-token"; got != want {
		t.Fatalf("mapped async Slack token = %q, want %q", got, want)
	}
	if got, want := deliveryMapped.Slack.Token, "delivery-token"; got != want {
		t.Fatalf("mapped delivery Slack token = %q, want %q", got, want)
	}
	if got, want := syncMapped.Slack.Timeout, 4*time.Second; got != want {
		t.Fatalf("mapped sync Slack Timeout = %s, want %s", got, want)
	}
	if got, want := asyncMapped.Slack.Timeout, 5*time.Second; got != want {
		t.Fatalf("mapped async Slack Timeout = %s, want %s", got, want)
	}
	if got, want := deliveryMapped.Slack.Timeout, 6*time.Second; got != want {
		t.Fatalf("mapped delivery Slack Timeout = %s, want %s", got, want)
	}
	if got, want := syncMapped.Slack.MaxRestThreshold, 7*time.Minute; got != want {
		t.Fatalf("mapped sync Slack MaxRestThreshold = %s, want %s", got, want)
	}
	if got, want := asyncMapped.Slack.MaxRestThreshold, 11*time.Minute; got != want {
		t.Fatalf("mapped async Slack MaxRestThreshold = %s, want %s", got, want)
	}
	if got, want := syncMapped.Slack.SlackPostWarnThresholdDuration, configslack.SlackPostWarnThresholdDuration; got != want {
		t.Fatalf("mapped sync Slack warn threshold = %s, want %s", got, want)
	}
	if got, want := asyncMapped.Slack.SlackPostErrorThresholdDuration, configslack.SlackPostErrorThresholdDuration; got != want {
		t.Fatalf("mapped async Slack error threshold = %s, want %s", got, want)
	}
	if got, want := deliveryMapped.Slack.SlackPostWarnThresholdDuration, configslack.SlackPostWarnThresholdDuration; got != want {
		t.Fatalf("mapped delivery Slack warn threshold = %s, want %s", got, want)
	}
	assertFileStdoutSlackStrings(t, "mapped sync slack handlers", syncMapped.Slack.HandlerConfig.OnErrorConfig, []string{"stdout"})
	assertFileStdoutSlackStrings(t, "mapped async slack handlers", asyncMapped.Slack.HandlerConfig.OnErrorConfig, []string{"file"})
	assertFileStdoutSlackStrings(t, "mapped delivery slack handlers", deliveryMapped.Slack.HandlerConfig.OnErrorConfig, []string{"stdout"})
}

func assertFileStdoutSlackInfrastructureAdapters(
	t *testing.T,
	syncMapped pkglogger.LoggerConfig,
	asyncMapped pkglogger.AsyncBestEffortMultiLoggerConfig,
	deliveryMapped pkglogger.DeliveryLoggerConfig,
) {
	t.Helper()

	if got, want := infralogger.NewMultiLoggerAdapter(syncMapped).Timeout(), 7*time.Second; got != want {
		t.Fatalf("sync multi adapter Timeout = %s, want %s", got, want)
	}
	asyncMulti := infralogger.NewAsyncBestEffortMultiLoggerAdapter(asyncMapped)
	if got, want := asyncMulti.Timeout(), 8*time.Second; got != want {
		t.Fatalf("async multi adapter Timeout = %s, want %s", got, want)
	}
	if got, want := asyncMulti.MaxParallel(), 13; got != want {
		t.Fatalf("async multi adapter MaxParallel = %d, want %d", got, want)
	}
	if got, want := infralogger.NewDeliveryMultiLoggerAdapter(deliveryMapped).Timeout(), 9*time.Second; got != want {
		t.Fatalf("delivery multi adapter Timeout = %s, want %s", got, want)
	}

	asyncFile := filelogger.NewConfigAdapter(*asyncMapped.File)
	if got, want := asyncFile.Fsstore().Rotation().MaxFileSize(), uint64(22)*BytesPerMegabyte; got != want {
		t.Fatalf("async file adapter MaxFileSize = %d, want %d", got, want)
	}
	assertFileStdoutSlackStrings(t, "async file adapter handlers", asyncFile.Handler().OnError(), []string{"slack"})

	asyncStdout := stdoutlogger.NewConfigAdapter(*asyncMapped.Stdout)
	if got, want := asyncStdout.Runtime().LogLevel(), pkglogger.LogLevelWarn; got != want {
		t.Fatalf("async stdout adapter LogLevel = %s, want %s", got, want)
	}
	assertFileStdoutSlackStrings(t, "async stdout adapter handlers", asyncStdout.Handler().OnError(), []string{"slack"})

	syncSlack := slacklogger.NewConfigAdapter(*syncMapped.Slack)
	if got, want := syncSlack.Timeout(), 4*time.Second; got != want {
		t.Fatalf("sync Slack adapter Timeout = %s, want %s", got, want)
	}
	if got, want := syncSlack.MaxRestThreshold(), 7*time.Minute; got != want {
		t.Fatalf("sync Slack adapter MaxRestThreshold = %s, want %s", got, want)
	}
	assertFileStdoutSlackStrings(t, "sync Slack adapter handlers", syncSlack.Handler().OnError(), []string{"stdout"})

	deliverySlack := slacklogger.NewDeliveryConfigAdapter(*deliveryMapped.Slack)
	if got, want := deliverySlack.Timeout(), 6*time.Second; got != want {
		t.Fatalf("delivery Slack adapter Timeout = %s, want %s", got, want)
	}
	if got, want := deliverySlack.Token(), "delivery-token"; got != want {
		t.Fatalf("delivery Slack adapter token = %q, want %q", got, want)
	}
}

func assertFileStdoutSlackStrings(t *testing.T, name string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
}

// fileStdoutSlackSharedEnvKeysは、この契約試験の中で複数のLoggerContractKindに効くkeyです。
var fileStdoutSlackSharedEnvKeys = map[string]string{
	"HETEROLOG_STDOUT_LOG_LEVEL":                    "sync and async stdout loggers share the stdout runtime settings",
	"HETEROLOG_ENABLE_LOG_STDOUT_SAMPLING":          "sync and async stdout loggers share the stdout sampling settings",
	"HETEROLOG_ENABLE_LOGGING_STDOUT_BY_REQUEST_ID": "sync and async stdout loggers share the stdout sampling settings",
	"HETEROLOG_LOG_STDOUT_REQUEST_ID_RATE":          "sync and async stdout loggers share the stdout sampling settings",
	"HETEROLOG_LOG_STDOUT_DEBUG_RATE":               "sync and async stdout loggers share the stdout sampling settings",
	"HETEROLOG_LOG_STDOUT_INFO_RATE":                "sync and async stdout loggers share the stdout sampling settings",
	"HETEROLOG_LOG_STDOUT_WARN_RATE":                "sync and async stdout loggers share the stdout sampling settings",
	"HETEROLOG_LOG_STDOUT_ERROR_RATE":               "sync and async stdout loggers share the stdout sampling settings",
	"HETEROLOG_ENABLE_STDOUT_OTEL_FORMAT":           "every stdout logger shares the OpenTelemetry output format",
}

// assertFileStdoutSlackLoadedEnvLinesは、契約試験が与えた全てのkeyについて、値が対応する
// LoggerContractKindの読込結果の項目に入っていることを確認します。項目の一覧にないkeyは
// 試験の対象漏れとして失敗します。
func assertFileStdoutSlackLoadedEnvLines(t *testing.T, cfg AppConfig, lines []string) {
	t.Helper()
	fields := fileStdoutSlackLoadedFields(cfg)
	for _, line := range lines {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("invalid contract env line %q", line)
		}
		field, ok := fields[key]
		if !ok {
			t.Errorf("%s has no loaded field assertion", key)
			continue
		}
		for _, got := range field.got {
			if want := field.want(t, value); !reflect.DeepEqual(got, want) {
				t.Errorf("loaded %s = %#v, want %#v", key, got, want)
			}
		}
	}
}

// fileStdoutSlackLoadedFieldは、keyの値を読み込んだ項目と、env値から期待値を作る関数です。
// 複数のLoggerContractKindで共有するkeyは、共有する全ての項目を並べます。
type fileStdoutSlackLoadedField struct {
	got  []any
	want func(*testing.T, string) any
}

func fileStdoutSlackLoadedFields(cfg AppConfig) map[string]fileStdoutSlackLoadedField {
	sync, async, delivery := cfg.Logger, cfg.AsyncBestEffortMultiLoggerConfig, cfg.DeliveryLogger
	field := func(want func(*testing.T, string) any, got ...any) fileStdoutSlackLoadedField {
		return fileStdoutSlackLoadedField{got: got, want: want}
	}
	fields := map[string]fileStdoutSlackLoadedField{
		"HETEROLOG_LOG_TYPE":                           field(contractEnvDestinations, sync.LogOutputDestinations),
		"HETEROLOG_ASYNC_LOG_TYPE":                     field(contractEnvDestinations, async.LogOutputDestinations),
		"HETEROLOG_DELIVERY_LOG_TYPE":                  field(contractEnvDestinations, delivery.LogOutputDestinations),
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY":          field(contractEnvTimeoutBudgetPolicy, sync.TimeoutBudgetPolicy),
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY":    field(contractEnvTimeoutBudgetPolicy, async.TimeoutBudgetPolicy),
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY": field(contractEnvTimeoutBudgetPolicy, delivery.TimeoutBudgetPolicy),
		"HETEROLOG_LOG_TIMEOUT_SECOND":                 field(contractEnvSeconds, sync.Timeout),
		"HETEROLOG_ASYNC_LOG_TIMEOUT_SECOND":           field(contractEnvSeconds, async.Timeout),
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_SECOND":        field(contractEnvSeconds, delivery.Timeout),
		"HETEROLOG_MAX_ASYNC_LOG_PARALLEL":             field(contractEnvInt, async.MaxParallel),

		"HETEROLOG_FILE_LOG_LEVEL":                 field(contractEnvLogLevel, sync.FileConfig.LogLevel),
		"HETEROLOG_MAX_LOG_SIZE_MB":                field(contractEnvMegabytes, sync.FileConfig.FileSystemStoreConfig.MaxFileSize),
		"HETEROLOG_MAX_LOG_FILE_DIR_SIZE_MB":       field(contractEnvMegabytes, sync.FileConfig.FileSystemStoreConfig.MaxDirSize),
		"HETEROLOG_LOG_DIR":                        field(contractEnvString, sync.FileConfig.FileSystemStoreConfig.LogDir),
		"HETEROLOG_ERROR_LOG_DIR":                  field(contractEnvString, sync.FileConfig.FileSystemStoreConfig.ErrorLogDir),
		"HETEROLOG_WRITE_LOG_FILE_LATENCY_ENABLED": field(contractEnvBool, sync.FileConfig.WriteFileLatencyEnabled),

		"HETEROLOG_ASYNC_FILE_LOG_LEVEL":                 field(contractEnvLogLevel, async.FileConfig.LogLevel),
		"HETEROLOG_MAX_ASYNC_LOG_SIZE_MB":                field(contractEnvMegabytes, async.FileConfig.FileSystemStoreConfig.MaxFileSize),
		"HETEROLOG_MAX_ASYNC_LOG_FILE_DIR_SIZE_MB":       field(contractEnvMegabytes, async.FileConfig.FileSystemStoreConfig.MaxDirSize),
		"HETEROLOG_ASYNC_LOG_DIR":                        field(contractEnvString, async.FileConfig.FileSystemStoreConfig.LogDir),
		"HETEROLOG_ASYNC_ERROR_LOG_DIR":                  field(contractEnvString, async.FileConfig.FileSystemStoreConfig.ErrorLogDir),
		"HETEROLOG_WRITE_ASYNC_LOG_FILE_LATENCY_ENABLED": field(contractEnvBool, async.FileConfig.WriteFileLatencyEnabled),

		"HETEROLOG_MAX_DELIVERY_LOG_SIZE_MB":                field(contractEnvMegabytes, delivery.DeliveryFileConfig.FileSystemStoreConfig.MaxFileSize),
		"HETEROLOG_MAX_DELIVERY_LOG_FILE_DIR_SIZE_MB":       field(contractEnvMegabytes, delivery.DeliveryFileConfig.FileSystemStoreConfig.MaxDirSize),
		"HETEROLOG_DELIVERY_LOG_DIR":                        field(contractEnvString, delivery.DeliveryFileConfig.FileSystemStoreConfig.LogDir),
		"HETEROLOG_DELIVERY_ERROR_LOG_DIR":                  field(contractEnvString, delivery.DeliveryFileConfig.FileSystemStoreConfig.ErrorLogDir),
		"HETEROLOG_WRITE_DELIVERY_LOG_FILE_LATENCY_ENABLED": field(contractEnvBool, delivery.DeliveryFileConfig.WriteFileLatencyEnabled),

		"HETEROLOG_STDOUT_LOG_LEVEL":                    field(contractEnvLogLevel, sync.StdoutConfig.LogLevel, async.StdoutConfig.LogLevel),
		"HETEROLOG_ENABLE_LOG_STDOUT_SAMPLING":          field(contractEnvBool, sync.StdoutConfig.SamplingConfig.IsSamplingEnabled, async.StdoutConfig.SamplingConfig.IsSamplingEnabled),
		"HETEROLOG_ENABLE_LOGGING_STDOUT_BY_REQUEST_ID": field(contractEnvBool, sync.StdoutConfig.SamplingConfig.EnableLoggingByRequestID, async.StdoutConfig.SamplingConfig.EnableLoggingByRequestID),
		"HETEROLOG_LOG_STDOUT_REQUEST_ID_RATE":          field(contractEnvInt, sync.StdoutConfig.SamplingConfig.LoggingByRequestIDRate, async.StdoutConfig.SamplingConfig.LoggingByRequestIDRate),
		"HETEROLOG_LOG_STDOUT_DEBUG_RATE":               field(contractEnvInt, sync.StdoutConfig.SamplingConfig.SamplingDebugRate, async.StdoutConfig.SamplingConfig.SamplingDebugRate),
		"HETEROLOG_LOG_STDOUT_INFO_RATE":                field(contractEnvInt, sync.StdoutConfig.SamplingConfig.SamplingInfoRate, async.StdoutConfig.SamplingConfig.SamplingInfoRate),
		"HETEROLOG_LOG_STDOUT_WARN_RATE":                field(contractEnvInt, sync.StdoutConfig.SamplingConfig.SamplingWarnRate, async.StdoutConfig.SamplingConfig.SamplingWarnRate),
		"HETEROLOG_LOG_STDOUT_ERROR_RATE":               field(contractEnvInt, sync.StdoutConfig.SamplingConfig.SamplingErrorRate, async.StdoutConfig.SamplingConfig.SamplingErrorRate),
		"HETEROLOG_ENABLE_STDOUT_OTEL_FORMAT": field(contractEnvBool,
			sync.StdoutConfig.OpenTelemetryFormatEnabled,
			async.StdoutConfig.OpenTelemetryFormatEnabled,
			delivery.DeliveryStdoutConfig.OpenTelemetryFormatEnabled,
		),

		"HETEROLOG_SLACK_LOG_LEVEL":                    field(contractEnvLogLevel, sync.SlackConfig.LogLevel),
		"HETEROLOG_SLACK_API_KEY":                      field(contractEnvString, sync.SlackConfig.Token),
		"HETEROLOG_SLACK_TIMEOUT_SECOND":               field(contractEnvSeconds, sync.SlackConfig.Timeout),
		"HETEROLOG_SLACK_NORMAL_CHANNEL":               field(contractEnvString, sync.SlackConfig.Channel),
		"HETEROLOG_SLACK_ERROR_CHANNEL":                field(contractEnvString, sync.SlackConfig.ErrorChannel),
		"HETEROLOG_SLACK_WARN_CHANNEL":                 field(contractEnvString, sync.SlackConfig.WarnChannel),
		"HETEROLOG_SLACK_INFO_CHANNEL":                 field(contractEnvString, sync.SlackConfig.InfoChannel),
		"HETEROLOG_SLACK_DEBUG_CHANNEL":                field(contractEnvString, sync.SlackConfig.DebugChannel),
		"HETEROLOG_MAX_LOGS_PER_SLACK_NOTIFICATION":    field(contractEnvInt, sync.SlackConfig.MaxLogsPerNotification),
		"HETEROLOG_MAX_SLACK_REST_THRESHOLD_MINUTE":    field(contractEnvMinutes, sync.SlackConfig.MaxRestThreshold),
		"HETEROLOG_SLACK_INTERVALS":                    field(contractEnvMinuteList, sync.SlackConfig.Intervals),
		"HETEROLOG_ENABLE_LOG_SLACK_SAMPLING":          field(contractEnvBool, sync.SlackConfig.SamplingConfig.IsSamplingEnabled),
		"HETEROLOG_ENABLE_LOGGING_SLACK_BY_REQUEST_ID": field(contractEnvBool, sync.SlackConfig.SamplingConfig.EnableLoggingByRequestID),
		"HETEROLOG_LOG_SLACK_REQUEST_ID_RATE":          field(contractEnvInt, sync.SlackConfig.SamplingConfig.LoggingByRequestIDRate),

		"HETEROLOG_ASYNC_SLACK_LOG_LEVEL":                    field(contractEnvLogLevel, async.SlackConfig.LogLevel),
		"HETEROLOG_ASYNC_SLACK_API_KEY":                      field(contractEnvString, async.SlackConfig.Token),
		"HETEROLOG_ASYNC_SLACK_TIMEOUT_SECOND":               field(contractEnvSeconds, async.SlackConfig.Timeout),
		"HETEROLOG_ASYNC_SLACK_NORMAL_CHANNEL":               field(contractEnvString, async.SlackConfig.Channel),
		"HETEROLOG_ASYNC_SLACK_ERROR_CHANNEL":                field(contractEnvString, async.SlackConfig.ErrorChannel),
		"HETEROLOG_ASYNC_SLACK_WARN_CHANNEL":                 field(contractEnvString, async.SlackConfig.WarnChannel),
		"HETEROLOG_ASYNC_SLACK_INFO_CHANNEL":                 field(contractEnvString, async.SlackConfig.InfoChannel),
		"HETEROLOG_ASYNC_SLACK_DEBUG_CHANNEL":                field(contractEnvString, async.SlackConfig.DebugChannel),
		"HETEROLOG_MAX_ASYNC_LOGS_PER_SLACK_NOTIFICATION":    field(contractEnvInt, async.SlackConfig.MaxLogsPerNotification),
		"HETEROLOG_MAX_ASYNC_SLACK_REST_THRESHOLD_MINUTE":    field(contractEnvMinutes, async.SlackConfig.MaxRestThreshold),
		"HETEROLOG_ASYNC_SLACK_INTERVALS":                    field(contractEnvMinuteList, async.SlackConfig.Intervals),
		"HETEROLOG_ENABLE_ASYNC_LOG_SLACK_SAMPLING":          field(contractEnvBool, async.SlackConfig.SamplingConfig.IsSamplingEnabled),
		"HETEROLOG_ENABLE_ASYNC_LOGGING_SLACK_BY_REQUEST_ID": field(contractEnvBool, async.SlackConfig.SamplingConfig.EnableLoggingByRequestID),
		"HETEROLOG_ASYNC_LOG_SLACK_REQUEST_ID_RATE":          field(contractEnvInt, async.SlackConfig.SamplingConfig.LoggingByRequestIDRate),

		"HETEROLOG_DELIVERY_SLACK_LOG_LEVEL":      field(contractEnvLogLevel, delivery.DeliverySlackConfig.LogLevel),
		"HETEROLOG_DELIVERY_SLACK_API_KEY":        field(contractEnvString, delivery.DeliverySlackConfig.Token),
		"HETEROLOG_DELIVERY_SLACK_TIMEOUT_SECOND": field(contractEnvSeconds, delivery.DeliverySlackConfig.Timeout),
		"HETEROLOG_DELIVERY_SLACK_NORMAL_CHANNEL": field(contractEnvString, delivery.DeliverySlackConfig.Channel),
		"HETEROLOG_DELIVERY_SLACK_ERROR_CHANNEL":  field(contractEnvString, delivery.DeliverySlackConfig.ErrorChannel),
		"HETEROLOG_DELIVERY_SLACK_WARN_CHANNEL":   field(contractEnvString, delivery.DeliverySlackConfig.WarnChannel),
		"HETEROLOG_DELIVERY_SLACK_INFO_CHANNEL":   field(contractEnvString, delivery.DeliverySlackConfig.InfoChannel),
		"HETEROLOG_DELIVERY_SLACK_DEBUG_CHANNEL":  field(contractEnvString, delivery.DeliverySlackConfig.DebugChannel),
	}
	// internal logの通知先は、LoggerContractKind・出力先・levelごとに独立した項目です。
	for prefix, backends := range map[string]map[string]internalerror.RoutingConfig{
		"": {
			"FILE": sync.FileConfig.HandlerConfig, "STDOUT": sync.StdoutConfig.HandlerConfig, "SLACK": sync.SlackConfig.HandlerConfig,
		},
		"ASYNC_": {
			"FILE": async.FileConfig.HandlerConfig, "STDOUT": async.StdoutConfig.HandlerConfig, "SLACK": async.SlackConfig.HandlerConfig,
		},
		"DELIVERY_": {
			"FILE":   delivery.DeliveryFileConfig.HandlerConfig,
			"STDOUT": delivery.DeliveryStdoutConfig.HandlerConfig,
			"SLACK":  delivery.DeliverySlackConfig.HandlerConfig,
		},
	} {
		for backend, routing := range backends {
			for level, handlers := range map[string][]string{
				"ERROR": routing.OnErrorConfig, "WARN": routing.OnWarnConfig,
				"INFO": routing.OnInfoConfig, "DEBUG": routing.OnDebugConfig,
			} {
				fields["HETEROLOG_"+prefix+backend+"_LOGGER_"+level+"_HANDLER"] = field(contractEnvList, handlers)
			}
		}
	}
	return fields
}

func contractEnvString(_ *testing.T, value string) any { return value }

func contractEnvList(_ *testing.T, value string) any { return splitDotEnvCSV(value) }

func contractEnvInt(t *testing.T, value string) any {
	t.Helper()
	number, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("parse %q as int: %v", value, err)
	}
	return number
}

func contractEnvBool(t *testing.T, value string) any {
	t.Helper()
	enabled, err := strconv.ParseBool(value)
	if err != nil {
		t.Fatalf("parse %q as bool: %v", value, err)
	}
	return enabled
}

func contractEnvSeconds(t *testing.T, value string) any {
	t.Helper()
	return time.Duration(contractEnvInt(t, value).(int)) * time.Second
}

func contractEnvMinutes(t *testing.T, value string) any {
	t.Helper()
	return time.Duration(contractEnvInt(t, value).(int)) * time.Minute
}

func contractEnvMinuteList(t *testing.T, value string) any {
	t.Helper()
	var intervals []time.Duration
	for _, part := range splitDotEnvCSV(value) {
		intervals = append(intervals, contractEnvMinutes(t, part).(time.Duration))
	}
	return intervals
}

func contractEnvMegabytes(t *testing.T, value string) any {
	t.Helper()
	return uint64(contractEnvInt(t, value).(int)) * BytesPerMegabyte
}

func contractEnvLogLevel(t *testing.T, value string) any {
	t.Helper()
	level, err := pkglogger.ParseLogLevel(value)
	if err != nil {
		t.Fatalf("parse %q as log level: %v", value, err)
	}
	return level
}

func contractEnvTimeoutBudgetPolicy(t *testing.T, value string) any {
	t.Helper()
	policy, err := timeoutbudget.ParseTimeoutBudgetPolicy(value)
	if err != nil {
		t.Fatalf("parse %q as timeout budget policy: %v", value, err)
	}
	return policy
}

func contractEnvDestinations(t *testing.T, value string) any {
	t.Helper()
	destinations, err := pkglogger.ParseLogOutputDestinations(splitDotEnvCSV(value))
	if err != nil {
		t.Fatalf("parse %q as log output destinations: %v", value, err)
	}
	return destinations
}
