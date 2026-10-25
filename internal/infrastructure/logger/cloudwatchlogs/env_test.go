package cloudwatchlogs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	appconfig "github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/testutil"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
)

func TestCloudWatchLogsLoggerUsesConfigFixtureAndPutsThroughMockClient(t *testing.T) {
	appCfg, envFile := loadConfigFixtureForCloudWatchLogsLoggerTest(t, map[string]string{
		"HETEROLOG_LOG_TYPE": "cloudwatchlogs",
	})
	cfg := appconfig.ToLoggerConfig(appCfg)
	require.NotNil(t, cfg.CloudWatchLogs)
	require.NotNil(t, cfg.CloudWatchLogsClient)
	require.Equal(t, requiredDotEnvStringForCloudWatchLogsLoggerTest(t, envFile, "HETEROLOG_CLOUDWATCH_REGION"), cfg.CloudWatchLogsClient.Region)
	require.Equal(t, requiredDotEnvStringForCloudWatchLogsLoggerTest(t, envFile, "HETEROLOG_CLOUDWATCH_ENDPOINT"), cfg.CloudWatchLogsClient.Endpoint)
	require.Equal(t, requiredDotEnvStringForCloudWatchLogsLoggerTest(t, envFile, "HETEROLOG_CLOUDWATCH_LOG_GROUP_NAME"), cfg.CloudWatchLogs.CloudWatchLogsFlushManagerConfig.LogGroupName)
	require.Equal(t, requiredDotEnvStringForCloudWatchLogsLoggerTest(t, envFile, "HETEROLOG_CLOUDWATCH_LOG_STREAM_NAME"), cfg.CloudWatchLogs.CloudWatchLogsFlushManagerConfig.LogStreamName)
	require.Equal(
		t,
		streamrouting.Normalize(streamrouting.Granularity(
			requiredDotEnvStringForCloudWatchLogsLoggerTest(t, envFile, "HETEROLOG_CLOUDWATCH_LOG_STREAM_ROTATION"),
		)),
		appCfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity,
	)
	require.Equal(t, requiredDotEnvBoolForCloudWatchLogsLoggerTest(t, envFile, "HETEROLOG_ENABLE_CLOUDWATCH_COMPRESS_REQUEST"), cfg.CloudWatchLogsClient.CompressRequest)
	require.Equal(t, requiredDotEnvStringForCloudWatchLogsLoggerTest(t, envFile, "HETEROLOG_CLOUDWATCH_LOG_LEVEL"), cfg.CloudWatchLogs.CloudWatchLogsLoggerBaseConfig.RuntimeConfig.LogLevel.String())

	ctx := context.Background()
	client := &recordingCloudWatchLogsClient{}
	loggerConfig := NewCloudWatchLogsLoggerConfigAdapter(cfg.CloudWatchLogs.CloudWatchLogsLoggerBaseConfig)
	flushConfig := NewFlushManagerConfigAdapter(
		cfg.CloudWatchLogs.CloudWatchLogsFlushManagerConfig,
		appCfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity,
	)
	resourceProvider := resource.NewResourceAdapter(cfg.CloudWatchLogs.CloudWatchLogsFlushManagerConfig.Resource)
	loggerBase := &LoggerBase{
		Clock:  fixedCloudWatchLogsClock{t: time.Date(2026, 6, 26, 12, 34, 56, 0, time.UTC)},
		Tracer: observer.NopTracer{},
		Meter:  observer.NopMeter{},
	}

	logger, err := NewCloudWatchLogsLogger(
		ctx,
		loggerConfig,
		client,
		flushConfig,
		resourceProvider,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		loggerBase,
	)
	require.NoError(t, err)

	message := "cloudwatchlogs config fixture logger write"
	require.NoError(t, handleTestError(ctx, logger, message))
	require.NoError(t, logger.Close(ctx))

	require.Len(t, client.events, 1)
	require.Contains(t, client.events[0].Message, message)
	require.True(t, client.events[0].Timestamp.Equal(time.Date(2026, 6, 26, 12, 34, 56, 0, time.UTC)))
}

// loadConfigFixtureForCloudWatchLogsLoggerTestは`.env.example`から生成した試験用`.env`を
// HETEROLOG_ENV_PATHへ指定して設定を読み込みます。開発者の`.env`は開きません。
func loadConfigFixtureForCloudWatchLogsLoggerTest(t *testing.T, overrides map[string]string) (appconfig.AppConfig, map[string]string) {
	t.Helper()

	fixtureRoot := testutil.ConfigFixtureRoot(t)
	envPath := filepath.Join(fixtureRoot, ".env")
	envFile, err := godotenv.Read(envPath)
	require.NoError(t, err)

	testutil.PreserveEnvironment(t)
	os.Clearenv()
	require.NoError(t, os.Setenv("HETEROLOG_ENV_PATH", envPath))
	for key, value := range overrides {
		require.NoError(t, os.Setenv(key, value))
	}

	cfg, err := appconfig.LoadConfig()
	require.NoError(t, err)
	return cfg, envFile
}

func requiredDotEnvStringForCloudWatchLogsLoggerTest(t *testing.T, envFile map[string]string, key string) string {
	t.Helper()

	value, ok := envFile[key]
	if !ok {
		t.Fatalf("environment file is missing %s", key)
	}
	return value
}

func requiredDotEnvBoolForCloudWatchLogsLoggerTest(t *testing.T, envFile map[string]string, key string) bool {
	t.Helper()

	switch strings.ToLower(requiredDotEnvStringForCloudWatchLogsLoggerTest(t, envFile, key)) {
	case "true", "1", "yes", "y", "on":
		return true
	case "false", "0", "no", "n", "off":
		return false
	default:
		t.Fatalf("environment file has invalid bool %s=%q", key, envFile[key])
		return false
	}
}
