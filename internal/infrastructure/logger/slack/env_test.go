package slack

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	appconfig "github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/testutil"

	"github.com/joho/godotenv"
	slackgo "github.com/slack-go/slack"
	"github.com/stretchr/testify/require"
)

func TestSlackLoggerUsesConfigFixtureAndPostsThroughMockTransport(t *testing.T) {
	appCfg, envFile := loadConfigFixtureForSlackLoggerTest(t, map[string]string{
		"HETEROLOG_LOG_TYPE": "slack",
	})
	cfg := appconfig.ToLoggerConfig(appCfg)
	require.NotNil(t, cfg.Slack)
	require.Equal(t, requiredDotEnvStringForSlackLoggerTest(t, envFile, "HETEROLOG_APP_NAME"), cfg.Slack.RuntimeConfig.AppName)
	require.Equal(t, requiredDotEnvStringForSlackLoggerTest(t, envFile, "HETEROLOG_TARGET"), cfg.Slack.RuntimeConfig.Target)
	require.Equal(t, requiredDotEnvStringForSlackLoggerTest(t, envFile, "HETEROLOG_SLACK_NORMAL_CHANNEL"), cfg.Slack.Channel)
	require.Equal(t, requiredDotEnvStringForSlackLoggerTest(t, envFile, "HETEROLOG_SLACK_ERROR_CHANNEL"), cfg.Slack.ErrorChannel)
	require.Equal(t, requiredDotEnvStringForSlackLoggerTest(t, envFile, "HETEROLOG_SLACK_LOG_LEVEL"), cfg.Slack.RuntimeConfig.LogLevel.String())
	// 環境設定からはHTTP clientを指定できないため、net/httpの既定transportを使います。
	require.Nil(t, cfg.Slack.HTTPClient)

	ctx := context.Background()
	logger, err := NewSlackLogger(ctx, NewConfigAdapter(*cfg.Slack), observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)

	client := &recordingSlackHTTPClient{}
	logger.slackAPI = slackgo.New(cfg.Slack.Token, slackgo.OptionHTTPClient(client))

	message := "slack config fixture logger write"
	require.NoError(t, logger.testError(ctx, message))
	require.NoError(t, logger.Close())

	client.requireTextContains(t, message)
	client.mu.Lock()
	calls := append([]recordingSlackPost(nil), client.calls...)
	client.mu.Unlock()
	require.NotEmpty(t, calls)
	require.Equal(t, cfg.Slack.ErrorChannel, calls[0].channel)
}

// loadConfigFixtureForSlackLoggerTestは`.env.example`から生成した試験用`.env`を
// HETEROLOG_ENV_PATHへ指定して設定を読み込みます。開発者の`.env`は開きません。
func loadConfigFixtureForSlackLoggerTest(t *testing.T, overrides map[string]string) (appconfig.AppConfig, map[string]string) {
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

func requiredDotEnvStringForSlackLoggerTest(t *testing.T, envFile map[string]string, key string) string {
	t.Helper()

	value, ok := envFile[key]
	if !ok {
		t.Fatalf("environment file is missing %s", key)
	}
	return value
}
