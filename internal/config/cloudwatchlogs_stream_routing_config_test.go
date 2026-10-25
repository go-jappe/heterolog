package config

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	cloudwatchlogscfg "github.com/go-jappe/heterolog/internal/config/cloudwatchlogs"
	mappedvalidation "github.com/go-jappe/heterolog/internal/config/validation/mapped"
	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/env"
)

func TestCloudWatchLogsStreamRoutingLoadingNormalizesEnvironmentValue(t *testing.T) {
	t.Setenv("TEST_CLOUDWATCH_LOG_STREAM_ROTATION", "  HoUrLy  ")

	got := loadCloudWatchLogsStreamRoutingGranularity(env.GetEnvString("TEST_CLOUDWATCH_LOG_STREAM_ROTATION", string(cloudwatchlogscfg.DefaultStreamRoutingGranularity)))
	if got != streamrouting.Hourly {
		t.Fatalf("loaded stream routing granularity = %q, want %q", got, streamrouting.Hourly)
	}
}

func TestCloudWatchLogsStreamRoutingLoadingUsesDefaultForEmptyEnvironment(t *testing.T) {
	const key = "TEST_CLOUDWATCH_LOG_STREAM_ROTATION_EMPTY"
	t.Setenv(key, "")

	got := loadCloudWatchLogsStreamRoutingGranularity(env.GetEnvString(key, string(cloudwatchlogscfg.DefaultStreamRoutingGranularity)))
	if got != cloudwatchlogscfg.DefaultStreamRoutingGranularity {
		t.Fatalf(
			"loaded stream routing granularity = %q, want default %q",
			got,
			cloudwatchlogscfg.DefaultStreamRoutingGranularity,
		)
	}
}

func TestMapLoggerConfigValidatesResolvedCloudWatchLogsStreamName(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, append(
		loggerContractTestBaseEnv("cloudwatchlogs"),
		"HETEROLOG_CLOUDWATCH_LOG_STREAM_NAME="+strings.Repeat("a", 500),
		"HETEROLOG_CLOUDWATCH_LOG_STREAM_ROTATION=hourly",
		"HETEROLOG_CLOUDWATCH_AWS_ACCESS_KEY_ID=sync-access",
		"HETEROLOG_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=sync-secret",
	))

	_, err := MapLoggerConfig(cfg)
	nameErr := requireCloudWatchLogsStreamNameError(t, err, "logger", pkgcloudwatchlogs.ErrLogStreamNameTooLong)
	// 基準名は上限内でも、時間区分のsuffixを付けた名前を検証します。
	if !strings.HasPrefix(nameErr.StreamName, strings.Repeat("a", 500)+"/") ||
		utf8.RuneCountInString(nameErr.StreamName) <= 512 {
		t.Fatalf("validated stream name = %q, want hourly suffix appended to the 500-character base name", nameErr.StreamName)
	}
}

func TestMapLoggerConfigRejectsOperationallyUnsafeCloudWatchLogsStreamName(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, append(
		loggerContractTestBaseEnv("cloudwatchlogs"),
		"HETEROLOG_CLOUDWATCH_LOG_STREAM_NAME=app"+"\u200b"+"worker",
		"HETEROLOG_CLOUDWATCH_AWS_ACCESS_KEY_ID=sync-access",
		"HETEROLOG_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=sync-secret",
	))

	_, err := MapLoggerConfig(cfg)
	nameErr := requireCloudWatchLogsStreamNameError(
		t,
		err,
		"logger",
		infracloudwatchlogs.ErrLogStreamNameFormatCharacter,
	)
	var characterErr *infracloudwatchlogs.LogStreamNameCharacterError
	if !errors.As(nameErr, &characterErr) || characterErr.Character != '\u200b' {
		t.Fatalf("stream name error = %v, want rejected character U+200B", nameErr)
	}
}

// requireCloudWatchLogsStreamNameErrorは、errがsourceのCloudWatch Logs stream名を、時間区分の
// suffixを付けた後にkindの理由で拒否したerrorを含むことを確認して、そのerrorを返します。
func requireCloudWatchLogsStreamNameError(
	t *testing.T,
	err error,
	source string,
	kind error,
) *mappedvalidation.CloudWatchLogsStreamNameError {
	t.Helper()
	nameErr, ok := errortree.Find(err, func(nameErr *mappedvalidation.CloudWatchLogsStreamNameError) bool {
		return nameErr.Source == source
	})
	if !ok || !errors.Is(nameErr, kind) {
		t.Fatalf("error = %v, want %s resolved stream name error %v", err, source, kind)
	}
	return nameErr
}
