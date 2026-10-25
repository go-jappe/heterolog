package config

import (
	"errors"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkgenv "github.com/go-jappe/heterolog/pkg/env"
)

// loadLoggerInternalLogDestinationConfigはLoggerContractSyncのinternal logで
// 通常backendと異なる保存先を使う場合のidentity上書き値を読み込みます。
// Lokiが通常出力先にもinternal logの委譲先にもない場合、未使用のlabelsは解析しません。
func loadLoggerInternalLogDestinationConfig(
	env pkgenv.Lookup,
	loadLokiLabels bool,
) (logconfig.InternalLogDestinationConfig, error) {
	var lokiLabels map[string]string
	var err error
	if loadLokiLabels {
		lokiLabels, err = pkgenv.GetEnvJSONFrom[map[string]string](
			env,
			"HETEROLOG_LOG_INTERNAL_LOG_LOKI_LABELS",
			map[string]string(nil),
			false,
		)
	}
	separateDebugLogDir, separateDebugLogDirErr := loadOptionalInternalLogBool(env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", ""), "HETEROLOG_LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR")

	return logconfig.InternalLogDestinationConfig{
		FileLogPath:                    env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_FILE_LOG_PATH", ""),
		FileTarget:                     env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_FILE_TARGET", ""),
		FileLogDir:                     env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_FILE_LOG_DIR", ""),
		FileErrorLogDir:                env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_FILE_ERROR_LOG_DIR", ""),
		FileDebugLogDir:                env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_FILE_DEBUG_LOG_DIR", ""),
		FileSeparateDebugLogDirEnabled: separateDebugLogDir,
		SlackChannel:                   env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_SLACK_CHANNEL", ""),
		SlackErrorChannel:              env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_SLACK_ERROR_CHANNEL", ""),
		SlackWarnChannel:               env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_SLACK_WARN_CHANNEL", ""),
		SlackInfoChannel:               env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_SLACK_INFO_CHANNEL", ""),
		SlackDebugChannel:              env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_SLACK_DEBUG_CHANNEL", ""),
		OpenSearchIndex:                env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_OPENSEARCH_INDEX", ""),
		LokiLabels:                     lokiLabels,

		CloudWatchLogsLogGroupName:  env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_CLOUDWATCH_LOG_GROUP_NAME", ""),
		CloudWatchLogsLogStreamName: env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_CLOUDWATCH_LOG_STREAM_NAME", ""),
		ClickHouseDatabase:          env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_CLICKHOUSE_DATABASE", ""),
		ClickHouseTable:             env.GetEnvString("HETEROLOG_LOG_INTERNAL_LOG_CLICKHOUSE_TABLE", ""),
	}, errors.Join(err, separateDebugLogDirErr)
}

// loadAsyncLoggerInternalLogDestinationConfigはLoggerContractAsyncBestEffortのinternal log用
// 保存先identity上書き値を読み込みます。Lokiが使われない場合はlabelsを解析しません。
func loadAsyncLoggerInternalLogDestinationConfig(
	env pkgenv.Lookup,
	loadLokiLabels bool,
) (logconfig.InternalLogDestinationConfig, error) {
	var lokiLabels map[string]string
	var err error
	if loadLokiLabels {
		lokiLabels, err = pkgenv.GetEnvJSONFrom[map[string]string](
			env,
			"HETEROLOG_ASYNC_LOG_INTERNAL_LOG_LOKI_LABELS",
			map[string]string(nil),
			false,
		)
	}
	separateDebugLogDir, separateDebugLogDirErr := loadOptionalInternalLogBool(env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", ""), "HETEROLOG_ASYNC_LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR")

	return logconfig.InternalLogDestinationConfig{
		FileLogPath:                    env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_FILE_LOG_PATH", ""),
		FileTarget:                     env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_FILE_TARGET", ""),
		FileLogDir:                     env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_FILE_LOG_DIR", ""),
		FileErrorLogDir:                env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_FILE_ERROR_LOG_DIR", ""),
		FileDebugLogDir:                env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_FILE_DEBUG_LOG_DIR", ""),
		FileSeparateDebugLogDirEnabled: separateDebugLogDir,
		SlackChannel:                   env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_SLACK_CHANNEL", ""),
		SlackErrorChannel:              env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_SLACK_ERROR_CHANNEL", ""),
		SlackWarnChannel:               env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_SLACK_WARN_CHANNEL", ""),
		SlackInfoChannel:               env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_SLACK_INFO_CHANNEL", ""),
		SlackDebugChannel:              env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_SLACK_DEBUG_CHANNEL", ""),
		OpenSearchIndex:                env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_OPENSEARCH_INDEX", ""),
		LokiLabels:                     lokiLabels,

		CloudWatchLogsLogGroupName:  env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_CLOUDWATCH_LOG_GROUP_NAME", ""),
		CloudWatchLogsLogStreamName: env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_CLOUDWATCH_LOG_STREAM_NAME", ""),
		ClickHouseDatabase:          env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_CLICKHOUSE_DATABASE", ""),
		ClickHouseTable:             env.GetEnvString("HETEROLOG_ASYNC_LOG_INTERNAL_LOG_CLICKHOUSE_TABLE", ""),
	}, errors.Join(err, separateDebugLogDirErr)
}

// loadDeliveryLoggerInternalLogDestinationConfigはLoggerContractDeliveryのinternal log用
// 保存先identity上書き値を読み込みます。Lokiが使われない場合はlabelsを解析しません。
func loadDeliveryLoggerInternalLogDestinationConfig(
	env pkgenv.Lookup,
	loadLokiLabels bool,
) (logconfig.InternalLogDestinationConfig, error) {
	var lokiLabels map[string]string
	var err error
	if loadLokiLabels {
		lokiLabels, err = pkgenv.GetEnvJSONFrom[map[string]string](
			env,
			"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_LOKI_LABELS",
			map[string]string(nil),
			false,
		)
	}
	separateDebugLogDir, separateDebugLogDirErr := loadOptionalInternalLogBool(env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", ""), "HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR")

	return logconfig.InternalLogDestinationConfig{
		FileLogPath:                    env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_FILE_LOG_PATH", ""),
		FileTarget:                     env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_FILE_TARGET", ""),
		FileLogDir:                     env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_FILE_LOG_DIR", ""),
		FileErrorLogDir:                env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_FILE_ERROR_LOG_DIR", ""),
		FileDebugLogDir:                env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_FILE_DEBUG_LOG_DIR", ""),
		FileSeparateDebugLogDirEnabled: separateDebugLogDir,
		SlackChannel:                   env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_SLACK_CHANNEL", ""),
		SlackErrorChannel:              env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_SLACK_ERROR_CHANNEL", ""),
		SlackWarnChannel:               env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_SLACK_WARN_CHANNEL", ""),
		SlackInfoChannel:               env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_SLACK_INFO_CHANNEL", ""),
		SlackDebugChannel:              env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_SLACK_DEBUG_CHANNEL", ""),
		OpenSearchIndex:                env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_INDEX", ""),
		LokiLabels:                     lokiLabels,

		CloudWatchLogsLogGroupName:  env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_LOG_GROUP_NAME", ""),
		CloudWatchLogsLogStreamName: env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_LOG_STREAM_NAME", ""),
		ClickHouseDatabase:          env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLICKHOUSE_DATABASE", ""),
		ClickHouseTable:             env.GetEnvString("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLICKHOUSE_TABLE", ""),
	}, errors.Join(err, separateDebugLogDirErr)
}

// loadOptionalInternalLogBoolは読み込み済みの値から、空文字の継承と明示的なfalseを区別します。
// 非空の入力は通常のboolean environment値と同じ規則で解析します。keyはエラー表示用です。
func loadOptionalInternalLogBool(raw, key string) (*bool, error) {
	if raw == "" {
		return nil, nil
	}
	value, err := pkgenv.ParseBool(raw)
	if err != nil {
		return nil, newInvalidEnvironmentValueError(key, raw, err, "environment variable %q %v", key, err)
	}
	return &value, nil
}
