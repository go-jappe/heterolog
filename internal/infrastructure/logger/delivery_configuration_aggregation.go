package logger

import (
	clickhouselogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	cloudwatchlogslogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	lokilogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki"
	opensearchlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/slack"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// getDeliveryHandlerはDelivery configに存在する全backendから、戻り値なしの
// internal fallback候補を構築します。LogOutputDestinationsには依存しないため、
// handler-only targetも通常Delivery sinkを起動せず利用できます。
// 設定providerはbackendごとに一度だけ構築し、各levelで共有します。
// 呼び出すたびに各destination・level専用のhandlerを新しく構築するため、生成結果を
// 複数のsource logger間で共有してはいけません。File handlerは同じDeliveryLoggerSetから
// role別DeliveryFileLoggerを取得し、handler状態ではなくfile loggerとそのresourceを共有します。
// stdoutTargetは、loggerの構築時に作ったstdoutの通知先です（newDeliveryStdoutNotificationTarget）。
// nilならstdoutの候補を作りません。通知先に選ばれたstdoutの設定が不正な場合は、構築が先に
// 設定errorで終わるため、通知の時点でstdoutの書き込み先を作り直すことはありません。
func getDeliveryHandler(
	cfg pkglogger.DeliveryLoggerConfig,
	cloudWatchLogsStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting,
	fileLoggerSet *file.DeliveryLoggerSet,
	stdoutTarget *stdout.NotificationTarget,
) handlerStruct {
	var available handlerStruct

	if cfg.File != nil && fileLoggerSet != nil {
		available.OnError = append(available.OnError, fileLoggerSet.NewNamedErrorFallbackHandler())
		available.OnWarn = append(available.OnWarn, fileLoggerSet.NewNamedWarnFallbackHandler())
		available.OnInfo = append(available.OnInfo, fileLoggerSet.NewNamedInfoFallbackHandler())
		available.OnDebug = append(available.OnDebug, fileLoggerSet.NewNamedDebugFallbackHandler())
	}

	if stdoutTarget != nil {
		available.OnError = append(available.OnError, stdoutTarget.NamedErrorHandler())
		available.OnWarn = append(available.OnWarn, stdoutTarget.NamedWarnHandler())
		available.OnInfo = append(available.OnInfo, stdoutTarget.NamedInfoHandler())
		available.OnDebug = append(available.OnDebug, stdoutTarget.NamedDebugHandler())
	}

	if cfg.Slack != nil {
		loggerCfg := deliverySlackNotificationConfig(cfg)
		available.OnError = append(available.OnError, slack.NewNamedErrorFallbackHandler(loggerCfg))
		available.OnWarn = append(available.OnWarn, slack.NewNamedWarnFallbackHandler(loggerCfg))
		available.OnInfo = append(available.OnInfo, slack.NewNamedInfoFallbackHandler(loggerCfg))
		available.OnDebug = append(available.OnDebug, slack.NewNamedDebugFallbackHandler(loggerCfg))
	}

	if cfg.OpenSearch != nil && cfg.OpenSearchClient != nil {
		loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg := deliveryOpenSearchNotificationConfig(cfg)
		available.OnError = append(available.OnError,
			opensearchlogger.NewNamedErrorFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnWarn = append(available.OnWarn,
			opensearchlogger.NewNamedWarnFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnInfo = append(available.OnInfo,
			opensearchlogger.NewNamedInfoFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnDebug = append(available.OnDebug,
			opensearchlogger.NewNamedDebugFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
	}

	if cfg.Loki != nil && cfg.LokiClient != nil {
		loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg := deliveryLokiNotificationConfig(cfg)
		available.OnError = append(available.OnError,
			lokilogger.NewNamedErrorFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnWarn = append(available.OnWarn,
			lokilogger.NewNamedWarnFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnInfo = append(available.OnInfo,
			lokilogger.NewNamedInfoFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnDebug = append(available.OnDebug,
			lokilogger.NewNamedDebugFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
	}

	if cfg.CloudWatchLogs != nil && cfg.CloudWatchLogsClient != nil {
		loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg := deliveryCloudWatchLogsNotificationConfig(
			cfg,
			cloudWatchLogsStreamRoutingGranularity,
		)
		available.OnError = append(available.OnError,
			cloudwatchlogslogger.NewNamedErrorFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnWarn = append(available.OnWarn,
			cloudwatchlogslogger.NewNamedWarnFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnInfo = append(available.OnInfo,
			cloudwatchlogslogger.NewNamedInfoFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnDebug = append(available.OnDebug,
			cloudwatchlogslogger.NewNamedDebugFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
	}

	if cfg.ClickHouse != nil && cfg.ClickHouseClient != nil {
		loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg := deliveryClickHouseNotificationConfig(cfg)
		available.OnError = append(available.OnError,
			clickhouselogger.NewNamedErrorFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnWarn = append(available.OnWarn,
			clickhouselogger.NewNamedWarnFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnInfo = append(available.OnInfo,
			clickhouselogger.NewNamedInfoFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
		available.OnDebug = append(available.OnDebug,
			clickhouselogger.NewNamedDebugFallbackHandler(
				loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg,
			),
		)
	}

	return available
}
