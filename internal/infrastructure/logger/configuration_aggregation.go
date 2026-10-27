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
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/onerrorhandler"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

type handlerStruct struct {
	OnError []logging.NamedLoggerErrorHandler
	OnWarn  []logging.NamedLoggerHandler
	OnInfo  []logging.NamedLoggerHandler
	OnDebug []logging.NamedLoggerHandler
}

// getHandlerは通知先ごとに設定providerを一度組み立て、各levelのfallback handlerへ渡します。
// 共有するのは設定だけです。handlerとその内部loggerはlevel・呼び出し元ごとに分離します。
// Fileだけは、同じtop-level loggerのLoggerSetからrole別writerを取得して共有します。
// stdoutTargetは、loggerの構築時に作ったstdoutの通知先です（newStdoutNotificationTarget）。
// nilならstdoutの候補を作りません。通知先に選ばれたstdoutの設定が不正な場合は、構築が先に
// 設定errorで終わるため、通知の時点でstdoutの書き込み先を作り直すことはありません。
func getHandler(
	cfg pkglogger.LoggerConfig,
	cloudWatchLogsStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting,
	fileLoggerSet *file.LoggerSet,
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
		loggerCfg := slack.NewConfigAdapter(*cfg.Slack)
		available.OnError = append(available.OnError, slack.NewNamedErrorFallbackHandler(loggerCfg))
		available.OnWarn = append(available.OnWarn, slack.NewNamedWarnFallbackHandler(loggerCfg))
		available.OnInfo = append(available.OnInfo, slack.NewNamedInfoFallbackHandler(loggerCfg))
		available.OnDebug = append(available.OnDebug, slack.NewNamedDebugFallbackHandler(loggerCfg))
	}
	if cfg.OpenSearch != nil && cfg.OpenSearchClient != nil {
		loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg := opensearchlogger.NewOpenSearchLoggerAdapter(cfg)
		available.OnError = append(available.OnError, opensearchlogger.NewNamedErrorFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnWarn = append(available.OnWarn, opensearchlogger.NewNamedWarnFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnInfo = append(available.OnInfo, opensearchlogger.NewNamedInfoFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnDebug = append(available.OnDebug, opensearchlogger.NewNamedDebugFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
	}
	if cfg.Loki != nil && cfg.LokiClient != nil {
		loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg := lokilogger.NewLokiLoggerAdapter(cfg)
		available.OnError = append(available.OnError, lokilogger.NewNamedErrorFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnWarn = append(available.OnWarn, lokilogger.NewNamedWarnFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnInfo = append(available.OnInfo, lokilogger.NewNamedInfoFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnDebug = append(available.OnDebug, lokilogger.NewNamedDebugFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
	}
	if cfg.CloudWatchLogs != nil && cfg.CloudWatchLogsClient != nil {
		loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg := cloudwatchlogslogger.NewCloudWatchLogsLoggerAdapter(cfg, cloudWatchLogsStreamRoutingGranularity)
		available.OnError = append(available.OnError, cloudwatchlogslogger.NewNamedErrorFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnWarn = append(available.OnWarn, cloudwatchlogslogger.NewNamedWarnFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnInfo = append(available.OnInfo, cloudwatchlogslogger.NewNamedInfoFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnDebug = append(available.OnDebug, cloudwatchlogslogger.NewNamedDebugFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
	}
	if cfg.ClickHouse != nil && cfg.ClickHouseClient != nil {
		loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg := clickhouselogger.NewClickHouseLoggerAdapter(cfg)
		available.OnError = append(available.OnError, clickhouselogger.NewNamedErrorFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnWarn = append(available.OnWarn, clickhouselogger.NewNamedWarnFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnInfo = append(available.OnInfo, clickhouselogger.NewNamedInfoFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
		available.OnDebug = append(available.OnDebug, clickhouselogger.NewNamedDebugFallbackHandler(loggerCfg, clientCfg, fmCfg, resourceProvider, retryCfg))
	}
	return available
}

// selectConfiguredHandlersはgetHandlerが構築した全候補から、個別loggerの
// HandlerConfigでlevelごとに指定されたsourceだけを選択します。
// handlerの合成や実行は行わず、availableとcfgも変更しません。
func selectConfiguredHandlers(
	available handlerStruct,
	cfg loggerhandler.ConfigProvider,
) handlerStruct {
	if cfg == nil {
		return handlerStruct{}
	}

	return handlerStruct{
		OnError: selectNamedErrorHandlers(cfg.OnError(), available.OnError),
		OnWarn:  selectNamedHandlers(cfg.OnWarn(), available.OnWarn),
		OnInfo:  selectNamedHandlers(cfg.OnInfo(), available.OnInfo),
		OnDebug: selectNamedHandlers(cfg.OnDebug(), available.OnDebug),
	}
}

// failureStoreInternalLogHandlerFactoryはdurable storeごとにfallback handler候補を新しく構築し、
// store固有のhandler名だけを選択して、既存loggerと同じ委譲・panic保護へ接続します。
// sourceには呼出元backendを受け取り、factoryへ渡されるdurable typeを加えることで、
// spoolとfile archiveのCloserが同じ診断sourceになることを防ぎます。
//
// newAvailableをfactory呼び出しの外で評価すると、複数のdurable storeやsource loggerが
// statefulなfallback loggerを共有してしまうため、必ずstore設定を解決するたびに呼びます。
func failureStoreInternalLogHandlerFactory(
	source string,
	newAvailable func(string) handlerStruct,
) flushfailure.InternalLogHandlerFactory {
	return func(
		durableType pkgdurable.DurableType,
		cfg loggerhandler.ConfigProvider,
	) flushfailure.InternalHandlers {
		handlerSource := source + ".failure_store." + string(durableType)
		selected := selectConfiguredHandlers(newAvailable(handlerSource), cfg)

		var resolved flushfailure.InternalHandlers
		if len(selected.OnError) > 0 {
			resolved.OnError = onerrorhandler.CombineErrorHandlers(selected.OnError...)
		}
		if len(selected.OnWarn) > 0 {
			resolved.OnWarn = onerrorhandler.CombineWarnHandlers(selected.OnWarn...)
		}
		if len(selected.OnInfo) > 0 {
			resolved.OnInfo = onerrorhandler.CombineInfoHandlers(selected.OnInfo...)
		}
		if len(selected.OnDebug) > 0 {
			resolved.OnDebug = onerrorhandler.CombineDebugHandlers(selected.OnDebug...)
		}
		return resolved
	}
}

func selectNamedErrorHandlers(
	names []string,
	available []logging.NamedLoggerErrorHandler,
) []logging.NamedLoggerErrorHandler {
	byName := make(map[string]logging.NamedLoggerErrorHandler, len(available))
	for _, candidate := range available {
		name := normalizeHandlerName(candidate.Name)
		if name != "" && candidate.Handler != nil {
			if _, exists := byName[name]; !exists {
				byName[name] = candidate
			}
		}
	}

	selected := make([]logging.NamedLoggerErrorHandler, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, configuredName := range names {
		name := normalizeHandlerName(configuredName)
		candidate, exists := byName[name]
		if !exists {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		selected = append(selected, candidate)
	}
	return selected
}

func selectNamedHandlers(
	names []string,
	available []logging.NamedLoggerHandler,
) []logging.NamedLoggerHandler {
	byName := make(map[string]logging.NamedLoggerHandler, len(available))
	for _, candidate := range available {
		name := normalizeHandlerName(candidate.Name)
		if name != "" && candidate.Handler != nil {
			if _, exists := byName[name]; !exists {
				byName[name] = candidate
			}
		}
	}

	selected := make([]logging.NamedLoggerHandler, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, configuredName := range names {
		name := normalizeHandlerName(configuredName)
		candidate, exists := byName[name]
		if !exists {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		selected = append(selected, candidate)
	}
	return selected
}

func normalizeHandlerName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
