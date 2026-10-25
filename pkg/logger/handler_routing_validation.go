package logger

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-jappe/heterolog/pkg/logger/durable"
	loggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

// ErrHandlerSelectorNotConfiguredは、handler selectorが同じ設定内のbase backendや
// internal-log専用保存先へ解決できないことを表します。
var ErrHandlerSelectorNotConfigured = errors.New("handler selector is not configured")

// HandlerSelectorErrorは、解決できないhandler selectorを表します。errors.Asで取り出すと
// selectorを指定したsource、log level、selectorを特定でき、errors.Isでは
// ErrHandlerSelectorNotConfiguredと一致します。
type HandlerSelectorError struct {
	// Sourceは"logger stdout"など、selectorを指定したsourceです。
	Source string
	// Levelはselectorを指定したlog levelで、"error"、"warn"、"info"、"debug"のいずれかです。
	Level string
	// Selectorは設定値の前後の空白を除いたselectorです。
	Selector string
}

func (e *HandlerSelectorError) Error() string {
	return fmt.Sprintf("%s %s handler selector %q is not configured", e.Source, e.Level, e.Selector)
}

func (e *HandlerSelectorError) Unwrap() error { return ErrHandlerSelectorNotConfigured }

// handlerRoutingSourceは、runtimeで実際に起動するsourceと、そのinternal logの
// 委譲先selectorを保持します。backend activationとrouting validationは同じsource
// 列挙を使用し、片方だけにlifecycleやdurable storeを追加する不整合を防ぎます。
type handlerRoutingSource struct {
	sourceSuffix string
	config       loggerhandler.HandlerConfig
}

// ValidateLoggerHandlerRoutingは、sync/async loggerの有効なsourceが指定した
// handler selectorを、同じLoggerConfigに存在するbase backendまたは
// internal-log専用保存先へ構造的に解決できることを検証します。client nilや
// endpoint/indexなどの実値はValidateLoggerBackendConfigsが検証します。
//
// 通常出力として未選択のbackendもhandler-only targetとして利用できます。一方、
// 未選択source自身のrouting、DurableTypeで未選択のdurable storeのroutingは
// runtimeで実行されないため検証対象に含めません。
func ValidateLoggerHandlerRouting(sourceLabel string, cfg LoggerConfig) error {
	available := loggerHandlerSelectors(cfg)
	var errs []error
	for _, source := range loggerActiveHandlerRoutingSources(cfg) {
		errs = append(errs, validateHandlerRouting(
			sourceLabel+" "+source.sourceSuffix,
			source.config,
			available,
		))
	}
	return errors.Join(errs...)
}

// ValidateDeliveryLoggerHandlerRoutingは、Delivery loggerの有効なsourceについて
// ValidateLoggerHandlerRoutingと同じselector契約を検証します。Delivery failure
// storeはfile archiveだけを使用するため、spool routingは対象に含めません。
func ValidateDeliveryLoggerHandlerRouting(
	sourceLabel string,
	cfg DeliveryLoggerConfig,
) error {
	available := deliveryLoggerHandlerSelectors(cfg)
	var errs []error
	for _, source := range deliveryLoggerActiveHandlerRoutingSources(cfg) {
		errs = append(errs, validateHandlerRouting(
			sourceLabel+" "+source.sourceSuffix,
			source.config,
			available,
		))
	}
	return errors.Join(errs...)
}

func validateHandlerRouting(
	sourceLabel string,
	routing loggerhandler.HandlerConfig,
	available map[string]struct{},
) error {
	checks := []struct {
		level     string
		selectors []string
	}{
		{level: "error", selectors: routing.OnErrorConfig},
		{level: "warn", selectors: routing.OnWarnConfig},
		{level: "info", selectors: routing.OnInfoConfig},
		{level: "debug", selectors: routing.OnDebugConfig},
	}

	var errs []error
	for _, check := range checks {
		for _, configured := range check.selectors {
			selector := normalizeHandlerSelector(configured)
			if selector == "" {
				continue
			}
			if _, exists := available[selector]; exists {
				continue
			}
			errs = append(errs, &HandlerSelectorError{
				Source:   sourceLabel,
				Level:    check.level,
				Selector: strings.TrimSpace(configured),
			})
		}
	}
	return errors.Join(errs...)
}

func loggerActiveHandlerRoutingSources(cfg LoggerConfig) []handlerRoutingSource {
	destinations := cfg.LogOutputDestinations
	sources := make([]handlerRoutingSource, 0, 16)

	if cfg.File != nil {
		if configLogOutputEnabled(destinations, LogOutputFile) {
			sources = appendHandlerRoutingSource(sources, "file", cfg.File.HandlerConfig)
		}
	}
	if configLogOutputEnabled(destinations, LogOutputStdout) && cfg.Stdout != nil {
		sources = appendHandlerRoutingSource(sources, "stdout", cfg.Stdout.HandlerConfig)
	}
	if configLogOutputEnabled(destinations, LogOutputSlack) && cfg.Slack != nil {
		sources = appendHandlerRoutingSource(sources, "slack", cfg.Slack.HandlerConfig)
	}
	if configLogOutputEnabled(destinations, LogOutputOpenSearch) && cfg.OpenSearch != nil {
		sources = appendHandlerRoutingSource(sources, "opensearch", cfg.OpenSearch.HandlerConfig)
		sources = appendFailureStoreHandlerRoutingSources(
			sources,
			"opensearch",
			cfg.OpenSearch.FailureStoreConfig,
		)
	}
	if configLogOutputEnabled(destinations, LogOutputLoki) && cfg.Loki != nil {
		sources = appendHandlerRoutingSource(sources, "loki", cfg.Loki.HandlerConfig)
		sources = appendFailureStoreHandlerRoutingSources(
			sources,
			"loki",
			cfg.Loki.FailureStoreConfig,
		)
	}
	if configLogOutputEnabled(destinations, LogOutputCloudWatchLogs) &&
		cfg.CloudWatchLogs != nil {
		sources = appendHandlerRoutingSource(
			sources,
			"cloudwatch logs",
			cfg.CloudWatchLogs.HandlerConfig,
		)
		sources = appendFailureStoreHandlerRoutingSources(
			sources,
			"cloudwatch logs",
			cfg.CloudWatchLogs.FailureStoreConfig,
		)
	}
	if configLogOutputEnabled(destinations, LogOutputClickHouse) && cfg.ClickHouse != nil {
		sources = appendHandlerRoutingSource(sources, "clickhouse", cfg.ClickHouse.HandlerConfig)
		sources = appendFailureStoreHandlerRoutingSources(
			sources,
			"clickhouse",
			cfg.ClickHouse.FailureStoreConfig,
		)
	}
	if loggerBaseFileRuntimeEnabled(cfg, sources) &&
		cfg.File.LifecycleRunnerConfig() != nil {
		sources = appendHandlerRoutingSource(
			sources,
			"appendlog maintenance runner",
			cfg.File.LifecycleHandlerConfig,
		)
	}
	return sources
}

func appendFailureStoreHandlerRoutingSources(
	sources []handlerRoutingSource,
	sourceSuffix string,
	cfg durable.FailureStoreConfig,
) []handlerRoutingSource {
	if containsConfiguredDurableType(cfg.DurableType, durable.DurableTypeFileArchive) {
		sources = appendHandlerRoutingSource(
			sources,
			sourceSuffix+" failure file archive",
			cfg.DurableStoreConfig.FileArchiveConfig.HandlerConfig,
		)
	}
	if containsConfiguredDurableType(cfg.DurableType, durable.DurableTypeSpool) {
		sources = appendHandlerRoutingSource(
			sources,
			sourceSuffix+" failure spool",
			cfg.DurableStoreConfig.SpoolConfig.HandlerConfig,
		)
	}
	return sources
}

func deliveryLoggerActiveHandlerRoutingSources(
	cfg DeliveryLoggerConfig,
) []handlerRoutingSource {
	destinations := cfg.LogOutputDestinations
	sources := make([]handlerRoutingSource, 0, 12)

	if cfg.File != nil {
		if configLogOutputEnabled(destinations, LogOutputFile) {
			sources = appendHandlerRoutingSource(sources, "file", cfg.File.HandlerConfig)
		}
	}
	if configLogOutputEnabled(destinations, LogOutputStdout) && cfg.Stdout != nil {
		sources = appendHandlerRoutingSource(sources, "stdout", cfg.Stdout.HandlerConfig)
	}
	if configLogOutputEnabled(destinations, LogOutputSlack) && cfg.Slack != nil {
		sources = appendHandlerRoutingSource(sources, "slack", cfg.Slack.HandlerConfig)
	}
	if configLogOutputEnabled(destinations, LogOutputOpenSearch) && cfg.OpenSearch != nil {
		sources = appendHandlerRoutingSource(sources, "opensearch", cfg.OpenSearch.HandlerConfig)
		sources = appendDeliveryFailureStoreHandlerRoutingSources(
			sources,
			"opensearch",
			cfg.OpenSearch.FailureStoreConfig,
		)
	}
	if configLogOutputEnabled(destinations, LogOutputLoki) && cfg.Loki != nil {
		sources = appendHandlerRoutingSource(sources, "loki", cfg.Loki.HandlerConfig)
		sources = appendDeliveryFailureStoreHandlerRoutingSources(
			sources,
			"loki",
			cfg.Loki.FailureStoreConfig,
		)
	}
	if configLogOutputEnabled(destinations, LogOutputCloudWatchLogs) &&
		cfg.CloudWatchLogs != nil {
		sources = appendHandlerRoutingSource(
			sources,
			"cloudwatch logs",
			cfg.CloudWatchLogs.HandlerConfig,
		)
		sources = appendDeliveryFailureStoreHandlerRoutingSources(
			sources,
			"cloudwatch logs",
			cfg.CloudWatchLogs.FailureStoreConfig,
		)
	}
	if configLogOutputEnabled(destinations, LogOutputClickHouse) && cfg.ClickHouse != nil {
		sources = appendHandlerRoutingSource(sources, "clickhouse", cfg.ClickHouse.HandlerConfig)
		sources = appendDeliveryFailureStoreHandlerRoutingSources(
			sources,
			"clickhouse",
			cfg.ClickHouse.FailureStoreConfig,
		)
	}
	if deliveryLoggerBaseFileRuntimeEnabled(cfg, sources) {
		if lifecycleCfg := cfg.File.LifecycleRunnerConfig(); lifecycleCfg != nil {
			sources = appendHandlerRoutingSource(
				sources,
				"appendlog maintenance runner",
				lifecycleCfg.HandlerConfig,
			)
		}
	}
	return sources
}

// loggerBaseFileRuntimeEnabledは、base FileLoggerSetが通常出力またはinternal logの
// 委譲先として実際に必要かを返します。専用internal-log Fileがある場合、base File
// 設定はその専用設定のtemplateにすぎないため、base lifecycleもactiveにしません。
//
// sourcesにはbase File lifecycle以外のactive sourceだけを渡します。これにより、
// 使用されないbase lifecycle自身のroutingがFile runtimeを自己起動する循環を防ぎます。
func loggerBaseFileRuntimeEnabled(
	cfg LoggerConfig,
	sources []handlerRoutingSource,
) bool {
	if cfg.File == nil {
		return false
	}
	if configLogOutputEnabled(cfg.LogOutputDestinations, LogOutputFile) {
		return true
	}
	if cfg.InternalLogDestinations != nil &&
		cfg.InternalLogDestinations.File != nil {
		return false
	}
	return handlerRoutingSourcesUseTarget(sources, LogOutputFile)
}

func deliveryLoggerBaseFileRuntimeEnabled(
	cfg DeliveryLoggerConfig,
	sources []handlerRoutingSource,
) bool {
	if cfg.File == nil {
		return false
	}
	if configLogOutputEnabled(cfg.LogOutputDestinations, LogOutputFile) {
		return true
	}
	if cfg.InternalLogDestinations != nil &&
		cfg.InternalLogDestinations.File != nil {
		return false
	}
	return handlerRoutingSourcesUseTarget(sources, LogOutputFile)
}

func handlerRoutingSourcesUseTarget(
	sources []handlerRoutingSource,
	target LogOutputDestination,
) bool {
	for _, source := range sources {
		if handlerRoutingUsesBaseTarget(source.config, target) {
			return true
		}
	}
	return false
}

func appendDeliveryFailureStoreHandlerRoutingSources(
	sources []handlerRoutingSource,
	sourceSuffix string,
	cfg durable.DeliveryFailureStoreConfig,
) []handlerRoutingSource {
	if !containsConfiguredDurableType(cfg.DurableType, durable.DurableTypeFileArchive) {
		return sources
	}
	return appendHandlerRoutingSource(
		sources,
		sourceSuffix+" failure file archive",
		cfg.FileArchiveConfig.HandlerConfig,
	)
}

func appendHandlerRoutingSource(
	sources []handlerRoutingSource,
	sourceSuffix string,
	cfg loggerhandler.HandlerConfig,
) []handlerRoutingSource {
	return append(sources, handlerRoutingSource{
		sourceSuffix: sourceSuffix,
		config:       cfg,
	})
}

func loggerHandlerSelectors(cfg LoggerConfig) map[string]struct{} {
	selectors := make(map[string]struct{})
	appendLoggerHandlerSelectors(selectors, cfg)
	if cfg.InternalLogDestinations != nil {
		appendLoggerHandlerSelectors(
			selectors,
			cfg.InternalLogDestinations.ToLoggerConfig(),
		)
	}
	return selectors
}

func appendLoggerHandlerSelectors(
	selectors map[string]struct{},
	cfg LoggerConfig,
) {
	addHandlerSelector(selectors, LogOutputFile, cfg.File != nil)
	addHandlerSelector(selectors, LogOutputStdout, cfg.Stdout != nil)
	addHandlerSelector(selectors, LogOutputSlack, cfg.Slack != nil)
	addHandlerSelector(
		selectors,
		LogOutputOpenSearch,
		cfg.OpenSearch != nil,
	)
	addHandlerSelector(
		selectors,
		LogOutputLoki,
		cfg.Loki != nil,
	)
	addHandlerSelector(
		selectors,
		LogOutputCloudWatchLogs,
		cfg.CloudWatchLogs != nil,
	)
	addHandlerSelector(
		selectors,
		LogOutputClickHouse,
		cfg.ClickHouse != nil,
	)
}

func deliveryLoggerHandlerSelectors(
	cfg DeliveryLoggerConfig,
) map[string]struct{} {
	selectors := make(map[string]struct{})
	appendDeliveryLoggerHandlerSelectors(selectors, cfg)
	if cfg.InternalLogDestinations != nil {
		appendDeliveryLoggerHandlerSelectors(
			selectors,
			cfg.InternalLogDestinations.ToLoggerConfig(),
		)
	}
	return selectors
}

func appendDeliveryLoggerHandlerSelectors(
	selectors map[string]struct{},
	cfg DeliveryLoggerConfig,
) {
	addHandlerSelector(selectors, LogOutputFile, cfg.File != nil)
	addHandlerSelector(selectors, LogOutputStdout, cfg.Stdout != nil)
	addHandlerSelector(selectors, LogOutputSlack, cfg.Slack != nil)
	addHandlerSelector(
		selectors,
		LogOutputOpenSearch,
		cfg.OpenSearch != nil,
	)
	addHandlerSelector(
		selectors,
		LogOutputLoki,
		cfg.Loki != nil,
	)
	addHandlerSelector(
		selectors,
		LogOutputCloudWatchLogs,
		cfg.CloudWatchLogs != nil,
	)
	addHandlerSelector(
		selectors,
		LogOutputClickHouse,
		cfg.ClickHouse != nil,
	)
}

func addHandlerSelector(
	selectors map[string]struct{},
	backend LogOutputDestination,
	available bool,
) {
	if !available {
		return
	}
	selectors[normalizeHandlerSelector(backend.String())] = struct{}{}
}

func containsConfiguredDurableType(
	types []durable.DurableType,
	want durable.DurableType,
) bool {
	for _, configured := range types {
		if configured == want {
			return true
		}
	}
	return false
}
