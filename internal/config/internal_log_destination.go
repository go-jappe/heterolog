package config

import (
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// newInternalLogDestinationsは、internal logで実際に使用され、かつ通常backendと異なる
// 保存先identityが指定されたbackendだけを、専用handler設定へ変換します。
//
// timeout、retry、認証などはbaseから継承します。overrideがないbackendは戻り値へ
// 含めず、runtimeがbase設定とresourceをそのまま共有します。
func newInternalLogDestinations(
	override logconfig.InternalLogDestinationConfig,
	usesTarget func(pkglogger.LogOutputDestination) bool,
	base pkglogger.LoggerConfig,
) *pkglogger.InternalLogDestinationConfig {
	destinations := pkglogger.InternalLogDestinationConfig{}
	configured := false

	if usesTarget(pkglogger.LogOutputFile) &&
		base.File != nil &&
		internalLogFileDestinationOverridden(override, base.File.FsstoreConfig.Layout) {
		cfg := *base.File
		overrideInternalLogFileLayout(&cfg.FsstoreConfig.Layout, override)
		cfg.ArchiveCheckpointConfig.SourcePrefix += "_internal_log"
		// internal-log fileはterminal sinkです。保持・圧縮・purge policyは継承しますが、
		// そのdirectoryのrunner結果を同じinternal routingへ再投入しません。
		cfg.LifecycleHandlerConfig = pkgloggerhandler.HandlerConfig{}
		destinations.File = &cfg
		configured = true
	}
	if usesTarget(pkglogger.LogOutputSlack) &&
		internalLogSlackDestinationOverridden(override) &&
		base.Slack != nil {
		cfg := *base.Slack
		overrideInternalLogSlackChannels(
			&cfg.Channel,
			&cfg.ErrorChannel,
			&cfg.WarnChannel,
			&cfg.InfoChannel,
			&cfg.DebugChannel,
			override,
		)
		destinations.Slack = &cfg
		configured = true
	}
	if usesTarget(pkglogger.LogOutputOpenSearch) &&
		override.OpenSearchIndex != "" &&
		base.OpenSearch != nil &&
		base.OpenSearchClient != nil {
		cfg := *base.OpenSearch
		cfg.Index = override.OpenSearchIndex
		destinations.OpenSearch = &cfg
		destinations.OpenSearchClient = base.OpenSearchClient
		configured = true
	}
	if usesTarget(pkglogger.LogOutputLoki) &&
		override.LokiLabels != nil &&
		base.Loki != nil &&
		base.LokiClient != nil {
		cfg := *base.Loki
		cfg.StaticLabels = override.LokiLabels
		destinations.Loki = &cfg
		destinations.LokiClient = base.LokiClient
		configured = true
	}
	if usesTarget(pkglogger.LogOutputCloudWatchLogs) &&
		internalLogCloudWatchLogsDestinationOverridden(override) &&
		base.CloudWatchLogs != nil &&
		base.CloudWatchLogsClient != nil {
		cfg := *base.CloudWatchLogs
		client := *base.CloudWatchLogsClient
		overrideInternalLogCloudWatchLogsDestination(
			&cfg.LogGroupName,
			&cfg.LogStreamName,
			&client.LogGroupName,
			override,
		)
		destinations.CloudWatchLogs = &cfg
		destinations.CloudWatchLogsClient = &client
		configured = true
	}
	if usesTarget(pkglogger.LogOutputClickHouse) &&
		internalLogClickHouseDestinationOverridden(override) &&
		base.ClickHouse != nil &&
		base.ClickHouseClient != nil {
		cfg := *base.ClickHouse
		client := *base.ClickHouseClient
		if override.ClickHouseDatabase != "" {
			client.Database = override.ClickHouseDatabase
		}
		if override.ClickHouseTable != "" {
			client.Table = override.ClickHouseTable
		}
		destinations.ClickHouse = &cfg
		destinations.ClickHouseClient = &client
		configured = true
	}

	if !configured {
		return nil
	}
	return &destinations
}

// newDeliveryInternalLogDestinationsはDelivery loggerについて、通常backendと
// 異なるinternal-log保存先だけを専用handler設定へ変換します。
func newDeliveryInternalLogDestinations(
	override logconfig.InternalLogDestinationConfig,
	usesTarget func(pkglogger.LogOutputDestination) bool,
	base pkglogger.DeliveryLoggerConfig,
) *pkglogger.DeliveryInternalLogDestinationConfig {
	destinations := pkglogger.DeliveryInternalLogDestinationConfig{}
	configured := false

	if usesTarget(pkglogger.LogOutputFile) &&
		base.File != nil &&
		internalLogFileDestinationOverridden(override, base.File.FsstoreConfig.Layout) {
		cfg := *base.File
		overrideInternalLogFileLayout(&cfg.FsstoreConfig.Layout, override)
		cfg.LifecycleRunnerSettings.ArchiveCheckpointConfig.SourcePrefix += "_internal_log"
		cfg.LifecycleRunnerSettings.LifecycleHandlerConfig = pkgloggerhandler.HandlerConfig{}
		destinations.File = &cfg
		configured = true
	}
	if usesTarget(pkglogger.LogOutputSlack) &&
		internalLogSlackDestinationOverridden(override) &&
		base.Slack != nil {
		cfg := *base.Slack
		overrideInternalLogSlackChannels(
			&cfg.Channel,
			&cfg.ErrorChannel,
			&cfg.WarnChannel,
			&cfg.InfoChannel,
			&cfg.DebugChannel,
			override,
		)
		destinations.Slack = &cfg
		configured = true
	}
	if usesTarget(pkglogger.LogOutputOpenSearch) &&
		override.OpenSearchIndex != "" &&
		base.OpenSearch != nil &&
		base.OpenSearchClient != nil {
		cfg := *base.OpenSearch
		cfg.Index = override.OpenSearchIndex
		destinations.OpenSearch = &cfg
		destinations.OpenSearchClient = base.OpenSearchClient
		configured = true
	}
	if usesTarget(pkglogger.LogOutputLoki) &&
		override.LokiLabels != nil &&
		base.Loki != nil &&
		base.LokiClient != nil {
		cfg := *base.Loki
		cfg.StaticLabels = override.LokiLabels
		destinations.Loki = &cfg
		destinations.LokiClient = base.LokiClient
		configured = true
	}
	if usesTarget(pkglogger.LogOutputCloudWatchLogs) &&
		internalLogCloudWatchLogsDestinationOverridden(override) &&
		base.CloudWatchLogs != nil &&
		base.CloudWatchLogsClient != nil {
		cfg := *base.CloudWatchLogs
		client := *base.CloudWatchLogsClient
		overrideInternalLogCloudWatchLogsDestination(
			&cfg.LogGroupName,
			&cfg.LogStreamName,
			&client.LogGroupName,
			override,
		)
		destinations.CloudWatchLogs = &cfg
		destinations.CloudWatchLogsClient = &client
		configured = true
	}
	if usesTarget(pkglogger.LogOutputClickHouse) &&
		internalLogClickHouseDestinationOverridden(override) &&
		base.ClickHouse != nil &&
		base.ClickHouseClient != nil {
		cfg := *base.ClickHouse
		client := *base.ClickHouseClient
		if override.ClickHouseDatabase != "" {
			client.Database = override.ClickHouseDatabase
		}
		if override.ClickHouseTable != "" {
			client.Table = override.ClickHouseTable
		}
		destinations.ClickHouse = &cfg
		destinations.ClickHouseClient = &client
		configured = true
	}

	if !configured {
		return nil
	}
	return &destinations
}

func internalLogFileDestinationOverridden(
	override logconfig.InternalLogDestinationConfig,
	base pkgfsstore.FsstoreConfig,
) bool {
	internal := base
	overrideInternalLogFileLayout(&internal, override)
	return normalizeLogFileLayoutForComparison(internal) !=
		normalizeLogFileLayoutForComparison(base)
}

// resolvedInternalLogFileDestinationOverriddenは、起動時の所有権検証で実体pathを比較します。
// errorを返さないTo*のfield変換は上の文字列比較に留め、Map*で同じ実体の専用設定を畳み込みます。
func resolvedInternalLogFileDestinationOverridden(
	override logconfig.InternalLogDestinationConfig,
	base pkgfsstore.FsstoreConfig,
) (bool, error) {
	internal := base
	overrideInternalLogFileLayout(&internal, override)
	shared, err := logFileLayoutsHaveSameResolvedIdentity(base, internal)
	return !shared, err
}

func logFileLayoutsHaveSameResolvedIdentity(base, internal pkgfsstore.FsstoreConfig) (bool, error) {
	resolvedBase, err := resolveLogFileLayoutForComparison(base)
	if err != nil {
		return false, &LogFileLayoutResolutionError{Layout: "base", Err: err}
	}
	resolvedInternal, err := resolveLogFileLayoutForComparison(internal)
	if err != nil {
		return false, &LogFileLayoutResolutionError{Layout: "internal log", Err: err}
	}
	return resolvedBase == resolvedInternal, nil
}

func collapseResolvedInternalLogFileDestination(
	base *pkglogger.FileLoggerConfig,
	destinations *pkglogger.InternalLogDestinationConfig,
	baseSelected bool,
) error {
	if base == nil || destinations == nil || destinations.File == nil {
		return nil
	}
	if !baseSelected {
		_, err := resolveLogFileLayoutForComparison(destinations.File.FsstoreConfig.Layout)
		if err != nil {
			return &LogFileLayoutResolutionError{Layout: "internal log", Err: err}
		}
		return nil
	}
	shared, err := logFileLayoutsHaveSameResolvedIdentity(base.FsstoreConfig.Layout, destinations.File.FsstoreConfig.Layout)
	if err != nil {
		return err
	}
	if shared {
		destinations.File = nil
	}
	return nil
}

func collapseResolvedDeliveryInternalLogFileDestination(
	base *pkglogger.DeliveryFileLoggerConfig,
	destinations *pkglogger.DeliveryInternalLogDestinationConfig,
	baseSelected bool,
) error {
	if base == nil || destinations == nil || destinations.File == nil {
		return nil
	}
	if !baseSelected {
		_, err := resolveLogFileLayoutForComparison(destinations.File.FsstoreConfig.Layout)
		if err != nil {
			return &LogFileLayoutResolutionError{Layout: "internal log", Err: err}
		}
		return nil
	}
	shared, err := logFileLayoutsHaveSameResolvedIdentity(base.FsstoreConfig.Layout, destinations.File.FsstoreConfig.Layout)
	if err != nil {
		return err
	}
	if shared {
		destinations.File = nil
	}
	return nil
}

// resolveLogFileLayoutForComparisonは、設定値を変更せず、使用する保存先の実体を比較します。
// directoryの作成は行わず、既存祖先の解決失敗は起動側へ返します。
func resolveLogFileLayoutForComparison(layout pkgfsstore.FsstoreConfig) (pkgfsstore.FsstoreConfig, error) {
	for _, directory := range []struct {
		value   *string
		enabled bool
	}{
		{&layout.LogDir, true},
		{&layout.ErrorLogDir, layout.IsSeparateErrorLogsEnabled},
		{&layout.DebugLogDir, layout.IsSeparateDebugLogsEnabled && layout.IsSeparateDebugLogDirEnabled},
	} {
		if !directory.enabled {
			*directory.value = ""
			continue
		}
		resolved, err := pathidentity.ResolveDirectory(filepath.Join(layout.LogPath, layout.Target, *directory.value))
		if err != nil {
			return pkgfsstore.FsstoreConfig{}, err
		}
		*directory.value = pathidentity.ComparisonKey(resolved)
	}
	resolved, err := pathidentity.ResolveDirectory(layout.LogPath)
	if err != nil {
		return pkgfsstore.FsstoreConfig{}, err
	}
	layout.LogPath = pathidentity.ComparisonKey(resolved)
	return layout, nil
}

// normalizeLogFileLayoutForComparisonは、元設定を変更せず保存先比較用のcopyを返します。
// 未使用の個別directoryを除外し、有効なpathはruntime共通のidentityへ揃えます。
// 分離flagや命名規則は保持します。過去rawの回復にも元pathを使うため、
// 戻り値を実際のfile操作用設定へ書き戻してはいけません。
func normalizeLogFileLayoutForComparison(layout pkgfsstore.FsstoreConfig) pkgfsstore.FsstoreConfig {
	layout.LogDir = pathidentity.ComparisonKey(filepath.Join(layout.LogPath, layout.Target, layout.LogDir))
	if layout.IsSeparateErrorLogsEnabled {
		layout.ErrorLogDir = pathidentity.ComparisonKey(filepath.Join(layout.LogPath, layout.Target, layout.ErrorLogDir))
	} else {
		layout.ErrorLogDir = ""
	}
	if layout.IsSeparateDebugLogDirEnabled {
		layout.DebugLogDir = pathidentity.ComparisonKey(filepath.Join(layout.LogPath, layout.Target, layout.DebugLogDir))
	} else {
		layout.DebugLogDir = ""
	}
	layout.LogPath = pathidentity.ComparisonKey(layout.LogPath)
	return layout
}

func internalLogSlackDestinationOverridden(
	override logconfig.InternalLogDestinationConfig,
) bool {
	return override.SlackChannel != "" ||
		override.SlackErrorChannel != "" ||
		override.SlackWarnChannel != "" ||
		override.SlackInfoChannel != "" ||
		override.SlackDebugChannel != ""
}

func internalLogCloudWatchLogsDestinationOverridden(
	override logconfig.InternalLogDestinationConfig,
) bool {
	return override.CloudWatchLogsLogGroupName != "" ||
		override.CloudWatchLogsLogStreamName != ""
}

func internalLogClickHouseDestinationOverridden(
	override logconfig.InternalLogDestinationConfig,
) bool {
	return override.ClickHouseDatabase != "" || override.ClickHouseTable != ""
}

func overrideInternalLogFileLayout(
	layout *pkgfsstore.FsstoreConfig,
	override logconfig.InternalLogDestinationConfig,
) {
	if override.FileLogPath != "" {
		layout.LogPath = override.FileLogPath
	}
	if override.FileTarget != "" {
		layout.Target = override.FileTarget
	}
	if override.FileLogDir != "" {
		layout.LogDir = override.FileLogDir
	}
	if override.FileErrorLogDir != "" {
		layout.ErrorLogDir = override.FileErrorLogDir
	}
	if override.FileDebugLogDir != "" {
		layout.DebugLogDir = override.FileDebugLogDir
	}
	if override.FileSeparateDebugLogDirEnabled != nil {
		layout.IsSeparateDebugLogDirEnabled = *override.FileSeparateDebugLogDirEnabled
	}
}

func overrideInternalLogSlackChannels(
	channel, errorChannel, warnChannel, infoChannel, debugChannel *string,
	override logconfig.InternalLogDestinationConfig,
) {
	if override.SlackChannel != "" {
		*channel = override.SlackChannel
	}
	if override.SlackErrorChannel != "" {
		*errorChannel = override.SlackErrorChannel
	}
	if override.SlackWarnChannel != "" {
		*warnChannel = override.SlackWarnChannel
	}
	if override.SlackInfoChannel != "" {
		*infoChannel = override.SlackInfoChannel
	}
	if override.SlackDebugChannel != "" {
		*debugChannel = override.SlackDebugChannel
	}
}

func overrideInternalLogCloudWatchLogsDestination(
	flushGroup, flushStream, clientGroup *string,
	override logconfig.InternalLogDestinationConfig,
) {
	if override.CloudWatchLogsLogGroupName != "" {
		*flushGroup = override.CloudWatchLogsLogGroupName
		*clientGroup = override.CloudWatchLogsLogGroupName
	}
	if override.CloudWatchLogsLogStreamName != "" {
		*flushStream = override.CloudWatchLogsLogStreamName
	}
}
