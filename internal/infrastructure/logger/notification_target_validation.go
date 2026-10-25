package logger

import (
	"errors"
	"fmt"

	clickhousewrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	cloudwatchlogswrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/wrapper"
	lokiwrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/wrapper"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	opensearchwrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/wrapper"
	clickhouselogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	cloudwatchlogslogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs"
	lokilogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki"
	opensearchlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/slack"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

// ErrInvalidNotificationTargetは、内部ログの通知先として選んだbackendの設定から、通知専用の
// 書き込み先を作れないことを表します。NotificationTargetErrorとして返し、errors.Isで判定できます。
var ErrInvalidNotificationTarget = errors.New("invalid internal log notification target")

// NotificationTargetErrorは、Sourceのloggerで内部ログの通知先（OnErrorConfig・OnWarnConfig・
// OnInfoConfig・OnDebugConfig）に選んだBackendの設定から、通知専用の書き込み先を作れないことを
// 表します。Errは原因です（例: 時刻表示のtimezoneを読み込めない）。通知の初回ではなく
// loggerの構築時に返すため、通知が黙って捨てられることはありません。
type NotificationTargetError struct {
	Source  string
	Backend pkglogger.LogOutputDestination
	Err     error
}

func (e *NotificationTargetError) Error() string {
	return fmt.Sprintf("%s internal log notification target %s: %v", e.Source, e.Backend, e.Err)
}

// UnwrapはErrInvalidNotificationTargetと原因を返します。
func (e *NotificationTargetError) Unwrap() []error {
	return []error{ErrInvalidNotificationTarget, e.Err}
}

// effectiveInternalLogConfigは、base設定へinternal log専用の保存先を反映した、内部ログの通知先が
// 実際に使う設定と、専用の保存先だけを持つ設定を返します。専用の保存先がなければbase設定を
// そのまま返します。effectiveからは専用保存先を指定するmetadataを外します。
func effectiveInternalLogConfig(base pkglogger.LoggerConfig) (effective, dedicated pkglogger.LoggerConfig) {
	effective = base
	if base.InternalLogDestinations == nil {
		return effective, pkglogger.LoggerConfig{}
	}
	dedicated = base.InternalLogDestinations.ToLoggerConfig()
	dedicated.File = terminalInternalLogFileConfig(dedicated.File)
	applyDedicatedInternalLogDestinations(&effective, dedicated)
	// internal destinationから同じ設定を再び解決しないよう、effective configでは
	// 専用保存先を指定するmetadataを外します。
	effective.InternalLogDestinations = nil
	return effective, dedicated
}

// effectiveDeliveryInternalLogConfigは、Delivery loggerについてeffectiveInternalLogConfigと
// 同じ設定を返します。
func effectiveDeliveryInternalLogConfig(
	base pkglogger.DeliveryLoggerConfig,
) (effective, dedicated pkglogger.DeliveryLoggerConfig) {
	effective = base
	if base.InternalLogDestinations == nil {
		return effective, pkglogger.DeliveryLoggerConfig{}
	}
	dedicated = base.InternalLogDestinations.ToLoggerConfig()
	dedicated.File = terminalDeliveryInternalLogFileConfig(dedicated.File)
	applyDedicatedDeliveryInternalLogDestinations(&effective, dedicated)
	effective.InternalLogDestinations = nil
	return effective, dedicated
}

// newStdoutNotificationTargetは、内部ログの通知先として使うstdoutの書き込み設定を作ります。
// stdout設定がなければnilです。
func newStdoutNotificationTarget(cfg pkglogger.LoggerConfig) (*stdout.NotificationTarget, error) {
	if cfg.Stdout == nil {
		return nil, nil
	}
	return stdout.NewNotificationTarget(
		stdout.NewConfigAdapter(*cfg.Stdout),
		resource.NewOtelResourceAdapter(otelResourceOrEmpty(cfg.Resource)),
	)
}

// newDeliveryStdoutNotificationTargetは、Delivery loggerについてnewStdoutNotificationTargetと
// 同じ書き込み設定を作ります。
func newDeliveryStdoutNotificationTarget(cfg pkglogger.DeliveryLoggerConfig) (*stdout.NotificationTarget, error) {
	if cfg.Stdout == nil {
		return nil, nil
	}
	return stdout.NewNotificationTarget(
		stdout.NewDeliveryFallbackConfigAdapter(*cfg.Stdout),
		resource.NewOtelResourceAdapter(otelResourceOrEmpty(cfg.Resource)),
	)
}

// notificationTargetChecksは、通知先に選ばれたbackendごとの構築時検証の結果をまとめます。
type notificationTargetChecks struct {
	source   string
	selected func(pkglogger.LogOutputDestination) bool
	errs     []error
}

// checkは、backendが通知先に選ばれていればvalidateを実行し、失敗をNotificationTargetErrorとして
// 記録します。選ばれていないbackendの設定は通知に使わないため検証しません。
func (c *notificationTargetChecks) check(backend pkglogger.LogOutputDestination, validate func() error) {
	if !c.selected(backend) {
		return
	}
	if err := validate(); err != nil {
		c.errs = append(c.errs, &NotificationTargetError{Source: c.source, Backend: backend, Err: err})
	}
}

// validateLoggerNotificationTargetsは、sync/async loggerで内部ログの通知先に選ばれたstdout・
// Slack・OpenSearch・Loki・CloudWatch Logs・ClickHouseについて、通知専用の書き込み先を
// 通知の初回と同じ設定・手順で作れることを、resourceを作る前に検証します。fileの通知先は
// top-level loggerのLoggerSetを構築時に作るため、ここでは扱いません。
func validateLoggerNotificationTargets(
	sourceLabel string,
	cfg pkglogger.LoggerConfig,
	cloudWatchLogsStreamRoutingGranularity streamrouting.Granularity,
) error {
	effective, _ := effectiveInternalLogConfig(cfg)
	checks := notificationTargetChecks{
		source: sourceLabel,
		selected: func(backend pkglogger.LogOutputDestination) bool {
			return pkglogger.LoggerConfigSelectsHandlerTarget(cfg, backend)
		},
	}
	if effective.Stdout != nil {
		checks.check(pkglogger.LogOutputStdout, func() error {
			_, err := newStdoutNotificationTarget(effective)
			return err
		})
	}
	if effective.Slack != nil {
		checks.check(pkglogger.LogOutputSlack, func() error {
			return slack.ValidateFallbackLoggerConfig(slack.NewConfigAdapter(*effective.Slack))
		})
	}
	if effective.OpenSearch != nil && effective.OpenSearchClient != nil {
		checks.check(pkglogger.LogOutputOpenSearch, func() error {
			return opensearchlogger.ValidateFallbackLoggerConfig(opensearchlogger.NewOpenSearchLoggerAdapter(effective))
		})
	}
	if effective.Loki != nil && effective.LokiClient != nil {
		checks.check(pkglogger.LogOutputLoki, func() error {
			return lokilogger.ValidateFallbackLoggerConfig(lokilogger.NewLokiLoggerAdapter(effective))
		})
	}
	if effective.CloudWatchLogs != nil && effective.CloudWatchLogsClient != nil {
		checks.check(pkglogger.LogOutputCloudWatchLogs, func() error {
			return cloudwatchlogslogger.ValidateFallbackLoggerConfig(
				cloudwatchlogslogger.NewCloudWatchLogsLoggerAdapter(effective, cloudWatchLogsStreamRoutingGranularity),
			)
		})
	}
	if effective.ClickHouse != nil && effective.ClickHouseClient != nil {
		checks.check(pkglogger.LogOutputClickHouse, func() error {
			return clickhouselogger.ValidateFallbackLoggerConfig(clickhouselogger.NewClickHouseLoggerAdapter(effective))
		})
	}
	return errors.Join(checks.errs...)
}

// validateDeliveryLoggerNotificationTargetsは、Delivery loggerについて
// validateLoggerNotificationTargetsと同じ検証を行います。
func validateDeliveryLoggerNotificationTargets(
	sourceLabel string,
	cfg pkglogger.DeliveryLoggerConfig,
	cloudWatchLogsStreamRoutingGranularity streamrouting.Granularity,
) error {
	effective, _ := effectiveDeliveryInternalLogConfig(cfg)
	checks := notificationTargetChecks{
		source: sourceLabel,
		selected: func(backend pkglogger.LogOutputDestination) bool {
			return pkglogger.DeliveryLoggerConfigSelectsHandlerTarget(cfg, backend)
		},
	}
	if effective.Stdout != nil {
		checks.check(pkglogger.LogOutputStdout, func() error {
			_, err := newDeliveryStdoutNotificationTarget(effective)
			return err
		})
	}
	if effective.Slack != nil {
		checks.check(pkglogger.LogOutputSlack, func() error {
			return slack.ValidateFallbackLoggerConfig(deliverySlackNotificationConfig(effective))
		})
	}
	if effective.OpenSearch != nil && effective.OpenSearchClient != nil {
		checks.check(pkglogger.LogOutputOpenSearch, func() error {
			return opensearchlogger.ValidateFallbackLoggerConfig(deliveryOpenSearchNotificationConfig(effective))
		})
	}
	if effective.Loki != nil && effective.LokiClient != nil {
		checks.check(pkglogger.LogOutputLoki, func() error {
			return lokilogger.ValidateFallbackLoggerConfig(deliveryLokiNotificationConfig(effective))
		})
	}
	if effective.CloudWatchLogs != nil && effective.CloudWatchLogsClient != nil {
		checks.check(pkglogger.LogOutputCloudWatchLogs, func() error {
			return cloudwatchlogslogger.ValidateFallbackLoggerConfig(
				deliveryCloudWatchLogsNotificationConfig(effective, cloudWatchLogsStreamRoutingGranularity),
			)
		})
	}
	if effective.ClickHouse != nil && effective.ClickHouseClient != nil {
		checks.check(pkglogger.LogOutputClickHouse, func() error {
			return clickhouselogger.ValidateFallbackLoggerConfig(deliveryClickHouseNotificationConfig(effective))
		})
	}
	return errors.Join(checks.errs...)
}

// deliverySlackNotificationConfigは、Deliveryの内部ログ通知先に使うSlackのfallback設定です。
// getDeliveryHandlerと構築時の検証が同じ設定を使います。
func deliverySlackNotificationConfig(cfg pkglogger.DeliveryLoggerConfig) slack.SlackLoggerConfigProvider {
	return slack.NewDeliveryFallbackConfigAdapter(*cfg.Slack, cfg.InternalLogBatchConfig.Slack)
}

// deliveryOpenSearchNotificationConfigは、Deliveryの内部ログ通知先に使うOpenSearchのfallback
// 設定です。getDeliveryHandlerと構築時の検証が同じ設定を使います。
func deliveryOpenSearchNotificationConfig(cfg pkglogger.DeliveryLoggerConfig) (
	opensearchlogger.OpenSearchLoggerConfigProvider,
	infraopensearch.OpenSearchClientConfigProvider,
	opensearchlogger.OpenSearchFlushManagerRuntimeConfigProvider,
	resource.ResourceProvider,
	opensearchwrapper.RetryConfigProvider,
) {
	openSearchCfg := *cfg.OpenSearch
	return opensearchlogger.NewDeliveryOpenSearchFallbackLoggerConfigAdapter(
			openSearchCfg.DeliveryOpenSearchLoggerBaseConfig,
			cfg.InternalLogBatchConfig.OpenSearch,
		),
		infraopensearch.NewOpenSearchClientConfigAdapter(cfg.OpenSearchClient),
		opensearchlogger.NewDeliveryFlushManagerConfigAdapter(
			openSearchCfg.DeliveryOpenSearchFlushManagerConfig,
		),
		resource.NewResourceAdapter(openSearchCfg.DeliveryOpenSearchFlushManagerConfig.Resource),
		opensearchlogger.NewRetryConfigProvider(
			openSearchCfg.OpenSearchRetryableClientCaseTransportErrorConfig,
		)
}

// deliveryLokiNotificationConfigは、Deliveryの内部ログ通知先に使うLokiのfallback設定です。
// getDeliveryHandlerと構築時の検証が同じ設定を使います。
func deliveryLokiNotificationConfig(cfg pkglogger.DeliveryLoggerConfig) (
	lokilogger.LokiConfigProvider,
	*pkgloki.Config,
	lokilogger.LokiFlushManagerRuntimeConfigProvider,
	resource.ResourceProvider,
	lokiwrapper.RetryConfigProvider,
) {
	lokiCfg := *cfg.Loki
	return lokilogger.NewDeliveryLokiFallbackLoggerConfigAdapter(
			lokiCfg.DeliveryLokiLoggerBaseConfig,
			cfg.InternalLogBatchConfig.Loki,
		),
		cfg.LokiClient,
		lokilogger.NewDeliveryFlushManagerConfigAdapter(lokiCfg.DeliveryLokiFlushManagerConfig),
		resource.NewResourceAdapter(lokiCfg.DeliveryLokiFlushManagerConfig.Resource),
		lokilogger.NewRetryConfigProvider(lokiCfg.LokiRetryableClientCaseTransportErrorConfig)
}

// deliveryCloudWatchLogsNotificationConfigは、Deliveryの内部ログ通知先に使うCloudWatch Logsの
// fallback設定です。getDeliveryHandlerと構築時の検証が同じ設定を使います。
func deliveryCloudWatchLogsNotificationConfig(
	cfg pkglogger.DeliveryLoggerConfig,
	cloudWatchLogsStreamRoutingGranularity streamrouting.Granularity,
) (
	cloudwatchlogslogger.CloudWatchLogsConfigProvider,
	*pkgcloudwatchlogs.Config,
	cloudwatchlogslogger.CloudWatchLogsFlushManagerRuntimeConfigProvider,
	resource.ResourceProvider,
	cloudwatchlogswrapper.RetryConfigProvider,
) {
	cloudWatchCfg := *cfg.CloudWatchLogs
	return cloudwatchlogslogger.NewDeliveryCloudWatchLogsFallbackLoggerConfigAdapter(
			cloudWatchCfg.DeliveryCloudWatchLogsLoggerBaseConfig,
			cfg.InternalLogBatchConfig.CloudWatchLogs,
		),
		cfg.CloudWatchLogsClient,
		cloudwatchlogslogger.NewDeliveryFlushManagerConfigAdapter(
			cloudWatchCfg.DeliveryCloudWatchLogsFlushManagerConfig,
			cloudWatchLogsStreamRoutingGranularity,
		),
		resource.NewResourceAdapter(cloudWatchCfg.DeliveryCloudWatchLogsFlushManagerConfig.Resource),
		cloudwatchlogslogger.NewRetryConfigProvider(
			cloudWatchCfg.CloudWatchLogsRetryableClientCaseTransportErrorConfig,
		)
}

// deliveryClickHouseNotificationConfigは、Deliveryの内部ログ通知先に使うClickHouseのfallback
// 設定です。getDeliveryHandlerと構築時の検証が同じ設定を使います。
func deliveryClickHouseNotificationConfig(cfg pkglogger.DeliveryLoggerConfig) (
	clickhouselogger.ClickHouseConfigProvider,
	*pkgclickhouse.Config,
	clickhouselogger.ClickHouseFlushManagerRuntimeConfigProvider,
	resource.ResourceProvider,
	clickhousewrapper.RetryConfigProvider,
) {
	clickHouseCfg := *cfg.ClickHouse
	return clickhouselogger.NewDeliveryClickHouseLoggerConfigAdapter(clickHouseCfg.DeliveryClickHouseLoggerBaseConfig),
		cfg.ClickHouseClient,
		clickhouselogger.NewDeliveryFlushManagerConfigAdapter(clickHouseCfg.DeliveryClickHouseFlushManagerConfig),
		resource.NewResourceAdapter(clickHouseCfg.DeliveryClickHouseFlushManagerConfig.Resource),
		clickhouselogger.NewRetryConfigProvider(clickHouseCfg.ClickHouseRetryableClientCaseTransportErrorConfig)
}
