package logger

import (
	"errors"
	"fmt"
)

// ValidateLoggerBackendConfigsは、sync/async loggerが実際に構築するbackendの
// 存在と値を検証します。通常出力、handler-onlyのbare selector、空destination時の
// non-nil base設定に加え、通常設定と異なるinternal-log保存先を対象にします。
// internal-log保存先のclient設定は、対応するlogger設定と組で指定する必要があります。
// 通常出力が一つもない設定はErrNoActiveLogOutputを含むerrorになります。
func ValidateLoggerBackendConfigs(sourceLabel string, cfg LoggerConfig) error {
	var errs []error
	errs = append(errs, validateActiveLogOutputExists(
		sourceLabel,
		cfg.LogOutputDestinations,
		func(destination LogOutputDestination) bool {
			return loggerConfigHasBaseBackend(cfg, destination)
		},
	))
	errs = append(errs, validateLoggerBaseBackendPresence(sourceLabel, cfg))
	errs = append(errs, validateRequiredBaseLoggerBackendValues(sourceLabel, cfg))
	if cfg.InternalLogDestinations != nil {
		errs = append(errs, validateLoggerInternalLogDestinationBackendValues(
			sourceLabel+" internal log destination",
			cfg.InternalLogDestinations.ToLoggerConfig(),
		))
	}
	return errors.Join(errs...)
}

// ValidateDeliveryLoggerBackendConfigsは、Delivery loggerについて通常出力、
// handler-only base、空destination時のbase、およびinternal-log専用保存先の
// backend存在・実値を同じ契約で検証します。送信先のないDeliveryReportを返す
// 構成にならないよう、通常出力が一つもない設定もErrNoActiveLogOutputで拒否します。
func ValidateDeliveryLoggerBackendConfigs(
	sourceLabel string,
	cfg DeliveryLoggerConfig,
) error {
	var errs []error
	errs = append(errs, validateActiveLogOutputExists(
		sourceLabel,
		cfg.LogOutputDestinations,
		func(destination LogOutputDestination) bool {
			return deliveryLoggerConfigHasBaseBackend(cfg, destination)
		},
	))
	errs = append(errs, validateDeliveryBaseBackendPresence(sourceLabel, cfg))
	errs = append(errs, validateRequiredBaseDeliveryBackendValues(sourceLabel, cfg))
	errs = append(errs, validateDeliveryInternalLogBatchConfig(sourceLabel, cfg))
	if cfg.InternalLogDestinations != nil {
		errs = append(errs, validateDeliveryInternalLogDestinationBackendValues(
			sourceLabel+" internal log destination",
			cfg.InternalLogDestinations.ToLoggerConfig(),
		))
	}
	return errors.Join(errs...)
}

func validateLoggerBaseBackendPresence(sourceLabel string, cfg LoggerConfig) error {
	return validateExplicitDestinationBackendPresence(
		sourceLabel,
		cfg.LogOutputDestinations,
		func(destination LogOutputDestination) bool {
			return loggerConfigHasBaseBackend(cfg, destination)
		},
	)
}

func validateDeliveryBaseBackendPresence(
	sourceLabel string,
	cfg DeliveryLoggerConfig,
) error {
	return validateExplicitDestinationBackendPresence(
		sourceLabel,
		cfg.LogOutputDestinations,
		func(destination LogOutputDestination) bool {
			return deliveryLoggerConfigHasBaseBackend(cfg, destination)
		},
	)
}

// validateActiveLogOutputExistsは、runtimeが通常出力を起動する規則と同じく、
// destinationが空ならnon-nilのbase設定すべて、明示した場合はその中で設定があるbackendを
// 通常出力として数え、一つもなければErrNoActiveLogOutputを返します。明示したbackendの
// 設定の欠落そのものはvalidateExplicitDestinationBackendPresenceが報告します。
func validateActiveLogOutputExists(
	sourceLabel string,
	destinations LogOutputDestinations,
	hasConfig func(LogOutputDestination) bool,
) error {
	for _, destination := range allLogOutputDestinations {
		if configLogOutputEnabled(destinations, destination) && hasConfig(destination) {
			return nil
		}
	}
	return fmt.Errorf("%s: %w", sourceLabel, ErrNoActiveLogOutput)
}

// validateExplicitDestinationBackendPresenceは通常出力として明示したbackendだけの
// 存在を検証します。handler selectorの解決はhandler routing validatorが所有し、
// 同じ欠落を二つのvalidatorから重複して報告しません。
func validateExplicitDestinationBackendPresence(
	sourceLabel string,
	destinations LogOutputDestinations,
	hasConfig func(LogOutputDestination) bool,
) error {
	var errs []error
	for _, destination := range destinations {
		if !destination.IsValid() {
			errs = append(errs, &ConfigFieldError{
				Source:  sourceLabel,
				Backend: destination,
				Field:   ConfigFieldLogOutputDestinations,
				Detail:  fmt.Sprintf("has unsupported log output destination %q", destination),
				Value:   destination,
				Err:     ErrUnsupportedLogOutputDestination,
			})
			continue
		}
		if !hasConfig(destination) {
			errs = append(errs, &ConfigFieldError{
				Source:  sourceLabel,
				Backend: destination,
				Field:   ConfigFieldBackendConfig,
				Detail:  fmt.Sprintf("%s config is nil", destination),
				Err:     ErrBackendConfigRequired,
			})
		}
	}
	return errors.Join(errs...)
}

// validateRequiredBaseLoggerBackendValuesは、通常出力またはinternal log handlerが
// 実際に使用するbase backendだけを検証します。internal-log専用backendに置き換えられた
// handler-only base設定は対象にしません。
func validateRequiredBaseLoggerBackendValues(sourceLabel string, cfg LoggerConfig) error {
	var errs []error
	if cfg.Slack != nil && LoggerConfigRequiresBaseBackend(cfg, LogOutputSlack) {
		errs = append(errs, validateLoggerSlackBackendConfig(sourceLabel, cfg.Slack))
	}
	if cfg.OpenSearch != nil && LoggerConfigRequiresBaseBackend(cfg, LogOutputOpenSearch) {
		errs = append(errs, validateLoggerOpenSearchBackendConfig(
			sourceLabel,
			cfg.OpenSearch,
			cfg.OpenSearchClient,
		))
	}
	if cfg.Loki != nil && LoggerConfigRequiresBaseBackend(cfg, LogOutputLoki) {
		errs = append(errs, validateLoggerLokiBackendConfig(
			sourceLabel,
			cfg.Loki,
			cfg.LokiClient,
		))
	}
	if cfg.CloudWatchLogs != nil &&
		LoggerConfigRequiresBaseBackend(cfg, LogOutputCloudWatchLogs) {
		errs = append(errs, validateLoggerCloudWatchLogsBackendConfig(
			sourceLabel,
			cfg.CloudWatchLogs,
			cfg.CloudWatchLogsClient,
		))
	}
	if cfg.ClickHouse != nil && LoggerConfigRequiresBaseBackend(cfg, LogOutputClickHouse) {
		errs = append(errs, validateLoggerClickHouseBackendConfig(
			sourceLabel,
			cfg.ClickHouse,
			cfg.ClickHouseClient,
		))
	}
	return errors.Join(errs...)
}

// validateRequiredBaseDeliveryBackendValuesはDelivery loggerが通常出力またはinternal
// log handlerで実際に使用するbase backendだけを検証します。
func validateRequiredBaseDeliveryBackendValues(
	sourceLabel string,
	cfg DeliveryLoggerConfig,
) error {
	var errs []error
	if cfg.Slack != nil && DeliveryLoggerConfigRequiresBaseBackend(cfg, LogOutputSlack) {
		errs = append(errs, validateDeliverySlackBackendConfig(sourceLabel, cfg.Slack))
	}
	if cfg.OpenSearch != nil &&
		DeliveryLoggerConfigRequiresBaseBackend(cfg, LogOutputOpenSearch) {
		errs = append(errs, validateDeliveryOpenSearchBackendConfig(
			sourceLabel,
			cfg.OpenSearch,
			cfg.OpenSearchClient,
		))
	}
	if cfg.Loki != nil && DeliveryLoggerConfigRequiresBaseBackend(cfg, LogOutputLoki) {
		errs = append(errs, validateDeliveryLokiBackendConfig(
			sourceLabel,
			cfg.Loki,
			cfg.LokiClient,
		))
	}
	if cfg.CloudWatchLogs != nil &&
		DeliveryLoggerConfigRequiresBaseBackend(cfg, LogOutputCloudWatchLogs) {
		errs = append(errs, validateDeliveryCloudWatchLogsBackendConfig(
			sourceLabel,
			cfg.CloudWatchLogs,
			cfg.CloudWatchLogsClient,
		))
	}
	if cfg.ClickHouse != nil &&
		DeliveryLoggerConfigRequiresBaseBackend(cfg, LogOutputClickHouse) {
		errs = append(errs, validateDeliveryClickHouseBackendConfig(
			sourceLabel,
			cfg.ClickHouse,
			cfg.ClickHouseClient,
		))
	}
	return errors.Join(errs...)
}

// validateLoggerInternalLogDestinationBackendValuesは、明示されたsync/async internal-log
// 専用backendをすべて検証します。どの設定を検証するかの選択だけを担当し、具体的な
// 規則はbackend別validatorへ委譲します。logger設定のない専用client設定も拒否します。
func validateLoggerInternalLogDestinationBackendValues(
	sourceLabel string,
	cfg LoggerConfig,
) error {
	var errs []error
	errs = append(errs, validateInternalLogDestinationClientPairs(
		sourceLabel,
		internalLogDestinationClientPair{
			LogOutputOpenSearch, "opensearch", cfg.OpenSearch != nil, cfg.OpenSearchClient != nil,
		},
		internalLogDestinationClientPair{
			LogOutputLoki, "loki", cfg.Loki != nil, cfg.LokiClient != nil,
		},
		internalLogDestinationClientPair{
			LogOutputCloudWatchLogs, "cloudwatch logs",
			cfg.CloudWatchLogs != nil, cfg.CloudWatchLogsClient != nil,
		},
		internalLogDestinationClientPair{
			LogOutputClickHouse, "clickhouse", cfg.ClickHouse != nil, cfg.ClickHouseClient != nil,
		},
	))
	if cfg.Slack != nil {
		errs = append(errs, validateLoggerSlackBackendConfig(sourceLabel, cfg.Slack))
	}
	if cfg.OpenSearch != nil {
		errs = append(errs, validateLoggerOpenSearchBackendConfig(
			sourceLabel,
			cfg.OpenSearch,
			cfg.OpenSearchClient,
		))
	}
	if cfg.Loki != nil {
		errs = append(errs, validateLoggerLokiBackendConfig(
			sourceLabel,
			cfg.Loki,
			cfg.LokiClient,
		))
	}
	if cfg.CloudWatchLogs != nil {
		errs = append(errs, validateLoggerCloudWatchLogsBackendConfig(
			sourceLabel,
			cfg.CloudWatchLogs,
			cfg.CloudWatchLogsClient,
		))
	}
	if cfg.ClickHouse != nil {
		errs = append(errs, validateLoggerClickHouseBackendConfig(
			sourceLabel,
			cfg.ClickHouse,
			cfg.ClickHouseClient,
		))
	}
	return errors.Join(errs...)
}

// validateDeliveryInternalLogDestinationBackendValuesは、明示されたDelivery internal-log
// 専用backendをすべてbackend別validatorで検証します。logger設定のない専用client設定も
// 拒否します。
func validateDeliveryInternalLogDestinationBackendValues(
	sourceLabel string,
	cfg DeliveryLoggerConfig,
) error {
	var errs []error
	errs = append(errs, validateInternalLogDestinationClientPairs(
		sourceLabel,
		internalLogDestinationClientPair{
			LogOutputOpenSearch, "opensearch", cfg.OpenSearch != nil, cfg.OpenSearchClient != nil,
		},
		internalLogDestinationClientPair{
			LogOutputLoki, "loki", cfg.Loki != nil, cfg.LokiClient != nil,
		},
		internalLogDestinationClientPair{
			LogOutputCloudWatchLogs, "cloudwatch logs",
			cfg.CloudWatchLogs != nil, cfg.CloudWatchLogsClient != nil,
		},
		internalLogDestinationClientPair{
			LogOutputClickHouse, "clickhouse", cfg.ClickHouse != nil, cfg.ClickHouseClient != nil,
		},
	))
	if cfg.Slack != nil {
		errs = append(errs, validateDeliverySlackBackendConfig(sourceLabel, cfg.Slack))
	}
	if cfg.OpenSearch != nil {
		errs = append(errs, validateDeliveryOpenSearchBackendConfig(
			sourceLabel,
			cfg.OpenSearch,
			cfg.OpenSearchClient,
		))
	}
	if cfg.Loki != nil {
		errs = append(errs, validateDeliveryLokiBackendConfig(
			sourceLabel,
			cfg.Loki,
			cfg.LokiClient,
		))
	}
	if cfg.CloudWatchLogs != nil {
		errs = append(errs, validateDeliveryCloudWatchLogsBackendConfig(
			sourceLabel,
			cfg.CloudWatchLogs,
			cfg.CloudWatchLogsClient,
		))
	}
	if cfg.ClickHouse != nil {
		errs = append(errs, validateDeliveryClickHouseBackendConfig(
			sourceLabel,
			cfg.ClickHouse,
			cfg.ClickHouseClient,
		))
	}
	return errors.Join(errs...)
}

// internalLogDestinationClientPairは、internal-log専用保存先でlogger設定とclient設定を
// 組で指定するbackendの指定状況です。
type internalLogDestinationClientPair struct {
	destination     LogOutputDestination
	backend         string
	hasLoggerConfig bool
	hasClientConfig bool
}

// validateInternalLogDestinationClientPairsは、internal-log専用client設定に対応する
// logger設定があることを検証します。runtimeはlogger設定がnon-nilのbackendだけを専用
// 保存先へ置き換えるため、client設定だけでは通常設定の保存先へ黙って送られます。
// 不正な組はErrUnpairedInternalLogClientを持つConfigFieldErrorで、Backendから対象を
// 特定できます。logger設定だけでclient設定がない逆の組はbackend別validatorが拒否します。
func validateInternalLogDestinationClientPairs(
	sourceLabel string,
	pairs ...internalLogDestinationClientPair,
) error {
	var errs []error
	for _, pair := range pairs {
		if pair.hasClientConfig && !pair.hasLoggerConfig {
			errs = append(errs, &ConfigFieldError{
				Source:  sourceLabel,
				Backend: pair.destination,
				Field:   ConfigFieldClient,
				Detail: fmt.Sprintf(
					"%s client config is set without %s logger config",
					pair.backend,
					pair.backend,
				),
				Err: ErrUnpairedInternalLogClient,
			})
		}
	}
	return errors.Join(errs...)
}
