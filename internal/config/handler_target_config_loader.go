package config

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkgenv "github.com/go-jappe/heterolog/pkg/env"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// loadLoggerHandlerTargetConfigs は、通常出力先に含まれず、internal logの委譲先として
// 必要になったsync logger設定だけを環境変数から読み込みます。
func loadLoggerHandlerTargetConfigs(env pkgenv.Lookup, cfg *logconfig.LoggerConfig) error {
	var errs []error
	for _, target := range cfg.HandlerTargetDestinations {
		switch target {
		case logconfig.LogOutputFile:
			fileConfig, err := loadFileConfig(env)
			cfg.FileConfig = fileConfig
			if err != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger file config: %w", err))
			}
		case logconfig.LogOutputStdout:
			stdoutConfig, err := loadStdoutConfig(env)
			cfg.StdoutConfig = stdoutConfig
			if err != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger stdout config: %w", err))
			}
		case logconfig.LogOutputSlack:
			slackConfig, err := loadSlackConfig(env)
			cfg.SlackConfig = slackConfig
			if err != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger Slack config: %w", err))
			}
		case logconfig.LogOutputOpenSearch:
			baseConfig, baseErr := loadOpenSearchConfig(env)
			flushManagerConfig, flushErr := loadOpenSearchFlushManagerConfig(env)
			retryConfig, retryErr := loadOpenSearchRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger OpenSearch base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger OpenSearch flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger OpenSearch retry config: %w", retryErr))
			}
			cfg.OpenSearchConfig = logconfig.OpenSearchConfig{
				OpenSearchLoggerBaseConfig:      baseConfig,
				OpenSearchFlushManagerConfig:    flushManagerConfig,
				OpenSearchRetryAbleClientConfig: retryConfig,
			}
		case logconfig.LogOutputLoki:
			baseConfig, baseErr := loadLokiConfig(env)
			flushManagerConfig, flushErr := loadLokiFlushManagerConfig(env)
			retryConfig, retryErr := loadLokiRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger Loki base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger Loki flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger Loki retry config: %w", retryErr))
			}
			cfg.LokiConfig = logconfig.LokiConfig{
				LokiLoggerBaseConfig:      baseConfig,
				LokiFlushManagerConfig:    flushManagerConfig,
				LokiRetryAbleClientConfig: retryConfig,
			}
		case logconfig.LogOutputCloudWatchLogs:
			baseConfig, baseErr := loadCloudWatchLogsConfig(env)
			flushManagerConfig, flushErr := loadCloudWatchLogsFlushManagerConfig(env)
			retryConfig, retryErr := loadCloudWatchLogsRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger CloudWatch Logs base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger CloudWatch Logs flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger CloudWatch Logs retry config: %w", retryErr))
			}
			cfg.CloudWatchLogsConfig = logconfig.CloudWatchLogsConfig{
				CloudWatchLogsLoggerBaseConfig:      baseConfig,
				CloudWatchLogsFlushManagerConfig:    flushManagerConfig,
				CloudWatchLogsRetryAbleClientConfig: retryConfig,
			}
		case logconfig.LogOutputClickHouse:
			baseConfig, baseErr := loadClickHouseConfig(env)
			flushManagerConfig, flushErr := loadClickHouseFlushManagerConfig(env)
			retryConfig, retryErr := loadClickHouseRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger ClickHouse base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger ClickHouse flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only logger ClickHouse retry config: %w", retryErr))
			}
			cfg.ClickHouseConfig = logconfig.ClickHouseConfig{
				ClickHouseLoggerBaseConfig:      baseConfig,
				ClickHouseFlushManagerConfig:    flushManagerConfig,
				ClickHouseRetryAbleClientConfig: retryConfig,
			}
		default:
			errs = append(errs, &UnimplementedHandlerTargetError{
				Contract: pkglogger.LoggerContractSync,
				Target:   target,
			})
		}
	}
	return errors.Join(errs...)
}

// loadAsyncLoggerHandlerTargetConfigs は、通常出力先に含まれず、internal logの委譲先として
// 必要になったasync logger設定だけを環境変数から読み込みます。
func loadAsyncLoggerHandlerTargetConfigs(env pkgenv.Lookup, cfg *logconfig.AsyncBestEffortMultiLoggerConfig) error {
	var errs []error
	for _, target := range cfg.HandlerTargetDestinations {
		switch target {
		case logconfig.LogOutputFile:
			fileConfig, err := loadAsyncFileConfig(env)
			cfg.FileConfig = fileConfig
			if err != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger file config: %w", err))
			}
		case logconfig.LogOutputStdout:
			stdoutConfig, err := loadAsyncStdoutConfig(env)
			cfg.StdoutConfig = stdoutConfig
			if err != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger stdout config: %w", err))
			}
		case logconfig.LogOutputSlack:
			slackConfig, err := loadAsyncSlackConfig(env)
			cfg.SlackConfig = slackConfig
			if err != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger Slack config: %w", err))
			}
		case logconfig.LogOutputOpenSearch:
			baseConfig, baseErr := loadAsyncOpenSearchConfig(env)
			flushManagerConfig, flushErr := loadAsyncOpenSearchFlushManagerConfig(env)
			retryConfig, retryErr := loadAsyncOpenSearchRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger OpenSearch base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger OpenSearch flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger OpenSearch retry config: %w", retryErr))
			}
			cfg.OpenSearchConfig = logconfig.OpenSearchConfig{
				OpenSearchLoggerBaseConfig:      baseConfig,
				OpenSearchFlushManagerConfig:    flushManagerConfig,
				OpenSearchRetryAbleClientConfig: retryConfig,
			}
		case logconfig.LogOutputLoki:
			baseConfig, baseErr := loadAsyncLokiConfig(env)
			flushManagerConfig, flushErr := loadAsyncLokiFlushManagerConfig(env)
			retryConfig, retryErr := loadAsyncLokiRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger Loki base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger Loki flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger Loki retry config: %w", retryErr))
			}
			cfg.LokiConfig = logconfig.LokiConfig{
				LokiLoggerBaseConfig:      baseConfig,
				LokiFlushManagerConfig:    flushManagerConfig,
				LokiRetryAbleClientConfig: retryConfig,
			}
		case logconfig.LogOutputCloudWatchLogs:
			baseConfig, baseErr := loadAsyncCloudWatchLogsConfig(env)
			flushManagerConfig, flushErr := loadAsyncCloudWatchLogsFlushManagerConfig(env)
			retryConfig, retryErr := loadAsyncCloudWatchLogsRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger CloudWatch Logs base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger CloudWatch Logs flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger CloudWatch Logs retry config: %w", retryErr))
			}
			cfg.CloudWatchLogsConfig = logconfig.CloudWatchLogsConfig{
				CloudWatchLogsLoggerBaseConfig:      baseConfig,
				CloudWatchLogsFlushManagerConfig:    flushManagerConfig,
				CloudWatchLogsRetryAbleClientConfig: retryConfig,
			}
		case logconfig.LogOutputClickHouse:
			baseConfig, baseErr := loadAsyncClickHouseConfig(env)
			flushManagerConfig, flushErr := loadAsyncClickHouseFlushManagerConfig(env)
			retryConfig, retryErr := loadAsyncClickHouseRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger ClickHouse base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger ClickHouse flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only async logger ClickHouse retry config: %w", retryErr))
			}
			cfg.ClickHouseConfig = logconfig.ClickHouseConfig{
				ClickHouseLoggerBaseConfig:      baseConfig,
				ClickHouseFlushManagerConfig:    flushManagerConfig,
				ClickHouseRetryAbleClientConfig: retryConfig,
			}
		default:
			errs = append(errs, &UnimplementedHandlerTargetError{
				Contract: pkglogger.LoggerContractAsyncBestEffort,
				Target:   target,
			})
		}
	}
	return errors.Join(errs...)
}

// loadDeliveryLoggerHandlerTargetConfigs は、通常出力先に含まれず、internal logの委譲先として
// 必要になったDelivery logger設定だけを環境変数から読み込みます。
func loadDeliveryLoggerHandlerTargetConfigs(env pkgenv.Lookup, cfg *logconfig.DeliveryLoggerConfig) error {
	var errs []error
	for _, target := range cfg.HandlerTargetDestinations {
		switch target {
		case logconfig.LogOutputFile:
			fileConfig, err := loadDeliveryFileConfig(env)
			cfg.DeliveryFileConfig = fileConfig
			if err != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger file config: %w", err))
			}
		case logconfig.LogOutputStdout:
			stdoutConfig, err := loadDeliveryStdoutConfig(env)
			cfg.DeliveryStdoutConfig = stdoutConfig
			if err != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger stdout config: %w", err))
			}
		case logconfig.LogOutputSlack:
			slackConfig, err := loadDeliverySlackConfig(env)
			cfg.DeliverySlackConfig = slackConfig
			if err != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger Slack config: %w", err))
			}
		case logconfig.LogOutputOpenSearch:
			baseConfig, baseErr := loadDeliveryOpenSearchConfig(env)
			flushManagerConfig, flushErr := loadDeliveryOpenSearchFlushManagerConfig(env)
			retryConfig, retryErr := loadDeliveryOpenSearchRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger OpenSearch base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger OpenSearch flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger OpenSearch retry config: %w", retryErr))
			}
			cfg.DeliveryOpenSearchConfig = logconfig.DeliveryOpenSearchConfig{
				DeliveryOpenSearchLoggerBaseConfig:   baseConfig,
				DeliveryOpenSearchFlushManagerConfig: flushManagerConfig,
				OpenSearchRetryAbleClientConfig:      retryConfig,
			}
		case logconfig.LogOutputLoki:
			baseConfig, baseErr := loadDeliveryLokiConfig(env)
			flushManagerConfig, flushErr := loadDeliveryLokiFlushManagerConfig(env)
			retryConfig, retryErr := loadDeliveryLokiRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger Loki base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger Loki flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger Loki retry config: %w", retryErr))
			}
			cfg.DeliveryLokiConfig = logconfig.DeliveryLokiConfig{
				DeliveryLokiLoggerBaseConfig:   baseConfig,
				DeliveryLokiFlushManagerConfig: flushManagerConfig,
				LokiRetryAbleClientConfig:      retryConfig,
			}
		case logconfig.LogOutputCloudWatchLogs:
			baseConfig, baseErr := loadDeliveryCloudWatchLogsConfig(env)
			flushManagerConfig, flushErr := loadDeliveryCloudWatchLogsFlushManagerConfig(env)
			retryConfig, retryErr := loadDeliveryCloudWatchLogsRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger CloudWatch Logs base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger CloudWatch Logs flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger CloudWatch Logs retry config: %w", retryErr))
			}
			cfg.DeliveryCloudWatchLogsConfig = logconfig.DeliveryCloudWatchLogsConfig{
				DeliveryCloudWatchLogsLoggerBaseConfig:   baseConfig,
				DeliveryCloudWatchLogsFlushManagerConfig: flushManagerConfig,
				CloudWatchLogsRetryAbleClientConfig:      retryConfig,
			}
		case logconfig.LogOutputClickHouse:
			baseConfig, baseErr := loadDeliveryClickHouseConfig(env)
			flushManagerConfig, flushErr := loadDeliveryClickHouseFlushManagerConfig(env)
			retryConfig, retryErr := loadDeliveryClickHouseRetryableClientConfig(env)
			if baseErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger ClickHouse base config: %w", baseErr))
			}
			if flushErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger ClickHouse flush config: %w", flushErr))
			}
			if retryErr != nil {
				errs = append(errs, fmt.Errorf("load handler-only delivery logger ClickHouse retry config: %w", retryErr))
			}
			cfg.DeliveryClickHouseConfig = logconfig.DeliveryClickHouseConfig{
				DeliveryClickHouseLoggerBaseConfig:   baseConfig,
				DeliveryClickHouseFlushManagerConfig: flushManagerConfig,
				ClickHouseRetryAbleClientConfig:      retryConfig,
			}
		default:
			errs = append(errs, &UnimplementedHandlerTargetError{
				Contract: pkglogger.LoggerContractDelivery,
				Target:   target,
			})
		}
	}
	return errors.Join(errs...)
}

// UnimplementedHandlerTargetErrorは、Contractのloggerでhandlerだけが使う出力先Targetについて、
// 設定を読み込むloaderが実装されていないことを表します。
type UnimplementedHandlerTargetError struct {
	Contract pkglogger.LoggerContractKind
	Target   pkglogger.LogOutputDestination
}

func (e *UnimplementedHandlerTargetError) Error() string {
	return fmt.Sprintf(
		"load handler-only %s target %q: target loader is not implemented",
		e.Contract.DisplayName(),
		e.Target,
	)
}
