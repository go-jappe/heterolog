package bootstrap

import (
	infralogger "github.com/go-jappe/heterolog/internal/infrastructure/logger"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// DefaultConfigは、infrastructure層の既定の同期loggerの設定を返します。
func DefaultConfig() pkglogger.LoggerConfig { return infralogger.DefaultConfig() }

// DefaultAsyncConfigは、infrastructure層の既定の非同期loggerの設定を返します。
func DefaultAsyncConfig() pkglogger.AsyncBestEffortMultiLoggerConfig {
	return infralogger.DefaultAsyncConfig()
}

// DefaultDeliveryConfigは、infrastructure層の既定のDelivery loggerの設定を返します。
func DefaultDeliveryConfig() pkglogger.DeliveryLoggerConfig {
	return infralogger.DefaultDeliveryConfig()
}

// DefaultSpoolConfigは、infrastructure層の既定の再送の設定を返します。
func DefaultSpoolConfig() pkgspool.SpoolConfig { return infralogger.DefaultSpoolConfig() }
