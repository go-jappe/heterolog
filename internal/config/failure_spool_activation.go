package config

import (
	"github.com/go-jappe/heterolog/internal/config/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

// SyncLoggerUsesMemoryPressureForFailureSpoolは、実際に構築するsync loggerの
// failure spoolがmemory-pressure判定を必要とするかを返します。
func (a AppConfig) SyncLoggerUsesMemoryPressureForFailureSpool() bool {
	return a.loggerContractUsesMemoryPressureForFailureSpool(pkglogger.LoggerContractSync)
}

// AsyncLoggerUsesMemoryPressureForFailureSpoolは、実際に構築するasync loggerの
// failure spoolがmemory-pressure判定を必要とするかを返します。
func (a AppConfig) AsyncLoggerUsesMemoryPressureForFailureSpool() bool {
	return a.loggerContractUsesMemoryPressureForFailureSpool(pkglogger.LoggerContractAsyncBestEffort)
}

func (a AppConfig) loggerContractUsesMemoryPressureForFailureSpool(
	contractKind pkglogger.LoggerContractKind,
) bool {
	for _, entry := range a.failureStoreCatalog() {
		if entry.support != failureStoreSupportSupported {
			continue
		}

		var store configuredFailureStore
		switch contractKind {
		case pkglogger.LoggerContractSync:
			store = entry.syncStore
		case pkglogger.LoggerContractAsyncBestEffort:
			store = entry.asyncStore
		default:
			continue
		}
		if store.selected && failureSpoolUsesMemoryPressure(store.config) {
			return true
		}
	}
	return false
}

func failureSpoolUsesMemoryPressure(cfg failure.FailureHandlerConfig) bool {
	if !cfg.Fsstore.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload {
		return false
	}
	for _, durableType := range cfg.DurableType {
		if durableType == pkgloggerdurable.DurableTypeSpool {
			return true
		}
	}
	return false
}
