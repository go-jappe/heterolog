package storageowner_test

import (
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/storageowner"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestSupportedAcceptsTheLoggerContractsAndTheTelemetry(t *testing.T) {
	for _, kind := range []pkglogger.LoggerContractKind{
		pkglogger.LoggerContractSync, pkglogger.LoggerContractAsyncBestEffort, pkglogger.LoggerContractDelivery,
		storageowner.Telemetry,
	} {
		if !storageowner.Supported(kind) {
			t.Errorf("Supported(%q) = false, want true", kind)
		}
	}
	for _, kind := range []pkglogger.LoggerContractKind{"", "loki", "../logger"} {
		if storageowner.Supported(kind) {
			t.Errorf("Supported(%q) = true, want false", kind)
		}
	}
}
