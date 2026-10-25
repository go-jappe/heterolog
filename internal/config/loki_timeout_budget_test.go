package config

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLokiTimeoutBudgetIncludesRetryAfterForEveryContract(t *testing.T) {
	for _, contract := range []string{"sync", "async", "delivery"} {
		for _, mode := range []pkglogger.RetryClientMode{
			pkglogger.RetryClientModeNone, pkglogger.RetryClientModeCircuitBreaker,
			pkglogger.RetryClientModeRetry, pkglogger.RetryClientModeRetryWithBreaker,
			pkglogger.RetryClientModeRetryPolicy, pkglogger.RetryClientModeRetryPolicyWithBreaker,
		} {
			t.Run(contract+"/"+string(mode), func(t *testing.T) {
				budget := 5 * time.Second // Three 1s attempts and two Retry-After waits capped at 1s.
				if mode == pkglogger.RetryClientModeNone || mode == pkglogger.RetryClientModeCircuitBreaker {
					budget = time.Second
				}
				for _, delta := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
					var cfg AppConfig
					switch contract {
					case "sync":
						cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
						cfg.Logger.Timeout = budget + delta
						cfg.Logger.TimeoutBudgetPolicy = timeoutbudget.TimeoutBudgetMustFit
						cfg.Logger.LokiConfig.LokiRetryAbleClientConfig = retryBudgetLokiConfig(mode)
						cfg.LokiClient.Timeout = time.Second
					case "async":
						cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
						cfg.AsyncBestEffortMultiLoggerConfig.Timeout = budget + delta
						cfg.AsyncBestEffortMultiLoggerConfig.TimeoutBudgetPolicy = timeoutbudget.TimeoutBudgetMustFit
						cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiRetryAbleClientConfig = retryBudgetLokiConfig(mode)
						cfg.AsyncLokiClient.Timeout = time.Second
					case "delivery":
						cfg.DeliveryLogger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
						cfg.DeliveryLogger.Timeout = budget + delta
						cfg.DeliveryLogger.TimeoutBudgetPolicy = timeoutbudget.TimeoutBudgetMustFit
						cfg.DeliveryLogger.DeliveryLokiConfig.LokiRetryAbleClientConfig = retryBudgetLokiConfig(mode)
						cfg.DeliveryLokiClient.Timeout = time.Second
					}
					err := validateLoggerTimeoutHierarchy(cfg)
					if (err != nil) != (delta <= 0) {
						t.Errorf("parent=%s: error = %v, want rejection iff parent <= %s", budget+delta, err, budget)
					}
				}
			})
		}
	}
}
