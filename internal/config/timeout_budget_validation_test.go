package config

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestRetryableTimeoutBudgetUsesModeSpecificWorstCase(t *testing.T) {
	const clientTimeout = time.Second

	tests := []struct {
		name string
		mode pkglogger.RetryClientMode
		want time.Duration
	}{
		{
			name: "none uses one request",
			mode: pkglogger.RetryClientModeNone,
			want: time.Second,
		},
		{
			name: "circuit breaker uses one request when closed",
			mode: pkglogger.RetryClientModeCircuitBreaker,
			want: time.Second,
		},
		{
			name: "retry uses linear backoff",
			mode: pkglogger.RetryClientModeRetry,
			want: 3600 * time.Millisecond,
		},
		{
			name: "retry with breaker keeps retry worst case",
			mode: pkglogger.RetryClientModeRetryWithBreaker,
			want: 3600 * time.Millisecond,
		},
		{
			name: "retry policy includes retry after up to max delay",
			mode: pkglogger.RetryClientModeRetryPolicy,
			want: 5 * time.Second,
		},
		{
			name: "retry policy with breaker keeps policy worst case",
			mode: pkglogger.RetryClientModeRetryPolicyWithBreaker,
			want: 5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newRetryableTimeoutBudgetConfig(
				tt.mode,
				2,
				200*time.Millisecond,
				time.Second,
			)
			if got := retryableTimeoutBudget(clientTimeout, cfg); got != tt.want {
				t.Fatalf("retryableTimeoutBudget() = %s, want %s", got, tt.want)
			}
			if err := validateClientTimeoutBudget(
				pkglogger.LoggerContractSync,
				pkglogger.LogOutputOpenSearch,
				tt.want,
				clientTimeout,
				cfg,
				timeoutbudget.TimeoutBudgetMustFit,
			); err == nil {
				t.Fatal("validateClientTimeoutBudget() error = nil at exact budget, want error")
			}
			if err := validateClientTimeoutBudget(
				pkglogger.LoggerContractSync,
				pkglogger.LogOutputOpenSearch,
				tt.want+time.Nanosecond,
				clientTimeout,
				cfg,
				timeoutbudget.TimeoutBudgetMustFit,
			); err != nil {
				t.Fatalf("validateClientTimeoutBudget() above budget error = %v, want nil", err)
			}
		})
	}
}

func TestTimeoutBudgetMatchesBackendRetryAfterSupport(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{pkglogger.RetryClientModeRetry, pkglogger.RetryClientModeRetryWithBreaker} {
		for _, backend := range []pkglogger.LogOutputDestination{pkglogger.LogOutputClickHouse, pkglogger.LogOutputCloudWatchLogs, pkglogger.LogOutputOpenSearch} {
			for _, async := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/async_%t", backend, mode, async), func(t *testing.T) {
					budget := 3600 * time.Millisecond
					if backend == pkglogger.LogOutputClickHouse {
						budget = 5 * time.Second
					}
					for _, delta := range []time.Duration{0, time.Nanosecond} {
						var cfg AppConfig
						logger := logconfig.LoggerConfig{
							LogOutputDestinations: pkglogger.LogOutputDestinations{backend},
							Timeout:               budget + delta,
							TimeoutBudgetPolicy:   timeoutbudget.TimeoutBudgetMustFit,
						}
						retryCfg := retryBudgetLokiConfig(mode)
						logger.ClickHouseConfig.ClickHouseRetryAbleClientConfig = logconfig.ClickHouseRetryAbleClientConfig(retryCfg)
						logger.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig = logconfig.CloudWatchLogsRetryAbleClientConfig(retryCfg)
						logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig = logconfig.OpenSearchRetryAbleClientConfig(retryCfg)
						logger.OpenSearchConfig.NoBulkFlushModeEnabled = true
						if async {
							cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = logger.LogOutputDestinations
							cfg.AsyncBestEffortMultiLoggerConfig.Timeout = logger.Timeout
							cfg.AsyncBestEffortMultiLoggerConfig.TimeoutBudgetPolicy = logger.TimeoutBudgetPolicy
							cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig = logger.ClickHouseConfig
							cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig = logger.CloudWatchLogsConfig
							cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig = logger.OpenSearchConfig
							cfg.AsyncClickHouseClient.Timeout = time.Second
							cfg.AsyncCloudWatchLogsClient.Timeout = time.Second
							cfg.AsyncOpenSearchClient.Timeout = time.Second
						} else {
							cfg.Logger = logger
							cfg.ClickHouseClient.Timeout = time.Second
							cfg.CloudWatchLogsClient.Timeout = time.Second
							cfg.OpenSearchClient.Timeout = time.Second
						}
						if err := validateLoggerTimeoutHierarchy(cfg); (err != nil) != (delta == 0) {
							t.Errorf("parent=%s: error = %v, want rejection iff parent <= %s", budget+delta, err, budget)
						}
					}
				})
			}
		}
	}
}

// 再試行0回は1回だけ送る予算として検証します。負の再試行回数は、pkg/loggerの設定へ
// 変換した後の検証が報告するため、timeout予算の検証では重ねて報告しません。
func TestValidateClientTimeoutBudgetLeavesNegativeRetriesToMappedValidation(t *testing.T) {
	for _, tc := range []struct {
		maxRetries    int
		parentTimeout time.Duration
	}{
		// 0回なら予算は1回分の1秒なので、親の2秒に収まります。
		{maxRetries: 0, parentTimeout: 2 * time.Second},
		// 負の値では予算を計算せず、親が短くてもerrorにしません。
		{maxRetries: -1, parentTimeout: time.Nanosecond},
	} {
		cfg := newRetryableTimeoutBudgetConfig(
			pkglogger.RetryClientModeRetryPolicyWithBreaker,
			tc.maxRetries,
			200*time.Millisecond,
			time.Second,
		)
		if err := validateClientTimeoutBudget(
			pkglogger.LoggerContractSync,
			pkglogger.LogOutputLoki,
			tc.parentTimeout,
			time.Second,
			cfg,
			timeoutbudget.TimeoutBudgetMustFit,
		); err != nil {
			t.Fatalf("validateClientTimeoutBudget(maxRetries=%d) error = %v, want nil", tc.maxRetries, err)
		}
	}
}

// 0以下の待機時間も、pkg/loggerの設定へ変換した後の検証が報告します。timeout予算の検証では
// 重ねて報告せず、待機時間で割る計算もしません。
func TestValidateClientTimeoutBudgetLeavesNonPositiveDelaysToMappedValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      pkglogger.RetryClientMode
		baseDelay time.Duration
		maxDelay  time.Duration
	}{
		{name: "zero base delay", mode: pkglogger.RetryClientModeRetry, baseDelay: 0, maxDelay: time.Second},
		{name: "negative base delay", mode: pkglogger.RetryClientModeRetry, baseDelay: -time.Millisecond, maxDelay: time.Second},
		{name: "zero max delay", mode: pkglogger.RetryClientModeRetryPolicy, baseDelay: 200 * time.Millisecond, maxDelay: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newRetryableTimeoutBudgetConfig(tc.mode, 1, tc.baseDelay, tc.maxDelay)
			if err := validateClientTimeoutBudget(
				pkglogger.LoggerContractSync,
				pkglogger.LogOutputOpenSearch,
				time.Nanosecond,
				time.Second,
				cfg,
				timeoutbudget.TimeoutBudgetMustFit,
			); err != nil {
				t.Fatalf("validateClientTimeoutBudget() error = %v, want nil", err)
			}
		})
	}
}

// OpenSearchのBulk itemの負の再送回数と負の待機時間は、flush managerの構築時の検証が
// 報告します。timeout予算の検証では重ねて報告しません。
func TestValidateClientTimeoutBudgetLeavesInvalidOperationRetriesToFlushManager(t *testing.T) {
	for _, tc := range []struct {
		name    string
		retries int
		wait    time.Duration
	}{
		{name: "negative item retries", retries: -1, wait: 0},
		{name: "negative item retry wait", retries: 0, wait: -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := withOperationRetries(newRetryableTimeoutBudgetConfig(pkglogger.RetryClientModeNone, 0, 0, 0), tc.retries, tc.wait)
			if err := validateClientTimeoutBudget(
				pkglogger.LoggerContractSync,
				pkglogger.LogOutputOpenSearch,
				time.Nanosecond,
				time.Second,
				cfg,
				timeoutbudget.TimeoutBudgetMustFit,
			); err != nil {
				t.Fatalf("validateClientTimeoutBudget() error = %v, want nil", err)
			}
		})
	}
}

func TestRetryableTimeoutBudgetIncludesOpenSearchBulkItemRetryWait(t *testing.T) {
	cfg := newRetryableTimeoutBudgetConfig(
		pkglogger.RetryClientModeNone,
		0,
		0,
		0,
	)
	cfg = withOperationRetries(cfg, 2, 250*time.Millisecond)

	// client呼び出し3回に加え、Bulk item retry間の待機2回を含めます。
	if got, want := retryableTimeoutBudget(time.Second, cfg), 3500*time.Millisecond; got != want {
		t.Fatalf("retryableTimeoutBudget() = %s, want %s", got, want)
	}
}

func TestValidateLoggerTimeoutHierarchyIncludesOpenSearchBulkItemRetryWait(t *testing.T) {
	var cfg AppConfig
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputOpenSearch}
	cfg.Logger.OpenSearchConfig.MaxItemRetries = 2
	cfg.Logger.OpenSearchConfig.RetryWaitTime = 250 * time.Millisecond
	cfg.Logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
	cfg.OpenSearchClient.Timeout = time.Second

	// client呼び出し3秒とitem retry待機0.5秒に等しい親timeoutは拒否します。
	cfg.Logger.Timeout = 3500 * time.Millisecond
	if err := validateLoggerTimeoutHierarchy(cfg); err == nil {
		t.Fatal("validateLoggerTimeoutHierarchy() error = nil at exact item retry budget, want error")
	}

	cfg.Logger.Timeout += time.Nanosecond
	if err := validateLoggerTimeoutHierarchy(cfg); err != nil {
		t.Fatalf("validateLoggerTimeoutHierarchy() above item retry budget error = %v, want nil", err)
	}
}

func TestValidateLoggerTimeoutHierarchyKeepsSynchronousEnqueueFlushInBudget(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*AppConfig)
	}{
		{
			name: "sync logger",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputLoki}
				cfg.Logger.Timeout = 3 * time.Second
				cfg.Logger.LokiConfig.LokiRetryAbleClientConfig = retryBudgetLokiConfig(pkglogger.RetryClientModeRetry)
				cfg.LokiClient.Timeout = time.Second
			},
		},
		{
			name: "async logger",
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputLoki}
				cfg.AsyncBestEffortMultiLoggerConfig.Timeout = 3 * time.Second
				cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiRetryAbleClientConfig = retryBudgetLokiConfig(pkglogger.RetryClientModeRetry)
				cfg.AsyncLokiClient.Timeout = time.Second
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg AppConfig
			tt.configure(&cfg)

			// sync/asyncのbackend loggerは通常はenqueueしますが、件数または時間の
			// 閾値到達時には同じ呼び出し内でflushします。LokiのRetry-After待機を含め5秒の予算が必要です。
			if err := validateLoggerTimeoutHierarchy(cfg); err == nil {
				t.Fatal("validateLoggerTimeoutHierarchy() error = nil, want synchronous flush budget error")
			}
		})
	}
}

func TestValidateLoggerTimeoutHierarchyCoversSyncAndAsyncBackendPaths(t *testing.T) {
	tests := []struct {
		name         string
		wantContract pkglogger.LoggerContractKind
		wantBackend  pkglogger.LogOutputDestination
		configure    func(*AppConfig)
	}{
		{
			name:         "sync opensearch",
			wantContract: pkglogger.LoggerContractSync,
			wantBackend:  logconfig.LogOutputOpenSearch,
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputOpenSearch}
				cfg.Logger.Timeout = time.Second
				cfg.Logger.OpenSearchConfig.MaxItemRetries = 0
				cfg.Logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
				cfg.OpenSearchClient.Timeout = time.Second
			},
		},
		{
			name:         "sync loki",
			wantContract: pkglogger.LoggerContractSync,
			wantBackend:  logconfig.LogOutputLoki,
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputLoki}
				cfg.Logger.Timeout = time.Second
				cfg.Logger.LokiConfig.LokiRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
				cfg.LokiClient.Timeout = time.Second
			},
		},
		{
			name:         "sync cloudwatch logs",
			wantContract: pkglogger.LoggerContractSync,
			wantBackend:  logconfig.LogOutputCloudWatchLogs,
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputCloudWatchLogs}
				cfg.Logger.Timeout = time.Second
				cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
				cfg.CloudWatchLogsClient.Timeout = time.Second
			},
		},
		{
			name:         "sync clickhouse",
			wantContract: pkglogger.LoggerContractSync,
			wantBackend:  logconfig.LogOutputClickHouse,
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputClickHouse}
				cfg.Logger.Timeout = time.Second
				cfg.Logger.ClickHouseConfig.ClickHouseRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
				cfg.ClickHouseClient.Timeout = time.Second
			},
		},
		{
			name:         "async opensearch",
			wantContract: pkglogger.LoggerContractAsyncBestEffort,
			wantBackend:  logconfig.LogOutputOpenSearch,
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputOpenSearch}
				cfg.AsyncBestEffortMultiLoggerConfig.Timeout = time.Second
				cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.MaxItemRetries = 0
				cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
				cfg.AsyncOpenSearchClient.Timeout = time.Second
			},
		},
		{
			name:         "async loki",
			wantContract: pkglogger.LoggerContractAsyncBestEffort,
			wantBackend:  logconfig.LogOutputLoki,
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputLoki}
				cfg.AsyncBestEffortMultiLoggerConfig.Timeout = time.Second
				cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
				cfg.AsyncLokiClient.Timeout = time.Second
			},
		},
		{
			name:         "async cloudwatch logs",
			wantContract: pkglogger.LoggerContractAsyncBestEffort,
			wantBackend:  logconfig.LogOutputCloudWatchLogs,
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputCloudWatchLogs}
				cfg.AsyncBestEffortMultiLoggerConfig.Timeout = time.Second
				cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
				cfg.AsyncCloudWatchLogsClient.Timeout = time.Second
			},
		},
		{
			name:         "async clickhouse",
			wantContract: pkglogger.LoggerContractAsyncBestEffort,
			wantBackend:  logconfig.LogOutputClickHouse,
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputClickHouse}
				cfg.AsyncBestEffortMultiLoggerConfig.Timeout = time.Second
				cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
				cfg.AsyncClickHouseClient.Timeout = time.Second
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg AppConfig
			tt.configure(&cfg)

			err := validateLoggerTimeoutHierarchy(cfg)
			requireTimeoutHierarchyError(t, err, tt.wantContract, tt.wantBackend)
		})
	}
}

func TestValidateLoggerTimeoutHierarchyExcludesDeliveryClickHouseBackgroundFlush(t *testing.T) {
	var cfg AppConfig
	cfg.DeliveryLogger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputClickHouse}
	cfg.DeliveryLogger.Timeout = time.Second
	cfg.DeliveryLogger.DeliveryClickHouseConfig.ClickHouseRetryAbleClientConfig = logconfig.ClickHouseRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicy,
		MaxRetries:      9,
		BaseDelay:       time.Second,
		MaxDelay:        10 * time.Second,
	}
	cfg.DeliveryClickHouseClient.Timeout = 10 * time.Second

	// Delivery ClickHouseの公開呼び出しはenqueueの確定までで、backend送信は
	// contextを切り離したbackground flushが行うため、親timeoutとは比較しません。
	if err := validateLoggerTimeoutHierarchy(cfg); err != nil {
		t.Fatalf("validateLoggerTimeoutHierarchy() error = %v, want nil", err)
	}
}

func TestValidateLoggerTimeoutHierarchyCoversSynchronousDeliveryBackends(t *testing.T) {
	tests := []struct {
		name         string
		wantContract pkglogger.LoggerContractKind
		wantBackend  pkglogger.LogOutputDestination
		configure    func(*AppConfig)
	}{
		{
			name:         "opensearch",
			wantContract: pkglogger.LoggerContractDelivery,
			wantBackend:  logconfig.LogOutputOpenSearch,
			configure: func(cfg *AppConfig) {
				cfg.DeliveryLogger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputOpenSearch}
				cfg.DeliveryLogger.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
				cfg.DeliveryOpenSearchClient.Timeout = time.Second
			},
		},
		{
			name:         "loki",
			wantContract: pkglogger.LoggerContractDelivery,
			wantBackend:  logconfig.LogOutputLoki,
			configure: func(cfg *AppConfig) {
				cfg.DeliveryLogger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputLoki}
				cfg.DeliveryLogger.DeliveryLokiConfig.LokiRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
				cfg.DeliveryLokiClient.Timeout = time.Second
			},
		},
		{
			name:         "cloudwatch logs",
			wantContract: pkglogger.LoggerContractDelivery,
			wantBackend:  logconfig.LogOutputCloudWatchLogs,
			configure: func(cfg *AppConfig) {
				cfg.DeliveryLogger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputCloudWatchLogs}
				cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.RetryClientMode = pkglogger.RetryClientModeNone
				cfg.DeliveryCloudWatchLogsClient.Timeout = time.Second
			},
		},
		{
			name:         "slack",
			wantContract: pkglogger.LoggerContractDelivery,
			wantBackend:  logconfig.LogOutputSlack,
			configure: func(cfg *AppConfig) {
				cfg.DeliveryLogger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputSlack}
				cfg.DeliveryLogger.DeliverySlackConfig.Timeout = time.Second
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg AppConfig
			cfg.DeliveryLogger.Timeout = time.Second
			tt.configure(&cfg)

			err := validateLoggerTimeoutHierarchy(cfg)
			requireTimeoutHierarchyError(t, err, tt.wantContract, tt.wantBackend)
		})
	}
}

func TestValidateLoggerTimeoutHierarchyIncludesDeliverySlack(t *testing.T) {
	var cfg AppConfig
	cfg.DeliveryLogger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputSlack}
	cfg.DeliveryLogger.DeliverySlackConfig.Timeout = 2 * time.Second

	t.Run("rejects timeout that cannot fit synchronous post", func(t *testing.T) {
		cfg.DeliveryLogger.Timeout = 2 * time.Second
		if err := validateLoggerTimeoutHierarchy(cfg); err == nil {
			t.Fatal("validateLoggerTimeoutHierarchy() error = nil, want Slack timeout budget error")
		}
	})

	t.Run("accepts timeout greater than synchronous post", func(t *testing.T) {
		cfg.DeliveryLogger.Timeout = 3 * time.Second
		if err := validateLoggerTimeoutHierarchy(cfg); err != nil {
			t.Fatalf("validateLoggerTimeoutHierarchy() error = %v, want nil", err)
		}
	})
}

func TestValidateLoggerTimeoutHierarchyOmitsOpenSearchBulkAttemptsInNoBulkMode(t *testing.T) {
	var cfg AppConfig
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputOpenSearch}
	cfg.Logger.Timeout = 4 * time.Second
	cfg.Logger.OpenSearchConfig.NoBulkFlushModeEnabled = true
	cfg.Logger.OpenSearchConfig.MaxItemRetries = 2
	cfg.Logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig = logconfig.OpenSearchRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetry,
		MaxRetries:      2,
		BaseDelay:       200 * time.Millisecond,
		MaxDelay:        time.Second,
	}
	cfg.OpenSearchClient.Timeout = time.Second

	// no-bulkは1件Indexだけなのでclient retry予算3.6秒で収まります。
	if err := validateLoggerTimeoutHierarchy(cfg); err != nil {
		t.Fatalf("validateLoggerTimeoutHierarchy() in no-bulk mode error = %v, want nil", err)
	}

	// bulk pathではitem retryの総試行回数も含み、同じ設定は10.8秒必要です。
	cfg.Logger.OpenSearchConfig.NoBulkFlushModeEnabled = false
	if err := validateLoggerTimeoutHierarchy(cfg); err == nil {
		t.Fatal("validateLoggerTimeoutHierarchy() in bulk mode error = nil, want bulk attempt budget error")
	}
}

func retryBudgetLokiConfig(mode pkglogger.RetryClientMode) logconfig.LokiRetryAbleClientConfig {
	return logconfig.LokiRetryAbleClientConfig{
		RetryClientMode: mode,
		MaxRetries:      2,
		BaseDelay:       200 * time.Millisecond,
		MaxDelay:        time.Second,
	}
}

func TestTimeoutBudgetRejectsArithmeticOverflow(t *testing.T) {
	const huge = time.Duration(1 << 62)
	tests := []struct {
		name   string
		client time.Duration
		cfg    retryableTimeoutBudgetConfig
	}{
		{"client attempts", huge, newRetryableTimeoutBudgetConfig(pkglogger.RetryClientModeRetryPolicy, 1, 1, 1)},
		{"retry delays", 1, newRetryableTimeoutBudgetConfig(pkglogger.RetryClientModeRetryPolicy, 2, 1, huge)},
		{"linear backoff", 1, newRetryableTimeoutBudgetConfig(pkglogger.RetryClientModeRetry, 2, huge, huge)},
		{"operation attempts", huge, withOperationRetries(newRetryableTimeoutBudgetConfig(pkglogger.RetryClientModeNone, 0, 0, 0), 1, 0)},
		{"operation waits", 1, withOperationRetries(newRetryableTimeoutBudgetConfig(pkglogger.RetryClientModeNone, 0, 0, 0), 2, huge)},
		{"many attempts", time.Second, newRetryableTimeoutBudgetConfig(pkglogger.RetryClientModeRetry, math.MaxInt, 1, 2)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryableTimeoutBudget(tt.client, tt.cfg); got != time.Duration(math.MaxInt64) {
				t.Fatalf("budget = %s, want saturated duration", got)
			}
			if err := validateClientTimeoutBudget(pkglogger.LoggerContractSync, pkglogger.LogOutputLoki, time.Duration(math.MaxInt64), tt.client, tt.cfg, timeoutbudget.TimeoutBudgetMustFit); err == nil {
				t.Fatal("must_fit accepted an unrepresentable budget")
			}
		})
	}
}

func TestTimeoutBudgetCappedLinearBackoffAndLargeValidBoundary(t *testing.T) {
	cfg := newRetryableTimeoutBudgetConfig(pkglogger.RetryClientModeRetry, 4, 2*time.Second, 3*time.Second)
	// request 5回と、2、3、3、3秒の待機です。
	if got := retryableTimeoutBudget(time.Second, cfg); got != 16*time.Second {
		t.Fatalf("budget = %s, want 16s", got)
	}
	cfg = newRetryableTimeoutBudgetConfig(pkglogger.RetryClientModeRetryPolicy, 1, 1, 1)
	client := time.Duration((math.MaxInt64 - 3) / 2)
	if got := retryableTimeoutBudget(client, cfg); got != time.Duration(math.MaxInt64-2) {
		t.Fatalf("large valid budget = %s", got)
	}
	if err := validateClientTimeoutBudget(pkglogger.LoggerContractSync, pkglogger.LogOutputLoki, time.Duration(math.MaxInt64-1), client, cfg, timeoutbudget.TimeoutBudgetMustFit); err != nil {
		t.Fatal(err)
	}
}

// requireTimeoutBudgetConfigErrorは、errがcontractのloggerの出力先backendについて、timeout予算の
// 設定をkindの理由で拒否したerrorであることを確認します。
func requireTimeoutBudgetConfigError(
	t *testing.T,
	err error,
	contract pkglogger.LoggerContractKind,
	backend pkglogger.LogOutputDestination,
	kind error,
) {
	t.Helper()
	var configErr *TimeoutBudgetConfigError
	if !errors.As(err, &configErr) || configErr.Contract != contract || configErr.Backend != backend ||
		!errors.Is(configErr, kind) {
		t.Fatalf("timeout budget error = %v, want %s %s %v", err, contract.DisplayName(), backend, kind)
	}
}

// requireTimeoutHierarchyErrorは、errがcontractのloggerの出力先backendについて、親loggerのtimeoutが
// clientのretry予算以下であることを報告していることを確認します。
func requireTimeoutHierarchyError(
	t *testing.T,
	err error,
	contract pkglogger.LoggerContractKind,
	backend pkglogger.LogOutputDestination,
) {
	t.Helper()
	if _, ok := errortree.Find(err, func(hierarchyErr *TimeoutHierarchyError) bool {
		return hierarchyErr.Contract == contract && hierarchyErr.Backend == backend
	}); !ok {
		t.Fatalf("timeout hierarchy error = %v, want %s %s timeout hierarchy error", err, contract.DisplayName(), backend)
	}
}
