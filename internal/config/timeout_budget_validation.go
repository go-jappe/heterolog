package config

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/go-jappe/heterolog/internal/config/configerror"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// retryableTimeoutBudgetConfig は、1 回の logger 呼び出しで client 側が使い得る最大待ち時間を計算するための設定です。
// operationAttempts は OpenSearch の bulk item retry のように、flush manager が同じ client 呼び出しを繰り返す総試行回数です。
type retryableTimeoutBudgetConfig struct {
	enabled            bool
	useMaxRetryDelay   bool
	maxAttempts        int
	baseDelay          time.Duration
	maxDelay           time.Duration
	operationAttempts  int
	operationRetryWait time.Duration
}

// validateLoggerTimeoutHierarchy は、選択された出力先ごとに MultiLogger > logger operation > client の timeout 階層を検証します。
// retry を含む retry client mode の場合は attempt と待機時間を含め、通常・非同期OpenSearchの
// bulk pathだけはflush managerのbulk item総試行回数も含めます。no-bulkとDelivery OpenSearchは
// 1件を直接Indexするため含めません。
// 通常・非同期のbackend loggerはenqueue後、件数または時間の閾値に達した呼び出し内で同期flushするため検証対象です。
// 通常・非同期Slackは常にtimer送信へ切り離すため親timeoutとは比較せず、同期投稿するDelivery Slackだけを含めます。
// Delivery ClickHouseは呼び出し内ではenqueueだけを確定し、送信をbackground flushへ切り離すため、
// Delivery loggerの親timeoutとの階層検証には含めません。
func validateLoggerTimeoutHierarchy(appCfg AppConfig) error {
	var errs []error
	loggerPolicy := appCfg.Logger.TimeoutBudgetPolicy
	asyncPolicy := appCfg.AsyncBestEffortMultiLoggerConfig.TimeoutBudgetPolicy
	deliveryPolicy := appCfg.DeliveryLogger.TimeoutBudgetPolicy

	if appCfg.Logger.LogOutputDestinations.Has(logconfig.LogOutputOpenSearch) {
		retryCfg := appCfg.Logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig
		timeoutCfg := newRetryableTimeoutBudgetConfig(
			retryCfg.RetryClientMode,
			retryCfg.MaxAttempts,
			retryCfg.BaseDelay,
			retryCfg.MaxDelay,
		)
		if !appCfg.Logger.OpenSearchConfig.NoBulkFlushModeEnabled {
			timeoutCfg = withOperationAttempts(
				timeoutCfg,
				appCfg.Logger.OpenSearchConfig.MaxRetryAttempts,
				appCfg.Logger.OpenSearchConfig.RetryWaitTime,
			)
		}
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractSync,
			logconfig.LogOutputOpenSearch,
			appCfg.Logger.Timeout,
			appCfg.OpenSearchClient.Timeout,
			timeoutCfg,
			loggerPolicy,
		))
	}
	if appCfg.Logger.LogOutputDestinations.Has(logconfig.LogOutputLoki) {
		retryCfg := appCfg.Logger.LokiConfig.LokiRetryAbleClientConfig
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractSync,
			logconfig.LogOutputLoki,
			appCfg.Logger.Timeout,
			appCfg.LokiClient.Timeout,
			newRetryAfterTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxAttempts, retryCfg.BaseDelay, retryCfg.MaxDelay),
			loggerPolicy,
		))
	}
	if appCfg.Logger.LogOutputDestinations.Has(logconfig.LogOutputCloudWatchLogs) {
		retryCfg := appCfg.Logger.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractSync,
			logconfig.LogOutputCloudWatchLogs,
			appCfg.Logger.Timeout,
			appCfg.CloudWatchLogsClient.Timeout,
			newRetryableTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxAttempts, retryCfg.BaseDelay, retryCfg.MaxDelay),
			loggerPolicy,
		))
	}
	if appCfg.Logger.LogOutputDestinations.Has(logconfig.LogOutputClickHouse) {
		retryCfg := appCfg.Logger.ClickHouseConfig.ClickHouseRetryAbleClientConfig
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractSync,
			logconfig.LogOutputClickHouse,
			appCfg.Logger.Timeout,
			appCfg.ClickHouseClient.Timeout,
			newRetryAfterTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxAttempts, retryCfg.BaseDelay, retryCfg.MaxDelay),
			loggerPolicy,
		))
	}

	if appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(logconfig.LogOutputOpenSearch) {
		retryCfg := appCfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchRetryAbleClientConfig
		timeoutCfg := newRetryableTimeoutBudgetConfig(
			retryCfg.RetryClientMode,
			retryCfg.MaxAttempts,
			retryCfg.BaseDelay,
			retryCfg.MaxDelay,
		)
		if !appCfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.NoBulkFlushModeEnabled {
			timeoutCfg = withOperationAttempts(
				timeoutCfg,
				appCfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.MaxRetryAttempts,
				appCfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.RetryWaitTime,
			)
		}
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractAsyncBestEffort,
			logconfig.LogOutputOpenSearch,
			appCfg.AsyncBestEffortMultiLoggerConfig.Timeout,
			appCfg.AsyncOpenSearchClient.Timeout,
			timeoutCfg,
			asyncPolicy,
		))
	}
	if appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(logconfig.LogOutputLoki) {
		retryCfg := appCfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiRetryAbleClientConfig
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractAsyncBestEffort,
			logconfig.LogOutputLoki,
			appCfg.AsyncBestEffortMultiLoggerConfig.Timeout,
			appCfg.AsyncLokiClient.Timeout,
			newRetryAfterTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxAttempts, retryCfg.BaseDelay, retryCfg.MaxDelay),
			asyncPolicy,
		))
	}
	if appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(logconfig.LogOutputCloudWatchLogs) {
		retryCfg := appCfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractAsyncBestEffort,
			logconfig.LogOutputCloudWatchLogs,
			appCfg.AsyncBestEffortMultiLoggerConfig.Timeout,
			appCfg.AsyncCloudWatchLogsClient.Timeout,
			newRetryableTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxAttempts, retryCfg.BaseDelay, retryCfg.MaxDelay),
			asyncPolicy,
		))
	}
	if appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(logconfig.LogOutputClickHouse) {
		retryCfg := appCfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseRetryAbleClientConfig
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractAsyncBestEffort,
			logconfig.LogOutputClickHouse,
			appCfg.AsyncBestEffortMultiLoggerConfig.Timeout,
			appCfg.AsyncClickHouseClient.Timeout,
			newRetryAfterTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxAttempts, retryCfg.BaseDelay, retryCfg.MaxDelay),
			asyncPolicy,
		))
	}

	if appCfg.DeliveryLogger.LogOutputDestinations.Has(logconfig.LogOutputOpenSearch) {
		retryCfg := appCfg.DeliveryLogger.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractDelivery,
			logconfig.LogOutputOpenSearch,
			appCfg.DeliveryLogger.Timeout,
			appCfg.DeliveryOpenSearchClient.Timeout,
			newRetryableTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxAttempts, retryCfg.BaseDelay, retryCfg.MaxDelay),
			deliveryPolicy,
		))
	}
	if appCfg.DeliveryLogger.LogOutputDestinations.Has(logconfig.LogOutputLoki) {
		retryCfg := appCfg.DeliveryLogger.DeliveryLokiConfig.LokiRetryAbleClientConfig
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractDelivery,
			logconfig.LogOutputLoki,
			appCfg.DeliveryLogger.Timeout,
			appCfg.DeliveryLokiClient.Timeout,
			newRetryAfterTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxAttempts, retryCfg.BaseDelay, retryCfg.MaxDelay),
			deliveryPolicy,
		))
	}
	if appCfg.DeliveryLogger.LogOutputDestinations.Has(logconfig.LogOutputCloudWatchLogs) {
		retryCfg := appCfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractDelivery,
			logconfig.LogOutputCloudWatchLogs,
			appCfg.DeliveryLogger.Timeout,
			appCfg.DeliveryCloudWatchLogsClient.Timeout,
			newRetryableTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxAttempts, retryCfg.BaseDelay, retryCfg.MaxDelay),
			deliveryPolicy,
		))
	}
	if appCfg.DeliveryLogger.LogOutputDestinations.Has(logconfig.LogOutputSlack) {
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractDelivery,
			logconfig.LogOutputSlack,
			appCfg.DeliveryLogger.Timeout,
			appCfg.DeliveryLogger.DeliverySlackConfig.Timeout,
			retryableTimeoutBudgetConfig{operationAttempts: 1},
			deliveryPolicy,
		))
	}
	return errors.Join(errs...)
}

func newRetryableTimeoutBudgetConfig(
	mode pkglogger.RetryClientMode,
	maxAttempts int,
	baseDelay time.Duration,
	maxDelay time.Duration,
) retryableTimeoutBudgetConfig {
	selection := pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
		RetryClientMode: mode,
	}.RetryClientSelection()

	// circuit breakerのopen cooldownは、このlogger呼び出しの中で待機する時間では
	// ありません。open中の呼び出しは即時に抑止されるため、timeout budgetには
	// 実際にattemptとretry待機を増やすretry wrapperだけを反映します。
	return retryableTimeoutBudgetConfig{
		enabled:           selection.UseRetryingClient || selection.UseRetryPolicyClient,
		useMaxRetryDelay:  selection.UseRetryPolicyClient,
		maxAttempts:       maxAttempts,
		baseDelay:         baseDelay,
		maxDelay:          maxDelay,
		operationAttempts: 1,
	}
}

// Loki and ClickHouse ordinary retry wrappers honor Retry-After on 429/503 responses.
// Every retry gap can therefore reach MaxDelay, in both retry and policy modes.
// Other backends keep the delay bound selected by their own retry mode.
func newRetryAfterTimeoutBudgetConfig(
	mode pkglogger.RetryClientMode,
	maxAttempts int,
	baseDelay time.Duration,
	maxDelay time.Duration,
) retryableTimeoutBudgetConfig {
	budget := newRetryableTimeoutBudgetConfig(mode, maxAttempts, baseDelay, maxDelay)
	budget.useMaxRetryDelay = true
	return budget
}

func withOperationAttempts(
	cfg retryableTimeoutBudgetConfig,
	operationAttempts int,
	operationRetryWait time.Duration,
) retryableTimeoutBudgetConfig {
	// OpenSearchはtransport retryの外側でretryable bulk itemを再送するため、client予算に
	// 総試行回数を掛け、item retry間の固定待機も別に加算します。
	cfg.operationAttempts = operationAttempts
	cfg.operationRetryWait = operationRetryWait
	return cfg
}

// validateClientTimeoutBudget は、親 logger timeout が client retry 予算を必ず上回ることを検証します。
func validateClientTimeoutBudget(
	contractKind pkglogger.LoggerContractKind,
	backend pkglogger.LogOutputDestination,
	parentTimeout time.Duration,
	clientTimeout time.Duration,
	retryCfg retryableTimeoutBudgetConfig,
	policy timeoutbudget.TimeoutBudgetPolicy,
) error {
	displayContract := contractKind.DisplayName()
	configError := func(err error, format string, args ...any) error {
		return &TimeoutBudgetConfigError{
			Contract: contractKind,
			Backend:  backend,
			Err:      err,
			message:  fmt.Sprintf(format, args...),
		}
	}
	if err := validateRetryableTimeoutBudgetConfig(retryCfg); err != nil {
		return configError(err, "%s %s retry timeout budget config is invalid: %v", displayContract, backend, err)
	}
	if clientTimeout <= 0 {
		return configError(
			ErrNonPositiveClientTimeout,
			"%s %s client timeout must be positive with timeout budget policy %q",
			displayContract,
			backend,
			policy,
		)
	}

	policy, err := timeoutbudget.NormalizeTimeoutBudgetPolicy(policy)
	if err != nil {
		return configError(err, "%s %s timeout budget policy is invalid: %v", displayContract, backend, err)
	}

	if policy == timeoutbudget.TimeoutBudgetUnbounded {
		return nil
	}

	if parentTimeout <= 0 {
		return configError(
			ErrNonPositiveParentTimeout,
			"%s %s parent timeout must be positive with timeout budget policy %q",
			displayContract,
			backend,
			policy,
		)
	}

	if policy == timeoutbudget.TimeoutBudgetMayCutoff {
		return nil
	}

	budget := retryableTimeoutBudget(clientTimeout, retryCfg)
	if parentTimeout <= budget {
		return &TimeoutHierarchyError{
			Contract:          contractKind,
			Backend:           backend,
			ParentTimeout:     parentTimeout,
			ClientRetryBudget: budget,
		}
	}
	return nil
}

var (
	// ErrNonPositiveClientTimeoutは、timeout予算を検証するbackend clientのtimeoutが0以下であることを表します。
	ErrNonPositiveClientTimeout = errors.New("client timeout must be positive")
	// ErrNonPositiveParentTimeoutは、unbounded以外のtimeout budget policyで、親loggerのtimeoutが
	// 0以下であることを表します。
	ErrNonPositiveParentTimeout = errors.New("parent timeout must be positive")
	// ErrNonPositiveTimeoutBudgetMaxAttemptsは、retryを行うretry client modeで最大試行回数が0以下であることを表します。
	ErrNonPositiveTimeoutBudgetMaxAttempts = errors.New("max attempts must be greater than zero")
	// ErrNonPositiveTimeoutBudgetBaseDelayは、retryを行うretry client modeで基準待機時間が0以下であることを表します。
	ErrNonPositiveTimeoutBudgetBaseDelay = errors.New("base delay must be greater than zero")
	// ErrNonPositiveTimeoutBudgetMaxDelayは、retryを行うretry client modeで最大待機時間が0以下であることを表します。
	ErrNonPositiveTimeoutBudgetMaxDelay = errors.New("max delay must be greater than zero")
	// ErrNonPositiveTimeoutBudgetOperationAttemptsは、flush managerがclient呼び出しを繰り返す総試行回数が
	// 0以下であることを表します。
	ErrNonPositiveTimeoutBudgetOperationAttempts = errors.New("operation attempts must be greater than zero")
	// ErrNegativeTimeoutBudgetOperationRetryWaitは、flush managerの再試行間の待機時間が負値であることを表します。
	ErrNegativeTimeoutBudgetOperationRetryWait = errors.New("operation retry wait must not be negative")
)

// TimeoutBudgetConfigErrorは、Contractのloggerの出力先Backendについて、timeout予算を検証する
// ための設定が不正であることを表します。Errは不正の種類を表すsentinel（ErrNonPositiveClientTimeout
// など）または原因で、errors.Isで判別できます。
type TimeoutBudgetConfigError struct {
	Contract pkglogger.LoggerContractKind
	Backend  pkglogger.LogOutputDestination
	Err      error

	message string
}

func (e *TimeoutBudgetConfigError) Error() string { return e.message }

func (e *TimeoutBudgetConfigError) Unwrap() error { return e.Err }

// TimeoutHierarchyErrorは、timeout budget policyがmust_fitのとき、親loggerのtimeoutが
// backend clientのretry予算（全試行のtimeoutと再試行の待機の合計）以下であることを表します。
// ValidateApplicationConfigは複数の設定エラーをまとめるため、errors.Asで取り出してください。
type TimeoutHierarchyError struct {
	Contract          pkglogger.LoggerContractKind
	Backend           pkglogger.LogOutputDestination
	ParentTimeout     time.Duration
	ClientRetryBudget time.Duration
}

func (e *TimeoutHierarchyError) Error() string {
	displayContract := e.Contract.DisplayName()
	return fmt.Sprintf(
		"%s %s timeout hierarchy is invalid: %s timeout %s must be greater than client retry budget %s",
		displayContract,
		e.Backend,
		displayContract,
		e.ParentTimeout,
		e.ClientRetryBudget,
	)
}

func validateRetryableTimeoutBudgetConfig(cfg retryableTimeoutBudgetConfig) error {
	if cfg.enabled {
		if cfg.maxAttempts <= 0 {
			return configerror.New(
				ErrNonPositiveTimeoutBudgetMaxAttempts,
				"max attempts must be greater than zero: %d",
				cfg.maxAttempts,
			)
		}
		if cfg.baseDelay <= 0 {
			return configerror.New(
				ErrNonPositiveTimeoutBudgetBaseDelay,
				"base delay must be greater than zero: %s",
				cfg.baseDelay,
			)
		}
		if cfg.maxDelay <= 0 {
			return configerror.New(
				ErrNonPositiveTimeoutBudgetMaxDelay,
				"max delay must be greater than zero: %s",
				cfg.maxDelay,
			)
		}
	}
	if cfg.operationAttempts <= 0 {
		return configerror.New(
			ErrNonPositiveTimeoutBudgetOperationAttempts,
			"operation attempts must be greater than zero: %d",
			cfg.operationAttempts,
		)
	}
	if cfg.operationRetryWait < 0 {
		return configerror.New(
			ErrNegativeTimeoutBudgetOperationRetryWait,
			"operation retry wait must not be negative: %s",
			cfg.operationRetryWait,
		)
	}
	return nil
}

// retryableTimeoutBudget は retryable wrapper の attempt 数、retry待機、通常・非同期OpenSearchの
// flush manager 側の総試行回数を合算します。
// retry policyのclassは実行時のerrorで決まるため、validationではtransient/throttledの
// 最悪ケースとしてMaxAttemptsと各retry gapのMaxDelayを使います。これにより、429/503の
// Retry-AfterがruntimeでMaxDelayまで採用される場合も予算に含めます。
// Loki/ClickHouseの通常retryもRetry-Afterを採用するため同じ上限で計算します。
// Overflow saturates to the largest duration. No parent timeout can exceed
// that value, so must_fit rejects the configuration instead of accepting a
// wrapped negative budget. Closed-form sums also avoid looping MaxAttempts times.
func retryableTimeoutBudget(clientTimeout time.Duration, retryCfg retryableTimeoutBudgetConfig) time.Duration {
	attempts := int64(1)
	if retryCfg.enabled {
		attempts = int64(retryCfg.maxAttempts)
	}
	budget := new(big.Int).Mul(big.NewInt(int64(clientTimeout)), big.NewInt(attempts))
	if retryCfg.enabled && attempts > 1 {
		gaps := attempts - 1
		delays := new(big.Int)
		if retryCfg.useMaxRetryDelay {
			delays.Mul(big.NewInt(gaps), big.NewInt(int64(retryCfg.maxDelay)))
		} else {
			// sum(min(n*baseDelay, maxDelay)), n=1..gaps
			linearGaps := min(gaps, int64(retryCfg.maxDelay/retryCfg.baseDelay))
			delays.Mul(big.NewInt(linearGaps), big.NewInt(linearGaps+1))
			delays.Quo(delays, big.NewInt(2))
			delays.Mul(delays, big.NewInt(int64(retryCfg.baseDelay)))
			capped := new(big.Int).Mul(big.NewInt(gaps-linearGaps), big.NewInt(int64(retryCfg.maxDelay)))
			delays.Add(delays, capped)
		}
		budget.Add(budget, delays)
	}
	budget.Mul(budget, big.NewInt(int64(retryCfg.operationAttempts)))
	if retryCfg.operationRetryWait > 0 && retryCfg.operationAttempts > 1 {
		wait := new(big.Int).Mul(big.NewInt(int64(retryCfg.operationRetryWait)), big.NewInt(int64(retryCfg.operationAttempts-1)))
		budget.Add(budget, wait)
	}
	if !budget.IsInt64() {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(budget.Int64())
}
