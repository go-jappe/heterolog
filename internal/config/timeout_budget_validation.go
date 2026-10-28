package config

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// retryableTimeoutBudgetConfig は、1 回の logger 呼び出しで client 側が使い得る最大待ち時間を計算するための設定です。
// operationAttempts は OpenSearch の bulk item retry のように、flush manager が同じ client 呼び出しを繰り返す総試行回数です。
type retryableTimeoutBudgetConfig struct {
	enabled            bool
	useMaxRetryDelay   bool
	maxRetries         int
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

	if appCfg.Logger.LogOutputDestinations.Has(logconfig.LogOutputOpenSearch) {
		retryCfg := appCfg.Logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig
		timeoutCfg := newRetryableTimeoutBudgetConfig(
			retryCfg.RetryClientMode,
			retryCfg.MaxRetries,
			retryCfg.BaseDelay,
			retryCfg.MaxDelay,
		)
		if !appCfg.Logger.OpenSearchConfig.NoBulkFlushModeEnabled {
			timeoutCfg = withOperationRetries(
				timeoutCfg,
				appCfg.Logger.OpenSearchConfig.MaxItemRetries,
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
			newRetryAfterTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxRetries, retryCfg.BaseDelay, retryCfg.MaxDelay),
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
			newRetryableTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxRetries, retryCfg.BaseDelay, retryCfg.MaxDelay),
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
			newRetryAfterTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxRetries, retryCfg.BaseDelay, retryCfg.MaxDelay),
			loggerPolicy,
		))
	}

	if appCfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(logconfig.LogOutputOpenSearch) {
		retryCfg := appCfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchRetryAbleClientConfig
		timeoutCfg := newRetryableTimeoutBudgetConfig(
			retryCfg.RetryClientMode,
			retryCfg.MaxRetries,
			retryCfg.BaseDelay,
			retryCfg.MaxDelay,
		)
		if !appCfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.NoBulkFlushModeEnabled {
			timeoutCfg = withOperationRetries(
				timeoutCfg,
				appCfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.MaxItemRetries,
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
			newRetryAfterTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxRetries, retryCfg.BaseDelay, retryCfg.MaxDelay),
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
			newRetryableTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxRetries, retryCfg.BaseDelay, retryCfg.MaxDelay),
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
			newRetryAfterTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxRetries, retryCfg.BaseDelay, retryCfg.MaxDelay),
			asyncPolicy,
		))
	}

	errs = append(errs, validateDeliveryTimeoutHierarchy(appCfg))
	// 管理用APIの監査logを記録する送達確認loggerも、Delivery loggerと同じtimeoutと
	// clientで、監査logの出力先へ送ります。
	if appCfg.AdminAudit.Enabled {
		if err := validateDeliveryTimeoutHierarchy(appCfg.adminAuditAppConfig()); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", adminAuditLoggerLabel, err))
		}
	}
	return errors.Join(errs...)
}

// validateDeliveryTimeoutHierarchyは、Delivery loggerの親timeoutと、出力先のclientの
// timeoutの階層を検証します。
func validateDeliveryTimeoutHierarchy(appCfg AppConfig) error {
	var errs []error
	deliveryPolicy := appCfg.DeliveryLogger.TimeoutBudgetPolicy
	if appCfg.DeliveryLogger.LogOutputDestinations.Has(logconfig.LogOutputOpenSearch) {
		retryCfg := appCfg.DeliveryLogger.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig
		errs = append(errs, validateClientTimeoutBudget(
			pkglogger.LoggerContractDelivery,
			logconfig.LogOutputOpenSearch,
			appCfg.DeliveryLogger.Timeout,
			appCfg.DeliveryOpenSearchClient.Timeout,
			newRetryableTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxRetries, retryCfg.BaseDelay, retryCfg.MaxDelay),
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
			newRetryAfterTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxRetries, retryCfg.BaseDelay, retryCfg.MaxDelay),
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
			newRetryableTimeoutBudgetConfig(retryCfg.RetryClientMode, retryCfg.MaxRetries, retryCfg.BaseDelay, retryCfg.MaxDelay),
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
	maxRetries int,
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
		maxRetries:        maxRetries,
		baseDelay:         baseDelay,
		maxDelay:          maxDelay,
		operationAttempts: 1,
	}
}

// LokiとClickHouseの通常retry wrapperは、429/503 responseのRetry-Afterに従います。
// そのため、retryとpolicyのどちらのmodeでも、全てのretry間隔がMaxDelayに達し得ます。
// 他のbackendは、それぞれのretry modeで選ばれる待機の上限をそのまま使います。
func newRetryAfterTimeoutBudgetConfig(
	mode pkglogger.RetryClientMode,
	maxRetries int,
	baseDelay time.Duration,
	maxDelay time.Duration,
) retryableTimeoutBudgetConfig {
	budget := newRetryableTimeoutBudgetConfig(mode, maxRetries, baseDelay, maxDelay)
	budget.useMaxRetryDelay = true
	return budget
}

func withOperationRetries(
	cfg retryableTimeoutBudgetConfig,
	operationRetries int,
	operationRetryWait time.Duration,
) retryableTimeoutBudgetConfig {
	// OpenSearchはtransport retryの外側でretryable bulk itemを再送するため、client予算に
	// 初回を含む総試行回数を掛け、item retry間の固定待機も別に加算します。
	cfg.operationAttempts = operationRetries + 1
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
	// 負の再試行回数と0以下の待機時間は、pkg/loggerの設定へ変換した後の検証
	// （ValidateRetryClientConfig）が報告します。ここで同じ誤りを重ねて報告せず、
	// 意味のない予算も計算しません。
	if retryCfg.enabled && (retryCfg.maxRetries < 0 || retryCfg.baseDelay <= 0 || retryCfg.maxDelay <= 0) {
		return nil
	}
	// OpenSearchのBulk itemの負の再送回数と負の待機時間は、flush managerの構築時の検証が
	// 報告します。
	if retryCfg.operationAttempts <= 0 || retryCfg.operationRetryWait < 0 {
		return nil
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

// retryableTimeoutBudget は retryable wrapper の attempt 数、retry待機、通常・非同期OpenSearchの
// flush manager 側の総試行回数を合算します。
// retry policyのclassは実行時のerrorで決まるため、validationではtransient/throttledの
// 最悪ケースとして、MaxRetriesに初回を足した試行回数と各retry gapのMaxDelayを使います。
// これにより、429/503のRetry-AfterがruntimeでMaxDelayまで採用される場合も予算に含めます。
// Loki/ClickHouseの通常retryもRetry-Afterを採用するため同じ上限で計算します。
// overflowした場合は表現できる最大のdurationに飽和させます。どの親timeoutもこの値を
// 超えられないため、must_fitは桁あふれで負になった予算を受け入れず、設定を拒否します。
// また、合計を閉じた式で求めるため、試行回数分のloopも避けられます。
func retryableTimeoutBudget(clientTimeout time.Duration, retryCfg retryableTimeoutBudgetConfig) time.Duration {
	// 再試行の回数がretry gapの数で、試行回数はそれに初回を足した数です。最大のintでも
	// 桁あふれしないよう、試行回数はbig.Intで求めます。
	gaps := int64(0)
	if retryCfg.enabled {
		gaps = int64(retryCfg.maxRetries)
	}
	attempts := new(big.Int).Add(big.NewInt(gaps), big.NewInt(1))
	budget := new(big.Int).Mul(big.NewInt(int64(clientTimeout)), attempts)
	if gaps > 0 {
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
