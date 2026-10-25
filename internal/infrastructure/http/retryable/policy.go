package retryable

import (
	"context"
	"time"
)

const (
	// DefaultPolicyTimeoutRetryAttemptTimeout は、timeout 系 error の後に行う確認 retry の上限です。
	DefaultPolicyTimeoutRetryAttemptTimeout = time.Second

	// DefaultPolicySingleProbeMaxAttempts は、確認試行を含む最大試行回数です。
	DefaultPolicySingleProbeMaxAttempts = 2
)

type RetryPolicyClass string

const (
	RetryPolicyClassNoRetry     RetryPolicyClass = "no_retry"     // 初回のみ
	RetryPolicyClassSingleProbe RetryPolicyClass = "single_probe" // 初回 + 確認retryの最大2回
	RetryPolicyClassTransient   RetryPolicyClass = "transient"    // MaxAttemptsまで。503はRetry-Afterを優先
	RetryPolicyClassThrottled   RetryPolicyClass = "throttled"    // MaxAttemptsまで。Retry-Afterを優先
)

// PolicyClass は retry decision を試行回数policyへ変換します。
// 成否不明の書き込みを実際に再試行してよいかは、呼び出し側が冪等性または重複許容を保証します。
func PolicyClass(decision RetryDecision) RetryPolicyClass {
	if !decision.Retry || decision.Reason == RetryReasonCircuitBreakerOpen {
		return RetryPolicyClassNoRetry
	}

	switch decision.Reason {
	case RetryReasonNetTimeout, RetryReasonCtxDeadline, RetryReasonEOF, RetryReasonDisconnect:
		return RetryPolicyClassSingleProbe
	case RetryReasonHTTPThrottle:
		return RetryPolicyClassThrottled
	case RetryReasonServiceTransient:
		return RetryPolicyClassTransient
	case RetryReasonHTTPStatus:
		switch decision.Code {
		case 408, 500, 504:
			return RetryPolicyClassSingleProbe
		case 429:
			return RetryPolicyClassThrottled
		case 502, 503:
			return RetryPolicyClassTransient
		}
	}

	return RetryPolicyClassNoRetry
}

// PolicyMaxAttempts は分類に応じた合計試行回数を返します。
// single_probe だけを初回 + 1 retryに制限し、transient / throttled は設定値を使います。
func PolicyMaxAttempts(decision RetryDecision, configuredMaxAttempts int) int {
	if configuredMaxAttempts <= 0 {
		return 1
	}
	class := PolicyClass(decision)
	if class == RetryPolicyClassSingleProbe && configuredMaxAttempts > DefaultPolicySingleProbeMaxAttempts {
		return DefaultPolicySingleProbeMaxAttempts
	}
	if class == RetryPolicyClassNoRetry {
		return 1
	}
	return configuredMaxAttempts
}

func PolicyShouldRetry(ctx context.Context, attempt int, decision RetryDecision, configuredMaxAttempts int) bool {
	if !decision.Retry {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	return attempt < PolicyMaxAttempts(decision, configuredMaxAttempts)
}

// PolicyDelay は retry 理由に応じた待機時間を返します。
// 429 など throttle 系と503の Retry-After はmaxDelayを上限とし、親 context の
// 残り時間に収まる場合だけ採用します。
// Retry-Afterを無制限に採用すると、設定値から算出する静的timeout budgetを実行時の
// 待機時間が超えるため、linear backoffと同じ上限に揃えます。
// それ以外の retryable error も既存設定の linear backoff を使い、親 context を
// 超える待機は行いません。
func PolicyDelay(
	ctx context.Context,
	retryN int,
	baseDelay time.Duration,
	maxDelay time.Duration,
	decision RetryDecision,
) (time.Duration, bool) {
	if maxDelay <= 0 {
		maxDelay = 5 * time.Second
	}

	if usesRetryAfter(decision) && decision.RetryAfter > 0 {
		delay := decision.RetryAfter
		if delay > maxDelay {
			delay = maxDelay
		}
		return delay, delayFitsContext(ctx, delay)
	}

	if baseDelay <= 0 {
		baseDelay = 200 * time.Millisecond
	}

	d := baseDelay * time.Duration(retryN)
	if d > maxDelay {
		d = maxDelay
	}
	return d, delayFitsContext(ctx, d)
}

func usesRetryAfter(decision RetryDecision) bool {
	if decision.Reason == RetryReasonHTTPThrottle {
		return true
	}
	return decision.Reason == RetryReasonHTTPStatus && decision.Code == 503
}

// PolicyNextAttemptTimeout は timeout 系 error の後の確認 retry だけを短い timeout にします。
// それ以外の retryable error は親 context の timeout をそのまま使います。
func PolicyNextAttemptTimeout(decision RetryDecision) time.Duration {
	if IsTimeoutDecision(decision) {
		return DefaultPolicyTimeoutRetryAttemptTimeout
	}
	return 0
}

func IsTimeoutDecision(decision RetryDecision) bool {
	switch decision.Reason {
	case RetryReasonNetTimeout, RetryReasonCtxDeadline:
		return true
	case RetryReasonHTTPStatus:
		return decision.Code == 408
	default:
		return false
	}
}

func delayFitsContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 || ctx == nil {
		return true
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return time.Until(deadline) > d
}
