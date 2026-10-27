// Package retryclassifierは、CloudWatch Logs送信失敗のretry可否を分類します。
package retryclassifier

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

// ErrMissingPutLogEventsOutputは、clientがerrorを返さずにPutLogEventsOutputも返さなかったことを
// 表します。HTTP応答を確認できないため受理の成否は不明です。circuit breaker clientは
// ResponseDecodeErrorで包み、応答のdecode失敗と同じく結果不明として扱います。spool senderも、
// 応答のない成功をこのerrorで示してquarantineへ隔離します。
var ErrMissingPutLogEventsOutput = errors.New("cloudwatch logs client returned no PutLogEventsOutput without error")

// ClassifyRetryは、CloudWatch Logs送信失敗をretry wrapper / spool sender向けに分類します。
//
// CloudWatch LogsはHTTP statusだけではretryableか判断しにくい失敗を返すことがあります。
// そのためHTTPStatusErrorのerror typeを先に見て、throttling系の失敗を
// HTTP 5xx/429などのstatus分類より優先して扱います。
//
// context.Canceled / DeadlineExceededは呼び出し元による停止なので、同じcontextを使う
// retry wrapperでは即時retryしません。後日再送するspool senderでは、この関数の結果を
// そのまま永続判定に使わず、呼び出し側でcontext errorとbackend errorを分けて扱います。
//
// CloudWatch Logs固有判定に当たらない場合だけ、transport error、EOF、HTTP 408/429/5xxなどの
// 共通判定をinternal/infrastructure/http/retryableに委譲します。
func ClassifyRetry(ctx context.Context, err error) retryable.RetryDecision {
	if err == nil {
		return retryable.RetryDecision{}
	}
	if ctx != nil && ctx.Err() != nil {
		return retryable.RetryDecision{}
	}
	if prior := priorAttemptFailure(err); prior != nil {
		return ClassifyRetry(ctx, prior.current)
	}
	if creationErr := autoCreateFailure(err); creationErr != nil {
		// streamの自動作成のきっかけとなった先のResourceNotFoundではなく、失敗した管理操作を
		// 分類します。
		return ClassifyRetry(ctx, creationErr.CreateError)
	}
	if unsent := requestNotSentFailure(err); unsent != nil {
		return ClassifyRetry(ctx, unsent.Err)
	}
	var decodeErr *pkgcloudwatchlogs.ResponseDecodeError
	if errors.As(err, &decodeErr) {
		return retryable.RetryDecision{Reason: retryable.RetryReasonUnknownOutcome}
	}

	var statusErr *pkgcloudwatchlogs.HTTPStatusError
	if errors.As(err, &statusErr) {
		errorType := statusErr.ErrorType()
		if isRequestRateThrottleError(errorType) {
			return retryable.RetryDecision{
				Retry:      true,
				Reason:     retryable.RetryReasonHTTPThrottle,
				Code:       statusErr.StatusCode(),
				RetryAfter: statusErr.RetryAfter(),
			}
		}
		if errorType == pkgcloudwatchlogs.ErrorTypeOperationAbortedException {
			return retryable.RetryDecision{
				Retry:      true,
				Reason:     retryable.RetryReasonServiceTransient,
				Code:       statusErr.StatusCode(),
				RetryAfter: statusErr.RetryAfter(),
			}
		}
		if errorType == pkgcloudwatchlogs.ErrorTypeRequestAbortedException {
			return retryable.RetryDecision{
				Retry:      true,
				Reason:     retryable.RetryReasonDisconnect,
				Code:       statusErr.StatusCode(),
				RetryAfter: statusErr.RetryAfter(),
			}
		}
	}

	return retryable.ClassifyRetry(ctx, err)
}

// ClassifyDurableRequestFailureは、失敗したCloudWatch Logs requestをdurable storeから
// 将来再送する際の分類を返します。Circuit Breaker openは今回のbackend attemptが
// 抑止されたことを表すため、即時retryとは分けて将来の再送対象として保持します。
// 原因を特定できないerrorはquarantineへ隔離し、自動再送しません。HTTP 413は、
// 複数eventなら呼び出し側が分割して再送するため、ここへ来るのは一件でも拒否された
// 場合で、上限設定の変更で配送できる可能性があるためquarantineとします。
// 明示的な応答の分類は、呼び出し元contextが終了した後も維持します。
func ClassifyDurableRequestFailure(ctx context.Context, err error) retryable.RetryDecision {
	if prior := priorAttemptFailure(err); prior != nil {
		return prior.decision
	}
	if creationErr := autoCreateFailure(err); creationErr != nil {
		return classifyAutoCreateFailure(creationErr)
	}
	if unsent := requestNotSentFailure(err); unsent != nil {
		if unsent.PreviousPutError != nil {
			var previous *pkgcloudwatchlogs.HTTPStatusError
			_ = errors.As(unsent.PreviousPutError, &previous)
			return retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonServiceTransient, Code: previous.Code}
		}
		return retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonRequestNotSent}
	}
	decision := ClassifyRetry(retryable.DurableClassificationContext(ctx), err)
	if !decision.Retry && IsRequestEntityTooLarge(err) {
		return retryable.RetryDecision{
			Quarantine: true,
			Reason:     retryable.RetryReasonRequestTooLarge,
			Code:       http.StatusRequestEntityTooLarge,
		}
	}
	return retryable.ForDurableReplay(ctx, err, decision)
}

// IsRequestEntityTooLargeは、CloudWatch Logsまたは中継先がPutLogEvents全体をHTTP 413で
// 拒否したか判定します。413ではどのeventも受理されていないため、複数eventのrequestは
// 分割して再送できます。呼び出し元の停止と同時に返ったerrorは、どのrequestの結果か
// 確定できないため含めません。
func IsRequestEntityTooLarge(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var statusErr *pkgcloudwatchlogs.HTTPStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode() == http.StatusRequestEntityTooLarge
}

// ClassifyDurableRequestOutcomeは、credentialの取得やstream作成の失敗を、送信済みの
// PutLogEventsのcancelと区別します。log eventが受理されたかどうかが不確かになり得るのは
// 後者だけです。
func ClassifyDurableRequestOutcome(ctx context.Context, err error) retryable.RetryDecision {
	if prior := priorAttemptFailure(err); prior != nil {
		return prior.decision
	}
	if creationErr := autoCreateFailure(err); creationErr != nil {
		return classifyAutoCreateFailure(creationErr)
	}
	if unsent := requestNotSentFailure(err); unsent != nil {
		return ClassifyDurableRequestFailure(ctx, err)
	}
	return retryable.ClassifyDurableRequestOutcome(ctx, err, ClassifyDurableRequestFailure)
}

func requestNotSentFailure(err error) *pkgcloudwatchlogs.RequestNotSentError {
	// 結果が不確かな送信も表すjoinからは、一つの枝だけを取り出しません。
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if _, unknown := cause.(*pkgcloudwatchlogs.ResponseDecodeError); unknown {
			return nil
		}
		if unsent, ok := cause.(*pkgcloudwatchlogs.RequestNotSentError); ok {
			if unsent.Err != nil && (unsent.PreviousPutError == nil || pkgcloudwatchlogs.IsResourceNotFound(unsent.PreviousPutError)) {
				return unsent
			}
			return nil
		}
	}
	return nil
}

func autoCreateFailure(err error) *pkgcloudwatchlogs.AutoCreateLogStreamError {
	// joinされた枝は、結果が不確かな別のPutLogEventsの試行を表している場合があります。
	// 通常のwrapだけをたどります。errors.Asでは、そうしたjoinも探索してしまいます。
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if creationErr, ok := cause.(*pkgcloudwatchlogs.AutoCreateLogStreamError); ok {
			if creationErr.CreateError != nil && pkgcloudwatchlogs.IsResourceNotFound(creationErr.PutError) {
				return creationErr
			}
			return nil
		}
	}
	return nil
}

func classifyAutoCreateFailure(err *pkgcloudwatchlogs.AutoCreateLogStreamError) retryable.RetryDecision {
	decision := ClassifyRetry(context.Background(), err.CreateError)
	if decision.Code >= 400 && decision.Code <= 599 {
		// 作成の恒久的な拒否（たとえばAccessDenied）は恒久的なままです。既知の一時的な拒否は、
		// そのretry policyを保ちます。
		return decision
	}
	// 作成はすでに成功したか、後で成功する可能性があります。logのrequestは拒否され、
	// その後再び送信されていないため、replayしても重複しません。
	// 確認済みのPutのstatusは、元のerrorの原因とは別に保持します。
	var putStatus *pkgcloudwatchlogs.HTTPStatusError
	_ = errors.As(err.PutError, &putStatus) // IsResourceNotFoundで保証されています
	return retryable.RetryDecision{
		Retry: true, Reason: retryable.RetryReasonServiceTransient, Code: putStatus.Code,
	}
}

// isRequestRateThrottleErrorは、AWS error typeがrequest頻度制限を表す場合だけtrueを返します。
// Throttling系とTooManyRequests系にはAPIや世代による名称差があるため共通語を拾います。
// 一方、LimitExceededExceptionはresource数などrequest頻度以外の上限にも使われるため、
// RequestLimitExceeded系だけを明示的に許可します。
func isRequestRateThrottleError(errorType string) bool {
	normalized := strings.ToLower(strings.TrimSpace(errorType))
	if strings.Contains(normalized, "throttl") || strings.Contains(normalized, "toomanyrequests") {
		return true
	}

	switch normalized {
	case "requestlimitexceeded", "requestlimitexceededexception":
		return true
	default:
		return false
	}
}
