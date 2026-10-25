package s3

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

const (
	durableFailureReasonContextCanceled = "context_canceled"
	durableFailureReasonUnknown         = "unknown"
)

// ClassifyRetry は S3 upload 失敗が後続のdurable再送対象かを判定します。
// transport、context、通常のHTTP statusは共有classifierへ委譲し、HTTP statusだけでは
// 判定できないS3 XML error codeを追加分類します。PutObjectは同じobject keyへの上書きに
// 収束するため、S3がHTTP 400で返すRequestTimeoutは一時的なservice failureとしてretryします。
//
// HTTP 307 TemporaryRedirectとHTTP 409 OperationAbortedもS3が再試行を求める一時状態です。
// 一方、ConditionalRequestConflictはIf-Match / If-None-Matchを付けた条件付きPutObjectで
// 発生します。このclientは条件headerを送らないため対象にせず、409 statusだけを見た
// 一括retryも行いません。
func ClassifyRetry(ctx context.Context, err error) retryable.RetryDecision {
	decision := retryable.ClassifyRetry(ctx, err)
	// 親contextまたはerror chainに含まれるcontext終了、circuit breaker open、通信error等の
	// より直接的な分類をS3 response bodyの分類で上書きしません。
	if ctx != nil && ctx.Err() != nil {
		return decision
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return decision
	}
	if decision.Retry || decision.Reason != retryable.RetryReasonHTTPStatus {
		return decision
	}

	var statusErr *pkgs3.HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr == nil {
		return decision
	}
	if isRetryableS3StatusError(statusErr) {
		return retryable.RetryDecision{
			Retry:      true,
			Reason:     retryable.RetryReasonServiceTransient,
			Code:       statusErr.StatusCode(),
			RetryAfter: statusErr.RetryAfter(),
		}
	}
	return decision
}

// ClassifyDurableUploadFailureは、一回のS3 upload失敗を、compressed log fileを
// 将来再送するretryableと、同じrequestを後で繰り返しても解消しないpermanentに分類します。
//
// process終了によるcontext cancellation、circuit breaker open、responseを得られない
// 通信失敗、および原因を特定できないerrorではupload未完了を安全に否定できません。
// そのためdurable分類ではretryableに倒します。permanentにするのは、S3から受け取った
// HTTP responseが既知の一時失敗ではない4xx、またはredirect（3xx）だった場合です。
// UploadはPUTのredirectを追従しないため、301 PermanentRedirectのregion・endpointの誤りや、
// proxyのlogin pageへのredirectなどは宛先・設定の誤りで配送不能が確定した応答です。同じ設定の
// まま再送しても解消しないため、再送し続けずpermanentとして保持します。S3がbucketのDNS更新中に
// 返す307 TemporaryRedirectだけは、ClassifyRetryが一時状態としてretryableにします。
// local compressed log fileの破損を表すquarantineはupload failureではないため、この
// classifierでは扱いません。
func ClassifyDurableUploadFailure(
	ctx context.Context,
	err error,
) uploader.DurableUploadFailureDecision {
	if ctx != nil {
		switch ctx.Err() {
		case context.Canceled:
			return newRetryableDurableFailure(durableFailureReasonContextCanceled, 0, 0)
		case context.DeadlineExceeded:
			return newRetryableDurableFailure(string(retryable.RetryReasonCtxDeadline), 0, 0)
		}
	}
	if errors.Is(err, context.Canceled) {
		return newRetryableDurableFailure(durableFailureReasonContextCanceled, 0, 0)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return newRetryableDurableFailure(string(retryable.RetryReasonCtxDeadline), 0, 0)
	}

	immediate := ClassifyRetry(context.Background(), err)
	if immediate.Retry || immediate.Reason == retryable.RetryReasonCircuitBreakerOpen {
		return newRetryableDurableFailure(
			string(immediate.Reason),
			immediate.Code,
			immediate.RetryAfter,
		)
	}

	var statusErr *pkgs3.HTTPStatusError
	if errors.As(err, &statusErr) && statusErr != nil {
		code := statusErr.StatusCode()
		// S3やintermediaryが返す5xxは、個別に列挙されていないstatusでもservice側の
		// 一時失敗としてcompressed log fileを保持します。
		if code >= http.StatusInternalServerError && code <= 599 {
			return newRetryableDurableFailure(
				string(retryable.RetryReasonHTTPStatus),
				code,
				statusErr.RetryAfter(),
			)
		}
		// 既知の一時状態はClassifyRetryで先に除外済みです。残るredirectと4xxは、宛先・設定・
		// requestの誤りで同じPUTを繰り返しても解消しません。
		if code >= http.StatusMultipleChoices && code < http.StatusInternalServerError {
			return uploader.DurableUploadFailureDecision{
				FailureCategory: uploader.FailureCategoryPermanent,
				Reason:          string(retryable.RetryReasonHTTPStatus),
				Code:            code,
				RetryAfter:      statusErr.RetryAfter(),
			}
		}
	}

	return newRetryableDurableFailure(durableFailureReasonUnknown, 0, 0)
}

func newRetryableDurableFailure(
	reason string,
	code int,
	retryAfter time.Duration,
) uploader.DurableUploadFailureDecision {
	return uploader.DurableUploadFailureDecision{
		FailureCategory: uploader.FailureCategoryRetryable,
		Reason:          reason,
		Code:            code,
		RetryAfter:      retryAfter,
	}
}

func isRetryableS3StatusError(err *pkgs3.HTTPStatusError) bool {
	if err == nil {
		return false
	}
	switch {
	case err.StatusCode() == http.StatusBadRequest && err.ErrorCode == pkgs3.ErrorCodeRequestTimeout:
		return true
	case err.StatusCode() == http.StatusTemporaryRedirect && err.ErrorCode == pkgs3.ErrorCodeTemporaryRedirect:
		return true
	case err.StatusCode() == http.StatusConflict && err.ErrorCode == pkgs3.ErrorCodeOperationAborted:
		return true
	default:
		return false
	}
}
