// Package retryclassifierはOpenSearchのrequest単位の失敗を分類します。
package retryclassifier

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

const FailureReasonResponseDecode retryable.RetryReason = "opensearch_response_decode"

// ErrMissingResponseは、clientがerrorを返さずに応答も返さなかったことを表します。
// HTTP応答を確認できないため書込みの成否は不明です。ResponseDecodeErrorで包み、
// 応答のdecode失敗と同じく結果不明として扱います。
var ErrMissingResponse = errors.New("opensearch client returned no response without error")

// ClassifyRetryは共有のtransport・HTTP status規則をOpenSearchのrequest単位errorへ
// 適用します。Bulk itemの失敗は、有効なBulkResponseを受け取った後で別途分類します。
func ClassifyRetry(ctx context.Context, err error) retryable.RetryDecision {
	if err != nil && (ctx == nil || ctx.Err() == nil) {
		var decodeErr *pkgopensearch.ResponseDecodeError
		if errors.As(err, &decodeErr) {
			return retryable.RetryDecision{Reason: FailureReasonResponseDecode}
		}
	}
	return retryable.ClassifyRetry(ctx, err)
}

// ClassifyDurableRequestFailureは、失敗したOpenSearch requestをdurable storeから
// 将来再送する際の分類を返します。Circuit Breaker openは今回のbackend attemptが
// 抑止されたことを表すため、即時retryとは分けて将来の再送対象として保持します。
// HTTP成功responseのdecode失敗や未分類errorでは、record別の成否が確定していません。
// 自動再送せずquarantineへ保存し、確認後の手動再開を必要とします。
// request全体がHTTP 413またはdocument内容の不正で拒否された場合は、複数documentなら
// 呼び出し側が分割して再送するため、ここへ来るのは分割できない一件で、quarantineとします。
// index・request設定など内容に依存しない拒否は配送不能です。明示的な応答の分類は、
// 呼び出し元contextが終了した後も維持します。
func ClassifyDurableRequestFailure(ctx context.Context, err error) retryable.RetryDecision {
	if err == nil {
		return retryable.RetryDecision{}
	}
	var responseDecodeErr *pkgopensearch.ResponseDecodeError
	if errors.As(err, &responseDecodeErr) {
		return retryable.RetryDecision{
			Quarantine: true,
			Reason:     FailureReasonResponseDecode,
		}
	}

	decision := ClassifyRetry(retryable.DurableClassificationContext(ctx), err)
	if !decision.Retry {
		if reason, code, ok := recordScopedRequestRejection(err); ok {
			return retryable.RetryDecision{Quarantine: true, Reason: reason, Code: code}
		}
	}
	return retryable.ForDurableReplay(ctx, err, decision)
}

// IsRecordPayloadErrorTypeは、Bulk itemまたは単件responseのerror typeが、個別documentを
// 点検・修復せずそのまま再送してはいけないpayload不正を表すか判定します。
// illegal_argument_exceptionはindex・request設定の不正にも使われるため含めません。
func IsRecordPayloadErrorType(errorType pkgopensearch.ErrorType) bool {
	switch errorType {
	case pkgopensearch.ErrorTypeMapperParsing,
		pkgopensearch.ErrorTypeDocumentParsing,
		pkgopensearch.ErrorTypeXContentParse,
		pkgopensearch.ErrorTypeParse,
		pkgopensearch.ErrorTypeStrictDynamicMapping:
		return true
	default:
		return false
	}
}

// IsRecordScopedRequestRejectionは、request全体の拒否のうち、HTTP 413またはdocument内容の
// 不正を示す4xx応答で、送ったdocumentを分ければ原因を特定できるものかを判定します。
// request全体が拒否されどのdocumentも処理されていないため、分割して再送しても重複しません。
// 呼び出し元の停止と同時に返ったerrorは、どのrequestの結果か確定できないため含めません。
func IsRecordScopedRequestRejection(err error) bool {
	_, _, ok := recordScopedRequestRejection(err)
	return ok
}

func recordScopedRequestRejection(err error) (retryable.RetryReason, int, bool) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "", 0, false
	}
	var statusErr *pkgopensearch.HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr == nil {
		return "", 0, false
	}
	code := statusErr.StatusCode()
	if code == http.StatusRequestEntityTooLarge {
		return retryable.RetryReasonRequestTooLarge, code, true
	}
	if code >= http.StatusBadRequest && code < http.StatusInternalServerError {
		if detail := statusErr.OpenSearchError(); detail != nil && IsRecordPayloadErrorType(detail.Type) {
			return retryable.RetryReasonRecordRejected, code, true
		}
	}
	return "", 0, false
}
