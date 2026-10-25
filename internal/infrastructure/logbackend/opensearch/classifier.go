package opensearch

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/retryclassifier"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

const FailureReasonResponseDecode = retryclassifier.FailureReasonResponseDecode

// IsRecordPayloadErrorTypeは、Bulk itemまたは単件responseのerror typeが、個別documentを
// 点検・修復せずそのまま再送してはいけないpayload不正を表すか判定します。
// illegal_argument_exceptionはindex・request設定の不正にも使われるため含めません。
func IsRecordPayloadErrorType(errorType pkgopensearch.ErrorType) bool {
	return retryclassifier.IsRecordPayloadErrorType(errorType)
}

// IsRecordScopedRequestRejectionは、request全体がHTTP 413またはdocument内容の不正で
// 拒否され、送ったdocumentを分ければ原因を特定できるかを判定します。どのdocumentも
// 処理されていないため、複数documentのrequestは分割して再送し、分割できない一件だけを
// ClassifyDurableRequestFailureでquarantineへ分類します。
func IsRecordScopedRequestRejection(err error) bool {
	return retryclassifier.IsRecordScopedRequestRejection(err)
}

// ClassifyRetry は、OpenSearch 送信失敗を retry wrapper / logger 向けに分類します。
//
// 成功HTTP応答のdecode失敗は再送せず、transport errorとHTTP statusは共有classifierへ委譲します。
// BulkResponse の item 単位失敗は ClassifyFailedBulkDocs で別途分類します。
func ClassifyRetry(ctx context.Context, err error) retryable.RetryDecision {
	return retryclassifier.ClassifyRetry(ctx, err)
}

// ClassifyDurableRequestFailureは、失敗したOpenSearch requestをdurable storeへ保存し、
// 将来再送する際の分類を返します。Circuit Breaker openは将来再送に残します。
// 応答のdecode失敗など結果不明error、分割できない一件のHTTP 413とdocument内容の不正は
// quarantine、その他の明確な拒否はClassifyRetryの分類を維持します。
func ClassifyDurableRequestFailure(ctx context.Context, err error) retryable.RetryDecision {
	return retryclassifier.ClassifyDurableRequestFailure(ctx, err)
}

// IsIdempotentCreateConflictは、固定IDのcreate requestがHTTP 409になり、以前の試行で
// すでに同じdocumentを作成済みと判断できるかを返します。単件送信とBulk item分類は
// この規則を共有し、retry wrapperやlogger固有処理には409の特例を持たせません。
func IsIdempotentCreateConflict(
	request pkgopensearch.DocumentWriteRequest,
	err error,
) bool {
	var statusErr *pkgopensearch.HTTPStatusError
	return errors.As(err, &statusErr) &&
		isIdempotentCreateConflictStatus(request, statusErr.StatusCode())
}

// isIdempotentCreateConflictStatusは、固定ID createのrequestとOpenSearch statusだけを使う
// 共通判定です。単件のHTTP errorとBulkResponse itemで結果の表現が異なるため、statusへ
// 揃えたこの境界で同じ冪等性規則を適用します。
func isIdempotentCreateConflictStatus(
	request pkgopensearch.DocumentWriteRequest,
	status int,
) bool {
	return request.Op == pkgopensearch.WriteOpCreate &&
		request.ID != "" &&
		status == http.StatusConflict
}

// IsRequestEntityTooLargeは、OpenSearchがrequest body全体をHTTP 413で拒否したか判定します。
// 複数documentを分割して再送するか、単一documentをquarantine対象とするかは
// 呼び出し側が判断します。document内容の不正による拒否も含めて判定する場合は
// IsRecordScopedRequestRejectionを使います。
func IsRequestEntityTooLarge(err error) bool {
	var statusErr *pkgopensearch.HTTPStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode() == http.StatusRequestEntityTooLarge
}
