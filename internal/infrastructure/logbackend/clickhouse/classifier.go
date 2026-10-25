package clickhouse

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/retryclassifier"
)

const (
	// RetryReasonRecordIsolationRequiredは、durable再送時に一件ずつ送信する必要がある
	// 複数record INSERTの分類理由です。
	RetryReasonRecordIsolationRequired = retryclassifier.RetryReasonRecordIsolationRequired
)

// ErrMissingInsertResponseは、clientがerrorなしでnil InsertResponseを返したことを表します。
// INSERTの成否を確認できないため、通常送信・spool再送ともにquarantineへ隔離します。
var ErrMissingInsertResponse = retryclassifier.ErrMissingInsertResponse

// ClassifyRetry は、ClickHouse 送信失敗を retry wrapper / logger 向けに分類します。
//
// ClickHouse は query / schema / input data の恒久不備でも HTTP 500 を返すことがあります。
// そのため HTTPStatusError body 内の ClickHouse error type が入力・schema 系なら、
// 共通 classifier が 5xx として retryable にした後でも permanent として扱います。
//
// 判別できない 5xx や 429/503、transport error などは共通 classifier の retry 判定を保ちます。
func ClassifyRetry(ctx context.Context, err error) retryable.RetryDecision {
	return retryclassifier.ClassifyRetry(ctx, err)
}

// ClassifyDurableInsertFailureは、失敗したClickHouse INSERTをdurable storeへ保存し、
// 将来再送する際の分類を返します。Circuit Breaker openは将来再送、結果不明errorはquarantineへ残します。
// HTTP送信前のrecord marshal error、record固有の入力値error、HTTP 413は、複数recordなら
// 個別再送の対象として保持し、一件ならquarantineへ隔離します。record内容に依存しない
// schema・setting等の不備は配送不能です。
func ClassifyDurableInsertFailure(
	ctx context.Context,
	err error,
	inputCount int,
) retryable.RetryDecision {
	return retryclassifier.ClassifyDurableInsertFailure(ctx, err, inputCount)
}
