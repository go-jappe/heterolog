package loki

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/retryclassifier"
)

// ClassifyDurableRequestFailureは、失敗したLoki Pushをdurable storeへ保存する際の
// 分類を返します。Circuit Breaker openと260 Ingestion Blockedは将来の再送対象、
// 結果不明error、HTTP 400、分割できない一件のHTTP 413は確認後の手動再開が必要な
// quarantineとして保持します。認証失敗など宛先・設定の不備は配送不能とします。
func ClassifyDurableRequestFailure(
	ctx context.Context,
	err error,
) retryable.RetryDecision {
	return retryclassifier.ClassifyDurableRequestFailure(ctx, err)
}

// IsRequestEntityTooLargeは、push request全体がHTTP 413で拒否されたか判定します。
// 複数entryのrequestはSplitPushRequestで分割して再送し、一件でも拒否された場合だけ
// ClassifyDurableRequestFailureでquarantineへ分類します。
func IsRequestEntityTooLarge(err error) bool {
	return retryclassifier.IsRequestEntityTooLarge(err)
}
