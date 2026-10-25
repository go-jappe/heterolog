package retryclassifier

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

const (
	// RetryReasonIngestionBlockedは、Lokiが現在の設定または運用policyによって
	// ingestionを停止しているため、durable storeから将来再送する必要があることを表します。
	RetryReasonIngestionBlocked retryable.RetryReason = "loki_ingestion_blocked"

	// RetryReasonPushRejectedは、LokiがHTTP 400でpushを拒否したことを表します。
	// 一件のentryなら入力不備、複数entryなら一部のstreamを受理した可能性があり、
	// どのentryが受理されたか確認できないため、いずれも自動再送せずquarantineへ隔離します。
	RetryReasonPushRejected retryable.RetryReason = "loki_push_rejected"
)

// ClassifyRetryは、Lokiへのrequest単位の基礎retry判定を返します。
// 260 Ingestion Blockedは解除前の再試行では解消しないため、ここではretryへ昇格しません。
// durable保存後の将来再送に固有の条件はClassifyDurableRequestFailureが追加します。
func ClassifyRetry(ctx context.Context, err error) retryable.RetryDecision {
	return retryable.ClassifyRetry(ctx, err)
}

// ClassifyDurableRequestFailureは、失敗したLoki Pushをdurable storeから将来再送するか判定します。
// Circuit Breaker openと260 Ingestion Blockedは将来再送、結果不明errorはquarantineとします。
// HTTP 400は入力不備または一部受理の可能性があるためquarantineとします。HTTP 413は、
// 複数entryなら呼び出し側が分割して再送するため、ここへ来るのは一件でも拒否された場合で、
// 上限設定の変更で配送できる可能性があるためquarantineとします。認証失敗など
// その他の恒久的なHTTP statusは、通常のHTTP分類どおり配送不能とします。
// 明示的な応答の分類は、呼び出し元contextが終了した後も維持します。
func ClassifyDurableRequestFailure(
	ctx context.Context,
	err error,
) retryable.RetryDecision {
	if err == nil {
		return retryable.RetryDecision{}
	}

	var statusErr *pkgloki.HTTPStatusError
	if errors.As(err, &statusErr) && statusErr != nil {
		switch {
		case statusErr.IsIngestionBlocked():
			return retryable.RetryDecision{
				Retry:      true,
				Reason:     RetryReasonIngestionBlocked,
				Code:       statusErr.StatusCode(),
				RetryAfter: statusErr.RetryAfter(),
			}
		case statusErr.StatusCode() == http.StatusRequestEntityTooLarge:
			return retryable.RetryDecision{
				Quarantine: true,
				Reason:     retryable.RetryReasonRequestTooLarge,
				Code:       statusErr.StatusCode(),
			}
		case statusErr.StatusCode() == http.StatusBadRequest:
			return retryable.RetryDecision{
				Quarantine: true,
				Reason:     RetryReasonPushRejected,
				Code:       statusErr.StatusCode(),
			}
		}
	}

	return retryable.ForDurableReplay(ctx, err, ClassifyRetry(retryable.DurableClassificationContext(ctx), err))
}

// IsRequestEntityTooLargeは、Lokiまたは中継先がpush request全体をHTTP 413で拒否したか判定します。
// 413ではどのentryも受理されていないため、複数entryのrequestは分割して再送できます。
func IsRequestEntityTooLarge(err error) bool {
	var statusErr *pkgloki.HTTPStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode() == http.StatusRequestEntityTooLarge
}
