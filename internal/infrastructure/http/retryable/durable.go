package retryable

import (
	"context"
	"errors"
)

const (
	// RetryReasonUnknownOutcome identifies an error that does not establish whether
	// the backend accepted the request. It requires inspection before replay.
	RetryReasonUnknownOutcome RetryReason = "outcome_unknown"

	// RetryReasonRequestTooLargeは、backendまたは中継先がrequestをsize超過で拒否し、
	// これ以上分割できない一件でも受理されなかったことを表します。上限設定の変更で
	// 配送できる可能性があるため、配送不能（dead）とはせずquarantineへ隔離します。
	// 複数recordのrequestは、この分類を使う前に呼び出し側が分割して再送します。
	RetryReasonRequestTooLarge RetryReason = "request_too_large"

	// RetryReasonRecordRejectedは、backendが一件のrecordの内容（値の形式・型・制約など）を
	// 拒否したことを表します。入力不備として内容を確認するまでquarantineへ隔離します。
	// 複数recordのrequestは、この分類を使う前に呼び出し側がrecordを分けて再送します。
	RetryReasonRecordRejected RetryReason = "record_rejected"
)

// DurableClassificationContextは、durable分類でerror自体を分類するためのcontextを返します。
// 呼び出し元のcancel・期限だけを除き、値は引き継ぎます。即時retry用の分類は
// 呼び出し元の停止を優先してretryしないため、durable分類へそのまま使うと
// 明示的な応答の分類を失います。
func DurableClassificationContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

// ForDurableReplay separates replayable failures from uncertain outcomes that
// must be quarantined. Callers must classify known input failures before using
// this fallback. Immediate retries retain their own bounded policy.
//
// decisionはDurableClassificationContextで得たcontextで分類し、明示的な応答の分類を
// 呼び出し元の停止状態に左右されずに渡してください。呼び出し元contextが終了した後の
// context errorは送信中の中断として結果不明に分類し、quarantineへ隔離します。
// 呼び出し元の停止を優先した即時retry用の分類（理由なし、または期限切れでretryしない
// 判定）を受け取った場合も、結果を確認できないためquarantineとします。
func ForDurableReplay(ctx context.Context, err error, decision RetryDecision) RetryDecision {
	if err == nil || decision.Quarantine {
		return decision
	}
	if ctx != nil && ctx.Err() != nil && decision.Reason != RetryReasonRequestNotSent &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return RetryDecision{Quarantine: true, Reason: RetryReasonUnknownOutcome}
	}
	switch decision.Reason {
	case RetryReasonCircuitBreakerOpen:
		decision.Retry = true
	case RetryReasonNone, RetryReasonUnknownOutcome:
		decision.Retry = false
		decision.Quarantine = true
		decision.Reason = RetryReasonUnknownOutcome
	case RetryReasonCtxDeadline:
		if !decision.Retry {
			return RetryDecision{Quarantine: true, Reason: RetryReasonUnknownOutcome}
		}
	}
	return decision
}
