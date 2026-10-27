package cloudwatchlogs

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/retryclassifier"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/utils/jsonutil"
)

// FailureCodeは、CloudWatch Logs固有の送信失敗分類を表します。
// AWSから受け取る値ではなく、このapplicationがdurable処理の判断に使用する識別値です。
type FailureCode string

const (
	// FailureCodeInvalidEventは、eventの不正理由を細分化できない場合の分類です。
	FailureCodeInvalidEvent FailureCode = "cloudwatch_logs_invalid_event"

	// FailureCodeInvalidEventMessageEmptyは、event messageが空の場合の分類です。
	FailureCodeInvalidEventMessageEmpty FailureCode = "cloudwatch_logs_invalid_event_message_empty"

	// FailureCodeInvalidEventTooLargeは、event messageが1 eventで送信できるmessage上限
	// （pkgcloudwatchlogs.MaxPutLogEventMessageBytes）を超えた場合の分類です。
	FailureCodeInvalidEventTooLarge FailureCode = "cloudwatch_logs_invalid_event_too_large"

	// FailureCodeRejectedExpiredは、CloudWatch Logsが保持期間外としてeventを拒否した分類です。
	FailureCodeRejectedExpired FailureCode = "cloudwatch_logs_rejected_expired"

	// FailureCodeRejectedTooOldは、CloudWatch Logsが古すぎるeventとして拒否した分類です。
	FailureCodeRejectedTooOld FailureCode = "cloudwatch_logs_rejected_too_old"

	// FailureCodeRejectedTooNewは、CloudWatch Logsが未来すぎるeventとして拒否した分類です。
	FailureCodeRejectedTooNew FailureCode = "cloudwatch_logs_rejected_too_new"
)

// EventValidationError は、record 単位で切り分けられる PutLogEvents event 不正を表します。
type EventValidationError struct {
	FailureCode  FailureCode
	MessageBytes int
	MaxBytes     int
}

func (e *EventValidationError) Error() string {
	if e == nil {
		return ""
	}
	switch e.FailureCode {
	case FailureCodeInvalidEventMessageEmpty:
		return "cloudwatch logs event message is empty"
	case FailureCodeInvalidEventTooLarge:
		return fmt.Sprintf("cloudwatch logs event message size %d exceeds %d bytes", e.MessageBytes, e.MaxBytes)
	default:
		return "cloudwatch logs event is invalid"
	}
}

// ErrMissingPutLogEventsOutputは、clientがerrorを返さずにPutLogEventsOutputも返さなかったことを
// 表します。受理の成否を確認できないため、結果不明として扱います。
var ErrMissingPutLogEventsOutput = retryclassifier.ErrMissingPutLogEventsOutput

// ClassifyRetryは、CloudWatch Logs送信失敗をretry wrapper / spool sender向けに分類します。
// 実際の分類は、wrapperからも循環依存せず利用できるretryclassifier packageが担当します。
func ClassifyRetry(ctx context.Context, err error) retryable.RetryDecision {
	return retryclassifier.ClassifyRetry(ctx, err)
}

// ClassifyDurableRequestFailureは、失敗したCloudWatch Logs requestをdurable storeへ
// 保存し、将来再送する際の分類を返します。即時retryでは停止理由となる
// Circuit Breaker openは将来再送、結果不明errorはquarantineへ残し、明確な拒否はその分類を維持します。
func ClassifyDurableRequestFailure(ctx context.Context, err error) retryable.RetryDecision {
	return retryclassifier.ClassifyDurableRequestFailure(ctx, err)
}

// ClassifyDurableRequestOutcomeは、streamの自動作成またはretry待機が中断されたとき、
// 既知のlogの拒否を保持します。
func ClassifyDurableRequestOutcome(ctx context.Context, err error) retryable.RetryDecision {
	return retryclassifier.ClassifyDurableRequestOutcome(ctx, err)
}

// IsRequestEntityTooLargeは、PutLogEvents全体がHTTP 413で拒否されたか判定します。
// 複数eventのrequestは分割して再送し、一件でも拒否された場合だけ
// ClassifyDurableRequestOutcomeでquarantineへ分類します。
func IsRequestEntityTooLarge(err error) bool {
	return retryclassifier.IsRequestEntityTooLarge(err)
}

// ClassifyEventMessage は record 単位で恒久失敗にできる CloudWatch Logs event message 不正を検出します。
// message の上限は、event ごとの付加サイズを含めて一回の PutLogEvents request に収まる
// pkgcloudwatchlogs.MaxPutLogEventMessageBytes です。request planner と同じ上限を使うため、
// この判定を通った event は単独の request として送信できます。
func ClassifyEventMessage(message string) error {
	messageBytes := jsonutil.UTF8StringBytes(message)
	switch {
	case message == "":
		return &EventValidationError{
			FailureCode: FailureCodeInvalidEventMessageEmpty,
		}
	case messageBytes > pkgcloudwatchlogs.MaxPutLogEventMessageBytes:
		return &EventValidationError{
			FailureCode:  FailureCodeInvalidEventTooLarge,
			MessageBytes: messageBytes,
			MaxBytes:     pkgcloudwatchlogs.MaxPutLogEventMessageBytes,
		}
	default:
		return nil
	}
}

// ClassifyRejectedLogEvent は HTTP 200 内の event 単位拒否を retry 判定へ変換します。
//
// too_new は時間経過で受理される可能性があるため retryable とし、
// too_old / expired は同じ timestamp のまま再送しても受理されないため permanent とします。
func ClassifyRejectedLogEvent(rejection pkgcloudwatchlogs.RejectedLogEvent) retryable.RetryDecision {
	switch rejection.Reason {
	case pkgcloudwatchlogs.RejectedLogEventReasonTooNew:
		return retryable.RetryDecision{
			Retry:  true,
			Reason: retryable.RetryReasonServiceTransient,
			Code:   http.StatusOK,
		}
	case pkgcloudwatchlogs.RejectedLogEventReasonTooOld,
		pkgcloudwatchlogs.RejectedLogEventReasonExpired:
		return retryable.RetryDecision{
			Retry:  false,
			Reason: retryable.RetryReasonHTTPStatus,
			Code:   http.StatusOK,
		}
	default:
		return retryable.RetryDecision{}
	}
}

// RejectedLogEventFailureCodeはCloudWatch Logsのevent拒否理由をdurable保存用の
// application failure codeへ変換します。未知の理由も診断値を失わないよう名前空間を
// 付けて保持します。
func RejectedLogEventFailureCode(reason pkgcloudwatchlogs.RejectedLogEventReason) FailureCode {
	switch reason {
	case pkgcloudwatchlogs.RejectedLogEventReasonExpired:
		return FailureCodeRejectedExpired
	case pkgcloudwatchlogs.RejectedLogEventReasonTooOld:
		return FailureCodeRejectedTooOld
	case pkgcloudwatchlogs.RejectedLogEventReasonTooNew:
		return FailureCodeRejectedTooNew
	default:
		return FailureCode("cloudwatch_logs_rejected_" + string(reason))
	}
}
