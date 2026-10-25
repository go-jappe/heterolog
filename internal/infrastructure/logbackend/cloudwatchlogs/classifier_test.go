package cloudwatchlogs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/retryclassifier"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

func TestClassifyEventMessageMeasuresJSONReplacementBytes(t *testing.T) {
	if err := ClassifyEventMessage("valid neighbor\xff"); err != nil {
		t.Fatalf("small invalid UTF-8 input must retain JSON replacement semantics: %v", err)
	}
	// U+FFFDへの置換後は3 byteずつになるため、置換後の長さだけがmessage上限を超えます。
	message := strings.Repeat("\xff", pkgcloudwatchlogs.MaxPutLogEventMessageBytes/3+1)
	if len(message) > pkgcloudwatchlogs.MaxPutLogEventMessageBytes {
		t.Fatalf("raw test message is %d bytes, want within the limit before replacement", len(message))
	}
	var validationErr *EventValidationError
	if err := ClassifyEventMessage(message); !errors.As(err, &validationErr) {
		t.Fatalf("decoded message exceeds the per-event message limit, got error=%v", err)
	}
}

// facadeはretryclassifierの分類をそのまま公開します。分類規則はretryclassifierの試験で確認します。
func TestClassifierFacadeDelegatesToRetryClassifier(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ctx := range []context.Context{context.Background(), canceled} {
		for _, err := range []error{
			nil,
			&pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusBadRequest, AwsErrorType: "ThrottlingException"},
			&pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusBadRequest, AwsErrorType: "InvalidParameterException"},
			fmt.Errorf("put: %w", &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusRequestEntityTooLarge}),
			&pkgcloudwatchlogs.ResponseDecodeError{Operation: "PutLogEvents", Err: io.ErrUnexpectedEOF},
			&pkgcloudwatchlogs.RequestNotSentError{Err: errors.New("credentials unavailable")},
			retryable.NewCircuitBreakerOpenError("cloudwatch logs send", time.Unix(1_700_000_000, 0)),
			errors.New("unclassified result"),
			fmt.Errorf("put: %w", context.Canceled),
		} {
			if got, want := ClassifyRetry(ctx, err), retryclassifier.ClassifyRetry(ctx, err); got != want {
				t.Fatalf("ClassifyRetry(%v) with ctx err %v = %+v, want %+v", err, ctx.Err(), got, want)
			}
			if got, want := ClassifyDurableRequestFailure(ctx, err), retryclassifier.ClassifyDurableRequestFailure(ctx, err); got != want {
				t.Fatalf("ClassifyDurableRequestFailure(%v) with ctx err %v = %+v, want %+v", err, ctx.Err(), got, want)
			}
			if got, want := ClassifyDurableRequestOutcome(ctx, err), retryclassifier.ClassifyDurableRequestOutcome(ctx, err); got != want {
				t.Fatalf("ClassifyDurableRequestOutcome(%v) with ctx err %v = %+v, want %+v", err, ctx.Err(), got, want)
			}
			if got, want := IsRequestEntityTooLarge(err), retryclassifier.IsRequestEntityTooLarge(err); got != want {
				t.Fatalf("IsRequestEntityTooLarge(%v) = %t, want %t", err, got, want)
			}
		}
	}
}

func TestClassifyRejectedLogEvent(t *testing.T) {
	tests := []struct {
		name   string
		reason pkgcloudwatchlogs.RejectedLogEventReason
		want   retryable.RetryDecision
	}{
		// 時間経過で受理される可能性があるため再送します。
		{name: "too new", reason: pkgcloudwatchlogs.RejectedLogEventReasonTooNew, want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonServiceTransient, Code: http.StatusOK}},
		// 同じtimestampのまま再送しても受理されないため配送不能です。
		{name: "too old", reason: pkgcloudwatchlogs.RejectedLogEventReasonTooOld, want: retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusOK}},
		{name: "expired", reason: pkgcloudwatchlogs.RejectedLogEventReasonExpired, want: retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusOK}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if decision := ClassifyRejectedLogEvent(pkgcloudwatchlogs.RejectedLogEvent{Reason: tt.reason}); decision != tt.want {
				t.Fatalf("ClassifyRejectedLogEvent() = %+v, want %+v", decision, tt.want)
			}
		})
	}
}

func TestClassifyEventMessage(t *testing.T) {
	tests := []struct {
		name      string
		message   string
		wantCode  FailureCode
		wantBytes int
	}{
		{
			name:     "empty",
			message:  "",
			wantCode: "cloudwatch_logs_invalid_event_message_empty",
		},
		{
			name:      "too large",
			message:   strings.Repeat("x", pkgcloudwatchlogs.MaxPutLogEventMessageBytes+1),
			wantCode:  "cloudwatch_logs_invalid_event_too_large",
			wantBytes: pkgcloudwatchlogs.MaxPutLogEventMessageBytes + 1,
		},
	}

	for _, valid := range []string{"valid", strings.Repeat("x", pkgcloudwatchlogs.MaxPutLogEventMessageBytes)} {
		if err := ClassifyEventMessage(valid); err != nil {
			t.Fatalf("ClassifyEventMessage(%d bytes) error = %v, want nil", len(valid), err)
		}
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := ClassifyEventMessage(tt.message)
			if err == nil {
				t.Fatal("ClassifyEventMessage() error = nil, want error")
			}
			var validationErr *EventValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("error type = %T, want *EventValidationError", err)
			}
			if validationErr.FailureCode != tt.wantCode {
				t.Fatalf("FailureCode = %q, want %q", validationErr.FailureCode, tt.wantCode)
			}
			if validationErr.MessageBytes != tt.wantBytes {
				t.Fatalf("MessageBytes = %d, want %d", validationErr.MessageBytes, tt.wantBytes)
			}
			if validationErr.FailureCode == FailureCodeInvalidEventTooLarge &&
				validationErr.MaxBytes != pkgcloudwatchlogs.MaxPutLogEventMessageBytes {
				t.Fatalf("MaxBytes = %d, want %d", validationErr.MaxBytes, pkgcloudwatchlogs.MaxPutLogEventMessageBytes)
			}
		})
	}
}

func TestFailureCodeSerializedValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  FailureCode
		want string
	}{
		{name: "invalid event", got: FailureCodeInvalidEvent, want: "cloudwatch_logs_invalid_event"},
		{name: "empty message", got: FailureCodeInvalidEventMessageEmpty, want: "cloudwatch_logs_invalid_event_message_empty"},
		{name: "event too large", got: FailureCodeInvalidEventTooLarge, want: "cloudwatch_logs_invalid_event_too_large"},
		{name: "rejected expired", got: FailureCodeRejectedExpired, want: "cloudwatch_logs_rejected_expired"},
		{name: "rejected too old", got: FailureCodeRejectedTooOld, want: "cloudwatch_logs_rejected_too_old"},
		{name: "rejected too new", got: FailureCodeRejectedTooNew, want: "cloudwatch_logs_rejected_too_new"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := string(tt.got); got != tt.want {
				t.Fatalf("serialized FailureCode = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRejectedLogEventFailureCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		reason pkgcloudwatchlogs.RejectedLogEventReason
		want   FailureCode
	}{
		{name: "expired", reason: pkgcloudwatchlogs.RejectedLogEventReasonExpired, want: FailureCodeRejectedExpired},
		{name: "too old", reason: pkgcloudwatchlogs.RejectedLogEventReasonTooOld, want: FailureCodeRejectedTooOld},
		{name: "too new", reason: pkgcloudwatchlogs.RejectedLogEventReasonTooNew, want: FailureCodeRejectedTooNew},
		{name: "unknown", reason: "future_reason", want: "cloudwatch_logs_rejected_future_reason"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := RejectedLogEventFailureCode(tt.reason); got != tt.want {
				t.Fatalf("RejectedLogEventFailureCode() = %q, want %q", got, tt.want)
			}
		})
	}
}
