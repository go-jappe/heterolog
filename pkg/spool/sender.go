package spool

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidExportReport reports a problem that ExportReport.Validate found.
// errors.Is matches an error returned by Validate with both
// ErrInvalidExportReport and the sentinel of the validation reason, such as
// ErrOutcomeCountMismatch. When there are several problems, the error includes
// every applicable reason. Each problem is an *ExportReportError, which holds
// the indexes of the invalid outcome and input.
var ErrInvalidExportReport = errors.New("invalid spool export report")

// ExportReportError is one problem that ExportReport.Validate found. errors.Is
// matches it with both ErrInvalidExportReport and Reason, and after extracting
// it with errors.As, OutcomeIndex and InputIndex identify the invalid element.
// Validate returns several problems joined with errors.Join.
type ExportReportError struct {
	// Reason is the sentinel of the validation reason, such as
	// ErrOutcomeCountMismatch.
	Reason error
	// OutcomeIndex is the position of the invalid outcome within the
	// ExportReport. It is -1 for a problem that is not tied to a specific
	// outcome, such as a count mismatch, an input without an outcome, or a
	// negative input count.
	OutcomeIndex int
	// InputIndex is the index of the input that the problem concerns. For a
	// problem with an outcome, it is the InputIndex of that outcome, kept as
	// is even when it is outside the input range. For an input without an
	// outcome, it is the index of that input. It is -1 for a problem that is
	// not tied to a specific input, such as a count mismatch or a negative
	// input count.
	InputIndex int

	message string
}

// Error returns ErrInvalidExportReport and Reason, followed by the details of
// the problem.
func (e *ExportReportError) Error() string { return e.message }

// Unwrap returns ErrInvalidExportReport and Reason.
func (e *ExportReportError) Unwrap() []error { return []error{ErrInvalidExportReport, e.Reason} }

// noExportReportIndexは、ExportReportErrorが特定のoutcomeまたは入力に結び付かないことを表します。
const noExportReportIndex = -1

func newExportReportError(reason error, outcomeIndex, inputIndex int, format string, args ...any) *ExportReportError {
	return &ExportReportError{
		Reason:       reason,
		OutcomeIndex: outcomeIndex,
		InputIndex:   inputIndex,
		message:      fmt.Sprintf("%v: %v: ", ErrInvalidExportReport, reason) + fmt.Sprintf(format, args...),
	}
}

// These errors are the reasons for the problems that ExportReport.Validate
// detects.
var (
	// ErrNegativeInputCount reports that the input count to validate is
	// negative.
	ErrNegativeInputCount = errors.New("input count must not be negative")
	// ErrOutcomeCountMismatch reports that the number of outcomes does not
	// match the input count.
	ErrOutcomeCountMismatch = errors.New("outcome count does not match input count")
	// ErrInputIndexOutOfRange reports that the InputIndex of an outcome is
	// outside the input range.
	ErrInputIndexOutOfRange = errors.New("outcome input index is outside input range")
	// ErrDuplicateInputIndex reports that the same input index was classified
	// more than once.
	ErrDuplicateInputIndex = errors.New("input index is classified more than once")
	// ErrUnclassifiedInput reports that an input index has no outcome, or that
	// the Status of its outcome is unclassified.
	ErrUnclassifiedInput = errors.New("input index is not classified")
	// ErrUnknownOutcomeStatus reports that the Status of an outcome is not a
	// known class.
	ErrUnknownOutcomeStatus = errors.New("outcome has unknown status")
	// ErrSuccessWithoutAttemptTime reports that the AttemptedAt of a success
	// outcome is zero.
	ErrSuccessWithoutAttemptTime = errors.New("successful outcome has zero attempted time")
	// ErrSuccessWithError reports that a success outcome has an Err.
	ErrSuccessWithError = errors.New("successful outcome has error")
	// ErrOutcomeWithoutError reports that a retryable, permanent or invalid
	// outcome has no Err.
	ErrOutcomeWithoutError = errors.New("unsuccessful outcome has no error")
	// ErrNegativeRetryAfter reports that the RetryAfter of an outcome is
	// negative.
	ErrNegativeRetryAfter = errors.New("outcome has negative retry-after")
	// ErrRetryAfterOnNonRetryableOutcome reports that an outcome other than
	// retryable has a positive RetryAfter.
	ErrRetryAfterOnNonRetryableOutcome = errors.New("non-retryable outcome has retry-after")
)

// ErrDeferredByRetryAfter reports that the backend returned Retry-After to a
// request sent earlier in the replay of the same batch, so the remaining
// records were not sent and were returned as retryable.
var ErrDeferredByRetryAfter = errors.New("spool replay deferred by backend retry-after")

// DeferredByRetryAfterError reports that a record was not sent because the
// backend returned Retry-After to a request sent earlier in the replay of the
// same batch. errors.Is matches it with both ErrDeferredByRetryAfter and
// the Err of the response that returned Retry-After, and errors.As extracts
// the wait.
type DeferredByRetryAfterError struct {
	// RetryAfter is the wait before a resend that the backend specified with
	// Retry-After.
	RetryAfter time.Duration
	// Err is the error of the response that returned Retry-After.
	Err error
}

// Error returns a description that includes the wait and the error of the
// response that returned Retry-After.
func (e *DeferredByRetryAfterError) Error() string {
	return fmt.Sprintf("%v for %s: %v", ErrDeferredByRetryAfter, e.RetryAfter, e.Err)
}

// Unwrap returns ErrDeferredByRetryAfter and, if it is set, Err.
func (e *DeferredByRetryAfterError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrDeferredByRetryAfter}
	}
	return []error{ErrDeferredByRetryAfter, e.Err}
}

// DeferredByRetryAfter returns the *DeferredByRetryAfterError to set on a
// record that was not sent in the replay of the same batch because of the
// Retry-After of throttled. errors.Is matches it with both
// ErrDeferredByRetryAfter and the error of the response that returned
// Retry-After.
func DeferredByRetryAfter(throttled RecordOutcome) error {
	return &DeferredByRetryAfterError{RetryAfter: throttled.RetryAfter, Err: throttled.Err}
}

// RecordOutcomeStatus is the settled processing result for one record of a
// replayed batch. The zero value means unclassified and is not allowed in a
// complete ExportReport.
type RecordOutcomeStatus string

const (
	RecordOutcomeStatusUnset     RecordOutcomeStatus = ""
	RecordOutcomeStatusSuccess   RecordOutcomeStatus = "success"
	RecordOutcomeStatusRetryable RecordOutcomeStatus = "retryable"
	RecordOutcomeStatusPermanent RecordOutcomeStatus = "permanent"
	RecordOutcomeStatusInvalid   RecordOutcomeStatus = "invalid"
)

// RecordOutcome is the send result for the input SpoolRecord specified by
// InputIndex. It does not return the SpoolRecord itself; InputIndex refers to
// an element of the replayed batch. Backend-specific processing may filter,
// group or reorder records before sending, so the send order can differ from
// the original input order; every backend therefore uses the same explicit
// index contract.
// Status is one of success, retryable, permanent and invalid.
// A permanent outcome means that the representation of the record can be
// trusted but its delivery is confirmed to be impossible; an invalid outcome
// must not be resent automatically until the representation of the record,
// or its correspondence with the backend response, has been inspected and
// repaired. A request error that cannot be classified, or a failure to decode
// a response, is saved to quarantine as invalid and is not resent
// automatically until its content and delivery status have been checked.
// Known temporary failures are retryable.
// AttemptedAt holds the attempt time only when a resend to the backend was
// attempted. It is zero when the outcome was settled by validation before
// sending, and non-zero when it was settled from the per-record response of
// the backend.
// Err is nil for success and otherwise holds the cause of the classification.
// RetryAfter is the wait before the next send to the same destination when
// the backend specified Retry-After in a retryable response. It is zero when
// none was specified. Negative values, and non-zero values for statuses other
// than retryable, are not allowed. The replay workers hold off sending to the
// whole destination until this time (or the configured maximum, when this time is
// longer) has passed, and saves the deadline in SpoolRecord.NotBefore of the
// records it returns to waiting for a resend.
type RecordOutcome struct {
	InputIndex  int
	Status      RecordOutcomeStatus
	AttemptedAt time.Time
	Err         error
	RetryAfter  time.Duration
}

// ExportReport is the set of per-record results obtained from the replay of
// one batch. It holds as many results as the records in the batch and
// classifies each input index exactly once, with no duplicates or omissions.
// Each element identifies its original input record by InputIndex, so the
// order of the slice has no meaning.
type ExportReport []RecordOutcome

// LongestRetryAfter returns, among the retryable outcomes with a positive
// RetryAfter, the one with the longest wait. It returns false when there is no
// such outcome. When several outcomes have the same wait, it returns the one
// that appears first in the report.
func (r ExportReport) LongestRetryAfter() (RecordOutcome, bool) {
	var longest RecordOutcome
	found := false
	for _, outcome := range r {
		if outcome.Status != RecordOutcomeStatusRetryable || outcome.RetryAfter <= 0 {
			continue
		}
		if !found || outcome.RetryAfter > longest.RetryAfter {
			longest = outcome
			found = true
		}
	}
	return longest, found
}

// Validate verifies that the input indexes and the results correspond
// one-to-one in structure, and that every record is settled into one of the
// four classes.
//
// It validates the range, duplicates and omissions of InputIndex, so it does
// not depend on the order within the ExportReport. It does not match records
// by guessing their identity from their content. Whether the backend
// responses were mapped to the right InputIndex is guaranteed by the response
// conversion for each backend and its tests. Each
// problem is an *ExportReportError, and several problems are returned joined
// with errors.Join.
func (r ExportReport) Validate(inputCount int) error {
	if inputCount < 0 {
		return newExportReportError(
			ErrNegativeInputCount, noExportReportIndex, noExportReportIndex,
			"%d", inputCount,
		)
	}

	var validationErrors []error
	if len(r) != inputCount {
		validationErrors = append(validationErrors, newExportReportError(
			ErrOutcomeCountMismatch, noExportReportIndex, noExportReportIndex,
			"outcome count = %d, input count = %d", len(r), inputCount,
		))
	}

	seen := make([]bool, inputCount)
	for outcomeIndex, outcome := range r {
		if outcome.InputIndex < 0 || outcome.InputIndex >= inputCount {
			validationErrors = append(validationErrors, newExportReportError(
				ErrInputIndexOutOfRange, outcomeIndex, outcome.InputIndex,
				"outcome[%d] input index %d is outside input range [0, %d)",
				outcomeIndex, outcome.InputIndex, inputCount,
			))
		} else if seen[outcome.InputIndex] {
			validationErrors = append(validationErrors, newExportReportError(
				ErrDuplicateInputIndex, outcomeIndex, outcome.InputIndex,
				"input index %d", outcome.InputIndex,
			))
		} else {
			seen[outcome.InputIndex] = true
		}

		validationErrors = append(validationErrors, validateRecordOutcome(outcomeIndex, outcome)...)
	}

	for inputIndex, classified := range seen {
		if !classified {
			validationErrors = append(validationErrors, newExportReportError(
				ErrUnclassifiedInput, noExportReportIndex, inputIndex,
				"input index %d has no outcome", inputIndex,
			))
		}
	}

	return errors.Join(validationErrors...)
}

// validateRecordOutcomeは、ExportReportのoutcomeIndex番目のoutcome単体の不正を返します。
func validateRecordOutcome(outcomeIndex int, outcome RecordOutcome) []error {
	var validationErrors []error
	invalid := func(reason error, format string, args ...any) {
		validationErrors = append(validationErrors, newExportReportError(
			reason, outcomeIndex, outcome.InputIndex, format, args...,
		))
	}

	// Retry-Afterはbackendがretryableの応答で指定する待機時間だけを表す。
	if outcome.RetryAfter < 0 {
		invalid(ErrNegativeRetryAfter,
			"input index %d has retry-after %s", outcome.InputIndex, outcome.RetryAfter)
	} else if outcome.RetryAfter > 0 && outcome.Status != RecordOutcomeStatusRetryable {
		invalid(ErrRetryAfterOnNonRetryableOutcome,
			"input index %d with status %s has retry-after %s", outcome.InputIndex, outcome.Status, outcome.RetryAfter)
	}

	switch outcome.Status {
	case RecordOutcomeStatusSuccess:
		if outcome.AttemptedAt.IsZero() {
			invalid(ErrSuccessWithoutAttemptTime,
				"input index %d with status %s", outcome.InputIndex, outcome.Status)
		}
		if outcome.Err != nil {
			invalid(ErrSuccessWithError,
				"input index %d has error: %v", outcome.InputIndex, outcome.Err)
		}
	case RecordOutcomeStatusPermanent:
		// backend応答でpermanentと分類した場合は非zero、送信前の
		// backend固有検証で拒否した場合はzeroのどちらも許可する。
		if outcome.Err == nil {
			invalid(ErrOutcomeWithoutError,
				"input index %d with status %s", outcome.InputIndex, outcome.Status)
		}
	case RecordOutcomeStatusRetryable:
		// context終了や先行groupの失敗で未送信の場合もretryableとなるため、
		// AttemptedAtはzeroと非zeroのどちらも許可する。
		if outcome.Err == nil {
			invalid(ErrOutcomeWithoutError,
				"input index %d with status %s", outcome.InputIndex, outcome.Status)
		}
	case RecordOutcomeStatusInvalid:
		// 送信前の検証で判明した場合はzero、backendのrecord別応答から
		// 判明した場合はnon-zeroのどちらも許可する。
		if outcome.Err == nil {
			invalid(ErrOutcomeWithoutError,
				"input index %d with status %s", outcome.InputIndex, outcome.Status)
		}
	case RecordOutcomeStatusUnset:
		invalid(ErrUnclassifiedInput,
			"input index %d has unset status", outcome.InputIndex)
	default:
		invalid(ErrUnknownOutcomeStatus,
			"input index %d has status %q", outcome.InputIndex, outcome.Status)
	}
	return validationErrors
}
