package spool

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestExportReportValidateAcceptsUnorderedOneToOneOutcomes(t *testing.T) {
	attemptedAt := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	retryableErr := errors.New("retry later")
	permanentErr := errors.New("permanent rejection")
	invalidErr := errors.New("invalid payload")
	report := ExportReport{
		{
			InputIndex:  2,
			Status:      RecordOutcomeStatusPermanent,
			AttemptedAt: attemptedAt,
			Err:         permanentErr,
		},
		{
			InputIndex: 0,
			Status:     RecordOutcomeStatusRetryable,
			Err:        retryableErr,
		},
		{
			InputIndex: 3,
			Status:     RecordOutcomeStatusInvalid,
			Err:        invalidErr,
		},
		{
			InputIndex:  1,
			Status:      RecordOutcomeStatusSuccess,
			AttemptedAt: attemptedAt,
		},
	}

	if err := report.Validate(4); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	if len(report) != 4 {
		t.Fatalf("len(report) = %d, want 4", len(report))
	}
}

func TestExportReportValidateAcceptsPermanentRejectionBeforeBackendAttempt(t *testing.T) {
	report := ExportReport{{
		InputIndex: 0,
		Status:     RecordOutcomeStatusPermanent,
		Err:        errors.New("rejected before backend attempt"),
	}}

	if err := report.Validate(1); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestExportReportValidateAcceptsInvalidResultAfterBackendAttempt(t *testing.T) {
	report := ExportReport{{
		InputIndex:  0,
		Status:      RecordOutcomeStatusInvalid,
		AttemptedAt: time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC),
		Err:         errors.New("backend rejected the record representation"),
	}}

	if err := report.Validate(1); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestExportReportValidateRejectsDuplicateAndMissingInputIndexes(t *testing.T) {
	retryableErr := errors.New("retry later")
	report := ExportReport{
		{InputIndex: 0, Status: RecordOutcomeStatusRetryable, Err: retryableErr},
		{InputIndex: 0, Status: RecordOutcomeStatusRetryable, Err: retryableErr},
	}

	err := report.Validate(2)
	// 二件目のoutcomeが入力0を重複して分類し、入力1にはoutcomeがありません。
	requireExportReportProblems(t, err,
		exportReportProblem{ErrDuplicateInputIndex, 1, 0},
		exportReportProblem{ErrUnclassifiedInput, noExportReportIndex, 1},
	)
}

func TestExportReportValidateCollectsIndependentStructuralErrors(t *testing.T) {
	report := ExportReport{
		{InputIndex: -1, Status: RecordOutcomeStatus("unknown")},
		{InputIndex: 3, Status: RecordOutcomeStatusInvalid},
	}

	err := report.Validate(1)
	// 独立した不正は最初の一件で止めず、すべて報告します。範囲外のInputIndexもそのまま示します。
	requireExportReportProblems(t, err,
		exportReportProblem{ErrOutcomeCountMismatch, noExportReportIndex, noExportReportIndex},
		exportReportProblem{ErrInputIndexOutOfRange, 0, -1},
		exportReportProblem{ErrUnknownOutcomeStatus, 0, -1},
		exportReportProblem{ErrInputIndexOutOfRange, 1, 3},
		exportReportProblem{ErrOutcomeWithoutError, 1, 3},
		exportReportProblem{ErrUnclassifiedInput, noExportReportIndex, 0},
	)
}

func TestExportReportValidateRejectsInvalidStatusAttemptCombinations(t *testing.T) {
	attemptedAt := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		outcome RecordOutcome
		want    error
	}{
		{
			name:    "success without attempt",
			outcome: RecordOutcome{Status: RecordOutcomeStatusSuccess},
			want:    ErrSuccessWithoutAttemptTime,
		},
		{
			name: "success with error",
			outcome: RecordOutcome{
				Status:      RecordOutcomeStatusSuccess,
				AttemptedAt: attemptedAt,
				Err:         errors.New("unexpected success error"),
			},
			want: ErrSuccessWithError,
		},
		{
			name: "permanent without error",
			outcome: RecordOutcome{
				Status:      RecordOutcomeStatusPermanent,
				AttemptedAt: attemptedAt,
			},
			want: ErrOutcomeWithoutError,
		},
		{
			name:    "retryable without error",
			outcome: RecordOutcome{Status: RecordOutcomeStatusRetryable},
			want:    ErrOutcomeWithoutError,
		},
		{
			name:    "invalid without error",
			outcome: RecordOutcome{Status: RecordOutcomeStatusInvalid},
			want:    ErrOutcomeWithoutError,
		},
		{
			name:    "unset",
			outcome: RecordOutcome{Status: RecordOutcomeStatusUnset},
			want:    ErrUnclassifiedInput,
		},
		{
			name:    "unknown",
			outcome: RecordOutcome{Status: RecordOutcomeStatus("unknown")},
			want:    ErrUnknownOutcomeStatus,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 不正なoutcomeを二件目に置き、outcomeの位置と入力indexを取り違えないことも確認します。
			err := reportWithInvalidSecondOutcome(attemptedAt, tt.outcome).Validate(2)
			requireExportReportProblems(t, err, exportReportProblem{tt.want, 1, 0})
		})
	}
}

// reportWithInvalidSecondOutcomeは、入力1の正しいoutcomeの後に、入力0を分類するinvalidを置いたreportです。
func reportWithInvalidSecondOutcome(attemptedAt time.Time, invalid RecordOutcome) ExportReport {
	invalid.InputIndex = 0
	return ExportReport{
		{InputIndex: 1, Status: RecordOutcomeStatusSuccess, AttemptedAt: attemptedAt},
		invalid,
	}
}

func TestExportReportValidateCollectsMultipleErrorsFromOneOutcome(t *testing.T) {
	report := ExportReport{{
		InputIndex: 0,
		Status:     RecordOutcomeStatusSuccess,
		Err:        errors.New("unexpected success error"),
	}}

	err := report.Validate(1)
	requireExportReportProblems(t, err,
		exportReportProblem{ErrSuccessWithoutAttemptTime, 0, 0},
		exportReportProblem{ErrSuccessWithError, 0, 0},
	)
}

func TestExportReportValidateEmptyInput(t *testing.T) {
	if err := (ExportReport{}).Validate(0); err != nil {
		t.Fatalf("Validate empty report returned error: %v", err)
	}
	requireExportReportProblems(t, (ExportReport{}).Validate(-1),
		exportReportProblem{ErrNegativeInputCount, noExportReportIndex, noExportReportIndex},
	)
}

func TestExportReportValidateAcceptsRetryAfterOnlyForRetryable(t *testing.T) {
	attemptedAt := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	accepted := ExportReport{{
		InputIndex:  0,
		Status:      RecordOutcomeStatusRetryable,
		AttemptedAt: attemptedAt,
		Err:         errors.New("throttled"),
		RetryAfter:  30 * time.Second,
	}}
	if err := accepted.Validate(1); err != nil {
		t.Fatalf("Validate retryable retry-after returned error: %v", err)
	}

	tests := []struct {
		name    string
		outcome RecordOutcome
		want    error
	}{
		{
			name: "negative retryable",
			outcome: RecordOutcome{
				Status:      RecordOutcomeStatusRetryable,
				AttemptedAt: attemptedAt,
				Err:         errors.New("throttled"),
				RetryAfter:  -time.Second,
			},
			want: ErrNegativeRetryAfter,
		},
		{
			name: "success",
			outcome: RecordOutcome{
				Status:      RecordOutcomeStatusSuccess,
				AttemptedAt: attemptedAt,
				RetryAfter:  time.Second,
			},
			want: ErrRetryAfterOnNonRetryableOutcome,
		},
		{
			name: "permanent",
			outcome: RecordOutcome{
				Status:      RecordOutcomeStatusPermanent,
				AttemptedAt: attemptedAt,
				Err:         errors.New("rejected"),
				RetryAfter:  time.Second,
			},
			want: ErrRetryAfterOnNonRetryableOutcome,
		},
		{
			name: "invalid",
			outcome: RecordOutcome{
				Status:      RecordOutcomeStatusInvalid,
				AttemptedAt: attemptedAt,
				Err:         errors.New("unknown outcome"),
				RetryAfter:  time.Second,
			},
			want: ErrRetryAfterOnNonRetryableOutcome,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := reportWithInvalidSecondOutcome(attemptedAt, tt.outcome).Validate(2)
			requireExportReportProblems(t, err, exportReportProblem{tt.want, 1, 0})
		})
	}
}

func TestExportReportLongestRetryAfterUsesOnlyRetryableOutcomes(t *testing.T) {
	throttledErr := errors.New("first throttle")
	report := ExportReport{
		{InputIndex: 0, Status: RecordOutcomeStatusRetryable, Err: errors.New("no hint")},
		{InputIndex: 1, Status: RecordOutcomeStatusRetryable, Err: throttledErr, RetryAfter: 30 * time.Second},
		{InputIndex: 2, Status: RecordOutcomeStatusRetryable, Err: errors.New("same hint"), RetryAfter: 30 * time.Second},
		{InputIndex: 3, Status: RecordOutcomeStatusRetryable, Err: errors.New("shorter"), RetryAfter: time.Second},
		// Validateが拒否する組み合わせでも、retryable以外の値を待機に使わない。
		{InputIndex: 4, Status: RecordOutcomeStatusPermanent, Err: errors.New("rejected"), RetryAfter: time.Hour},
	}

	got, ok := report.LongestRetryAfter()
	if !ok {
		t.Fatal("LongestRetryAfter found no outcome")
	}
	if got.InputIndex != 1 || got.RetryAfter != 30*time.Second || got.Err != throttledErr {
		t.Fatalf("LongestRetryAfter = %+v, want first 30s retryable outcome", got)
	}

	if got, ok := (ExportReport{report[0], report[4]}).LongestRetryAfter(); ok {
		t.Fatalf("LongestRetryAfter = %+v, want no retryable retry-after", got)
	}
}

func TestDeferredByRetryAfterKeepsThrottleCause(t *testing.T) {
	throttledErr := errors.New("http 429")
	err := DeferredByRetryAfter(RecordOutcome{
		Status:     RecordOutcomeStatusRetryable,
		Err:        throttledErr,
		RetryAfter: 30 * time.Second,
	})
	if !errors.Is(err, ErrDeferredByRetryAfter) || !errors.Is(err, throttledErr) {
		t.Fatalf("DeferredByRetryAfter error = %v, want deferral and throttle cause", err)
	}
	var deferredErr *DeferredByRetryAfterError
	if !errors.As(err, &deferredErr) || deferredErr.RetryAfter != 30*time.Second {
		t.Fatalf("DeferredByRetryAfter error = %v, want 30s wait", err)
	}
	// 表示形式は再送待ちにした理由と待機時間をoperatorが読むerror文字列なので、一度だけ固定します。
	if got, want := err.Error(), "spool replay deferred by backend retry-after for 30s: http 429"; got != want {
		t.Fatalf("DeferredByRetryAfter error text = %q, want %q", got, want)
	}
}

// exportReportProblemは、ExportReportErrorのうち比較する検証理由と不正な要素の位置です。
type exportReportProblem struct {
	reason       error
	outcomeIndex int
	inputIndex   int
}

func (p exportReportProblem) String() string {
	return fmt.Sprintf("{%v outcome=%d input=%d}", p.reason, p.outcomeIndex, p.inputIndex)
}

// exportReportErrorsは、Validateのerrorに含まれるExportReportErrorを出現順に返します。
// errors.Joinとfmt.Errorfのwrapをたどります。
func exportReportErrors(err error) []*ExportReportError {
	var found []*ExportReportError
	var visit func(error)
	visit = func(err error) {
		switch x := err.(type) {
		case nil:
		case *ExportReportError:
			found = append(found, x)
		case interface{ Unwrap() []error }:
			for _, child := range x.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(x.Unwrap())
		}
	}
	visit(err)
	return found
}

// requireExportReportProblemsは、Validateのerrorが期待した不正だけを、順序に依存せず含むことを
// 確認します。各不正はErrInvalidExportReportとその検証理由の両方をerrors.Isで判定できます。
func requireExportReportProblems(t *testing.T, err error, want ...exportReportProblem) {
	t.Helper()
	if !errors.Is(err, ErrInvalidExportReport) {
		t.Fatalf("Validate error = %v, want ErrInvalidExportReport", err)
	}
	remaining := append([]exportReportProblem(nil), want...)
	var unexpected []exportReportProblem
	for _, reportErr := range exportReportErrors(err) {
		if !errors.Is(reportErr, ErrInvalidExportReport) || !errors.Is(reportErr, reportErr.Reason) {
			t.Fatalf("ExportReportError %v must match ErrInvalidExportReport and its reason", reportErr)
		}
		got := exportReportProblem{reportErr.Reason, reportErr.OutcomeIndex, reportErr.InputIndex}
		if index := slices.Index(remaining, got); index >= 0 {
			remaining = slices.Delete(remaining, index, index+1)
			continue
		}
		unexpected = append(unexpected, got)
	}
	if len(remaining) != 0 || len(unexpected) != 0 {
		t.Fatalf("Validate problems missing %v, unexpected %v; error: %v", remaining, unexpected, err)
	}
}

// ExportReportErrorの表示形式は、不正なreportの位置をoperatorがlogで調べるerror文字列なので、
// 一度だけ文字列で固定します。
func TestExportReportErrorFormat(t *testing.T) {
	err := (ExportReport{{InputIndex: 3, Status: RecordOutcomeStatusRetryable, Err: errors.New("retry")}}).Validate(1)
	var got []string
	for _, reportErr := range exportReportErrors(err) {
		got = append(got, reportErr.Error())
	}
	want := []string{
		"invalid spool export report: outcome input index is outside input range: outcome[0] input index 3 is outside input range [0, 1)",
		"invalid spool export report: input index is not classified: input index 0 has no outcome",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ExportReportError messages = %q, want %q", got, want)
	}
}
