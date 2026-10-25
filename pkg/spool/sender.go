package spool

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidExportReportは、ExportReport.Validateが検出した不正を表します。
// Validateが返すerrorは、ErrInvalidExportReportと次の検証理由のsentinelの両方を
// errors.Isで判定できます。複数の不正があれば、該当する理由をすべて含みます。
// 個々の不正は*ExportReportErrorで、不正なoutcomeと入力のindexを持ちます。
var ErrInvalidExportReport = errors.New("invalid spool export report")

// ExportReportErrorは、ExportReport.Validateが検出した一つの不正です。errors.Isで
// ErrInvalidExportReportとReasonの両方を判定でき、errors.Asで取り出すとOutcomeIndexと
// InputIndexから不正な要素を特定できます。Validateは複数の不正をerrors.Joinでまとめて返します。
type ExportReportError struct {
	// Reasonは、ErrOutcomeCountMismatchなどの検証理由のsentinelです。
	Reason error
	// OutcomeIndexは、不正なoutcomeのExportReport内の位置です。件数の不一致、outcomeのない入力、
	// 負の入力件数のように、特定のoutcomeに結び付かない不正では-1です。
	OutcomeIndex int
	// InputIndexは、不正が対象とする入力のindexです。outcomeの不正ではそのoutcomeのInputIndexで、
	// 入力範囲外の値もそのまま保持します。outcomeのない入力ではその入力のindexです。件数の不一致と
	// 負の入力件数のように、特定の入力に結び付かない不正では-1です。
	InputIndex int

	message string
}

// Errorは、ErrInvalidExportReportとReasonに続けて、不正の内容を返します。
func (e *ExportReportError) Error() string { return e.message }

// Unwrapは、ErrInvalidExportReportとReasonを返します。
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

// ExportReport.Validateが検出する不正の理由です。
var (
	// ErrNegativeInputCountは、検証する入力件数が負であることを表します。
	ErrNegativeInputCount = errors.New("input count must not be negative")
	// ErrOutcomeCountMismatchは、outcomeの件数が入力件数と一致しないことを表します。
	ErrOutcomeCountMismatch = errors.New("outcome count does not match input count")
	// ErrInputIndexOutOfRangeは、outcomeのInputIndexが入力の範囲外であることを表します。
	ErrInputIndexOutOfRange = errors.New("outcome input index is outside input range")
	// ErrDuplicateInputIndexは、同じ入力indexを二度以上分類したことを表します。
	ErrDuplicateInputIndex = errors.New("input index is classified more than once")
	// ErrUnclassifiedInputは、入力indexのoutcomeがないか、Statusが未分類であることを表します。
	ErrUnclassifiedInput = errors.New("input index is not classified")
	// ErrUnknownOutcomeStatusは、outcomeのStatusが既知の分類でないことを表します。
	ErrUnknownOutcomeStatus = errors.New("outcome has unknown status")
	// ErrSuccessWithoutAttemptTimeは、successのoutcomeのAttemptedAtがzeroであることを表します。
	ErrSuccessWithoutAttemptTime = errors.New("successful outcome has zero attempted time")
	// ErrSuccessWithErrorは、successのoutcomeがErrを持つことを表します。
	ErrSuccessWithError = errors.New("successful outcome has error")
	// ErrOutcomeWithoutErrorは、success以外のoutcomeがErrを持たないことを表します。
	ErrOutcomeWithoutError = errors.New("unsuccessful outcome has no error")
	// ErrNegativeRetryAfterは、outcomeのRetryAfterが負であることを表します。
	ErrNegativeRetryAfter = errors.New("outcome has negative retry-after")
	// ErrRetryAfterOnNonRetryableOutcomeは、retryable以外のoutcomeが非zeroのRetryAfterを持つことを表します。
	ErrRetryAfterOnNonRetryableOutcome = errors.New("non-retryable outcome has retry-after")
)

// ErrDeferredByRetryAfterは、同じSender.Sendで先に送信したrequestへbackendがRetry-Afterを
// 返したため、残りのrecordを送信せずretryableとして返したことを表します。
var ErrDeferredByRetryAfter = errors.New("spool replay deferred by backend retry-after")

// DeferredByRetryAfterErrorは、同じSender.Sendで先に送信したrequestへbackendがRetry-Afterを
// 返したため、recordを送信しなかったことを表します。errors.Isで、ErrDeferredByRetryAfterと
// Retry-Afterを返した応答のErrの両方を判定でき、errors.Asで待機時間を取り出せます。
type DeferredByRetryAfterError struct {
	// RetryAfterは、backendがRetry-Afterで指定した再送までの待機時間です。
	RetryAfter time.Duration
	// Errは、Retry-Afterを返した応答のerrorです。
	Err error
}

// Errorは、待機時間とRetry-Afterを返した応答のerrorを含む説明を返します。
func (e *DeferredByRetryAfterError) Error() string {
	return fmt.Sprintf("%v for %s: %v", ErrDeferredByRetryAfter, e.RetryAfter, e.Err)
}

// Unwrapは、ErrDeferredByRetryAfterと、Errがあればそれを返します。
func (e *DeferredByRetryAfterError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrDeferredByRetryAfter}
	}
	return []error{ErrDeferredByRetryAfter, e.Err}
}

// DeferredByRetryAfterは、throttledのRetry-Afterにより同じSender.Sendで送信しなかった
// recordへ設定する*DeferredByRetryAfterErrorを返します。errors.Isで、ErrDeferredByRetryAfterと
// Retry-Afterを返した応答のerrorの両方を判定できます。
func DeferredByRetryAfter(throttled RecordOutcome) error {
	return &DeferredByRetryAfterError{RetryAfter: throttled.RetryAfter, Err: throttled.Err}
}

// RecordOutcomeStatusは、Senderへ渡した1件のrecordについて確定した処理結果です。
// zero valueは未分類を表し、完成したExportReportでは許可しません。
type RecordOutcomeStatus string

const (
	RecordOutcomeStatusUnset     RecordOutcomeStatus = ""
	RecordOutcomeStatusSuccess   RecordOutcomeStatus = "success"
	RecordOutcomeStatusRetryable RecordOutcomeStatus = "retryable"
	RecordOutcomeStatusPermanent RecordOutcomeStatus = "permanent"
	RecordOutcomeStatusInvalid   RecordOutcomeStatus = "invalid"
)

// RecordOutcomeは、InputIndexで指定した入力SpoolRecordに対する送信結果です。
// SpoolRecord自体は返さず、InputIndexでSenderに渡したsliceの要素を参照します。
// backend固有処理では送信前のfilter、group化、並べ替えによって送信順が元入力順と
// 一致しない場合があるため、全Senderで同じ明示的なindex契約を使用します。
// Statusはsuccess、retryable、permanent、invalidのいずれかです。
// permanentはrecordの表現を信頼できる一方で配送不能が確定した結果、invalidは
// recordの表現またはbackend応答との対応を点検・修復するまで自動再送してはいけない結果です。
// 分類不能のrequest errorや応答decode失敗はinvalidとしてquarantineへ保存し、
// 内容と配送状況を確認するまで自動再送しません。既知の一時的障害はretryableです。
// AttemptedAtはbackendへの再送を試みた場合だけ、その試行時刻を保持します。
// 送信前の検証で確定した場合はzero、backendのrecord別応答から確定した場合はnon-zeroです。
// Errはsuccessの場合はnil、それ以外の場合はその分類原因を保持します。
// RetryAfterは、backendがretryableの応答でRetry-Afterを指定した場合の、同じ送信先へ
// 次に送信するまでの待機時間です。指定がなければzeroです。負値と、retryable以外での
// 非zeroは許可しません。Workerはこの時間（設定した上限より長い場合は上限）が過ぎるまで
// 送信先全体への送信を控え、再送待ちへ戻すrecordのSpoolRecord.NotBeforeへ期限を保存します。
type RecordOutcome struct {
	InputIndex  int
	Status      RecordOutcomeStatus
	AttemptedAt time.Time
	Err         error
	RetryAfter  time.Duration
}

// ExportReportは、1回のSender.Sendで得られたrecord別結果の集合です。
// Senderに渡したrecordsと同じ件数を保持し、各入力indexを重複・欠落なく
// 一度だけ分類します。各要素はInputIndexで元の入力recordを明示するため、
// sliceの並び順には意味を持ちません。
type ExportReport []RecordOutcome

// LongestRetryAfterは、正のRetryAfterを持つretryableのoutcomeのうち、待機時間が
// 最長のものを返します。該当するoutcomeがなければfalseを返します。
// 同じ長さのoutcomeが複数ある場合は、reportで先に現れたものを返します。
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

// Validateは、入力indexと結果が構造上1対1に対応し、全recordが4分類の
// いずれかに確定していることを検証します。
//
// InputIndexの範囲、重複、欠落を検証するため、ExportReport内の並び順には依存しません。
// record内容からidentityを推測する照合は行いません。
// Senderがbackend応答を正しいInputIndexへ対応付けたかという意味上の正しさは、
// 各Senderの応答変換処理とその試験が保証します。
// 不正はそれぞれ*ExportReportErrorで、複数あればerrors.Joinでまとめて返します。
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
