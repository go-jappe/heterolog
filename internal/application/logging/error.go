package logging

import (
	"errors"
	"fmt"
)

var (
	// ErrLoggerClosedはClose開始後に同期Loggerへ新しい記録要求が渡されたことを表します。
	// Close処理の完了前でも、以後の要求を受理しない状態になった時点からこのerrorを返します。
	ErrLoggerClosed = errors.New("logger is closed")

	// ErrLogEntryTooLargeはapplicationから受け取った一つのlogical log entryが、
	// backendへfan-outする前のapplication共通上限を超えたことを表します。
	ErrLogEntryTooLarge = errors.New("log entry is too large")
)

// LogEntryTooLargeErrorは、上限を超えたlogical log entryの実際のbyte数と
// 適用した上限を保持します。errors.IsでErrLogEntryTooLargeを判定できます。
type LogEntryTooLargeError struct {
	ActualBytes int
	MaxBytes    int
}

func (e *LogEntryTooLargeError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"%v: entry has %d bytes, limit is %d",
		ErrLogEntryTooLarge,
		e.ActualBytes,
		e.MaxBytes,
	)
}

func (e *LogEntryTooLargeError) Unwrap() error {
	return ErrLogEntryTooLarge
}

type LoggerFailure struct {
	HandlerType string // 失敗したphysical handlerの具象型名
	Op          string // "Info" など
	Err         error  // 元エラー（wrap済みでもOK）
}

func (f LoggerFailure) Error() string {
	return fmt.Sprintf("%s: handler=%s: %v", f.Op, f.HandlerType, f.Err)
}

// Unwrapは、どのloggerが失敗したかというLoggerFailureの情報を保ったまま、
// errors.Is / errors.Asで元の原因をたどれるようにします。
func (f LoggerFailure) Unwrap() error {
	return f.Err
}

// MultiLoggerError は「複数physical handlerのうち一部が失敗」を表す型付きエラー
// - errors.As(err, *MultiLoggerError) で拾える
// - Unwrap() []error により各LoggerFailureとその原因をerrors.Is/Asでたどれる
type MultiLoggerError struct {
	Op       string
	Failures []LoggerFailure
}

func (e *MultiLoggerError) Unwrap() []error {
	errs := make([]error, 0, len(e.Failures))
	for _, f := range e.Failures {
		// ここは「元エラー」でも「f」でも良いが、どのloggerか追えるよう f を入れる
		errs = append(errs, f)
	}
	return errs
}

func (e *MultiLoggerError) Error() string {
	if e == nil {
		return "<nil>"
	}

	message := fmt.Sprintf("%s: %d handler(s) failed", e.Op, len(e.Failures))
	for index, failure := range e.Failures {
		message += fmt.Sprintf(
			"; failure[%d]: handler=%s: %v",
			index,
			failure.HandlerType,
			failure.Err,
		)
	}
	return message
}
