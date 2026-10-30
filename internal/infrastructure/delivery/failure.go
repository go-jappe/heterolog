// Package deliveryは、NewTelemetryがspanとmetricsを送る仕組みのうち、送るものの種類に
// よらない部分です。送り先（Exporter）、送れなかったものの範囲と扱い（Failure）、
// 送れなかったものを残すfailure store、配送のerrorを受け取るerror handlerを定義します。
package delivery

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
)

// Exporterは、送るもの（spanやmetric）のbatchを1つの送り先へ送ります。Exportは、同じ
// Exporterに対して並行には呼ばれません。
type Exporter[T any] interface {
	// Nameは、metricsのexporter属性の値（otlp・file・stdoutなど）です。
	Name() string
	// Exportは、itemsを送ります。itemsは、Exportが返った後に再利用されるため、
	// 保持しません。送れなかったものの範囲と扱いが分かる場合は、*ExportErrorを返します。
	// ほかのerrorは、すべてを送れなかったことを表します。
	Export(ctx context.Context, items []T) error
	// Closeは、送り先の資源（fileなど）を手放します。最後のExportの後に1回だけ呼ばれます。
	Close() error
}

// FailureStoreは、送れなかったものを、後で送り直すか確かめられるよう保存します。
type FailureStore[T any] interface {
	// Storeは、batchのうちfailuresの範囲のものを保存します。保存できなかった場合は
	// errorを返します。batchは、Storeが返った後に再利用されるため、保持しません。
	Store(ctx context.Context, batch []T, failures []Failure) error
	// Shutdownは、保存と送り直しを止め、資源を手放します。最後のStoreの後に1回だけ
	// 呼ばれます。
	Shutdown(ctx context.Context) error
}

// Dispositionは、送れなかったものをfailure storeでどう扱うかです。
type Disposition int

const (
	// DispositionRetryは、後で送り直します。受信側が受け付けたか分からない失敗も、
	// 欠落より重複を選んで送り直します。
	DispositionRetry Disposition = iota
	// DispositionDeadは、受信側が拒否したため、送り直さずに残します。
	DispositionDead
	// DispositionQuarantineは、一部だけ受け付けられ、どれか分からないため、
	// 送り直さずに残します。
	DispositionQuarantine
)

// Outcomeは、failure storeへ保存したものを数えるoutcome属性の値です。
func (d Disposition) Outcome() string {
	switch d {
	case DispositionDead:
		return telemetry.OutcomeDead
	case DispositionQuarantine:
		return telemetry.OutcomeQuarantine
	default:
		return telemetry.OutcomeRetryable
	}
}

// Failureは、batchのうちStartからEndの手前までを送れなかったことを表します。
type Failure struct {
	Start, End  int
	Disposition Disposition
	// Rejectedは、部分的な成功の応答で受信側が受け付けなかった数です。0なら、範囲の
	// すべてが届いていません。数える単位（spanやdata point）は送るものによります。
	Rejected int
	// StatusCodeは、受信側が返したHTTP statusです。応答がなければ0です。
	StatusCode int
	// Reasonは、失敗の種類（http_status、net_timeoutなど）です。
	Reason string
	// RetryAfterは、受信側がRetry-Afterで求めた、次に送るまでの待ち時間です。
	RetryAfter time.Duration
	Err        error
}

// Failedは、範囲のうち届かなかった数です。部分的な成功では、受信側が数えた単位の数で、
// 範囲の大きさを超えません。
func (f Failure) Failed() int {
	if f.Rejected > 0 {
		return min(f.Rejected, f.End-f.Start)
	}
	return f.End - f.Start
}

// ExportErrorは、batchの一部またはすべてを送れなかったことを、範囲ごとに表します。
// 範囲は重ならず、Startの順に並びます。
type ExportError struct {
	Failures []Failure
}

// Failedは、batchのうち届かなかった数です。
func (e *ExportError) Failed() int {
	failed := 0
	for _, failure := range e.Failures {
		failed += failure.Failed()
	}
	return failed
}

func (e *ExportError) Error() string {
	if len(e.Failures) == 1 {
		return e.Failures[0].Err.Error()
	}
	return fmt.Sprintf("%d requests failed: %v", len(e.Failures), errors.Join(e.Unwrap()...))
}

func (e *ExportError) Unwrap() []error {
	errs := make([]error, 0, len(e.Failures))
	for _, failure := range e.Failures {
		errs = append(errs, failure.Err)
	}
	return errs
}

// ExportFailuresは、Exportが返したerrorを範囲ごとの失敗にします。ExportErrorでない
// errorは、batchのすべてを、結果の分からない失敗として送り直す扱いにします。
func ExportFailures(err error, n int) []Failure {
	var exportErr *ExportError
	if errors.As(err, &exportErr) {
		return exportErr.Failures
	}
	return []Failure{{Start: 0, End: n, Disposition: DispositionRetry, Err: err}}
}

// ErrorHandlerは、配送のerrorを受け取る関数です。複数のgoroutineから並行に呼ばれます。
type ErrorHandler func(error)

// NewErrorHandlerは、reportのpanicで配送のgoroutineが止まらないようにします。reportが
// nilなら、errorを標準のloggerへ1行ずつ書きます。panicした場合も、errorを標準のloggerへ
// 書きます。
func NewErrorHandler(report ErrorHandler) ErrorHandler {
	if report == nil {
		return logError
	}
	return func(err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[INTERNAL][ERROR] telemetry: the error handler panicked: %v; error: %v", r, err)
			}
		}()
		report(err)
	}
}

func logError(err error) {
	log.Printf("[INTERNAL][ERROR] telemetry: %v", err)
}
