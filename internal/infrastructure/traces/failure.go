package traces

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
)

// Dispositionは、送れなかったspanをfailure storeでどう扱うかです。
type Disposition int

const (
	// DispositionRetryは、後で送り直します。受信側が受け付けたか分からない失敗も、
	// 欠落より重複を選んで送り直します。
	DispositionRetry Disposition = iota
	// DispositionDeadは、受信側が拒否したため、送り直さずに残します。
	DispositionDead
	// DispositionQuarantineは、一部だけ受け付けられ、どのspanか分からないため、
	// 送り直さずに残します。
	DispositionQuarantine
)

// Outcomeは、failure storeへ保存したspanを数えるoutcome属性の値です。
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

// SpanFailureは、batchのうちStartからEndの手前までのspanを送れなかったことを表します。
type SpanFailure struct {
	Start, End  int
	Disposition Disposition
	// Rejectedは、部分的な成功の応答で受信側が受け付けなかったspanの数です。0なら、
	// 範囲のすべてのspanが届いていません。
	Rejected int
	// StatusCodeは、受信側が返したHTTP statusです。応答がなければ0です。
	StatusCode int
	// Reasonは、失敗の種類（http_status、net_timeoutなど）です。
	Reason string
	// RetryAfterは、受信側がRetry-Afterで求めた、次に送るまでの待ち時間です。
	RetryAfter time.Duration
	Err        error
}

// Failedは、範囲のうち届かなかったspanの数です。
func (f SpanFailure) Failed() int {
	if f.Rejected > 0 {
		return min(f.Rejected, f.End-f.Start)
	}
	return f.End - f.Start
}

// ExportErrorは、batchの一部またはすべてのspanを送れなかったことを、範囲ごとに表します。
// 範囲は重ならず、Startの順に並びます。
type ExportError struct {
	Failures []SpanFailure
}

// Failedは、batchのうち届かなかったspanの数です。
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

// exportFailuresは、Exportが返したerrorを範囲ごとの失敗にします。ExportErrorでない
// errorは、batchのすべてのspanを、結果の分からない失敗として送り直す扱いにします。
func exportFailures(err error, n int) []SpanFailure {
	var exportErr *ExportError
	if errors.As(err, &exportErr) {
		return exportErr.Failures
	}
	return []SpanFailure{{Start: 0, End: n, Disposition: DispositionRetry, Err: err}}
}

// FailureStoreは、送れなかったspanを、後で送り直すか確かめられるよう保存します。
type FailureStore interface {
	// Storeは、batchのうちfailuresの範囲のspanを保存します。保存できなかった場合は
	// errorを返します。batchは、Storeが返った後に再利用されるため、保持しません。
	Store(ctx context.Context, batch []tracing.SpanData, failures []SpanFailure) error
	// Shutdownは、保存と送り直しを止め、資源を手放します。最後のStoreの後に1回だけ
	// 呼ばれます。
	Shutdown(ctx context.Context) error
}
