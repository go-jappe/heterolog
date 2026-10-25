package appendlog

import (
	"context"
	"fmt"
	"time"
)

// LatencyHookは書き込み・圧縮が成功した直後に、経過時間と入力のmetaを受け取ります。
// ログレベルや通知の要否は呼び出し側が判断します。nilなら通知しません。
// 同期的に呼ぶため、外部への通知は呼び出し側でlock解放後などへ遅延してください。
type LatencyHook func(ctx context.Context, elapsed time.Duration, meta any)

// MeasuredOperationErrorは、処理時間を計測した操作の失敗を表します。Operationは
// 失敗した操作名、Elapsedは失敗までの経過時間です。元のerrorはErrに保持し、
// errors.Is／errors.Asで判別できます。
type MeasuredOperationError struct {
	Operation string
	Elapsed   time.Duration
	Err       error
}

func (e *MeasuredOperationError) Error() string {
	return fmt.Sprintf("failed to %s; delay %v; %v", e.Operation, e.Elapsed, e.Err)
}

func (e *MeasuredOperationError) Unwrap() error {
	return e.Err
}

// measureLatencyは処理時間を測り、成功時だけhookへ渡します。
// 失敗時は経過時間を含むMeasuredOperationErrorを返し、遅延通知とは区別します。
func measureLatency(ctx context.Context, operation string, meta any, task func() error, hook LatencyHook) error {
	start := time.Now()
	err := task()
	elapsed := time.Since(start)
	if err != nil {
		return &MeasuredOperationError{Operation: operation, Elapsed: elapsed, Err: err}
	}
	if hook != nil {
		hook(ctx, elapsed, meta)
	}
	return nil
}
