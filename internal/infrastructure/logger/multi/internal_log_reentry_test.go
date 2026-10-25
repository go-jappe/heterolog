package multi

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler/reentrytest"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

// TestAsyncBestEffortMultiLoggerRunsReentrantWriteInlineWhenSlotsAreExhaustedは、childが内部ログを
// 通知している間（実行枠を保持したまま）に、その通知の中から同じasync loggerへ書き込んでも、空きを
// 待たず現在のgoroutineで実行し、停止しないことを確認します。空きを待つと、通知元のchildが
// 実行枠を返さないため永久に停止します。完了はhang検出の上限付きで待ちます。
func TestAsyncBestEffortMultiLoggerRunsReentrantWriteInlineWhenSlotsAreExhausted(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			var async *AsyncBestEffortMultiLogger
			// childの内部ログ通知を、file loggerなどと同じReentryGuardのDispatchで再現します。
			var notifier handler.ReentryGuard
			nestedHandled := make(chan struct{}, 1)
			child := &syncHandlerStub{handleFn: func(ctx context.Context, entry *infralogging.LogEntry) error {
				if entry.Message == "write from the internal log handler" {
					nestedHandled <- struct{}{}
					return nil
				}
				notifier.Dispatch(ctx, func(ctx context.Context) {
					mode.Write(t, ctx, func(ctx context.Context) {
						async.Error(ctx, "write from the internal log handler")
					})
				})
				return nil
			}}
			var err error
			async, err = NewAsyncBestEffortMultiLogger(
				asyncBestEffortConfigStub{timeout: time.Minute, maxParallel: 1},
				child,
			)
			if err != nil {
				t.Fatal(err)
			}

			async.Info(context.Background(), "original write")
			select {
			case <-nestedHandled:
			case <-time.After(reentrytest.HangTimeout):
				// 停止したchildが実行枠とCloseの待機対象を保持したままのため、Closeは呼びません。
				t.Fatalf("a write from the internal log handler was not handled within %s: it waited for an execution slot held by its own notifier", reentrytest.HangTimeout)
			}
			if _, finished := reentrytest.CompleteWithin(t, "Close", async.Close); !finished {
				t.FailNow()
			}
			if got := child.handleCalls.Load(); got != 2 {
				t.Fatalf("child calls = %d, want the original and the nested write", got)
			}
		})
	}
}
