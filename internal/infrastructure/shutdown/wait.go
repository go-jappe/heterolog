// Package shutdownは、受理済み処理の完了を待つ正常終了の進捗を通知します。
package shutdown

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

// ReportIntervalは終了待ちの状態を標準エラー出力へ通知する間隔です。
const ReportInterval = 5 * time.Second

// Waitはdoneが閉じるまで期限を設けず待ち、処理の名前と待機時間を定期的に通知します。
// 終了中のloggerやそのOnErrorへ通知すると、同じ終了処理への再入や通知先の終了に
// 依存してしまうため、ここでは標準エラー出力を使います。待機対象の処理は中止しません。
func Wait(description string, done <-chan struct{}) {
	_ = WaitContext(context.Background(), description, done)
}

// WaitContextは呼び出し元が明示した中止で待機だけを終了できます。処理本体は中止しません。
// 正常終了の呼び出し元は期限のないContextを渡します。内部では期限を追加しません。
func WaitContext(ctx context.Context, description string, done <-chan struct{}) error {
	return wait(ctx, description, done, func(description string, elapsed time.Duration) {
		_, _ = fmt.Fprintf(os.Stderr, "waiting for graceful shutdown: %s (elapsed=%s)\n", description, elapsed.Round(time.Second))
	})
}

func wait(ctx context.Context, description string, done <-chan struct{}, report func(string, time.Duration)) error {
	if done == nil {
		return nil
	}
	ticker := time.NewTicker(ReportInterval)
	defer ticker.Stop()
	return waitTicks(ctx, description, done, ticker.C, report)
}

// waitTicksはticksを受け取るたびに待機状況を通知します。selectは準備済みのcaseを
// 無作為に選ぶため、完了と通知の契機が同時に届いた場合は完了を優先して通知しません。
func waitTicks(ctx context.Context, description string, done <-chan struct{}, ticks <-chan time.Time, report func(string, time.Duration)) error {
	started := time.Now()
	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticks:
			select {
			case <-done:
				return nil
			default:
			}
			report(description, time.Since(started))
		}
	}
}

// WaitGroupは新規登録を止めたgroupの完了を待ちます。待機用goroutineも完了時に終了します。
func WaitGroup(description string, group *sync.WaitGroup) {
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	Wait(description, done)
}
