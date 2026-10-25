package compressedlog

import (
	"context"
	"fmt"
	"time"
)

// runReplayAndPurgeLoopは再送とpurgeを同じgoroutineで順に実行します。
// 再送は初回も指定時間だけ待ち、以後は処理・結果通知の完了後から次の待機を始めます。
// purgeの周期は独立していますが、再送中に期限が来た場合は再送完了まで待ちます。
// 各callbackは結果通知とエラー処理も担当します。runPurgeがnilならpurgeは行いません。
func runReplayAndPurgeLoop(
	ctx context.Context,
	replayInterval time.Duration,
	runReplay func(),
	purgeInterval time.Duration,
	runPurge func(),
) error {
	if replayInterval <= 0 {
		return ErrInvalidRunnerInterval
	}
	if runPurge != nil && purgeInterval <= 0 {
		return fmt.Errorf("%w: lifecycle interval must be positive", ErrInvalidCompressedLogFileUploadSpoolConfig)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	replayTimer := time.NewTimer(replayInterval)
	defer replayTimer.Stop()
	var purgeTicks <-chan time.Time
	if runPurge != nil {
		purgeTicker := time.NewTicker(purgeInterval)
		defer purgeTicker.Stop()
		purgeTicks = purgeTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-replayTimer.C:
			if err := ctx.Err(); err != nil {
				return err
			}
			runReplay()
			replayTimer.Reset(replayInterval)
		case <-purgeTicks:
			if err := ctx.Err(); err != nil {
				return err
			}
			runPurge()
		}
	}
}
