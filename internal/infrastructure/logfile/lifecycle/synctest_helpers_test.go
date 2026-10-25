package lifecycle

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
)

// synctestFromWallClockは、bubbleの仮想時計を開始時点の実時刻まで進めてからfを実行します。
// 試験中に作るfileの更新時刻とmanagerの時計を揃え、再試行の待機だけを仮想時間で進めます。
func synctestFromWallClock(t *testing.T, f func(*testing.T)) {
	t.Helper()
	start := time.Now()
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(start.Sub(time.Now()))
		f(t)
	})
}

// requireRecoveryClosesForTestは、受理済みの再開処理の完了とCloseの戻りを、bubble時間の期限付きで
// 待ちます。期限内に戻らなければForceCloseで止めて失敗させ、再試行のtimerで仮想時間が進み続けて
// 試験が終わらなくなることを防ぎます。synctestのbubble内で使います。
func requireRecoveryClosesForTest(t *testing.T, manager *FileSeriesManager) {
	t.Helper()
	require.NoError(t, boundedwait.CloseInBubble(t, "manager.CloseLogFileProcessingRecovery()",
		manager.CloseLogFileProcessingRecovery, manager.ForceClose))
}

// forceCloseManagerOnFailureは、bubble内の試験が途中で失敗した場合に、受理済みの再試行を中止してから
// managerをbubble時間の期限付きで閉じます。正常に終了した試験の終了処理は変えません。
func forceCloseManagerOnFailure(t *testing.T, manager *FileSeriesManager) {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			_ = manager.ForceClose()
			_ = boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose)
		}
	})
}

// closeInBackgroundForTestは、closeFuncを別goroutineで始め、戻り値を受け取るchannelを返します。
// 試験が途中で失敗しても、cleanupでforceによる中止を通知してからcloseFuncの戻りを
// bubble時間の期限付きで待ち、Closeのgoroutineをbubbleに残しません。
func closeInBackgroundForTest(t *testing.T, description string, closeFunc, force func() error) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		result <- closeFunc()
		close(finished)
	}()
	t.Cleanup(func() {
		select {
		case <-finished:
			return
		default:
		}
		if force != nil {
			_ = force()
		}
		boundedwait.ReceiveInBubble(t, finished, description+" after ForceClose")
	})
	return result
}

// forceCloseLifecycleOnFailureは、bubble内の試験が途中で失敗した場合に、受理済みの処理を中止してから
// lifecycleの資源をbubble時間の期限付きで解放します。正常に終了した試験の終了処理は変えません。
func forceCloseLifecycleOnFailure(t *testing.T, lifecycle *DirectoryLifecycle) {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			_ = lifecycle.ForceClose()
			_ = boundedwait.CloseInBubble(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()",
				lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, lifecycle.ForceClose)
		}
	})
}
