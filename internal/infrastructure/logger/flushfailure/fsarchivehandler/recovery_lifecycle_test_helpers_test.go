package fsarchivehandler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
)

// 受理済み操作の完了後にも追加の試験操作を行うため、終了済みqueueだけを置き換えます。
// Shutdownと同じく、古い世代を待つ世代もqueueへ移してから閉じます。
// 置き換え後のqueueは既定の再開間隔を使います。
func drainArchiveRecoveryForTest(t *testing.T, handler *FsArchiveHandler) {
	t.Helper()
	handler.closeArchiveGenerationOrderWaits()
	require.NoError(t, handler.logFileProcessingRecovery.Close())
	waitArchiveRecoveryNotificationsForTest(t, handler)
	handler.logFileProcessingRecovery = &logfile.LogFileProcessingRecovery[archiveLogFileProcessingRecoveryKey]{}
	reopenArchiveGenerationOrderWaitsForTest(handler)
}

// Store・uploaderだけを差し替える既存の再起動fixtureで、以前の再試行を明示的に止めます。
func restartArchiveRecoveryForTest(t *testing.T, handler *FsArchiveHandler) {
	t.Helper()
	require.ErrorIs(t, handler.logFileProcessingRecovery.ForceClose(), shutdown.ErrForcedClose)
	// 強制終了したqueueは受け付けないため、順序待ちの世代も保護を解除して破棄します。
	handler.closeArchiveGenerationOrderWaits()
	require.ErrorIs(t, handler.logFileProcessingRecovery.Close(), shutdown.ErrForcedClose)
	waitArchiveRecoveryNotificationsForTest(t, handler)
	handler.logFileProcessingRecovery = &logfile.LogFileProcessingRecovery[archiveLogFileProcessingRecoveryKey]{}
	reopenArchiveGenerationOrderWaitsForTest(handler)
}

// waitArchiveRecoveryNotificationsForTestは、閉じた再開処理の通知の完了を、停止の検出だけに使う
// 期限まで待ちます。
func waitArchiveRecoveryNotificationsForTest(t *testing.T, handler *FsArchiveHandler) {
	t.Helper()
	select {
	case <-handler.logFileProcessingRecovery.NotificationsDone():
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("log file processing recovery notifications did not finish")
	}
}

// reopenArchiveGenerationOrderWaitsForTestは、置き換えたqueueに合わせて順序待ちの受付を再開します。
func reopenArchiveGenerationOrderWaitsForTest(handler *FsArchiveHandler) {
	waits := &handler.generationOrderWaits
	waits.mu.Lock()
	defer waits.mu.Unlock()
	waits.closed = false
}

// setArchiveLogFileProcessingRecoveryIntervalForTestは、実HTTP serverを使うため仮想時間で
// 進められない試験向けに、未使用の再開処理を短い再開間隔のものへ置き換えます。
// 構築直後、受理済み処理の登録前に呼んでください。
func setArchiveLogFileProcessingRecoveryIntervalForTest(t *testing.T, handler *FsArchiveHandler, interval time.Duration) {
	t.Helper()
	require.NoError(t, handler.logFileProcessingRecovery.Close())
	recovery, err := logfile.NewLogFileProcessingRecovery[archiveLogFileProcessingRecoveryKey](interval)
	require.NoError(t, err)
	handler.logFileProcessingRecovery = recovery
	waits := &handler.generationOrderWaits
	waits.mu.Lock()
	waits.fallbackInterval = interval
	waits.mu.Unlock()
}
