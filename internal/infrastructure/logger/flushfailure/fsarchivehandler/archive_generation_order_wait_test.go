package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// 古い世代の送信中に新しい世代は送信せず、古い世代の完了記録が進んだ時点で
// 再開間隔を待たずに送信します。終了処理も再開間隔を待ちません。
func TestFailureArchiveDeferredGenerationResumesWhenOlderGenerationCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)
		category := failurearchive.FailureCategoryRetryable
		remote := newGatedFailureArchiveUploader()
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
		})
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		older := filepath.Join(fileName.Dir, "test-app_20260827_1.jsonl.gz")
		newer := filepath.Join(fileName.Dir, "test-app_20260828_1.jsonl.gz")
		for _, path := range []string{older, newer} {
			require.NoError(t, os.WriteFile(path, []byte("compressed generation"), 0o600))
		}
		releaseOlder := remote.gate(t, older)

		olderDone := make(chan archiveFileOperationResult, 1)
		go func() {
			olderDone <- handler.uploadCheckpointedArchiveGroup(ctx, []string{older}, category, fileName)
		}()
		require.Equal(t, older, remote.receiveStarted(t))

		// 古い世代の送信中は、新しい世代を送信せず、保護したまま順序待ちとして保持します。
		outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{newer}, category, fileName)
		require.True(t, outcome.retryRequired)
		require.Zero(t, remote.attemptCount(newer))
		require.FileExists(t, newer)
		newerKey := failureArchiveCompressedLogCheckpointKey(t, handler, category, newer)
		require.True(t, handler.archiveGenerationContinuationAccepted(category, newerKey))
		assertArchivePathProtected(t, handler.retryableDirectory.guard, newer, true)

		waitStarted := time.Now()
		releaseOlder(nil)
		require.False(t, receiveArchiveOperationResultForTest(t, olderDone).retryRequired)
		synctest.Wait()
		require.NoFileExists(t, older)
		require.Equal(t, 1, remote.attemptCount(newer))
		require.NoFileExists(t, newer)
		require.Zero(t, time.Since(waitStarted), "the deferred generation must not wait for the retry interval")
		require.False(t, handler.archiveGenerationContinuationAccepted(category, newerKey))
		assertArchivePathProtected(t, handler.retryableDirectory.guard, newer, false)

		require.NoError(t, shutdownFailureArchiveForTest(pipeline))
		require.Zero(t, time.Since(waitStarted), "Shutdown must not wait for a retry interval after the generations completed")
	})
}

// 新しい世代の試行が古い世代を未完了と判断した後、順序待ちへ移る前に古い世代の完了記録が
// 進んだ場合も、その完了の通知を取りこぼさず、再開間隔を待たずに再試行して送信します。
func TestFailureArchiveDeferredGenerationRetriesWhenOlderCompletesDuringItsAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 8, 29, 13, 0, 0, 0, time.UTC)
		category := failurearchive.FailureCategoryRetryable
		remote := newGatedFailureArchiveUploader()
		store := newStaleLoadArchiveCheckpointStore()
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
			cfg.embedArchiveCheckpointStore(category, &store.Store, store)
		})
		// 試験が途中で失敗しても、止めた読み取りを再開してからPipelineを閉じます。
		t.Cleanup(store.releaseLoad)
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		older := filepath.Join(fileName.Dir, "test-app_20260827_1.jsonl.gz")
		newer := filepath.Join(fileName.Dir, "test-app_20260828_1.jsonl.gz")
		for _, path := range []string{older, newer} {
			require.NoError(t, os.WriteFile(path, []byte("compressed generation"), 0o600))
		}
		releaseOlder := remote.gate(t, older)
		olderDone := make(chan archiveFileOperationResult, 1)
		go func() {
			olderDone <- handler.uploadCheckpointedArchiveGroup(ctx, []string{older}, category, fileName)
		}()
		require.Equal(t, older, remote.receiveStarted(t))

		// 新しい世代の順序確認が古い世代の未完了の状態を読んだところで止めます。
		store.armStaleLoad(failureArchiveCompressedLogCheckpointKey(t, handler, category, older))
		newerDone := make(chan archiveFileOperationResult, 1)
		go func() {
			newerDone <- handler.uploadCheckpointedArchiveGroup(ctx, []string{newer}, category, fileName)
		}()
		select {
		case <-store.loaded:
		case outcome := <-newerDone:
			t.Fatalf("newer generation finished without checking the older generation: %+v", outcome)
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("newer generation did not check the older generation")
		}

		// その間に古い世代の送信を終え、完了記録を進めます。
		before := handler.archiveGenerationOrderProgress(category)
		releaseOlder(nil)
		synctest.Wait()
		require.Greater(t, handler.archiveGenerationOrderProgress(category), before,
			"the older generation's completion must be recorded while the newer attempt is in flight")

		started := time.Now()
		store.releaseLoad()
		synctest.Wait()
		require.Equal(t, 1, remote.attemptCount(newer), "a completion recorded during the attempt must not be missed")
		require.Zero(t, time.Since(started), "the deferred generation must not wait for the fallback interval")
		require.False(t, receiveArchiveOperationResultForTest(t, olderDone).retryRequired)
		require.False(t, receiveArchiveOperationResultForTest(t, newerDone).retryRequired)
		require.NoFileExists(t, older)
		require.NoFileExists(t, newer)
		require.NoError(t, shutdownFailureArchiveForTest(pipeline))
	})
}

// 古い世代が完了記録を伴わずに取り除かれた場合は完了の通知がないため、
// 待機中の世代は遅くとも予備の再開間隔の経過までに再試行します。
func TestFailureArchiveDeferredGenerationRetriesAfterFallbackInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 8, 29, 10, 30, 0, 0, time.UTC)
		category := failurearchive.FailureCategoryRetryable
		remote := newGatedFailureArchiveUploader()
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
		})
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		// 未送信の古いrawが残る間は、新しい世代を送信できません。
		olderRaw := filepath.Join(fileName.Dir, "test-app_20260827_1.jsonl")
		newer := filepath.Join(fileName.Dir, "test-app_20260828_1.jsonl.gz")
		require.NoError(t, os.WriteFile(olderRaw, []byte("raw generation"), 0o600))
		require.NoError(t, os.WriteFile(newer, []byte("compressed generation"), 0o600))

		outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{newer}, category, fileName)
		require.True(t, outcome.retryRequired)
		require.Zero(t, remote.attemptCount(newer))

		require.NoError(t, os.Remove(olderRaw))
		// 再試行の時刻は予備の間隔より早くてもかまいません。間隔の経過までに送信を終えることだけを確認します。
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		require.Equal(t, 1, remote.attemptCount(newer), "the fallback retry must send the deferred generation")
		require.NoFileExists(t, newer)
		require.Zero(t, remote.attemptCount(olderRaw))
	})
}

// 古い世代の送信が失敗して受理済みの再開処理で待つ間、新しい世代は順序待ちに保持します。
// 終了処理では古い順に再開処理へ移すため、古い世代の再送に続けて同じ回で新しい世代を送信し、
// 世代数に比例した再開間隔を待ちません。
func TestFailureArchiveShutdownRetriesDeferredGenerationsInGenerationOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 8, 29, 11, 0, 0, 0, time.UTC)
		category := failurearchive.FailureCategoryRetryable
		remote := newGatedFailureArchiveUploader()
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
		})
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		older := filepath.Join(fileName.Dir, "test-app_20260826_1.jsonl.gz")
		newer := []string{
			filepath.Join(fileName.Dir, "test-app_20260827_1.jsonl.gz"),
			filepath.Join(fileName.Dir, "test-app_20260828_1.jsonl.gz"),
		}
		for _, path := range append([]string{older}, newer...) {
			require.NoError(t, os.WriteFile(path, []byte("compressed generation"), 0o600))
		}
		releaseOlder := remote.gate(t, older)

		olderDone := make(chan archiveFileOperationResult, 1)
		go func() {
			olderDone <- handler.uploadCheckpointedArchiveGroup(ctx, []string{older}, category, fileName)
		}()
		require.Equal(t, older, remote.receiveStarted(t))
		// 新しい世代ほど先に順序待ちへ入れ、登録順と世代順を逆にします。
		for index := len(newer) - 1; index >= 0; index-- {
			require.True(t, handler.uploadCheckpointedArchiveGroup(ctx, []string{newer[index]}, category, fileName).retryRequired)
		}
		releaseOlder(errors.New("object storage unavailable"))
		require.True(t, receiveArchiveOperationResultForTest(t, olderDone).retryRequired)

		// 古い世代の再送がいつ行われるかは固定しません。新しい世代が同じ回に続き、
		// 世代ごとに再開間隔を待たないことだけを上限で確認します。
		shutdownStarted := time.Now()
		require.NoError(t, shutdownFailureArchiveForTest(pipeline))
		elapsed := time.Since(shutdownStarted)
		require.Less(t, elapsed, 2*logfile.DefaultLogFileProcessingRecoveryInterval,
			"the deferred generations must follow the older retry in the same recovery round")
		require.Equal(t, 2, remote.attemptCount(older))
		for _, path := range append([]string{older}, newer...) {
			require.NoFileExists(t, path)
		}
		for _, path := range newer {
			require.Equal(t, 1, remote.attemptCount(path), path)
		}
	})
}

// 受理済みの再開処理による再試行が古い世代の未完了で止まった場合も、順序待ちへ移して
// 古い世代の完了記録が進んだ時点で再開間隔を待たずに送信し、終了処理も待たせません。
func TestFailureArchiveRecoveryRetryWaitsForOlderGenerationAndResumesOnCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
		category := failurearchive.FailureCategoryRetryable
		remote := newGatedFailureArchiveUploader()
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
		})
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		older := filepath.Join(fileName.Dir, "test-app_20260827_1.jsonl.gz")
		newer := filepath.Join(fileName.Dir, "test-app_20260828_1.jsonl.gz")
		require.NoError(t, os.WriteFile(newer, []byte("compressed generation"), 0o600))
		// 新しい世代の初回送信を失敗させ、受理済みの再開処理へ登録します。
		remote.gate(t, newer)(errors.New("object storage unavailable"))
		require.True(t, handler.uploadCheckpointedArchiveGroup(ctx, []string{newer}, category, fileName).retryRequired)
		newerKey := failureArchiveCompressedLogCheckpointKey(t, handler, category, newer)
		require.True(t, handler.logFileProcessingRecovery.Has(failureArchiveRecoveryKey(category, newerKey)))

		// 再試行までに未完了の古い世代が現れると、再試行は送信せずに順序待ちへ移ります。
		require.NoError(t, os.WriteFile(older, []byte("compressed generation"), 0o600))
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		require.Equal(t, 1, remote.attemptCount(newer))
		require.False(t, handler.logFileProcessingRecovery.Has(failureArchiveRecoveryKey(category, newerKey)))
		require.True(t, handler.archiveGenerationContinuationAccepted(category, newerKey))
		assertArchivePathProtected(t, handler.retryableDirectory.guard, newer, true)

		completed := time.Now()
		require.False(t, handler.uploadCheckpointedArchiveGroup(ctx, []string{older}, category, fileName).retryRequired)
		synctest.Wait()
		require.Equal(t, 2, remote.attemptCount(newer))
		require.NoFileExists(t, older)
		require.NoFileExists(t, newer)
		require.Zero(t, time.Since(completed), "the deferred retry must not wait for the retry interval")

		require.NoError(t, shutdownFailureArchiveForTest(pipeline))
		require.Zero(t, time.Since(completed))
	})
}

// 受理済みの再開処理から順序待ちへ移した直後は、元の登録が同じkeyで残っています。その間に古い世代が
// 完了しても順序待ちが所有と保護を保ち、元の登録の解除後に再開間隔を待たずに送信します。
func TestFailureArchiveGenerationOrderHandoffResumesAfterRecoveryRegistrationIsReleased(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 29, 11, 30, 0, 0, time.UTC)
		category := failurearchive.FailureCategoryRetryable
		remote := newGatedFailureArchiveUploader()
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
		})
		handler := pipeline.handler
		path := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260828_1.jsonl.gz")
		require.NoError(t, os.WriteFile(path, []byte("compressed generation"), 0o600))
		// 予備の再開間隔による再試行と区別できるよう、予備の間隔を試験の時間より長くします。
		waits := &handler.generationOrderWaits
		waits.mu.Lock()
		waits.fallbackInterval = time.Hour
		waits.mu.Unlock()
		handoff := handOffArchiveGenerationOrderForTest(t, handler, category, path)

		// 古い世代の完了記録が進んでも、同じkeyで再登録できない間は順序待ちが保持します。
		handler.resumeArchiveGenerationOrderWaits(category)
		synctest.Wait()
		require.True(t, handler.logFileProcessingRecovery.Has(handoff.key), "the original registration must still be held")
		require.True(t, handler.archiveGenerationContinuationAccepted(category, handoff.operation.key))
		require.Zero(t, remote.attemptCount(path))
		assertArchivePathProtected(t, handler.retryableDirectory.guard, path, true)

		// 元の登録の解除後、要求済みの再開を直ちに行います。
		released := time.Now()
		handoff.releaseRegistration()
		synctest.Wait()
		require.Equal(t, 1, remote.attemptCount(path))
		require.NoFileExists(t, path)
		require.Zero(t, time.Since(released))
		require.False(t, handler.archiveGenerationContinuationAccepted(category, handoff.operation.key))
		require.False(t, handler.logFileProcessingRecovery.Has(handoff.key))
	})
}

// 終了処理で順序待ちを閉じるとき、引き渡し中の世代は元の登録の解除を待ってから
// 受理済みの再開処理へ移します。再開処理を閉じる前に移すため、終了処理の中で送信します。
func TestFailureArchiveGenerationOrderCloseWaitsForPendingHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 29, 11, 45, 0, 0, time.UTC)
		category := failurearchive.FailureCategoryRetryable
		remote := newGatedFailureArchiveUploader()
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
		})
		handler := pipeline.handler
		path := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260828_1.jsonl.gz")
		require.NoError(t, os.WriteFile(path, []byte("compressed generation"), 0o600))
		handoff := handOffArchiveGenerationOrderForTest(t, handler, category, path)

		// 終了処理と同じく、順序待ちを閉じた直後に同じgoroutineで受理済みの再開処理を閉じます。
		recovery := handler.logFileProcessingRecovery
		closed := make(chan struct{})
		recoveryClosed := make(chan error, 1)
		go func() {
			handler.closeArchiveGenerationOrderWaits()
			close(closed)
			recoveryClosed <- recovery.Close()
		}()
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("closing the generation order waits did not wait for the pending handoff")
		default:
		}
		require.True(t, handler.archiveGenerationContinuationAccepted(category, handoff.operation.key))

		// 引き渡しの完了を通知する時点で、世代を受理済みの再開処理へ戻し終えていることを記録します。
		// 先に通知すると、待っていた終了処理が直後に再開処理を閉じ、戻す登録が拒否されて世代を
		// 送信しません。この順序は実行のたびに偶然に左右されず、通知の時点の登録で確かめます。
		registeredAtNotify := make(chan bool, 1)
		waits := &handler.generationOrderWaits
		waits.mu.Lock()
		waits.handoffNotifyHookForTest = func(key archiveLogFileProcessingRecoveryKey) {
			if key != handoff.key {
				return
			}
			select {
			case registeredAtNotify <- handler.logFileProcessingRecovery.Has(key):
			default:
			}
		}
		waits.mu.Unlock()

		// 元の登録を解除すると、引き渡しを完了して再開処理へ戻してから順序待ちの終了が戻ります。
		// 直後に再開処理を閉じても、戻した世代は送信してから終了します。
		handoff.releaseRegistration()
		select {
		case registered := <-registeredAtNotify:
			require.True(t, registered,
				"the handed-off generation must be registered with the accepted recovery before the handoff is notified")
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("the handoff was not notified after the original registration was released")
		}
		select {
		case err := <-recoveryClosed:
			require.NoError(t, err)
		case <-time.After(failureArchiveHangTimeout):
			_ = recovery.ForceClose()
			t.Fatal("the accepted recovery did not close after the handoff completed")
		}
		require.Equal(t, 1, remote.attemptCount(path), "the handed-off generation must be sent before the recovery closes")
		require.NoFileExists(t, path)
	})
}

// receiveArchiveOperationResultForTestは、別goroutineで実行した一世代の処理の結果を、停止の検出
// だけに使う期限まで待って返します。
func receiveArchiveOperationResultForTest(t *testing.T, done <-chan archiveFileOperationResult) archiveFileOperationResult {
	t.Helper()
	select {
	case outcome := <-done:
		return outcome
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("failure archive operation did not finish")
		return archiveFileOperationResult{}
	}
}

// archiveGenerationOrderHandoffForTestは、受理済みの再開処理の試行が世代を順序待ちへ移し、
// 元の登録の解除を待っている状態です。releaseRegistrationで解除を再開します。
type archiveGenerationOrderHandoffForTest struct {
	operation           *archiveFileOperation
	key                 archiveLogFileProcessingRecoveryKey
	releaseRegistration func()
}

// handOffArchiveGenerationOrderForTestは、pathの世代を受理済みの再開処理から順序待ちへ移した直後の
// 状態を作ります。本番の再開処理と同じく、登録した試行の中で順序待ちへ移して完了し、
// 元の登録の解除（保護の解除）で止めます。この間は同じkeyの登録が残ります。解除を再開すると、
// 本番と同じく登録の解除後の通知で引き渡しを完了します。試験が途中で失敗しても解除を再開し、
// 引き渡しを完了してからPipelineを閉じます。
func handOffArchiveGenerationOrderForTest(
	t *testing.T,
	handler *FsArchiveHandler,
	category failurearchive.FailureCategory,
	path string,
) archiveGenerationOrderHandoffForTest {
	t.Helper()
	destinationUploader, ok := handler.archiveUploader(category).(uploader.DestinationUploader)
	require.True(t, ok)
	fileName, err := handler.archiveFileNamePattern(category)
	require.NoError(t, err)
	operation := &archiveFileOperation{
		store:    handler.compressedArchiveUploadCheckpointStore(category),
		key:      failureArchiveCompressedLogCheckpointKey(t, handler, category, path),
		path:     path,
		category: category,
		fileName: fileName,
		uploader: destinationUploader,
	}
	key := failureArchiveRecoveryKey(category, operation.key)
	handedOff := make(chan archiveGenerationOrderDecision, 1)
	releasing := make(chan struct{})
	resume := make(chan struct{})
	var releasingOnce, resumeOnce sync.Once
	releaseRegistration := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(releaseRegistration)
	require.True(t, handler.logFileProcessingRecovery.Schedule(
		key,
		nil,
		func(context.Context) (bool, error) {
			handedOff <- handler.awaitArchiveGenerationOrder(operation, handler.archiveGenerationOrderProgress(category), true)
			return true, nil
		},
		func(error) func() {
			handler.completeArchiveGenerationOrderHandoff(key)
			return nil
		},
		func() {
			releasingOnce.Do(func() { close(releasing) })
			<-resume
		},
	))
	select {
	case decision := <-handedOff:
		require.Equal(t, archiveGenerationOrderWaiting, decision)
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("the accepted recovery attempt did not hand the generation off to the order wait")
	}
	select {
	case <-releasing:
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("the accepted recovery did not release the original registration after the handoff")
	}
	return archiveGenerationOrderHandoffForTest{operation: operation, key: key, releaseRegistration: releaseRegistration}
}

// gatedFailureArchiveUploaderは、指定したpathの初回送信をgateが開くまで止め、
// gateから受け取ったerrorを返します。その他の送信は成功します。
type gatedFailureArchiveUploader struct {
	mu       sync.Mutex
	attempts map[string]int
	gates    map[string]chan error
	started  chan string
}

func newGatedFailureArchiveUploader() *gatedFailureArchiveUploader {
	return &gatedFailureArchiveUploader{
		attempts: make(map[string]int),
		gates:    make(map[string]chan error),
		started:  make(chan string, 16),
	}
}

// gateはpathの初回送信を止め、送信を終える関数を返します。試験の失敗時もcleanupで送信を終えます。
func (u *gatedFailureArchiveUploader) gate(t *testing.T, path string) func(error) {
	t.Helper()
	gate := make(chan error, 1)
	u.mu.Lock()
	u.gates[path] = gate
	u.mu.Unlock()
	var once sync.Once
	release := func(err error) {
		once.Do(func() { gate <- err })
	}
	t.Cleanup(func() { release(nil) })
	return release
}

func (u *gatedFailureArchiveUploader) Upload(ctx context.Context, request uploader.UploadRequest) error {
	path := request.FilePath()
	u.mu.Lock()
	u.attempts[path]++
	attempt := u.attempts[path]
	gate := u.gates[path]
	u.mu.Unlock()
	select {
	case u.started <- path:
	default:
	}
	if gate == nil || attempt != 1 {
		return nil
	}
	select {
	case err := <-gate:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (u *gatedFailureArchiveUploader) attemptCount(path string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.attempts[path]
}

func (u *gatedFailureArchiveUploader) receiveStarted(t *testing.T) string {
	t.Helper()
	select {
	case path := <-u.started:
		return path
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("timed out waiting for archive upload to start")
		return ""
	}
}

// staleLoadArchiveCheckpointStoreは、armStaleLoadで指定した世代の次の一回のLoadで、
// 読み取った状態を返す前にreleaseLoadまで止めます。順序確認が古い状態を読んだ後に
// 古い世代の完了記録を進め、その古い状態で判断させるために使います。
type staleLoadArchiveCheckpointStore struct {
	checkpointstore.Store
	mu          sync.Mutex
	armed       *checkpointstore.CheckpointKey
	loaded      chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func newStaleLoadArchiveCheckpointStore() *staleLoadArchiveCheckpointStore {
	return &staleLoadArchiveCheckpointStore{loaded: make(chan struct{}), release: make(chan struct{})}
}

func (s *staleLoadArchiveCheckpointStore) armStaleLoad(key checkpointstore.CheckpointKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = &key
}

// releaseLoadは止めたLoadを再開させます。複数回呼べます。
func (s *staleLoadArchiveCheckpointStore) releaseLoad() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func (s *staleLoadArchiveCheckpointStore) Load(
	ctx context.Context,
	key checkpointstore.CheckpointKey,
) (checkpointstore.Checkpoint, error) {
	s.mu.Lock()
	stale := s.armed != nil && s.armed.Generation == key.Generation
	if stale {
		s.armed = nil
	}
	s.mu.Unlock()
	checkpoint, err := s.Store.Load(ctx, key)
	if stale {
		close(s.loaded)
		<-s.release
	}
	return checkpoint, err
}
