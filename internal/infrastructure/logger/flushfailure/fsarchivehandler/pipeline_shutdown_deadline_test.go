package fsarchivehandler

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFailureArchiveShutdownDeadlineWhileAcceptedUploadIsRunning(t *testing.T) {
	for _, failureCategory := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		t.Run(string(failureCategory), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
				release := make(chan struct{})
				var releaseOnce sync.Once
				upload := newRecordingFailureArchiveUploader(release, nil)
				pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
					cfg.setArchiveUploader(failureCategory, upload)
				})
				// 試験が途中で失敗しても、止めたuploadを再開してからPipelineを閉じます。
				t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
				path := filepath.Join(failureArchiveDirectoryForFailureCategory(pipeline.handler, failureCategory), "test-app_20260908_1.jsonl.gz")
				require.NoError(t, os.WriteFile(path, []byte("retained archive"), 0o600))
				result := failureArchiveResultForFailureCategory(failureCategory, "accepted-before-shutdown")
				handleDone := make(chan error, 1)
				go func() { handleDone <- pipeline.HandleFailure(context.Background(), result) }()
				require.Equal(t, path, receiveArchiveUploadInput(t, upload).FilePath())

				// 通常HandleFailureのtarget-only uploadが継続中でも、Shutdown自身の待機だけは
				// deadlineで返ります。受理済み処理・writer・leaseを取り消してはいけません。
				require.ErrorIs(t, shutdownFailureArchiveWithDeadlineForTest(t, pipeline), context.DeadlineExceeded)
				synctest.Wait()
				require.NotNil(t, pipeline.archiveRootLease)
				require.FileExists(t, path)
				activePath, err := pipeline.ActiveArchivePath(failureCategory)
				require.NoError(t, err)
				records := readArchiveRecords(t, activePath)
				require.Len(t, records, 1)
				require.Equal(t, "accepted-before-shutdown", records[0].Payload["id"])
				require.ErrorIs(t, pipeline.HandleFailure(context.Background(), result), ErrFailureArchiveClosing)
				_, err = pipeline.HandleFailureWithReport(context.Background(), result)
				require.ErrorIs(t, err, ErrFailureArchiveClosing)
				select {
				case err := <-handleDone:
					t.Fatalf("accepted upload was interrupted by Shutdown: %v", err)
				default:
				}

				releaseOnce.Do(func() { close(release) })
				require.NoError(t, <-handleDone)
				require.NoError(t, pipeline.Shutdown(context.Background()))
				require.NoFileExists(t, path)
				require.FileExists(t, activePath)
				require.Nil(t, pipeline.archiveRootLease)
			})
		})
	}
}

func TestFailureArchiveConcurrentShutdownDeadlineWhilePriorAttemptWaitsForTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
		release := make(chan struct{})
		var releaseOnce sync.Once
		upload := newRecordingFailureArchiveUploader(release, nil)
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.enableRotate = true
			cfg.enableFileCompress = true
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, upload)
		})
		// 試験が途中で失敗しても、止めたuploadを再開してからPipelineを閉じます。
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
		// Windowsでもappend用handleの権限内で、実際に追記してrotationを準備します。
		_, err := pipeline.handler.Fsstore.FileRetryable.Write(make([]byte, pipeline.handler.Config.MaxFileSize()+1))
		require.NoError(t, err)
		_, err = pipeline.HandleFailureWithReport(context.Background(), retryableArchiveResult(now, "accepted-before-shutdown"))
		require.NoError(t, err)
		archivePath := receiveArchiveUploadInput(t, upload).FilePath()
		firstDone := make(chan error, 1)
		go func() { firstDone <- pipeline.Shutdown(context.Background()) }()
		synctest.Wait()
		pipeline.handler.archiveTaskMu.Lock()
		taskClosing := pipeline.handler.archiveTaskClosing
		pipeline.handler.archiveTaskMu.Unlock()
		require.True(t, taskClosing, "the first Shutdown must be waiting for the accepted task")

		// 先行Shutdownがtaskを待っている間、後発呼出しだけをdeadlineで終了します。
		// 後発のtimeoutが先行試行やuploadを取り消したり、leaseを早期解放したりしません。
		require.ErrorIs(t, shutdownFailureArchiveWithDeadlineForTest(t, pipeline), context.DeadlineExceeded)
		synctest.Wait()
		require.NotNil(t, pipeline.archiveRootLease)
		require.FileExists(t, archivePath)
		select {
		case err := <-firstDone:
			t.Fatalf("first Shutdown was interrupted by another caller's deadline: %v", err)
		default:
		}
		releaseOnce.Do(func() { close(release) })
		require.NoError(t, <-firstDone)
		require.NoError(t, pipeline.Shutdown(context.Background()))
		require.NoFileExists(t, archivePath)
		require.Nil(t, pipeline.archiveRootLease)
	})
}

func TestFailureArchiveShutdownTimeoutBeforeAcceptedRotationTaskRegistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
		release := make(chan struct{})
		var releaseOnce sync.Once
		upload := newRecordingFailureArchiveUploader(release, nil)
		writeGate := newFailureWriteGateTracer()
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.enableRotate = true
			cfg.enableFileCompress = true
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, upload)
			cfg.tracer = writeGate
		})
		// 試験が途中で失敗しても、止めた書き込みとuploadを再開してからPipelineを閉じます。
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
		t.Cleanup(writeGate.open)
		oldPath := pipeline.handler.Fsstore.ActiveFilePathRetryable
		_, err := pipeline.handler.Fsstore.FileRetryable.Write(make([]byte, pipeline.handler.Config.MaxFileSize()+1))
		require.NoError(t, err)

		// rotationとbackground taskの登録の前で受理済みwriteを止めます。
		// 最初のShutdownがtimeoutしても、このwriteのtask登録は拒否してはいけません。
		handleDone := make(chan error, 1)
		go func() {
			_, err := pipeline.HandleFailureWithReport(context.Background(), retryableArchiveResult(now, "late-task-registration"))
			handleDone <- err
		}()
		select {
		case <-writeGate.entered:
		case err := <-handleDone:
			t.Fatalf("HandleFailureWithReport returned before the accepted write started: %v", err)
		}
		require.ErrorIs(t, shutdownFailureArchiveWithDeadlineForTest(t, pipeline), context.DeadlineExceeded)
		synctest.Wait()
		require.NotNil(t, pipeline.archiveRootLease)
		pipeline.handler.archiveTaskMu.Lock()
		taskClosing := pipeline.handler.archiveTaskClosing
		pipeline.handler.archiveTaskMu.Unlock()
		require.False(t, taskClosing)
		require.ErrorIs(t, pipeline.HandleFailure(context.Background(), retryableArchiveResult(now, "rejected")), ErrFailureArchiveClosing)
		writeGate.open()

		require.Equal(t, oldPath+pkgfsstore.ExtensionCompressGzip, receiveArchiveUploadInput(t, upload).FilePath())
		require.NoError(t, <-handleDone)
		newPath := strings.TrimSuffix(oldPath, "_1.jsonl") + "_2.jsonl"
		records := readArchiveRecords(t, newPath)
		require.Len(t, records, 1)
		require.Equal(t, "late-task-registration", records[0].Payload["id"])
		// 次のShutdownは登録済みtaskを同じように待ち、再びtimeoutしても成果物を保持します。
		require.ErrorIs(t, shutdownFailureArchiveWithDeadlineForTest(t, pipeline), context.DeadlineExceeded)
		synctest.Wait()
		require.FileExists(t, oldPath+pkgfsstore.ExtensionCompressGzip)
		require.NotNil(t, pipeline.archiveRootLease)
		releaseOnce.Do(func() { close(release) })
		require.NoError(t, pipeline.Shutdown(context.Background()))
		require.NoFileExists(t, oldPath+pkgfsstore.ExtensionCompressGzip)
		require.Nil(t, pipeline.archiveRootLease)
	})
}

func TestFailureArchiveCanceledShutdownAfterOperationPanic(t *testing.T) {
	for _, withReport := range []bool{false, true} {
		name := "HandleFailure"
		if withReport {
			name = "HandleFailureWithReport"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = pkgfsstore.FileLifecycleModeDisable
			})
			result := retryableArchiveResult(now, "after-operation-panic")
			handle := func() error {
				if withReport {
					_, err := pipeline.HandleFailureWithReport(context.Background(), result)
					return err
				}
				return pipeline.HandleFailure(context.Background(), result)
			}
			pipeline.handler.appendJSONLWriter = func(io.Writer, ...any) (uint64, error) {
				panic("injected archive write panic")
			}
			require.Panics(t, func() { _ = handle() })
			require.Zero(t, pipeline.operationCount)
			require.Nil(t, pipeline.operationDone)
			pipeline.handler.appendJSONLWriter = nil
			require.NoError(t, handle())
			require.NoError(t, handle())
			require.Zero(t, pipeline.operationCount)
			require.Nil(t, pipeline.operationDone)

			// cancel済みcontextでも新規受付は停止します。受理済みtaskが残る間は
			// leaseを保持し、panicした旧operationの完了待ちには戻りません。
			require.True(t, pipeline.handler.beginArchiveTask())
			taskFinished := false
			defer func() {
				if !taskFinished {
					pipeline.handler.endArchiveTask()
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, pipeline.Shutdown(ctx), context.Canceled)
			require.NotNil(t, pipeline.archiveRootLease)
			require.ErrorIs(t, handle(), ErrFailureArchiveClosing)
			activePath, err := pipeline.ActiveArchivePath(failurearchive.FailureCategoryRetryable)
			require.NoError(t, err)
			require.Len(t, readArchiveRecords(t, activePath), 2)
			pipeline.handler.endArchiveTask()
			taskFinished = true
			require.NoError(t, shutdownFailureArchiveForTest(pipeline))
			require.Nil(t, pipeline.archiveRootLease)
		})
	}
}

func TestFailureArchiveShutdownDeadlineDoesNotCancelRecoveryDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &transientArchiveCheckpointStore{
			recordFailures: 100,
		}
		defer func() {
			store.mu.Lock()
			store.recordFailures = 0
			store.mu.Unlock()
		}()
		remote := newDestinationFailureArchiveUploader("destination")
		pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, remote)
			cfg.embedArchiveCheckpointStore(failurearchive.FailureCategoryRetryable, &store.Store, store)
		})
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		path := filepath.Join(fileName.Dir, "test-app_20260922_1.jsonl.gz")
		require.NoError(t, os.WriteFile(path, []byte("accepted archive"), 0o600))
		handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, failurearchive.FailureCategoryRetryable, fileName)
		require.ErrorIs(t, shutdownFailureArchiveWithDeadlineForTest(t, pipeline), context.DeadlineExceeded)
		require.FileExists(t, path)
		require.NotNil(t, pipeline.archiveRootLease)
		store.mu.Lock()
		store.recordFailures = 0
		store.mu.Unlock()
		pipeline.operationMu.Lock()
		done := pipeline.shutdownDone
		pipeline.operationMu.Unlock()
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			t.Fatal("呼び出し元のtimeout後に終了workerが復旧を完了しなかった")
		}
		require.NoFileExists(t, path)
		require.Nil(t, pipeline.archiveRootLease)
		require.Equal(t, 1, remote.attemptCount(path, "destination"))
		require.NoError(t, pipeline.Shutdown(context.Background()))
	})
}

// shutdownFailureArchiveWithDeadlineForTestは、受理済みの処理が止まっている間に短い期限でShutdownを呼びます。
// synctestのbubble内で呼び、期限は仮想時間で経過します。
func shutdownFailureArchiveWithDeadlineForTest(t *testing.T, pipeline *FailureArchivePipeline) error {
	t.Helper()
	const deadline = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pipeline.Shutdown(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(deadline + time.Nanosecond):
		t.Fatal("Shutdown remained blocked after its context deadline")
		return nil
	}
}
