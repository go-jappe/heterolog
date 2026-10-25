package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestInlineCloseWithoutCheckpointRetriesOnlyFailedDestination(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		logger := newInlineCompressionCheckpointTestLogger(t, time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC), "accepted-no-checkpoint")
		require.Nil(t, logger.archiveUploadCheckpoints)
		primary := &sequenceArchiveUploader{}
		secondary := &sequenceArchiveUploader{errs: []error{errors.New("secondary unavailable")}}
		logger.ArchiveUploader = newMultiArchiveUploader(t,
			uploader.Destination{ID: "primary", Uploader: primary},
			uploader.Destination{ID: "secondary", Uploader: secondary},
		)
		source := filepath.Join(logger.Fsstore.FileName.Dir, "accepted_20260923_1.log")
		require.NoError(t, os.WriteFile(source, []byte("accepted archive"), 0o600))
		logger.tryArchiveRotatedFile(context.Background(), source, logger.Fsstore.FileName)
		require.Equal(t, 1, primary.attemptCount())
		require.Equal(t, 1, secondary.attemptCount())
		requireFileLoggerClosesForTest(t, logger)
		require.Equal(t, 1, primary.attemptCount())
		require.Equal(t, 2, secondary.attemptCount())
		require.NoFileExists(t, source)
		require.NoFileExists(t, source+".gz")
	})
}

func TestInlineCloseWithoutCheckpointPreservesPartialSuccessAfterNotificationPanic(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		logger := newInlineCompressionCheckpointTestLogger(t, time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC), "accepted-partial-notification-panic")
		require.Nil(t, logger.archiveUploadCheckpoints)
		primary := &sequenceArchiveUploader{}
		secondary := &sequenceArchiveUploader{errs: []error{errors.New("secondary unavailable")}}
		logger.ArchiveUploader = newMultiArchiveUploader(t,
			uploader.Destination{ID: "primary", Uploader: primary},
			uploader.Destination{ID: "secondary", Uploader: secondary},
		)
		replacement := &sequenceArchiveUploader{}
		var notifications atomic.Int32
		logger.OnError = func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {
			if notifications.Add(1) == 1 {
				// 復旧登録時に現在のuploaderへ取り替えず、送信を開始した宛先を維持します。
				logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "replacement", Uploader: replacement})
				panic("partial upload notification panic")
			}
		}
		source := filepath.Join(logger.Fsstore.FileName.Dir, "accepted_20260923_1.log")
		require.NoError(t, os.WriteFile(source, []byte("accepted archive"), 0o600))
		require.PanicsWithValue(t, "partial upload notification panic", func() {
			logger.tryArchiveRotatedFile(context.Background(), source, logger.Fsstore.FileName)
		})
		require.EqualValues(t, 1, notifications.Load())
		require.Equal(t, 1, primary.attemptCount())
		require.Equal(t, 1, secondary.attemptCount())
		require.FileExists(t, source+".gz")

		requireFileLoggerClosesForTest(t, logger)
		require.Equal(t, 1, primary.attemptCount(), "通知panicでも成功済み宛先は再送しない")
		require.Equal(t, 2, secondary.attemptCount())
		require.Zero(t, replacement.attemptCount(), "再試行で新しい送信先へ切り替えない")
		require.NoFileExists(t, source)
		require.NoFileExists(t, source+".gz")
	})
}

func TestInlineCloseWithoutCheckpointRetriesCleanupWithoutRepeatingUpload(t *testing.T) {
	for _, tc := range []struct {
		name              string
		notificationPanic bool
	}{
		{name: "removal-failure"},
		{name: "removal-notification-panic", notificationPanic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctestFromWallClock(t, func(t *testing.T) {
				logger := newInlineCompressionCheckpointTestLogger(t, time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC), "accepted-cleanup-"+tc.name)
				require.Nil(t, logger.archiveUploadCheckpoints)
				var attempts atomic.Int32
				var archiveBytes []byte
				remote := archiveUploaderFunc(func(_ context.Context, request uploader.UploadRequest) error {
					if attempts.Add(1) == 1 {
						var err error
						archiveBytes, err = os.ReadFile(request.FilePath())
						require.NoError(t, err)
						// 送信成功後の削除だけを失敗させ、remoteへの再送が必要ない状態を作ります。
						require.NoError(t, os.Remove(request.FilePath()))
						require.NoError(t, os.Mkdir(request.FilePath(), 0o700))
						require.NoError(t, os.WriteFile(filepath.Join(request.FilePath(), "child"), []byte("obstruction"), 0o600))
					}
					return nil
				})
				secondary := &sequenceArchiveUploader{}
				logger.ArchiveUploader = newMultiArchiveUploader(t,
					uploader.Destination{ID: "primary", Uploader: remote},
					uploader.Destination{ID: "secondary", Uploader: secondary},
				)
				var notifications atomic.Int32
				logger.OnError = func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {
					if notifications.Add(1) == 1 && tc.notificationPanic {
						panic("accepted cleanup notification panic")
					}
				}
				source := filepath.Join(logger.Fsstore.FileName.Dir, "accepted_20260923_1.log")
				require.NoError(t, os.WriteFile(source, []byte("accepted archive"), 0o600))
				if tc.notificationPanic {
					require.PanicsWithValue(t, "accepted cleanup notification panic", func() {
						logger.tryArchiveRotatedFile(context.Background(), source, logger.Fsstore.FileName)
					})
				} else {
					logger.tryArchiveRotatedFile(context.Background(), source, logger.Fsstore.FileName)
				}
				require.EqualValues(t, 1, attempts.Load())
				require.EqualValues(t, 1, notifications.Load())
				require.NoFileExists(t, source)
				compressed := source + ".gz"
				require.DirExists(t, compressed)

				// 同じ成果物を復元し、受理済み復旧にlocal削除だけを完了させます。
				require.NoError(t, os.Remove(filepath.Join(compressed, "child")))
				require.NoError(t, os.Remove(compressed))
				require.NoError(t, os.WriteFile(compressed, archiveBytes, 0o600))
				requireFileLoggerClosesForTest(t, logger)
				require.EqualValues(t, 1, attempts.Load(), "送信成功後の削除再試行ではremoteへ再送しない")
				require.Equal(t, 1, secondary.attemptCount())
				require.NoFileExists(t, compressed)
			})
		})
	}
}

func TestInlineCloseWithoutCheckpointCompletesRawUploadAndPreservesFile(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		logger := newInlineUsageTestLogger(t, time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC), "accepted-raw")
		require.False(t, logger.config.Fsstore().Lifecycle().EnableFileCompress())
		require.Nil(t, logger.archiveUploadCheckpoints)
		remote := &sequenceArchiveUploader{errs: []error{errors.New("uploader unavailable")}}
		logger.ArchiveUploader = newSingleArchiveUploader(t, remote)
		source := filepath.Join(logger.Fsstore.FileName.Dir, "accepted_20260923_1.log")
		require.NoError(t, os.WriteFile(source, []byte("accepted raw archive"), 0o600))
		logger.tryArchiveRotatedFile(context.Background(), source, logger.Fsstore.FileName)
		require.Equal(t, 1, remote.attemptCount())

		requireFileLoggerClosesForTest(t, logger)
		require.Equal(t, 2, remote.attemptCount())
		bytes, err := os.ReadFile(source)
		require.NoError(t, err)
		require.Equal(t, "accepted raw archive", string(bytes), "未圧縮fileは送信成功後も保持する")
	})
}

type acceptedPanicOnceUploader struct{ calls atomic.Int32 }

func (*acceptedPanicOnceUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"primary"}
}

func (u *acceptedPanicOnceUploader) UploadDestinations(ctx context.Context, request uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
	var report uploader.UploadReport
	for _, destination := range destinations {
		report.Results = append(report.Results, uploader.DestinationResult{DestinationID: destination, Err: u.Upload(ctx, request)})
	}
	return report
}

func (u *acceptedPanicOnceUploader) Upload(context.Context, uploader.UploadRequest) error {
	if u.calls.Add(1) == 1 {
		panic("initial accepted upload panic")
	}
	return nil
}

func TestInlineCloseDrainsInitialUploadPanicWithoutAnotherWrite(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		logger := newInlineCompressionCheckpointTestLogger(t, time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC), "accepted-upload-panic")
		remote := &acceptedPanicOnceUploader{}
		logger.ArchiveUploader = remote
		source := filepath.Join(logger.Fsstore.FileName.Dir, "accepted_20260923_1.log")
		require.NoError(t, os.WriteFile(source, []byte("accepted archive"), 0o600))
		require.PanicsWithValue(t, "initial accepted upload panic", func() {
			logger.tryArchiveRotatedFile(context.Background(), source, logger.Fsstore.FileName)
		})
		require.FileExists(t, source+".gz")
		closed := make(chan error, 1)
		go func() { closed <- logger.Close() }()
		synctest.Wait()
		require.Empty(t, closed, "最初のpanic後も受理済みuploadを継続して待つ")
		requireStartedCloseCompletesForTest(t, logger, closed)
		require.EqualValues(t, 2, remote.calls.Load())
		require.NoFileExists(t, source+".gz")
	})
}

type acceptedMarkPanicOnceStore struct {
	checkpointstore.Store
	calls atomic.Int32
}

func (s *acceptedMarkPanicOnceStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, ids []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	if s.calls.Add(1) == 1 {
		panic("initial checkpoint update panic")
	}
	return s.Store.AdvanceCompletionCursor(ctx, key, ids)
}

func TestInlineInitialCheckpointPanicPreservesCompletedUpload(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		logger, store, remote, path, _ := newInlineLogFileProcessingRecoveryFixture(t)
		logger.archiveUploadCheckpoints = &acceptedMarkPanicOnceStore{Store: store}
		require.PanicsWithValue(t, "initial checkpoint update panic", func() {
			logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately)
		})
		requireFileLoggerClosesForTest(t, logger)
		require.Equal(t, 1, remote.attemptCount(), "既知の送信成功をpanic後も保持する")
		require.NoFileExists(t, path)
	})
}

func TestInlineCheckpointNotificationPanicAfterRecoveryDoesNotRepeatUpload(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		logger, store, remote, path, _ := newInlineLogFileProcessingRecoveryFixture(t)
		require.NoError(t, os.Remove(path))
		source := writeCanonicalRawLog(t, logger, "20260920", 1)
		store.failRecord.Store(true)
		var notifications atomic.Int32
		logger.OnError = func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {
			if notifications.Add(1) == 1 {
				store.failRecord.Store(false)
				// 通知先の待機中に復旧が完了しても、初回側は共有operationを書き換えません。
				time.Sleep(2 * logfile.DefaultLogFileProcessingRecoveryInterval)
				synctest.Wait()
				panic("checkpoint notification panic after recovery")
			}
		}
		require.PanicsWithValue(t, "checkpoint notification panic after recovery", func() {
			logger.tryArchiveRotatedFile(context.Background(), source, logger.Fsstore.FileName)
		})
		requireFileLoggerClosesForTest(t, logger)
		require.Equal(t, 1, remote.attemptCount(), "復旧完了後の通知panicでも成功宛先を再送しない")
		require.FileExists(t, source, "未圧縮ログは完了後も保持する")
	})
}
