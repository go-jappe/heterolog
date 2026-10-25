package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestLogFileProcessingRecoveryReleasesManuallyDeletedFile(t *testing.T) {
	for _, failure := range []string{"load", "mark"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			const destination = objectstorage.DestinationID("archive")
			f := newLogFileProcessingRecoveryFixture(t, destination)
			f.store.failLoad.Store(failure == "load")
			f.store.failRecord.Store(failure == "mark")
			_, err := f.lifecycle.RunOnce(ctx)
			require.ErrorIs(t, err, errRecoveryCheckpoint)
			require.True(t, f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)))
			requests := f.remote.pathsFor(destination)

			// checkpoint操作がまだ失敗していても、不在と確認できたpathを再送し続けません。
			require.NoError(t, f.manager.directoryGuard.RunWithLifecycleLock(func() error {
				return os.Remove(f.path)
			}))
			waitLogFileProcessingRecovery(t, f.manager, f.cursor)
			require.Equal(t, requests, f.remote.pathsFor(destination))

			f.store.failLoad.Store(false)
			f.store.failRecord.Store(false)
			writeFileSeriesCheckpointLog(t, filepath.Dir(f.path), "checkpoint-recovery", f.cursor.Period, 4)
			_, err = f.lifecycle.RunOnce(ctx)
			require.NoError(t, err)
			require.Equal(t, append(requests, filepath.Join(filepath.Dir(f.path), "checkpoint-recovery_"+f.cursor.Period+"_3.log")), f.remote.pathsFor(destination))
		})
	}
}

func TestLogFileProcessingRecoveryCompletesLaterFileWithoutCrossingUnsafePeer(t *testing.T) {
	ctx := context.Background()
	const destination = objectstorage.DestinationID("archive")
	f := newLogFileProcessingRecoveryFixture(t, destination)
	f.store.failRecord.Store(true)
	_, err := f.lifecycle.RunOnce(ctx)
	require.ErrorIs(t, err, errRecoveryCheckpoint)
	lowerCursor := fileGeneration{Period: f.cursor.Period, Index: 1}
	lowerPath := filepath.Join(filepath.Dir(f.path), "checkpoint-recovery_"+f.cursor.Period+"_1.log.gz")
	require.NoError(t, f.manager.directoryGuard.RunWithLifecycleLock(func() error {
		// 以前の成功記録があっても、検査不能なfileは操作せず、別fileの再開だけを行います。
		testutil.Symlink(t, filepath.Join(resolvedTempDirForTest(t), "absent.gz"), lowerPath)
		_, err := f.store.Store.AdvanceCompletionCursor(ctx, fileCheckpointKey(lowerCursor, true), []objectstorage.DestinationID{destination})
		return err
	}))
	f.store.failRecord.Store(false)
	waitLogFileProcessingRecovery(t, f.manager, f.cursor)
	require.NoFileExists(t, f.path, "completed later file is removable despite the earlier inspection error")
	require.Equal(t, []string{f.path}, f.remote.pathsFor(destination))
	info, err := os.Lstat(lowerPath)
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSymlink)
}

func TestCheckpointRemovalRecoveryDoesNotBlockLaterCompletedGenerationProgress(t *testing.T) {
	for _, rawExists := range []bool{false, true} {
		name := "absent raw alias"
		if rawExists {
			name = "existing raw shares generation completion"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				const destination = objectstorage.DestinationID("archive")
				f := newLogFileProcessingRecoveryFixture(t, destination)
				setLogFileProcessingRecoveryInterval(t, f.manager, time.Hour)
				_, err := f.store.AdvanceCompletionCursor(ctx, fileCheckpointKey(f.cursor, true), []objectstorage.DestinationID{destination})
				require.NoError(t, err)
				if rawExists {
					require.NoError(t, os.WriteFile(f.raw, []byte("raw representation of the completed generation"), 0o600))
				}
				directory := filepath.Dir(f.path)
				nextRaw := filepath.Join(directory, "checkpoint-recovery_"+f.cursor.Period+"_3.log")
				require.NoError(t, os.Remove(nextRaw))
				next := nextRaw + ".gz"
				writeGzipFile(t, next, []byte("following compressed generation"))
				writeFileSeriesCheckpointLog(t, directory, "checkpoint-recovery", f.cursor.Period, 4)
				f.manager.scheduleLogFileProcessingRecovery(f.path, f.cursor, true, false, errors.New("retry local removal"))
				require.True(t, f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)))

				_, err = f.lifecycle.RunOnce(ctx)
				require.NoError(t, err)
				require.NoFileExists(t, next)
				require.Equal(t, []string{next}, f.remote.pathsFor(destination))
				require.FileExists(t, f.path, "the earlier removal retry remains independent")

				// 先行世代の削除の再試行は、後続世代の処理とは独立に自身の再試行間隔で完了します。
				// fixtureの終了処理は短い上限で待つため、ここで間隔を経過させて完了を確認します。
				// 同じ世代の元ログが残る場合は通常周期が削除を引き継ぐため、一周期後の状態を確認します。
				time.Sleep(time.Hour)
				synctest.Wait()
				require.False(t, f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)),
					"the accepted removal retry completes at its own interval")
				_, err = f.lifecycle.RunOnce(ctx)
				require.NoError(t, err)
				require.NoFileExists(t, f.path)
				require.Equal(t, []string{next}, f.remote.pathsFor(destination), "a completed generation is removed without another upload")
			})
		})
	}
}
