package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestUnconfirmedArchiveDurabilityRetainsSourceAndCheckpointUntilRetry(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const (
			target      = "unconfirmed-archive-upload"
			source      = "unconfirmed_archive_upload"
			destination = objectstorage.DestinationID("primary")
		)
		logDirectory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDirectory, 0o750))
		// 保持期間1日を過ぎた期間の世代にし、保護がなければ同じ周期のage purgeで削除される状態にします。
		period := logDateDaysBefore(10)
		generation := fileGeneration{Period: period, Index: 1}
		compressedPath := filepath.Join(logDirectory, target+"_"+period+"_1.log.gz")
		latestPath := filepath.Join(logDirectory, target+"_"+currentLogDate()+"_1.log")
		writeGzipFile(t, compressedPath, []byte("source retained until durable acceptance"))
		require.NoError(t, os.WriteFile(latestPath, []byte("current writer"), 0o600))
		original, err := os.ReadFile(compressedPath)
		require.NoError(t, err)
		checkpointDirectory := newCheckpointDirectoryForTest(t, root)
		store, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
		require.NoError(t, err)

		remote := newDestinationUploaderRecorder(destination)
		remote.failNext(destination, fmt.Errorf("durable handoff: %w: %w",
			pkgfsstore.ErrDurabilityUnconfirmed, os.ErrPermission,
		))
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: logDirectory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, false),
			Uploader: remote, CheckpointStore: store,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, boundedwait.NoError)
		report, err := manager.RunOnce(ctx)
		require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
		require.ErrorIs(t, err, os.ErrPermission)
		require.Empty(t, report.ObjectStorageUploads)
		require.Empty(t, report.UploadedFileRemovals)
		// 退避の完了を確認できない圧縮fileは、同じ周期のageとdirectory-size purgeでも削除しません。
		require.True(t, report.Purge.ByAge.Ran)
		require.Empty(t, report.Purge.ByAge.RemovalResults)
		require.True(t, report.Purge.ByDirSize.Ran)
		require.Empty(t, report.Purge.ByDirSize.RemovalResults)
		retained, err := os.ReadFile(compressedPath)
		require.NoError(t, err)
		require.Equal(t, original, retained)

		// process 内の進捗だけでなく、再構築した Store にも成功が残っていないことを確認します。
		restarted, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
		require.NoError(t, err)
		state, err := restarted.Load(ctx, fileCheckpointKey(generation, true))
		require.NoError(t, err)
		require.Empty(t, state.CompletedDestinationIDs)
		require.NoError(t, boundedwait.CloseInBubble(t, "restarted.Close()", restarted.Close, restarted.ForceClose))

		// 通常周期を追加せず、受理済みの退避・checkpoint・削除の完了を待ちます。
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		require.NoFileExists(t, compressedPath)
		require.FileExists(t, latestPath)
		require.Equal(t, []string{compressedPath, compressedPath}, remote.pathsFor(destination))
		completed, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
		require.NoError(t, err)
		state, err = completed.Load(ctx, fileCheckpointKey(generation, true))
		require.NoError(t, err)
		require.Equal(t, []objectstorage.DestinationID{destination}, state.CompletedDestinationIDs, "completed source removal retains its destination cursor")
		require.NoError(t, boundedwait.CloseInBubble(t, "completed.Close()", completed.Close, completed.ForceClose))
	})
}
