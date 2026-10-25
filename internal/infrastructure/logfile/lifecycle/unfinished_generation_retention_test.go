package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// 圧縮または送信を終えていない世代と、その完了を待つ後続の送信待ち世代は、
// 同じ周期の世代数purgeの対象にしません。登録前に完結した世代は通常どおり世代数purgeで削除します。
func TestFileSeriesManagerKeepsUnfinishedGenerationsThroughGenerationPurge(t *testing.T) {
	for _, failure := range []string{"compression publish not durable", "upload durability unconfirmed"} {
		t.Run(failure, func(t *testing.T) {
			// 受理済み処理の再試行は実時間で待たず、bubble内の時刻で進めます。
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				root := resolvedTempDirForTest(t)
				const target = "unfinished-generation-retention"
				const destination = objectstorage.DestinationID("archive")
				logDir := filepath.Join(root, target, "logs")
				require.NoError(t, os.MkdirAll(logDir, 0o750))
				period := currentLogDate()
				pathFor := func(index uint64) string {
					return filepath.Join(logDir, target+"_"+period+"_"+strconv.FormatUint(index, 10)+".log")
				}
				writeCompressed := func(index uint64) string {
					path := pathFor(index) + pkgfsstore.ExtensionCompressGzip
					writeGzipFile(t, path, []byte(filepath.Base(path)))
					return path
				}
				// 世代1・2は宛先の登録前に完結した世代で、送信せず通常の保持に任せます。
				// 世代数の上限は1なので、保護対象を数えなくても世代1は上限を超えます。
				outOfScope := []string{writeCompressed(1), writeCompressed(2)}
				store := newTestFileCheckpointStore(t)
				require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{destination},
					[]checkpointstore.CheckpointKey{fileCheckpointKey(fileGeneration{Period: period, Index: 2}, true)}))

				// 世代3の圧縮または送信が失敗し、世代4・5はその完了を待つ送信待ちになります。
				// 保護がなければ、非activeの世代が上限を超えるため世代3・4も削除対象になります。
				remote := newDestinationUploaderRecorder(destination)
				var unfinished []string
				var failureErr error
				switch failure {
				case "compression publish not durable":
					unfinished = append(unfinished, writeFileSeriesCheckpointLog(t, logDir, target, period, 3))
				case "upload durability unconfirmed":
					unfinished = append(unfinished, writeCompressed(3))
					failureErr = pkgfsstore.ErrDurabilityUnconfirmed
					remote.failNext(destination, fmt.Errorf("durable handoff: %w", failureErr))
				}
				later := []string{writeCompressed(4), writeCompressed(5)}
				latest := writeFileSeriesCheckpointLog(t, logDir, target, period, 6)
				manager := NewFileSeriesManager(FileSeriesManagerOptions{
					FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
					Config:   testFileSeriesManagerConfig(root, target, 1, 0, 0, true),
					Uploader: remote, CheckpointStore: store,
				})
				boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, boundedwait.NoError)
				if failure == "compression publish not durable" {
					source := unfinished[0]
					peer := source + pkgfsstore.ExtensionCompressGzip
					failureErr = errors.New("sync compressed file parent directory")
					manager.compressFile = func(_ context.Context, path string) (string, error) {
						require.Equal(t, source, path)
						content, err := os.ReadFile(path)
						require.NoError(t, err)
						// final renameの後で親directoryの同期だけが失敗し、圧縮を確定できない状態です。
						writeGzipFile(t, peer, content)
						return peer, failureErr
					}
					unfinished = append(unfinished, peer)
				}

				report, err := manager.RunOnce(ctx)
				require.ErrorIs(t, err, failureErr)
				require.Empty(t, report.UploadedFileRemovals)
				require.True(t, report.Purge.ByGeneration.Ran)
				removed := make([]string, 0, len(report.Purge.ByGeneration.RemovalResults))
				for _, removal := range report.Purge.ByGeneration.RemovalResults {
					require.NoError(t, removal.Err)
					removed = append(removed, removal.File.Path)
				}
				require.Equal(t, outOfScope[:1], removed)
				require.NoFileExists(t, outOfScope[0])
				for _, path := range append(append([]string{outOfScope[1], latest}, unfinished...), later...) {
					require.FileExists(t, path)
				}
			})
		})
	}
}
