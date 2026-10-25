//go:build !windows

package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestGenerationContinuationDoesNotTreatLstatPermissionFailureAsMissing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can bypass directory search permissions")
	}
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "uninspectable-not-missing"
	const destination = objectstorage.DestinationID("archive")
	logDirectory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDirectory, 0o750))
	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	remote := newDestinationUploaderRecorder(destination)
	manager, compressedStore := newGenerationContinuationManager(t, root, target, checkpointDirectory, remote)
	period := currentLogDate()
	raw := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 1)
	peer := raw + ".gz"
	// 送信済みの世代を圧縮した、元ログと同じ内容のpeerです。
	rawContent, err := os.ReadFile(raw)
	require.NoError(t, err)
	writeGzipFile(t, peer, rawContent)
	latest := writeFileSeriesCheckpointLog(t, logDirectory, target, period, 2)
	key := fileCheckpointKey(fileGeneration{Period: period, Index: 1}, true)
	_, err = compressedStore.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{destination})
	require.NoError(t, err)
	require.NoError(t, manager.validate())

	// 名前の列挙は許可し、子fileのLstatに必要なdirectoryの検索権限だけを外します。
	// 不存在ではない検査エラーを、失われた世代としてPruneしてはいけません。
	require.NoError(t, os.Chmod(logDirectory, 0o400))
	defer func() { require.NoError(t, os.Chmod(logDirectory, 0o750)) }()
	scan, err := manager.scanLifecycleTargetFiles()
	require.NoError(t, err)
	require.Equal(t, []string{raw, peer}, scan.files)
	require.Empty(t, scan.sourceGroups.Files.RegularFiles)
	require.ElementsMatch(t, []string{raw, peer, latest}, scan.sourceGroups.Files.Paths)
	sourceGroup, found := scan.sourceGroups.Source(raw)
	require.True(t, found)
	require.ErrorIs(t, sourceGroup.Err, os.ErrPermission)

	_, err = manager.RunOnce(ctx)
	require.ErrorIs(t, err, os.ErrPermission)
	require.Empty(t, remote.pathsFor(destination))
	checkpoint, err := compressedStore.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)

	require.NoError(t, os.Chmod(logDirectory, 0o750))
	require.FileExists(t, raw)
	require.FileExists(t, peer)
	_, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, remote.pathsFor(destination), "inspection failure must not discard the successful destination record")
	require.NoFileExists(t, raw)
	require.NoFileExists(t, peer)
	require.FileExists(t, latest)
}
