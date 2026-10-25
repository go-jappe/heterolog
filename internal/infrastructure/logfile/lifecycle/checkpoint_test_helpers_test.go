package lifecycle

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	infraobjectstorage "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// forceCloseWrappedCheckpointStoreは、試験用に包んだcheckpoint Storeへ、所有者のForceCloseを届けます。
// 包装はcheckpointstore.Store interfaceを埋め込むためForceCloseを持たず、そのままでは
// managerのForceCloseが包んだ実Storeの保存の再試行を中止できません。保存が終わらない回帰でも、
// 期限付きの終了処理がForceClose後にStoreの終了を待てるよう、包装ごとに明示して転送します。
func forceCloseWrappedCheckpointStore(store checkpoint.Store) error {
	if force := boundedwait.ForceCloserOf(store); force != nil {
		return force()
	}
	return nil
}

func newCheckpointDirectoryForTest(t *testing.T, parent string) string {
	t.Helper()
	directory := filepath.Join(parent, "checkpoints")
	require.NoError(t, checkpoint.PrepareDirectory(directory))
	return directory
}

// newFileSeriesManagerWithExistingDestinationsは、既存宛先の登録後に作られたfileを
// 扱う復旧・保持試験のfixtureです。新規宛先の試験はNewFileSeriesManagerを直接使います。
func newFileSeriesManagerWithExistingDestinations(t *testing.T, opts FileSeriesManagerOptions) *FileSeriesManager {
	t.Helper()
	if opts.Uploader != nil {
		opts.Uploader = newTestDestinationUploader(t, opts.Uploader)
		if opts.CheckpointStore == nil {
			opts.CheckpointStore = newTestFileCheckpointStore(t)
		}
		client := opts.Uploader.(uploader.DestinationUploader)
		require.NoError(t, opts.CheckpointStore.RegisterDestinations(context.Background(), client.DestinationIDs(), nil))
	}
	return NewFileSeriesManager(opts)
}

// newTestDestinationUploaderは、単一宛先の試験用Uploaderを宛先ID付きの送信経路で包みます。
// 元の送信動作はそのまま使い、lifecycleが要求する宛先別の結果を返します。
func newTestDestinationUploader(t *testing.T, client uploader.Uploader) uploader.DestinationUploader {
	t.Helper()
	if destinations, ok := client.(uploader.DestinationUploader); ok {
		return destinations
	}
	destinations, err := infraobjectstorage.NewMultiUploader(uploader.Destination{
		ID: objectstorage.DestinationID("primary"), Uploader: client,
	})
	require.NoError(t, err)
	return destinations
}
