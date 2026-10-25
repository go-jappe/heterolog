package file

import (
	"context"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// archiveDestinationIDsAllowedByGenerationOrderは、指定された宛先のうち、対象ファイルより古い未完了世代がない宛先を返します。
// 未圧縮ログと処理中の保護パスも世代の確認対象に含め、圧縮やcheckpoint更新に失敗した
// 古い世代を残したまま、後続世代の送信・退避を進めることを防ぎます。
func (l *FileLogger) archiveDestinationIDsAllowedByGenerationOrder(ctx context.Context, group archiveUploadGroup, destinations []objectstorage.DestinationID) ([]objectstorage.DestinationID, error) {
	var ready []objectstorage.DestinationID
	err := l.lifecycleGuard.RunWithLifecycleLockContext(ctx, func() error {
		keys, err := checkpointstore.ScanExistingFileKeys(group.fileName)
		if err != nil {
			return err
		}
		l.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
			paths := make([]string, 0, len(protected))
			for path := range protected {
				paths = append(paths, path)
			}
			for _, generation := range checkpointstore.ArchiveCheckpointGenerationKeys(paths, group.fileName) {
				keys = append(keys, checkpointstore.CheckpointKey{Generation: generation})
			}
		})
		ready, err = checkpointstore.ReadyDestinationIDs(ctx, l.archiveUploadCheckpoints, group.key, destinations, keys)
		return err
	})
	return ready, err
}
