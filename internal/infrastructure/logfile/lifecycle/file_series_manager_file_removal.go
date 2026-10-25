package lifecycle

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// removeCompressedFileAndSyncDirectoryは、圧縮ファイルの削除と検証cacheの破棄を
// 一組で行います。削除後の親ディレクトリ同期やCloseが失敗した場合もcacheは破棄し、
// 同期・Closeのエラーは削除結果と分けて返します。
// upload成功記録とcheckpointの更新は呼び出し側が行います。
func (m *FileSeriesManager) removeCompressedFileAndSyncDirectory(path string) (fsstore.FileRemovalResult, error) {
	result, directorySyncErr := fsstore.RemoveRegularFileAndSyncDirectory(path)
	m.forgetRemovedCompressedPeers(result)
	return result, directorySyncErr
}

// forgetRemovedCompressedPeersは、削除済み・不存在と確認できたパスの検証cacheだけを
// 破棄します。purgeが一部失敗した場合も、成功分はエラーを返す前にこの処理へ渡します。
// ファイルを調べ直さず、削除処理が返した結果を使います。
func (m *FileSeriesManager) forgetRemovedCompressedPeers(results ...fsstore.FileRemovalResult) {
	for _, result := range results {
		if result.Err == nil {
			m.forgetVerifiedCompressedPeer(result.File.Path)
		}
	}
}

func (m *FileSeriesManager) forgetVerifiedCompressedPeer(path string) {
	key := pathidentity.ComparisonKey(path)
	m.compressedPeerCacheMu.Lock()
	delete(m.verifiedCompressedPeers, key)
	m.compressedPeerCacheMu.Unlock()
}
