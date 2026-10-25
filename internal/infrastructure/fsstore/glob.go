package fsstore

import (
	"log"
	"os"
	"time"
)

// FileInfoは、purgeの容量・日数・世代数の判定に使う通常ファイルの観測値です。
type FileInfo struct {
	Path    string
	Size    uint64
	ModTime time.Time
}

// GetFilesInfoは、候補パスのうち通常ファイルだけのサイズと更新日時を返します。
// Lstatでリンク自身を調べ、シンボリックリンク・ディレクトリ・特殊ファイルは
// 計測とpurge候補から除外します。個別の情報取得に失敗したパスも除外します。
func GetFilesInfo(fileNames []string) []FileInfo {
	var logFiles []FileInfo
	for _, file := range fileNames {
		info, err := os.Lstat(file)
		if err != nil {
			log.Printf("failed to stat log file: %s, err: %v", file, err)
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		logFiles = append(logFiles, FileInfo{Path: file, Size: uint64(info.Size()), ModTime: info.ModTime()})
	}
	return logFiles
}
