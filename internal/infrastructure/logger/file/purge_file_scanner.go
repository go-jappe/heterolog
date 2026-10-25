package file

import (
	"fmt"
	"log"
	"os"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
)

// purgeFileScannerは、inline lifecycleが使用するfile一覧をfilesystemから走査します。
// 名前、通常fileの情報、検査できなかったpathを一回の観測として保持します。
type purgeFileScanner interface {
	Scan(
		logpattern.FileNamePattern,
		logpattern.GlobMode,
	) (fsstore.FileInspection, error)
}

// filesystemPurgeFileScannerは、通常実行時に名前の列挙とLstatでfile一覧を走査します。
type filesystemPurgeFileScanner struct{}

func (filesystemPurgeFileScanner) Scan(
	fileName logpattern.FileNamePattern,
	mode logpattern.GlobMode,
) (fsstore.FileInspection, error) {
	paths, err := logpattern.GlobLifecycleFileNames(fileName, mode)
	if err != nil {
		return fsstore.FileInspection{}, fmt.Errorf("read lifecycle files: %w", err)
	}
	inspection := fsstore.InspectFilePaths(paths)
	for path, err := range inspection.PathErrors {
		// GetFilesInfoと同じく個別Lstat失敗を診断し、名前はcheckpoint存在集合へ残します。
		if _, statFailed := err.(*os.PathError); statFailed {
			log.Printf("failed to stat log file: %s, err: %v", path, err)
		}
	}
	return inspection, nil
}
