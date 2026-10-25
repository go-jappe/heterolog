package fsarchivehandler

import (
	"errors"
	"log"
	"os"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
)

// archiveDirectoryInspectionは、一回のディレクトリ走査とファイル検査の結果を保持します。
// 復旧・送信候補の選択・purgeで同じ結果を共有し、ファイルが変化した後は再検査します。
// 各パスを順に検査した結果であり、ディレクトリ全体を原子的に読み取ったものではありません。
type archiveDirectoryInspection struct {
	// directoryFilesは、別系列や検査エラーも含むディレクトリ全体の検査結果です。
	// 容量計算では、この中から対象拡張子の通常ファイルを使います。
	directoryFiles fsstore.FileInspection
	// seriesPatternは、今回処理する系列のディレクトリ・接頭辞・対象名・拡張子などの命名規則です。
	seriesPattern logpattern.FileNamePattern
	// seriesSourcesは、命名規則に一致した検査結果を、元ファイルと圧縮ファイルの組に分類したものです。
	// 検査エラーも保持します。命名規則自体は保持しないため、seriesPatternと併せて使います。
	seriesSources fsstore.CompressionSourceGroups
}

// inspectArchivePathsは、指定パスの種類・サイズ・更新日時と検査エラーを取得します。
// ディレクトリ走査は行わず、個別の情報取得エラーも結果へ残して後続の判断に渡します。
func (f *FsArchiveHandler) inspectArchivePaths(paths []string) fsstore.FileInspection {
	inspect := f.inspectArchiveFiles
	if inspect == nil {
		inspect = fsstore.InspectFilePaths
	}
	inspection := inspect(paths)
	// GetFilesInfoと同じく個別のLstat失敗を通知します。候補の処理を止める必要が
	// あるerrorは、取得済みPathErrorsから各処理へ引き継ぎます。
	for _, path := range inspection.Paths {
		var pathErr *os.PathError
		if errors.As(inspection.PathErrors[path], &pathErr) {
			log.Printf("failed to stat log file: %s, err: %v", path, pathErr)
		}
	}
	return inspection
}

func (inspection archiveDirectoryInspection) allFileInfos() []fsstore.FileInfo {
	files := make([]fsstore.FileInfo, 0, len(inspection.directoryFiles.RegularFiles))
	matchName := logpattern.FileNameMatcher(inspection.seriesPattern, logpattern.GlobAllFiles)
	for _, file := range inspection.directoryFiles.RegularFiles {
		if matchName(file.Path) {
			files = append(files, file)
		}
	}
	return files
}

// newArchiveDirectoryInspectionは、検査済みの全ファイルを保持し、指定系列の情報を分類します。
// ファイルの再検査は行いません。seriesPatternは走査時に確定した命名規則を渡してください。
func newArchiveDirectoryInspection(
	files fsstore.FileInspection,
	seriesPattern logpattern.FileNamePattern,
) archiveDirectoryInspection {
	paths := make([]string, 0, len(files.Paths))
	matchName := logpattern.FileNameMatcher(seriesPattern, logpattern.GlobByTarget)
	for _, path := range files.Paths {
		if matchName(path) {
			paths = append(paths, path)
		}
	}
	return archiveDirectoryInspection{
		directoryFiles: files,
		seriesPattern:  seriesPattern,
		seriesSources: fsstore.GroupFilesByCompressionSource(
			selectArchiveFileInspection(files, paths),
			seriesPattern.Extension,
		),
	}
}

// selectArchiveFileInspectionは検査済みのパス一覧から部分集合を作り、名前順と検査エラーを維持します。
func selectArchiveFileInspection(inspection fsstore.FileInspection, paths []string,
) fsstore.FileInspection {
	selected := fsstore.FileInspection{Paths: paths}
	allowed := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		allowed[path] = struct{}{}
		if err := inspection.PathErrors[path]; err != nil {
			if selected.PathErrors == nil {
				selected.PathErrors = make(map[string]error)
			}
			selected.PathErrors[path] = err
		}
	}
	for _, file := range inspection.RegularFiles {
		if _, ok := allowed[file.Path]; ok {
			selected.RegularFiles = append(selected.RegularFiles, file)
		}
	}
	return selected
}
