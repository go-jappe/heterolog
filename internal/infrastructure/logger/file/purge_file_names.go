package file

import (
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
)

// purgeFileNamesは、一回のcleanupで使う命名規則と、ファイル名の解析結果を保持します。
// 保持日数・世代数・容量のpurgeへ同じ値を渡し、同じ名前と命名規則の組み合わせを
// 解析し直さないようにします。規則に一致しなかった結果も保持します。
// Guardの保護状態やcheckpointの完了状態は保持せず、各purgeで確認します。
// lifecycle lock内で直列に使い、cleanup終了後は保持しません。
type purgeFileNames struct {
	currentSeries logpattern.FileNamePattern
	generations   map[logpattern.FileNamePattern]map[string]parsedPurgeFileName
}

type parsedPurgeFileName struct {
	generation logpattern.CanonicalGenerationFile
	canonical  bool
}

func newPurgeFileNames(currentSeries logpattern.FileNamePattern) *purgeFileNames {
	return &purgeFileNames{
		currentSeries: currentSeries,
		generations:   make(map[logpattern.FileNamePattern]map[string]parsedPurgeFileName),
	}
}

// generationは、名前から取得した世代と圧縮種別を返します。ディレクトリの一致や
// ファイルの存在は判定しません。Guardから後で追加された名前も、初めて参照した際に解析します。
func (names *purgeFileNames) generation(
	path string,
	pattern logpattern.FileNamePattern,
) (logpattern.CanonicalGenerationFile, bool) {
	byName := names.generations[pattern]
	name := logpattern.FileNameComparisonKey(filepath.Base(path))
	if parsed, exists := byName[name]; exists {
		return parsed.generation, parsed.canonical
	}
	generation, canonical := logpattern.ParseCanonicalGenerationFilePath(path, pattern)
	if byName == nil {
		byName = make(map[string]parsedPurgeFileName)
		names.generations[pattern] = byName
	}
	byName[name] = parsedPurgeFileName{generation: generation, canonical: canonical}
	return generation, canonical
}
