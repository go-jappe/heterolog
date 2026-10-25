package logpattern

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// GenerationFileKindは、canonicalなlog generationが未圧縮sourceか、公開済みの
// 圧縮成果物かを表します。
type GenerationFileKind uint8

const (
	GenerationFileKindUncompressed GenerationFileKind = iota + 1
	GenerationFileKindCompressed
)

// CanonicalGenerationFileは、writerの命名規則から確定したlogical generationです。
// PeriodとIndexは圧縮suffixに依存せず、同じraw/圧縮fileでは同じ値になります。
type CanonicalGenerationFile struct {
	// ex.) "20260915"ファイル名の日付
	Period string
	// ex.) 3同じ期間内のファイル番号
	Index uint64
	// 圧縮ファイルかどうか
	Kind GenerationFileKind
}

// ParseCanonicalGenerationFilePathは、pathがwriterの生成する
// `<prefix><target>_<YYYYMMDD>_<canonical positive index>.<base>[.<compression>]`
// に一致する場合だけ、そのlogical generationを返します。
//
// 圧縮suffixはSplitBaseAndOptionalCompressionExtensionsと同じく、安全な一段suffixを
// 許可します。target prefixが偶然一致する利用者file、0や先頭0を持つindex、存在しない
// calendar日付、圧縮途中のtemporary fileはcanonical generationとして扱いません。
// prefix・target・extensionの大小文字はfileNameのEffectiveNameCase（未指定ならDirの
// 判定結果）で比較します。pathの所在ディレクトリは照合しません。
func ParseCanonicalGenerationFilePath(
	path string,
	fileName FileNamePattern,
) (CanonicalGenerationFile, bool) {
	return parseCanonicalGenerationFilePath(path, fileName, fileName.EffectiveNameCase())
}

// parseCanonicalGenerationFilePathは、判定済みの規則nameCaseで
// ParseCanonicalGenerationFilePathと同じ解析を行います。
func parseCanonicalGenerationFilePath(
	path string,
	fileName FileNamePattern,
	nameCase pathidentity.NameCase,
) (CanonicalGenerationFile, bool) {
	baseExtension := strings.TrimPrefix(fileName.Extension, ".")
	stem, generationExtension, ok := fsstore.SplitBaseAndOptionalCompressionExtensionsForNameCase(
		filepath.Base(path),
		baseExtension,
		nameCase,
	)
	if !ok {
		return CanonicalGenerationFile{}, false
	}

	prefix := nameCase.Key(fileName.Prefix + fileName.Target + "_")
	remainder, matched := strings.CutPrefix(nameCase.Key(stem), prefix)
	if !matched {
		return CanonicalGenerationFile{}, false
	}
	parts := strings.Split(remainder, "_")
	if len(parts) != 2 || len(parts[0]) != len(pkgfsstore.FileDateFormat) {
		return CanonicalGenerationFile{}, false
	}
	if _, err := time.Parse(pkgfsstore.FileDateFormat, parts[0]); err != nil {
		return CanonicalGenerationFile{}, false
	}

	// writerはFileNamePattern.Indexをintで保持し、strconv.Itoaと同じcanonicalな
	// 10進表記をfile名へ出力します。uint64としては解釈できても実行環境のintへ
	// 収まらない番号はwriterが生成できないため、managed generationに含めません。
	index, err := strconv.Atoi(parts[1])
	if err != nil || index <= 0 || parts[1] != strconv.Itoa(index) {
		return CanonicalGenerationFile{}, false
	}

	kind := GenerationFileKindCompressed
	if generationExtension == "."+baseExtension {
		kind = GenerationFileKindUncompressed
	}
	return CanonicalGenerationFile{
		Period: parts[0],
		Index:  uint64(index),
		Kind:   kind,
	}, true
}
