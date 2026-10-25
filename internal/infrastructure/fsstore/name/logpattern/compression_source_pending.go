package logpattern

import (
	"fmt"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"strings"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// CompressionSourceDeletePendingSuffixは、正常圧縮後にcanonical raw名から
// 退避され、削除だけが残ったsource用の予約suffixです。通常のmanaged
// file globには含めませんが、同じlogical generationの新しいraw fileを
// 再作成しないよう、index選択では使用済みpathとして扱います。
const CompressionSourceDeletePendingSuffix = ".compression-source-delete-pending.tmp"

// CompressionSourceDeletePendingPathは、raw sourceを圧縮完了後の削除待ちへ
// 遷移させる予約pathを返します。
func CompressionSourceDeletePendingPath(sourcePath string) string {
	return sourcePath + CompressionSourceDeletePendingSuffix
}

// IsCompressionSourceDeletePendingPathは、pathが圧縮完了後のsource削除待ち用に
// 予約したsuffixを持つかを返します。suffixの大小文字はpathの親ディレクトリの規則で照合します。
func IsCompressionSourceDeletePendingPath(path string) bool {
	return isCompressionSourceDeletePendingPath(path, pathNameCase(path))
}

func isCompressionSourceDeletePendingPath(path string, nameCase pathidentity.NameCase) bool {
	_, matched := fsstore.TrimFileNameSuffixForNameCase(path, CompressionSourceDeletePendingSuffix, nameCase)
	return path != "" && matched
}

// CompressionSourcePathFromDeletePendingPathは、予約pending pathから元のcanonical
// source pathを復元します。予約suffixを持たないpathと、復元結果が空になるsuffix単体は拒否します。
// suffixの大小文字はpathの親ディレクトリの規則で照合し、復元したpathは元の表記を保ちます。
func CompressionSourcePathFromDeletePendingPath(path string) (string, bool) {
	return compressionSourcePathFromDeletePendingPath(path, pathNameCase(path))
}

func compressionSourcePathFromDeletePendingPath(path string, nameCase pathidentity.NameCase) (string, bool) {
	sourcePath, matched := fsstore.TrimFileNameSuffixForNameCase(path, CompressionSourceDeletePendingSuffix, nameCase)
	if !matched || sourcePath == "" {
		return "", false
	}
	return sourcePath, true
}

// FindCompressionSourceDeletePendingPathsは、指定file patternに対応するsource削除待ち
// pathを列挙します。通常のGlobFileNamesとは別の予約patternだけを検索するため、pending
// sourceを圧縮済みfileやgeneration purge対象へ混入させません。
// 大小文字はfileName.Dirの規則（fileNameのEffectiveNameCase）で照合します。
func FindCompressionSourceDeletePendingPaths(fileName FileNamePattern, mode GlobMode) ([]string, error) {
	nameCase := fileName.EffectiveNameCase()
	var prefix string
	switch mode {
	case GlobByTarget:
		prefix = nameCase.Key(fileName.Prefix + fileName.Target + "_")
	case GlobAllFiles:
	default:
		return nil, fmt.Errorf("unsupported appendlog glob mode for pending compression sources: %d", mode)
	}
	paths, err := findDirectoryFileNames(fileName.Dir, func(name string) bool {
		return matchesCompressionSourceDeletePendingName(name, fileName.Extension, prefix, nameCase)
	})
	if err != nil {
		return nil, fmt.Errorf("list appendlog compression source delete-pending files: %w", err)
	}
	return paths, nil
}

func matchesCompressionSourceDeletePendingName(
	name, baseExtension, prefix string,
	nameCase pathidentity.NameCase,
) bool {
	source, ok := compressionSourcePathFromDeletePendingPath(name, nameCase)
	if !ok {
		return false
	}
	stem, ok := fsstore.TrimFileNameSuffixForNameCase(source, "."+baseExtension, nameCase)
	return ok && strings.HasPrefix(nameCase.Key(stem), prefix)
}
