package logpattern

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

type GlobMode int

const (
	GlobAllFiles GlobMode = iota
	GlobByTarget
)

// GlobFileNamesはf.Dirを検索し、modeに応じてこのfile名patternのtargetだけ、または
// 同じ拡張子を持つ全targetの未圧縮fileと圧縮済みfileを返します。
// 圧縮済みfileは現在の設定形式や固定catalogに限定せず、base extensionの直後に安全な
// 一段suffixを持つfileを対象にします。これにより、利用者が追加した形式や変更前の形式も
// upload・purge対象から外れず、圧縮途中の`.gz.tmp`などは対象に含めません。
// 大小文字はf.Dirの規則（fのEffectiveNameCase）で照合し、返却pathは実際の表記を保ちます。
func GlobFileNames(f FileNamePattern, mode GlobMode) ([]string, error) {
	switch mode {
	case GlobByTarget, GlobAllFiles:
	default:
		return nil, nil
	}

	// 任意の日付・番号部分だけを許容し、prefix/target/extensionは文字どおり照合します。
	nameCase := f.EffectiveNameCase()
	candidates, err := findDirectoryFileNames(f.Dir, fileNameMatcher(f, mode, nameCase))
	if err != nil {
		return nil, fmt.Errorf("failed to list files (.%s): %w", f.Extension, err)
	}

	// 従来どおり、未圧縮fileの名前順、その後に圧縮fileの名前順で返します。
	var matches, compressed []string
	for _, path := range candidates {
		if isCompressedGenerationFile(path, f.Extension, nameCase) {
			compressed = append(compressed, path)
		} else {
			matches = append(matches, path)
		}
	}
	return append(matches, compressed...), nil
}

// GlobLifecycleFileNamesは、通常のログ名と圧縮元の削除待ち名を一回の一覧取得で返します。
// 通常のGlobFileNamesと同じ順序の後ろに、削除待ち名を名前順で追加します。
// 型の検査は行いません。呼び出し元は削除待ちを通常の圧縮・purge候補から分けてください。
// 大小文字はGlobFileNamesと同じくf.Dirの規則で照合します。
func GlobLifecycleFileNames(f FileNamePattern, mode GlobMode) ([]string, error) {
	nameCase := f.EffectiveNameCase()
	var prefix string
	switch mode {
	case GlobByTarget:
		prefix = nameCase.Key(f.Prefix + f.Target + "_")
	case GlobAllFiles:
	default:
		return nil, fmt.Errorf("unsupported appendlog lifecycle glob mode: %d", mode)
	}
	matchName := fileNameMatcher(f, mode, nameCase)
	paths, err := findDirectoryFileNames(f.Dir, func(name string) bool {
		return matchName(name) ||
			matchesCompressionSourceDeletePendingName(name, f.Extension, prefix, nameCase)
	})
	if err != nil {
		return nil, fmt.Errorf("list appendlog lifecycle files: %w", err)
	}
	var raw, compressed, pending []string
	for _, path := range paths {
		switch {
		case isCompressionSourceDeletePendingPath(path, nameCase):
			pending = append(pending, path)
		case isCompressedGenerationFile(path, f.Extension, nameCase):
			compressed = append(compressed, path)
		default:
			raw = append(raw, path)
		}
	}
	return append(append(raw, compressed...), pending...), nil
}

// TrimBaseAndOptionalCompressionExtensionsは、file名末尾のbase extensionと、その直後に
// 任意で一つ付く圧縮extensionを除き、拡張子を含まないfile名を返します。
// 例えばbaseExtensionが`log`の場合、`app_20260804_1.log`と
// `app_20260804_1.log.gz`はいずれも`app_20260804_1`になります。
//
// 圧縮extensionは利用者が追加できるためformat名のcatalogとは照合せず、安全な一つの
// filename componentであることだけを検証します。`.tmp`はatomic圧縮の一時file用に
// 予約し、`.log.gz.tmp`のような複数段extensionも未commit残骸として拒否します。
// base extensionの大小文字は、nameの親ディレクトリ（ファイル名だけなら現在のディレクトリ）の
// 規則で照合します。
func TrimBaseAndOptionalCompressionExtensions(name string, baseExtension string) (string, bool) {
	return trimBaseAndOptionalCompressionExtensions(name, baseExtension, pathNameCase(name))
}

func trimBaseAndOptionalCompressionExtensions(
	name string,
	baseExtension string,
	nameCase pathidentity.NameCase,
) (string, bool) {
	stem, _, ok := fsstore.SplitBaseAndOptionalCompressionExtensionsForNameCase(name, baseExtension, nameCase)
	return stem, ok
}

// IsCompressedGenerationFileは、pathがbase extensionの直後に一段の有効な圧縮suffixを
// 持つgeneration fileかを返します。未圧縮fileと`.tmp`などの未commit残骸はfalseです。
// base extensionの大小文字は、pathの親ディレクトリの規則で照合します。
func IsCompressedGenerationFile(path string, baseExtension string) bool {
	return isCompressedGenerationFile(path, baseExtension, pathNameCase(path))
}

func isCompressedGenerationFile(path string, baseExtension string, nameCase pathidentity.NameCase) bool {
	_, extension, ok := fsstore.SplitBaseAndOptionalCompressionExtensionsForNameCase(
		filepath.Base(path),
		baseExtension,
		nameCase,
	)
	if !ok {
		return false
	}
	baseExtension = strings.TrimPrefix(baseExtension, ".")
	return extension != "."+baseExtension
}

// pathNameCaseは、patternを受け取らない名前の判定で、pathの親ディレクトリの規則を返します。
// ファイル名だけを渡された場合は、ファイル一覧の取得と同じく現在のディレクトリで判定します。
func pathNameCase(path string) pathidentity.NameCase {
	return pathidentity.DirectoryNameCase(filepath.Dir(path))
}
