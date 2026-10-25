// Package pathidentityは、保存先の初期化時の実体パス解決と、パスの比較キー生成を行います。
// ResolveDirectoryでリンクを解決した結果を保持し、通常処理ではComparisonKeyで比較します。
// 比較時にリンク・8.3短縮名の解決は行いません。DarwinとLinuxでは既存祖先のfilesystem特性を読み取ります。
// Windowsの比較は標準のcase-insensitive directoryを対象とします。
// directory直下のfile名を比較する大小文字の規則は、同じ判定からDirectoryNameCaseで取得します。
// identity keyを実際のfile操作へ渡さず、操作対象の元pathは別に保持してください。
package pathidentity

import (
	"path/filepath"
	"runtime"
	"strings"

	"github.com/go-jappe/heterolog/internal/pathcaserule"
)

// AbsoluteKeyは、パスを絶対化・正規化した比較用キーを返します。
// Windowsの通常directoryでは大小文字とextended path表記を統一します。
// DarwinとLinuxでは対象filesystemのcase特性を使用します。リンクの解決は行いません。
func AbsoluteKey(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return platformKey(absolute), nil
}

// ComparisonKeyは、パス同士の比較に使うキーを返します。
// パスを絶対化して整理し、Windowsでは大小文字と通常の拡張パス表記の違いを揃えます。
// 空のパスは空のまま返します。絶対化に失敗した場合は、Clean済みのパスからキーを作ります。
// DarwinとLinuxでは既存祖先を読み取り、未作成の子にも同じcase特性を適用します。
// 戻り値は実際のファイル操作には使わないでください。
func ComparisonKey(path string) string {
	if path == "" {
		return ""
	}
	key, err := AbsoluteKey(path)
	if err != nil {
		return platformKey(filepath.Clean(path))
	}
	return key
}

// Containsは、Clean+Abs済みの元表記をkeyに持つsnapshotとのpath照合に使います。
// 完全一致を優先し、filesystem上のcase aliasやWindows extended表記も照合します。
// mapのkeyは書き換えないため、callerは元表記をfile操作に使い続けられます。
func Contains(paths map[string]struct{}, path string) bool {
	if len(paths) == 0 {
		return false
	}
	if _, exists := paths[path]; exists {
		return true
	}
	// purgeの除外集合はClean済みの相対pathも受け取るため、その契約も維持します。
	if _, exists := paths[filepath.Clean(path)]; exists {
		return true
	}
	key := ComparisonKey(path)
	if _, exists := paths[key]; exists {
		return true
	}
	// Linuxでは、大小文字を区別しないdirectoryを通るpathだけ別表記を探します。
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" || pathcaserule.Active() ||
		filesystemFoldsCase(AbsoluteOrCleanPath(path)) {
		for candidate := range paths {
			if ComparisonKey(candidate) == key {
				return true
			}
		}
	}
	return false
}

func platformKey(path string) string {
	if pathcaserule.Active() && filepath.IsAbs(path) {
		return overriddenPathKey(path)
	}
	if runtime.GOOS == "windows" {
		return windowsPathKey(path)
	}
	return filesystemPathKey(path)
}

// overriddenPathKeyは、試験用の規則（internal/pathcaserule）が設定されている間の比較keyです。
// 各階層の名前を、規則を差し替えたdirectoryではその規則、それ以外ではOSの既定の判定で揃えます。
// Windowsでは通常のextended表記とdriveの大小文字だけを先に揃えます。
func overriddenPathKey(path string) string {
	if runtime.GOOS == "windows" {
		path = windowsNormalPath(path)
		volume := filepath.VolumeName(path)
		path = strings.ToLower(volume) + path[len(volume):]
	}
	return caseKey(path, func(parent string) (bool, error) {
		if sensitive, ok := pathcaserule.Lookup(parent); ok {
			return sensitive, nil
		}
		return filesystemCaseSensitive(parent)
	})
}

// windowsNormalPathは、通常のdrive/UNC pathのextended表記を通常表記へ戻します。
// windowsPathKeyと同じ表記を対象にし、大小文字は変更しません。
func windowsNormalPath(path string) string {
	if len(path) >= 8 && strings.EqualFold(path[:8], `\\?\UNC\`) {
		return `\\` + path[8:]
	}
	if len(path) >= 7 && strings.HasPrefix(path, `\\?\`) &&
		((path[4] >= 'a' && path[4] <= 'z') || (path[4] >= 'A' && path[4] <= 'Z')) &&
		path[5] == ':' && path[6] == '\\' {
		return path[4:]
	}
	return path
}

// windowsPathKeyは通常のdrive/UNC pathだけをextended表記と統合します。
// volume GUIDなど別のdevice namespaceを通常drive pathとして解釈し直しません。
func windowsPathKey(path string) string {
	key := strings.ToLower(path)
	if strings.HasPrefix(key, `\\?\unc\`) {
		return `\\` + key[len(`\\?\unc\`):]
	}
	if len(key) >= 7 && strings.HasPrefix(key, `\\?\`) &&
		key[4] >= 'a' && key[4] <= 'z' && key[5] == ':' && key[6] == '\\' {
		return key[4:]
	}
	return key
}
