package config

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// validateStorageDirectoryOwnership は、filesystem上の保存領域が安全に分離されて
// いることを検証します。
//
// 検証方針は次のとおりです。
//   - rawな環境変数値ではなく、実際にstoreが使用する解決済みpathを比較する
//   - 有効なlogger、選択されたbackend、有効なfailure storeだけを対象にする
//   - durable typeで選択されていないarchive/spoolの設定値は対象外にする
//   - 同一pathだけでなく、一方が他方を内包するpathも衝突として扱う
//   - endpointやbackendが同じでも、異なるowner間のdirectory共有は許可しない
//   - S3のbucket/prefixは検証対象外とし、異なるowner間の重複を許容する
//
// archiveや通常ログのdirectoryを複数の所有者が共有すると、それぞれのrotation、
// purge、uploadが他方のファイルを処理する可能性があります。record spoolで異なる
// backendのqueueが衝突すると誤送信につながります。compressed log file upload failure spoolは
// 元file名を実行時のfile系列へ照合するため、rootとlifecycle coordinatorを共有できるのは
// 同じobject storage serviceかつLoggerContractKind内の意図した系列だけです。その共有rootは
// 一つのclaimにまとめ、それ以外のstoreとの衝突は起動時の設定エラーとして返します。
//
// claimの収集に一部失敗しても、収集済みclaim間の衝突は引き続き検証し、両方のerrorを
// まとめて返します。S3 object storageはこのアプリケーション外で所有権や上書きを
// 管理するため、同じbucket/prefixが設定されてもdirectory衝突エラーにはしません。
func validateStorageDirectoryOwnership(a AppConfig) error {
	claims, collectErr := a.collectStorageDirectoryClaims()
	return errors.Join(
		collectErr,
		validateStorageDirectoryClaims(claims),
	)
}

// validateStorageDirectoryClaimsは、収集済みのdirectory所有権宣言を全組み合わせで
// 比較し、検出した衝突をerrors.Joinでまとめて返します。最初の一件だけで終了しないため、
// 起動時に複数の設定ミスを一度に確認できます。
//
// 一つのclaimにまとめた意図的な共有を除き、file logger、failure archive、
// record spool、object-storage compressed log file storeはownerごとに独立したpolicyとlifecycleを
// 持つため、種類やbackend、再送先を問わずoverlapを禁止します。
func validateStorageDirectoryClaims(claims []storageDirectoryClaim) error {
	var errs []error
	for i := 0; i < len(claims); i++ {
		for j := i + 1; j < len(claims); j++ {
			left, right := claims[i], claims[j]
			if !storagePathsOverlap(left.path, right.path) {
				continue
			}
			errs = append(errs, newStorageDirectoryCollisionError(left, right))
		}
	}

	return errors.Join(errs...)
}

// storagePathsOverlap は完全一致に加え、どちらかがもう一方の祖先directoryである場合も
// overlapとして返します。例えば`/archive/loki`と`/archive/loki/retryable`を別々の
// lifecycle ownerへ割り当てることも禁止します。
func storagePathsOverlap(leftPath, rightPath string) bool {
	leftPath = pathidentity.ComparisonKey(leftPath)
	rightPath = pathidentity.ComparisonKey(rightPath)
	if leftPath == rightPath {
		return true
	}
	return isStoragePathWithin(leftPath, rightPath) || isStoragePathWithin(rightPath, leftPath)
}

// isStoragePathWithin は区切り単位の一致で親子関係を判定します。
// これにより`/data/log`と`/data/logger`のようなprefixだけが一致するpathを誤って
// 親子関係と判断しません。両方のpathにはComparisonKeyで大小文字の規則を適用済みのため、
// 表記どおりに比較します。Windowsのfilepath.Relは常に大小文字を区別しないため、
// 大小文字を区別するdirectoryの子を同じ保存先と誤判定します。
func isStoragePathWithin(parent, candidate string) bool {
	parent = filepath.Clean(parent)
	candidate = filepath.Clean(candidate)
	if parent == candidate {
		return false
	}
	prefix := parent
	if !strings.HasSuffix(prefix, string(filepath.Separator)) {
		prefix += string(filepath.Separator)
	}
	return strings.HasPrefix(candidate, prefix)
}
