// Package filelifecycle は、通常FileLoggerとfailure archiveの解決済みlifecycle modeごとに
// 実行時に有効となる責務を定義します。defaultはconfig loaderで適用し、
// 使用対象の設定に残った空値・未知値はmapped validationで拒否します。
// inline lifecycleを持たないDelivery FileLoggerは、このpolicyの対象外です。
package filelifecycle

import pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

// UsesUsageCacheは、directory使用量の概算cacheを実際に使うmodeだけを返します。
// 空値にはdefaultを補わずfalseを返します。
func UsesUsageCache(mode pkgfsstore.FileLifecycleMode) bool {
	switch mode {
	case pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner:
		return true
	default:
		return false
	}
}

// UsesObjectStorageUploadは、inline処理か専用runnerのどちらかがarchive uploadを
// 所有するmodeを返します。空値にはdefaultを補わずfalseを返します。
func UsesObjectStorageUpload(mode pkgfsstore.FileLifecycleMode) bool {
	switch mode {
	case pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner:
		return true
	default:
		return false
	}
}
