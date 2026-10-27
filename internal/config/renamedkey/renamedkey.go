// Package renamedkeyは、公開前に綴りを修正した環境変数keyと、値の意味を変えて名前を
// 変えたkeyについて、旧名と現在のkey名の対応を定義します。設定loader（internal/config）と、試験で設定項目を隔離する
// internal/testutilが同じ規則を使えるよう、他のpackageに依存しないleaf packageにしています。
//
// 対象は、internal logの通知先（HETEROLOG_*_{ERROR,WARN,INFO,DEBUG}_HANDLER）、
// HETEROLOG_MEMORY_CRITICAL_RATIO、HETEROLOG_MAX_*_SPOOL_SCANNED_CACHE_USAGE_COUNTの旧綴りと、
// 初回を含む試行回数から再試行の回数へ意味を変えたHETEROLOG_MAX_*_RETRYABLE_CLIENT_RETRIESの
// 旧名（HETEROLOG_MAX_*_RETRYABLE_CLIENT_ATTEMPTS）です。
// 利用元のアプリの環境変数を旧名と取り違えないよう、HETEROLOG_で始まるkeyだけを対象にします。
package renamedkey

import "strings"

// keyPrefixは、設定loaderが読む環境変数keyに共通の接頭辞です。
const keyPrefix = "HETEROLOG_"

// 旧名の一部です。検索で正しい綴りと混同しないよう、旧名は分割した文字列から組み立てます。
const (
	misspelledHandlerSuffix          = "_HAND" + "ER"
	misspelledMemoryCriticalRatioKey = keyPrefix + "MEMORY_CRITICAL_RAI" + "TO"
	misspelledScannedCacheSuffix     = "_SPOOL_SCAN" + "ED_CACHE_USAGE_COUNT"
	// retryAttemptsSuffixは、再試行の設定を初回を含む試行回数で表していた旧名の末尾です。
	retryAttemptsSuffix = "_RETRYABLE_CLIENT_ATTEMPTS"
)

const (
	handlerSuffix          = "_HANDLER"
	memoryCriticalRatioKey = keyPrefix + "MEMORY_CRITICAL_RATIO"
	scannedCacheSuffix     = "_SPOOL_SCANNED_CACHE_USAGE_COUNT"
	scannedCachePrefix     = keyPrefix + "MAX_"
	retryRetriesSuffix     = "_RETRYABLE_CLIENT_RETRIES"
)

// handlerLevelsは、internal logの通知先keyの名前に含まれるlog levelです。
var handlerLevels = []string{"ERROR", "WARN", "INFO", "DEBUG"}

// Currentは、旧名のkeyに対応する現在のkey名を返します。keyが旧名でなければfalseを返します。
// 現在のkey名は旧名として扱いません。
func Current(key string) (string, bool) {
	if key == misspelledMemoryCriticalRatioKey {
		return memoryCriticalRatioKey, true
	}
	for _, level := range handlerLevels {
		oldSuffix := "_" + level + misspelledHandlerSuffix
		if strings.HasPrefix(key, keyPrefix) && strings.HasSuffix(key, oldSuffix) && len(key) > len(keyPrefix)+len(oldSuffix) {
			return strings.TrimSuffix(key, misspelledHandlerSuffix) + handlerSuffix, true
		}
	}
	if strings.HasPrefix(key, scannedCachePrefix) && strings.HasSuffix(key, misspelledScannedCacheSuffix) &&
		len(key) > len(scannedCachePrefix)+len(misspelledScannedCacheSuffix) {
		return strings.TrimSuffix(key, misspelledScannedCacheSuffix) + scannedCacheSuffix, true
	}
	if strings.HasPrefix(key, scannedCachePrefix) && strings.HasSuffix(key, retryAttemptsSuffix) &&
		len(key) > len(scannedCachePrefix)+len(retryAttemptsSuffix) {
		return strings.TrimSuffix(key, retryAttemptsSuffix) + retryRetriesSuffix, true
	}
	return "", false
}

// Oldは、現在のkey名に対応する旧名を返します。綴りを修正したkeyでなければfalseを返します。
// 試験で旧名を組み立てるために使い、Current(Old(key))はkeyを返します。
func Old(current string) (string, bool) {
	if current == memoryCriticalRatioKey {
		return misspelledMemoryCriticalRatioKey, true
	}
	for _, level := range handlerLevels {
		currentSuffix := "_" + level + handlerSuffix
		if strings.HasPrefix(current, keyPrefix) && strings.HasSuffix(current, currentSuffix) && len(current) > len(keyPrefix)+len(currentSuffix) {
			return strings.TrimSuffix(current, handlerSuffix) + misspelledHandlerSuffix, true
		}
	}
	if strings.HasPrefix(current, scannedCachePrefix) && strings.HasSuffix(current, scannedCacheSuffix) &&
		len(current) > len(scannedCachePrefix)+len(scannedCacheSuffix) {
		return strings.TrimSuffix(current, scannedCacheSuffix) + misspelledScannedCacheSuffix, true
	}
	if strings.HasPrefix(current, scannedCachePrefix) && strings.HasSuffix(current, retryRetriesSuffix) &&
		len(current) > len(scannedCachePrefix)+len(retryRetriesSuffix) {
		return strings.TrimSuffix(current, retryRetriesSuffix) + retryAttemptsSuffix, true
	}
	return "", false
}
