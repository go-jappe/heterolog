package renamedkey

import "testing"

// 旧名と現在のkey名を相互に対応付け、現在のkey名や規則に一致しない名前を旧名として
// 扱わないことを確認します。旧名は検索で正しい綴りと混同しないよう、Oldで組み立てます。
func TestCurrentAndOldMapRenamedKeysBothWays(t *testing.T) {
	for _, current := range []string{
		"HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
		"HETEROLOG_ASYNC_LOG_INTERNAL_LOG_WARN_HANDLER",
		"HETEROLOG_DELIVERY_FILE_LOGGER_INFO_HANDLER",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_DEBUG_HANDLER",
		"HETEROLOG_MEMORY_CRITICAL_RATIO",
		"HETEROLOG_MAX_ASYNC_LOKI_SPOOL_SCANNED_CACHE_USAGE_COUNT",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_RETRIES",
	} {
		old, ok := Old(current)
		if !ok || old == current {
			t.Fatalf("Old(%s) = %q, %t; want a different misspelled name", current, old, ok)
		}
		if got, ok := Current(old); !ok || got != current {
			t.Errorf("Current(Old(%s)) = %q, %t; want %q", current, got, ok, current)
		}
		if got, ok := Current(current); ok {
			t.Errorf("current key %s is treated as renamed to %s", current, got)
		}
	}
}

// 綴りを修正する前の旧名を規則ごとに一つ固定し、対応表が旧綴りを保持していることを確認します。
// Oldから旧名を組み立てる上の試験だけでは、表の旧綴りを誤っても検出できないためです。
// 検索で正しい綴りと混同しないよう、旧名は文字列を分けて連結します。
func TestCurrentAndOldKeepPreRenameSpellings(t *testing.T) {
	for _, tt := range []struct{ old, current string }{
		{old: "HETEROLOG_STDOUT_LOGGER_ERROR_HAND" + "ER", current: "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER"},
		{old: "HETEROLOG_MEMORY_CRITICAL_RAI" + "TO", current: "HETEROLOG_MEMORY_CRITICAL_RATIO"},
		{old: "HETEROLOG_MAX_OPENSEARCH_SPOOL_SCAN" + "ED_CACHE_USAGE_COUNT", current: "HETEROLOG_MAX_OPENSEARCH_SPOOL_SCANNED_CACHE_USAGE_COUNT"},
		{old: "HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_ATTEM" + "PTS", current: "HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_RETRIES"},
	} {
		if got, ok := Current(tt.old); !ok || got != tt.current {
			t.Errorf("Current(%s) = %q, %t; want %q", tt.old, got, ok, tt.current)
		}
		if got, ok := Old(tt.current); !ok || got != tt.old {
			t.Errorf("Old(%s) = %q, %t; want %q", tt.current, got, ok, tt.old)
		}
	}
}

func TestCurrentAndOldIgnoreNamesOutsideTheRules(t *testing.T) {
	handlerOld, _ := Old("HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER")
	bareHandler := "HETEROLOG" + handlerOld[len("HETEROLOG_STDOUT_LOGGER"):] // 接頭辞の後が「_ERROR_…」だけの名前
	scannedOld, _ := Old("HETEROLOG_MAX_LOKI_SPOOL_SCANNED_CACHE_USAGE_COUNT")
	bareScanned := "HETEROLOG_MAX" + scannedOld[len("HETEROLOG_MAX_LOKI"):] // MAX_の後にbackend名がない名前
	for _, key := range []string{
		bareHandler,
		bareScanned,
		"HETEROLOG_ERROR_HANDLER",
		"HETEROLOG_STDOUT_LOGGER_TRACE_HANDLER",
		"HETEROLOG_MAX_SPOOL_SCANNED_CACHE_USAGE_COUNT",
		"HETEROLOG_LOKI_SPOOL_SCANNED_CACHE_USAGE_COUNT",
		"HETEROLOG_TESTUTIL_ERROR_HANDLING",
	} {
		if got, ok := Current(key); ok {
			t.Errorf("Current(%q) = %q, true; want not renamed", key, got)
		}
		if got, ok := Old(key); ok {
			t.Errorf("Old(%q) = %q, true; want no misspelled name", key, got)
		}
	}
}

// 利用元のアプリが使う、HETEROLOG_で始まらない環境変数は、旧名の規則に一致しても旧名として
// 扱いません。heterologの設定keyと取り違えて設定エラーにしないためです。
func TestCurrentIgnoresNamesWithoutTheHeterologPrefix(t *testing.T) {
	for _, key := range []string{
		"STDOUT_LOGGER_ERROR_HAND" + "ER",
		"APP_WORKER_WARN_HAND" + "ER",
		"MEMORY_CRITICAL_RAI" + "TO",
		"MAX_OPENSEARCH_SPOOL_SCAN" + "ED_CACHE_USAGE_COUNT",
	} {
		if got, ok := Current(key); ok {
			t.Errorf("Current(%q) = %q, true; want names without HETEROLOG_ left to the application", key, got)
		}
	}
	for _, key := range []string{"STDOUT_LOGGER_ERROR_HANDLER", "MEMORY_CRITICAL_RATIO", "MAX_OPENSEARCH_SPOOL_SCANNED_CACHE_USAGE_COUNT"} {
		if got, ok := Old(key); ok {
			t.Errorf("Old(%q) = %q, true; want no misspelled name for a name without HETEROLOG_", key, got)
		}
	}
}
