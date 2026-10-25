package clickhouse

const (
	// SettingWaitEndOfQueryはquery完了前にHTTP responseを確定しないようClickHouseへ要求します。
	// INSERTの送達結果をHTTP statusへ可能な限り反映させるため、clientが常に1を設定します。
	SettingWaitEndOfQuery = "wait_end_of_query"

	// SettingInsertDeduplicate は INSERT deduplication を有効化する ClickHouse setting 名です。
	SettingInsertDeduplicate = "insert_deduplicate"

	// SettingInsertDeduplicationToken は retry 時に同一 INSERT を識別する ClickHouse setting 名です。
	SettingInsertDeduplicationToken = "insert_deduplication_token"
)
