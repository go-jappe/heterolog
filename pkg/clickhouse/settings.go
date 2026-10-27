package clickhouse

const (
	// SettingWaitEndOfQuery asks ClickHouse not to finalize the HTTP response
	// before the query completes. The client always sets it to 1 so that the HTTP
	// status reflects the delivery result of the INSERT as far as possible.
	SettingWaitEndOfQuery = "wait_end_of_query"

	// SettingInsertDeduplicate is the name of the ClickHouse setting that enables
	// INSERT deduplication.
	SettingInsertDeduplicate = "insert_deduplicate"

	// SettingInsertDeduplicationToken is the name of the ClickHouse setting that
	// identifies the same INSERT on retry.
	SettingInsertDeduplicationToken = "insert_deduplication_token"
)
