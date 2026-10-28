package opensearch

const (
	SpanOpenSearchLoggerFlush  = "heterolog.opensearch.flush"
	SpanOpenSearchLoggerCreate = "heterolog.opensearch.create"
	// SpanOpenSearchBulkとSpanOpenSearchIndexは、Bulk APIとIndex APIへの1回の送信です。
	// client wrapperのretryを含みます。
	SpanOpenSearchBulk  = "heterolog.opensearch.bulk"
	SpanOpenSearchIndex = "heterolog.opensearch.index"
)
