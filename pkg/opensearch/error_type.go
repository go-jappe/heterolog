package opensearch

// ErrorTypeはOpenSearchがerror responseのtype fieldへ返す機械判定用の識別値です。
type ErrorType string

const (
	// ErrorTypeMapperParsing は OpenSearch が mapping 変換失敗時に返す error type です。
	ErrorTypeMapperParsing ErrorType = "mapper_parsing_exception"

	// ErrorTypeDocumentParsing は OpenSearch が document parse 失敗時に返す error type です。
	ErrorTypeDocumentParsing ErrorType = "document_parsing_exception"

	// ErrorTypeXContentParse は OpenSearch が x_content parse 失敗時に返す error type です。
	ErrorTypeXContentParse ErrorType = "x_content_parse_exception"

	// ErrorTypeParse は OpenSearch が parse 失敗時に返す error type です。
	ErrorTypeParse ErrorType = "parse_exception"

	// ErrorTypeIllegalArgument は OpenSearch が不正引数時に返す error type です。
	ErrorTypeIllegalArgument ErrorType = "illegal_argument_exception"

	// ErrorTypeStrictDynamicMappingはdynamic=strictのmappingへ未定義fieldを送った場合の
	// error typeです。同じpayloadを再送しても解消しないためquarantine分類に使用します。
	ErrorTypeStrictDynamicMapping ErrorType = "strict_dynamic_mapping_exception"
)
