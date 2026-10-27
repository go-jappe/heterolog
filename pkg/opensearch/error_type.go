package opensearch

// ErrorType is the machine-readable identifier that OpenSearch returns in the
// type field of an error response.
type ErrorType string

const (
	// ErrorTypeMapperParsing is the error type that OpenSearch returns when
	// mapping conversion fails.
	ErrorTypeMapperParsing ErrorType = "mapper_parsing_exception"

	// ErrorTypeDocumentParsing is the error type that OpenSearch returns when
	// parsing a document fails.
	ErrorTypeDocumentParsing ErrorType = "document_parsing_exception"

	// ErrorTypeXContentParse is the error type that OpenSearch returns when
	// x_content parsing fails.
	ErrorTypeXContentParse ErrorType = "x_content_parse_exception"

	// ErrorTypeParse is the error type that OpenSearch returns when parsing fails.
	ErrorTypeParse ErrorType = "parse_exception"

	// ErrorTypeIllegalArgument is the error type that OpenSearch returns for an
	// illegal argument.
	ErrorTypeIllegalArgument ErrorType = "illegal_argument_exception"

	// ErrorTypeStrictDynamicMapping is the error type for sending an undefined
	// field to a mapping with dynamic=strict. Sending the same payload again does
	// not resolve it, so it is used for the quarantine classification.
	ErrorTypeStrictDynamicMapping ErrorType = "strict_dynamic_mapping_exception"
)
