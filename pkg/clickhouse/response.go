package clickhouse

import "strings"

// ErrorTypeはClickHouseがDB::Exceptionの末尾に返す機械判定用のerror typeです。
// HTTP statusだけでは入力不正と一時障害を区別できない場合に使用します。
type ErrorType string

const (
	// ErrorTypeUnknownTable は ClickHouse が存在しない table 参照に対して返す error type です。
	ErrorTypeUnknownTable ErrorType = "UNKNOWN_TABLE"

	// ErrorTypeUnknownDatabase は ClickHouse が存在しない database 参照に対して返す error type です。
	ErrorTypeUnknownDatabase ErrorType = "UNKNOWN_DATABASE"

	// ErrorTypeUnknownIdentifier は ClickHouse が存在しない column / identifier 参照に対して返す error type です。
	ErrorTypeUnknownIdentifier ErrorType = "UNKNOWN_IDENTIFIER"

	// ErrorTypeUnknownFunction は ClickHouse が存在しない function 参照に対して返す error type です。
	ErrorTypeUnknownFunction ErrorType = "UNKNOWN_FUNCTION"

	// ErrorTypeUnknownType は ClickHouse が存在しない type 参照に対して返す error type です。
	ErrorTypeUnknownType ErrorType = "UNKNOWN_TYPE"

	// ErrorTypeNoSuchColumnInTable は ClickHouse が table 内に存在しない column 参照に対して返す error type です。
	ErrorTypeNoSuchColumnInTable ErrorType = "NO_SUCH_COLUMN_IN_TABLE"

	// ErrorTypeThereIsNoColumn は ClickHouse が存在しない column 参照に対して返す error type です。
	ErrorTypeThereIsNoColumn ErrorType = "THERE_IS_NO_COLUMN"

	// ErrorTypeSyntaxError は ClickHouse が query 構文不正に対して返す error type です。
	ErrorTypeSyntaxError ErrorType = "SYNTAX_ERROR"

	// ErrorTypeTypeMismatch は ClickHouse が型不一致に対して返す error type です。
	ErrorTypeTypeMismatch ErrorType = "TYPE_MISMATCH"

	// ErrorTypeIllegalTypeOfArgument は ClickHouse が関数引数などの型不正に対して返す error type です。
	ErrorTypeIllegalTypeOfArgument ErrorType = "ILLEGAL_TYPE_OF_ARGUMENT"

	// ErrorTypeBadArguments は ClickHouse が不正引数に対して返す error type です。
	ErrorTypeBadArguments ErrorType = "BAD_ARGUMENTS"

	// ErrorTypeBadCast は ClickHouse が不正な cast に対して返す error type です。
	ErrorTypeBadCast ErrorType = "BAD_CAST"

	// ErrorTypeCannotConvertType は ClickHouse が変換不能な型に対して返す error type です。
	ErrorTypeCannotConvertType ErrorType = "CANNOT_CONVERT_TYPE"

	// ErrorTypeCannotParseInputAssertionFailed は ClickHouse が入力 parse 不正に対して返す error type です。
	ErrorTypeCannotParseInputAssertionFailed ErrorType = "CANNOT_PARSE_INPUT_ASSERTION_FAILED"

	// ErrorTypeCannotParseQuotedString は ClickHouse が quoted string parse 不正に対して返す error type です。
	ErrorTypeCannotParseQuotedString ErrorType = "CANNOT_PARSE_QUOTED_STRING"

	// ErrorTypeCannotParseNumber は ClickHouse が数値 parse 不正に対して返す error type です。
	ErrorTypeCannotParseNumber ErrorType = "CANNOT_PARSE_NUMBER"

	// ErrorTypeCannotParseDate は ClickHouse が date parse 不正に対して返す error type です。
	ErrorTypeCannotParseDate ErrorType = "CANNOT_PARSE_DATE"

	// ErrorTypeCannotParseDateTime は ClickHouse が datetime parse 不正に対して返す error type です。
	ErrorTypeCannotParseDateTime ErrorType = "CANNOT_PARSE_DATETIME"

	// ErrorTypeIncorrectData は ClickHouse が入力 data 不正に対して返す error type です。
	ErrorTypeIncorrectData ErrorType = "INCORRECT_DATA"

	// ErrorTypeUnknownFormat は ClickHouse が未知の input/output format に対して返す error type です。
	ErrorTypeUnknownFormat ErrorType = "UNKNOWN_FORMAT"

	// ErrorTypeBlocksHaveDifferentStructure は ClickHouse が block structure 不一致に対して返す error type です。
	ErrorTypeBlocksHaveDifferentStructure ErrorType = "BLOCKS_HAVE_DIFFERENT_STRUCTURE"

	// ErrorTypeTooFewColumns は ClickHouse が column 数不足に対して返す error type です。
	ErrorTypeTooFewColumns ErrorType = "TOO_FEW_COLUMNS"

	// ErrorTypeTooManyColumns は ClickHouse が column 数過多に対して返す error type です。
	ErrorTypeTooManyColumns ErrorType = "TOO_MANY_COLUMNS"

	// ErrorTypeNumberOfColumnsDoesntMatch は ClickHouse が column 数不一致に対して返す error type です。
	ErrorTypeNumberOfColumnsDoesntMatch ErrorType = "NUMBER_OF_COLUMNS_DOESNT_MATCH"

	// ErrorTypeUnknownSetting は ClickHouse が未知の setting に対して返す error type です。
	ErrorTypeUnknownSetting ErrorType = "UNKNOWN_SETTING"

	// ErrorTypeReadonlySetting は ClickHouse が変更不可 setting に対して返す error type です。
	ErrorTypeReadonlySetting ErrorType = "READONLY_SETTING"

	// ErrorTypeSettingConstraintViolation は ClickHouse が setting 制約違反に対して返す error type です。
	ErrorTypeSettingConstraintViolation ErrorType = "SETTING_CONSTRAINT_VIOLATION"

	// 以下はJSONEachRowの入力値、column構成、設定値を同じまま再送しても解消しないerror typeです。
	ErrorTypeCannotParseText                           ErrorType = "CANNOT_PARSE_TEXT"
	ErrorTypeIncorrectNumberOfColumns                  ErrorType = "INCORRECT_NUMBER_OF_COLUMNS"
	ErrorTypeSizesOfColumnsDoesntMatch                 ErrorType = "SIZES_OF_COLUMNS_DOESNT_MATCH"
	ErrorTypeNotFoundColumnInBlock                     ErrorType = "NOT_FOUND_COLUMN_IN_BLOCK"
	ErrorTypeDuplicateColumn                           ErrorType = "DUPLICATE_COLUMN"
	ErrorTypeSizeOfFixedStringDoesntMatch              ErrorType = "SIZE_OF_FIXED_STRING_DOESNT_MATCH"
	ErrorTypeCannotParseEscapeSequence                 ErrorType = "CANNOT_PARSE_ESCAPE_SEQUENCE"
	ErrorTypeIllegalColumn                             ErrorType = "ILLEGAL_COLUMN"
	ErrorTypeFormatIsNotSuitableForInput               ErrorType = "FORMAT_IS_NOT_SUITABLE_FOR_INPUT"
	ErrorTypeEmptyDataPassed                           ErrorType = "EMPTY_DATA_PASSED"
	ErrorTypeCannotInsertValueOfDifferentSizeIntoTuple ErrorType = "CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE"
	ErrorTypeIncompatibleColumns                       ErrorType = "INCOMPATIBLE_COLUMNS"
	ErrorTypeCannotReadArrayFromText                   ErrorType = "CANNOT_READ_ARRAY_FROM_TEXT"
	ErrorTypeTooLargeStringSize                        ErrorType = "TOO_LARGE_STRING_SIZE"
	ErrorTypeCannotParseUUID                           ErrorType = "CANNOT_PARSE_UUID"
	ErrorTypeBadQueryParameter                         ErrorType = "BAD_QUERY_PARAMETER"
	ErrorTypeCannotParseBool                           ErrorType = "CANNOT_PARSE_BOOL"
	ErrorTypeViolatedConstraint                        ErrorType = "VIOLATED_CONSTRAINT"
	ErrorTypeInvalidSettingValue                       ErrorType = "INVALID_SETTING_VALUE"
	ErrorTypeCannotParseIPv4                           ErrorType = "CANNOT_PARSE_IPV4"
	ErrorTypeCannotParseIPv6                           ErrorType = "CANNOT_PARSE_IPV6"
	ErrorTypeUnknownElementOfEnum                      ErrorType = "UNKNOWN_ELEMENT_OF_ENUM"
	ErrorTypeIncompatibleSchema                        ErrorType = "INCOMPATIBLE_SCHEMA"
)

// parseClickHouseExceptionはClickHouseの標準的なDB::Exception本文から数値codeと
// error typeを取り出します。本文にはqueryやversion由来の括弧が含まれ得るため、
// error typeには最後に現れる大文字・数字・underscoreだけのtokenを使用します。
func parseClickHouseException(body []byte) (int, ErrorType) {
	text := string(body)
	return parseClickHouseExceptionCode(text), parseClickHouseErrorType(text)
}

func parseClickHouseExceptionCode(body string) int {
	const marker = "Code:"
	start := strings.Index(body, marker)
	if start < 0 {
		return 0
	}
	remainder := strings.TrimLeft(body[start+len(marker):], " \t\r\n")
	end := 0
	for end < len(remainder) && remainder[end] >= '0' && remainder[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0
	}
	code, ok := parsePositiveInt(remainder[:end])
	if !ok {
		return 0
	}
	return code
}

func parseClickHouseErrorType(body string) ErrorType {
	var found ErrorType
	for {
		start := strings.IndexByte(body, '(')
		if start < 0 {
			return found
		}
		body = body[start+1:]
		end := strings.IndexByte(body, ')')
		if end < 0 {
			return found
		}
		token := strings.TrimSpace(body[:end])
		if isClickHouseErrorTypeToken(token) {
			found = ErrorType(token)
		}
		body = body[end+1:]
	}
}

func isClickHouseErrorTypeToken(token string) bool {
	if token == "" || strings.HasPrefix(strings.ToLower(token), "version ") {
		return false
	}
	hasLetter := false
	for _, character := range token {
		switch {
		case character >= 'A' && character <= 'Z':
			hasLetter = true
		case character >= '0' && character <= '9':
		case character == '_':
		default:
			return false
		}
	}
	return hasLetter
}
