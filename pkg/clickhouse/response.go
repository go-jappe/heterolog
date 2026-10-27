package clickhouse

import "strings"

// ErrorType is the machine-readable error type that ClickHouse returns at the
// end of a DB::Exception. It is used when the HTTP status alone cannot
// distinguish invalid input from a temporary failure.
type ErrorType string

const (
	// ErrorTypeUnknownTable is the error type that ClickHouse returns for a
	// reference to a table that does not exist.
	ErrorTypeUnknownTable ErrorType = "UNKNOWN_TABLE"

	// ErrorTypeUnknownDatabase is the error type that ClickHouse returns for a
	// reference to a database that does not exist.
	ErrorTypeUnknownDatabase ErrorType = "UNKNOWN_DATABASE"

	// ErrorTypeUnknownIdentifier is the error type that ClickHouse returns for a
	// reference to a column or identifier that does not exist.
	ErrorTypeUnknownIdentifier ErrorType = "UNKNOWN_IDENTIFIER"

	// ErrorTypeUnknownFunction is the error type that ClickHouse returns for a
	// reference to a function that does not exist.
	ErrorTypeUnknownFunction ErrorType = "UNKNOWN_FUNCTION"

	// ErrorTypeUnknownType is the error type that ClickHouse returns for a
	// reference to a type that does not exist.
	ErrorTypeUnknownType ErrorType = "UNKNOWN_TYPE"

	// ErrorTypeNoSuchColumnInTable is the error type that ClickHouse returns for a
	// reference to a column that does not exist in the table.
	ErrorTypeNoSuchColumnInTable ErrorType = "NO_SUCH_COLUMN_IN_TABLE"

	// ErrorTypeThereIsNoColumn is the error type that ClickHouse returns for a
	// reference to a column that does not exist.
	ErrorTypeThereIsNoColumn ErrorType = "THERE_IS_NO_COLUMN"

	// ErrorTypeSyntaxError is the error type that ClickHouse returns for invalid
	// query syntax.
	ErrorTypeSyntaxError ErrorType = "SYNTAX_ERROR"

	// ErrorTypeTypeMismatch is the error type that ClickHouse returns for a type
	// mismatch.
	ErrorTypeTypeMismatch ErrorType = "TYPE_MISMATCH"

	// ErrorTypeIllegalTypeOfArgument is the error type that ClickHouse returns for
	// an invalid type, such as that of a function argument.
	ErrorTypeIllegalTypeOfArgument ErrorType = "ILLEGAL_TYPE_OF_ARGUMENT"

	// ErrorTypeBadArguments is the error type that ClickHouse returns for invalid
	// arguments.
	ErrorTypeBadArguments ErrorType = "BAD_ARGUMENTS"

	// ErrorTypeBadCast is the error type that ClickHouse returns for an invalid
	// cast.
	ErrorTypeBadCast ErrorType = "BAD_CAST"

	// ErrorTypeCannotConvertType is the error type that ClickHouse returns for a
	// type that cannot be converted.
	ErrorTypeCannotConvertType ErrorType = "CANNOT_CONVERT_TYPE"

	// ErrorTypeCannotParseInputAssertionFailed is the error type that ClickHouse
	// returns for a failure to parse input.
	ErrorTypeCannotParseInputAssertionFailed ErrorType = "CANNOT_PARSE_INPUT_ASSERTION_FAILED"

	// ErrorTypeCannotParseQuotedString is the error type that ClickHouse returns
	// for a failure to parse a quoted string.
	ErrorTypeCannotParseQuotedString ErrorType = "CANNOT_PARSE_QUOTED_STRING"

	// ErrorTypeCannotParseNumber is the error type that ClickHouse returns for a
	// failure to parse a number.
	ErrorTypeCannotParseNumber ErrorType = "CANNOT_PARSE_NUMBER"

	// ErrorTypeCannotParseDate is the error type that ClickHouse returns for a
	// failure to parse a date.
	ErrorTypeCannotParseDate ErrorType = "CANNOT_PARSE_DATE"

	// ErrorTypeCannotParseDateTime is the error type that ClickHouse returns for a
	// failure to parse a datetime.
	ErrorTypeCannotParseDateTime ErrorType = "CANNOT_PARSE_DATETIME"

	// ErrorTypeIncorrectData is the error type that ClickHouse returns for invalid
	// input data.
	ErrorTypeIncorrectData ErrorType = "INCORRECT_DATA"

	// ErrorTypeUnknownFormat is the error type that ClickHouse returns for an
	// unknown input or output format.
	ErrorTypeUnknownFormat ErrorType = "UNKNOWN_FORMAT"

	// ErrorTypeBlocksHaveDifferentStructure is the error type that ClickHouse
	// returns for blocks whose structures do not match.
	ErrorTypeBlocksHaveDifferentStructure ErrorType = "BLOCKS_HAVE_DIFFERENT_STRUCTURE"

	// ErrorTypeTooFewColumns is the error type that ClickHouse returns for too few
	// columns.
	ErrorTypeTooFewColumns ErrorType = "TOO_FEW_COLUMNS"

	// ErrorTypeTooManyColumns is the error type that ClickHouse returns for too
	// many columns.
	ErrorTypeTooManyColumns ErrorType = "TOO_MANY_COLUMNS"

	// ErrorTypeNumberOfColumnsDoesntMatch is the error type that ClickHouse returns
	// for a mismatch in the number of columns.
	ErrorTypeNumberOfColumnsDoesntMatch ErrorType = "NUMBER_OF_COLUMNS_DOESNT_MATCH"

	// ErrorTypeUnknownSetting is the error type that ClickHouse returns for an
	// unknown setting.
	ErrorTypeUnknownSetting ErrorType = "UNKNOWN_SETTING"

	// ErrorTypeReadonlySetting is the error type that ClickHouse returns for a
	// setting that cannot be changed.
	ErrorTypeReadonlySetting ErrorType = "READONLY_SETTING"

	// ErrorTypeSettingConstraintViolation is the error type that ClickHouse returns
	// for a violation of a setting constraint.
	ErrorTypeSettingConstraintViolation ErrorType = "SETTING_CONSTRAINT_VIOLATION"

	// The following error types are not resolved by resending with the same
	// JSONEachRow input values, column structure and settings.
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
