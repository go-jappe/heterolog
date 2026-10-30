package clickhouse

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/pkg/internal/endpointurl"
)

const (
	defaultTimeout                      = 10 * time.Second
	maxResponseInspectionBodyBytes      = 64 * 1024
	truncatedResponseInspectionBodyText = "...(body truncated)"
	clickHouseExceptionCodeHeader       = "X-ClickHouse-Exception-Code"
)

// Client writes rows, such as log records, to a ClickHouse table through its
// HTTP API.
type Client struct {
	endpoint        string
	database        string
	table           string
	username        string
	password        string
	headers         map[string]string
	compressRequest bool
	httpClient      *http.Client
}

// Config is the application-independent ClickHouse client configuration.
type Config struct {
	Endpoint string
	Database string
	Table    string
	// Timeout is used when HTTPClient is nil. Zero is the unset sentinel that
	// selects the default timeout, and a negative value is invalid.
	Timeout time.Duration

	Username string
	Password string
	Headers  map[string]string

	CompressRequest bool
	// HTTPClient settings are retained, except that an INSERT does not follow
	// redirects of the POST and returns a 3xx response as an HTTPStatusError.
	// The supplied client is never modified.
	HTTPClient *http.Client
}

// LogRecord is a generic row shape for application logs.
type LogRecord struct {
	Timestamp       time.Time                `json:"timestamp"`
	Level           string                   `json:"level"`
	Message         string                   `json:"message"`
	Line            string                   `json:"line,omitempty"`
	RequestID       string                   `json:"request_id,omitempty"`
	TraceID         string                   `json:"trace_id,omitempty"`
	SpanID          string                   `json:"span_id,omitempty"`
	Error           string                   `json:"error,omitempty"`
	Fields          map[string]any           `json:"fields,omitempty"`
	StackTrace      []map[string]interface{} `json:"stack_trace,omitempty"`
	AppName         string                   `json:"app_name,omitempty"`
	HostName        string                   `json:"host_name,omitempty"`
	Environment     string                   `json:"environment,omitempty"`
	RuntimeType     string                   `json:"runtime_type,omitempty"`
	ExecutionUnitID string                   `json:"execution_unit_id,omitempty"`
	Version         string                   `json:"version,omitempty"`
	Source          string                   `json:"source,omitempty"`
}

type InsertResponse struct {
	StatusCode int
}

type InsertOptions struct {
	// InsertDeduplicationToken identifies an INSERT independently of its data.
	// The value is opaque and preserved exactly; only an empty string omits it.
	InsertDeduplicationToken string
}

// LogRecordMarshalError holds the input index of a LogRecord that could not be
// converted to JSONEachRow before the HTTP request was created. Nothing has
// been sent to ClickHouse yet, so callers can safely separate that record from
// the unsent records and handle them durably.
type LogRecordMarshalError struct {
	Index int
	Err   error
}

func (e *LogRecordMarshalError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("failed to marshal clickhouse log record %d: %v", e.Index, e.Err)
}

func (e *LogRecordMarshalError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// HTTPStatusError reports that a ClickHouse HTTP response did not confirm that
// the INSERT succeeded. The status is usually non-2xx, but Code can be 2xx when
// ClickHouse writes a DB::Exception to the body after sending the headers.
type HTTPStatusError struct {
	Code          int
	Status        string
	Body          []byte
	BodyTruncated bool
	After         time.Duration
	// ExceptionCode and ExceptionType hold the ClickHouse DB::Exception in
	// structured form. If ClickHouse fails after sending the headers, they are set
	// even when Code is 2xx.
	ExceptionCode int
	ExceptionType ErrorType
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"clickhouse response error: http_code=%d status=%s exception_code=%d error_type=%s",
		e.Code,
		e.Status,
		e.ClickHouseExceptionCode(),
		e.ClickHouseErrorType(),
	)
}

func (e *HTTPStatusError) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.Code
}

func (e *HTTPStatusError) RetryAfter() time.Duration {
	if e == nil {
		return 0
	}
	return e.After
}

// ClickHouseExceptionCode returns the ClickHouse-specific exception code. For
// consistency with errors constructed by hand, it recovers the code from the
// response body when the ExceptionCode field is unset.
func (e *HTTPStatusError) ClickHouseExceptionCode() int {
	if e == nil {
		return 0
	}
	if e.ExceptionCode != 0 {
		return e.ExceptionCode
	}
	code, _ := parseClickHouseException(e.Body)
	return code
}

// ClickHouseErrorType returns the ClickHouse-specific error type. When the
// ExceptionType field is unset, it uses the last parenthesized token of the
// response body that looks like a type, such as (UNKNOWN_TABLE): upper-case
// letters, digits and underscores with at least one letter. It returns an
// empty ErrorType when there is no such token.
func (e *HTTPStatusError) ClickHouseErrorType() ErrorType {
	if e == nil {
		return ""
	}
	if e.ExceptionType != "" {
		return e.ExceptionType
	}
	_, errorType := parseClickHouseException(e.Body)
	return errorType
}

func New(cfg Config) (*Client, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}

	endpoint, err := normalizeEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	table := strings.TrimSpace(cfg.Table)

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		timeout := cfg.Timeout
		if timeout == 0 {
			timeout = defaultTimeout
		}
		httpClient = &http.Client{Timeout: timeout}
	}

	return &Client{
		endpoint:        endpoint,
		database:        strings.TrimSpace(cfg.Database),
		table:           table,
		username:        cfg.Username,
		password:        cfg.Password,
		headers:         copyHeaders(cfg.Headers),
		compressRequest: cfg.CompressRequest,
		httpClient:      httpClient,
	}, nil
}

func (c *Client) InsertLog(ctx context.Context, record LogRecord) (*InsertResponse, error) {
	return c.InsertLogs(ctx, []LogRecord{record})
}

func (c *Client) InsertLogs(ctx context.Context, records []LogRecord) (*InsertResponse, error) {
	return c.InsertLogsWithOptions(ctx, records, InsertOptions{})
}

func (c *Client) InsertLogWithOptions(ctx context.Context, record LogRecord, opts InsertOptions) (*InsertResponse, error) {
	return c.InsertLogsWithOptions(ctx, []LogRecord{record}, opts)
}

func (c *Client) InsertLogsWithOptions(
	ctx context.Context,
	records []LogRecord,
	opts InsertOptions,
) (*InsertResponse, error) {
	if len(records) == 0 {
		return &InsertResponse{}, nil
	}

	body, err := MarshalJSONEachRow(records)
	if err != nil {
		return nil, err
	}
	return c.InsertJSONEachRow(ctx, body, opts)
}

// InsertJSONEachRow inserts the rows of body, in the JSONEachRow format, into
// the table of the client in one request. Each row is one JSON object followed
// by a newline; the object keys are column names. The body is sent as given,
// compressed with gzip when CompressRequest is set, so it can hold the rows of
// any table, not only log records. An empty body sends nothing. The result is
// reported as for InsertLogsWithOptions.
func (c *Client) InsertJSONEachRow(ctx context.Context, body []byte, opts InsertOptions) (*InsertResponse, error) {
	if len(body) == 0 {
		return &InsertResponse{}, nil
	}
	requestBody, compressed, err := prepareRequestBody(body, c.compressRequest)
	if err != nil {
		return nil, err
	}

	insertURL, err := c.insertURL(opts)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, insertURL, bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create clickhouse request: %w", err)
	}

	for key, value := range c.headers {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if c.database != "" {
		req.Header.Set("X-ClickHouse-Database", c.database)
	}
	if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.doPost(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send clickhouse request: %w", err)
	}
	defer resp.Body.Close()

	body, truncated, readErr := readClickHouseResponseBody(resp.Body)
	if readErr != nil {
		if !isSuccessStatus(resp.StatusCode) {
			// 本文の読み込みerrorは文言だけを残し、原因としては包みません。再試行の判定は
			// EOFやcontextなどの型付きerrorをHTTP statusより優先するため、包むとstatusによる判定が変わります。
			return nil, &HTTPStatusError{
				Code:   resp.StatusCode,
				Status: resp.Status,
				Body:   []byte(readErr.Error()),
				After:  parseRetryAfter(resp),
			}
		}
		return nil, &ResponseDecodeError{Err: readErr}
	}
	exceptionCode, errorType := parseClickHouseException(body)
	headerExceptionCode := strings.TrimSpace(resp.Header.Get(clickHouseExceptionCodeHeader))
	if code, ok := parsePositiveInt(headerExceptionCode); ok {
		exceptionCode = code
	}
	hasClickHouseException := headerExceptionCode != "" || exceptionCode != 0 || errorType != ""

	// ClickHouseはresponse headerを送った後にquery実行が失敗すると、HTTP 2xxのbodyへ
	// DB::Exceptionを書き込む場合があります。statusだけでsuccessとせず、headerとbodyの
	// exception情報も送達失敗として返します。
	if !isSuccessStatus(resp.StatusCode) || hasClickHouseException {
		return nil, &HTTPStatusError{
			Code:          resp.StatusCode,
			Status:        resp.Status,
			Body:          body,
			BodyTruncated: truncated,
			After:         parseRetryAfter(resp),
			ExceptionCode: exceptionCode,
			ExceptionType: errorType,
		}
	}

	return &InsertResponse{StatusCode: resp.StatusCode}, nil
}

// doPostはPOSTを一度だけ送信し、redirectを追従せずにそのresponseを返します。redirectは
// 設定した送信先が本来の送信先ではないことを表し、追従すると301/302/303では本文なしの
// GETへ変わり、307/308ではINSERTの本文を別URLへ再送します。どれも追従せず、既存の
// HTTP statusによる分類に回すため、元のredirect responseを返します。
// 元のhttp.Clientは変更せず、shallow copyだけでredirectを停止します。
func (c *Client) doPost(req *http.Request) (*http.Response, error) {
	httpClient := *c.httpClient
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return httpClient.Do(req)
}

func (c *Client) insertURL(opts InsertOptions) (string, error) {
	parsed, err := url.Parse(c.endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid clickhouse endpoint: %w", err)
	}
	values := parsed.Query()
	values.Set("query", insertQuery(c.database, c.table))
	values.Set(SettingWaitEndOfQuery, "1")
	if token := opts.InsertDeduplicationToken; token != "" {
		values.Set(SettingInsertDeduplicate, "1")
		values.Set(SettingInsertDeduplicationToken, token)
	}
	parsed.RawQuery = values.Encode()
	return parsed.String(), nil
}

func insertQuery(database, table string) string {
	return "INSERT INTO " + tableExpression(database, table) + " FORMAT JSONEachRow"
}

func tableExpression(database, table string) string {
	if database != "" {
		return quoteIdentifier(database) + "." + quoteIdentifier(table)
	}

	parts := strings.Split(table, ".")
	if len(parts) == 2 && strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != "" {
		return quoteIdentifier(parts[0]) + "." + quoteIdentifier(parts[1])
	}
	return quoteIdentifier(table)
}

func quoteIdentifier(identifier string) string {
	// ClickHouseはquoteされたidentifier内のbackslash escapeも解釈します。identifierの
	// byte列を保つよう、backtickを二重にする前にbackslashをescapeします。
	identifier = strings.ReplaceAll(strings.TrimSpace(identifier), "\\", "\\\\")
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

// MarshalJSONEachRow converts records into the uncompressed JSONEachRow body
// that a ClickHouse HTTP INSERT actually sends. The result includes the newline
// at the end of each row. A zero Timestamp is replaced with the current time on
// each call, so callers that determine request byte boundaries can use the len
// of the result as the size actually sent if every record has a non-zero
// Timestamp.
func MarshalJSONEachRow(records []LogRecord) ([]byte, error) {
	var buf bytes.Buffer
	now := time.Now().UTC()
	for i, record := range records {
		if record.Timestamp.IsZero() {
			record.Timestamp = now
		}
		line, err := json.Marshal(record)
		if err != nil {
			return nil, &LogRecordMarshalError{Index: i, Err: err}
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

func prepareRequestBody(body []byte, compress bool) ([]byte, bool, error) {
	if !compress {
		return body, false, nil
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(body); err != nil {
		_ = zw.Close()
		return nil, false, fmt.Errorf("failed to gzip clickhouse request body: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close gzip writer: %w", err)
	}

	return buf.Bytes(), true, nil
}

func normalizeEndpoint(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", ErrEndpointRequired
	}

	// 解析できないendpointのerrorにも、URLのuser info（パスワードなど）を含めません。
	parsed, err := endpointurl.Parse(endpoint)
	if err != nil {
		return "", newValidationError(ErrInvalidEndpoint, err, "invalid clickhouse endpoint: %v", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", newValidationError(ErrInvalidEndpoint, nil, "clickhouse endpoint must include scheme and host")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", newValidationError(ErrInvalidEndpoint, nil, "clickhouse endpoint scheme must be http or https")
	}
	if parsed.User != nil {
		return "", newValidationError(ErrInvalidEndpoint, nil, "clickhouse endpoint must not include user info")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", newValidationError(ErrInvalidEndpoint, nil, "clickhouse endpoint must not include query or fragment")
	}

	return parsed.String(), nil
}

func copyHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string]string, len(headers))
	for key, value := range headers {
		if strings.TrimSpace(key) == "" {
			continue
		}
		out[key] = value
	}
	return out
}

func isSuccessStatus(code int) bool {
	return code >= 200 && code < 300
}

// readClickHouseResponseBodyはnon-2xxの診断本文と、2xxへ遅れて書かれたDB::Exceptionを
// 検査できる上限まで読み込みます。上限超過時も先頭部分はerrorへ保持します。
func readClickHouseResponseBody(r io.Reader) ([]byte, bool, error) {
	var buf bytes.Buffer
	limited := io.LimitReader(r, maxResponseInspectionBodyBytes+1)
	if _, err := io.Copy(&buf, limited); err != nil {
		return nil, false, err
	}
	body := buf.Bytes()
	if len(body) <= maxResponseInspectionBodyBytes {
		return body, false, nil
	}
	body = body[:maxResponseInspectionBodyBytes]
	body = append(body, []byte(truncatedResponseInspectionBodyText)...)
	return body, true, nil
}

func parseRetryAfter(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	value := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if value == "" {
		return 0
	}
	if seconds, ok := parseRetryAfterSeconds(value); ok {
		// time.Durationで表せない秒数は最大値へ飽和させ、呼び出し側の待機上限を適用させます。
		// 0にすると、serverが長い待機を求めたのに即時再送になります。HTTP-dateも同じく飽和します。
		const maxDelay = time.Duration(1<<63 - 1)
		if seconds > int64(maxDelay/time.Second) {
			return maxDelay
		}
		return time.Duration(seconds) * time.Second
	}
	if t, err := http.ParseTime(value); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// parseRetryAfterSecondsはRetry-Afterのdelay-secondsを解析します。int64を超える桁数の
// 値も有効なdelay-secondsなので、最大値へ飽和させます。数字以外を含む値は解析しません。
func parseRetryAfterSeconds(value string) (int64, bool) {
	if value == "" {
		return 0, false
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	var seconds int64
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		digit := int64(c - '0')
		if seconds > (maxInt64-digit)/10 {
			seconds = maxInt64
			continue
		}
		seconds = seconds*10 + digit
	}
	return seconds, true
}

func parseInt64(s string) (int64, bool) {
	var n int64
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := int64(c - '0')
		if n > (int64(^uint64(0)>>1)-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	return n, true
}

func parsePositiveInt(value string) (int, bool) {
	parsed, ok := parseInt64(value)
	if !ok || parsed <= 0 || int64(int(parsed)) != parsed {
		return 0, false
	}
	return int(parsed), true
}
