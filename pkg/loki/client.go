package loki

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
	defaultTimeout = 3 * time.Second
	pushPath       = "/loki/api/v1/push"
	tenantHeader   = "X-Scope-OrgID"

	statusIngestionBlocked          = 260
	maxTenantIDBytes                = 150
	maxHTTPStatusErrorBodyBytes     = 64 * 1024
	truncatedHTTPStatusErrorMessage = "...(body truncated)"
)

// Client sends logs to the Loki HTTP Push API.
type Client struct {
	pushURL     string
	httpClient  *http.Client
	tenantID    string
	basicAuth   *BasicAuthConfig
	bearerToken string
	headers     map[string]string

	compressRequest bool
}

// Config is the configuration of a Loki client.
type Config struct {
	// Endpoint is an http or https Loki base URL, or a URL that includes the path
	// up to /loki/api/v1/push. If the path is empty, /loki/api/v1/push is added; if
	// there is a path, it must end with /loki/api/v1/push. Escaped path segments
	// are kept, and only trailing unescaped / characters are removed. The / that
	// separates the segments of the Push API path cannot be escaped. A query, an
	// empty query and a fragment are not accepted.
	Endpoint string

	// Timeout is used as the Timeout of the newly created http.Client only when
	// HTTPClient is nil. Zero is the unset sentinel that uses the default timeout,
	// and a negative value is a configuration error.
	Timeout time.Duration

	// TenantID is sent as X-Scope-OrgID in multi-tenant environments. A non-empty
	// value follows the Loki tenant ID constraints: only alphanumeric characters
	// and ! - _ . * ' ( ) are allowed, up to 150 bytes, and "." and ".." are
	// rejected.
	TenantID string

	BasicAuth   *BasicAuthConfig
	BearerToken string
	Headers     map[string]string

	// CompressRequest gzip-compresses the JSON body sent to the Loki Push API.
	CompressRequest bool

	// HTTPClient, if set, is used with its own settings, and Timeout is not
	// applied. Redirects are additionally restricted so that the POST body is
	// preserved. The original client is not modified.
	HTTPClient *http.Client
}

// BasicAuthConfig is the configuration for Basic authentication.
type BasicAuthConfig struct {
	Username string
	Password string
}

// Stream holds the logs of one Loki stream.
type Stream struct {
	Labels  map[string]string
	Entries []Entry
}

// Entry is one log line to send to Loki.
type Entry struct {
	Timestamp time.Time
	Line      string
}

// New creates a new Loki client.
func New(cfg Config) (*Client, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}

	pushURL, err := normalizePushURL(cfg.Endpoint)
	if err != nil {
		return nil, err
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		timeout := cfg.Timeout
		if timeout == 0 {
			timeout = defaultTimeout
		}
		httpClient = &http.Client{Timeout: timeout}
	}

	return &Client{
		pushURL:     pushURL,
		httpClient:  httpClient,
		tenantID:    cfg.TenantID,
		basicAuth:   copyBasicAuth(cfg.BasicAuth),
		bearerToken: cfg.BearerToken,
		headers:     copyHeaders(cfg.Headers),

		compressRequest: cfg.CompressRequest,
	}, nil
}

// PushLog sends one log line with the current time.
func (c *Client) PushLog(ctx context.Context, labels map[string]string, line string) error {
	return c.PushEntry(ctx, labels, Entry{
		Timestamp: time.Now(),
		Line:      line,
	})
}

// PushEntry sends one log line with one label set.
func (c *Client) PushEntry(ctx context.Context, labels map[string]string, entry Entry) error {
	return c.Push(ctx, []Stream{{
		Labels:  labels,
		Entries: []Entry{entry},
	}})
}

// Push sends multiple streams to Loki's /loki/api/v1/push.
func (c *Client) Push(ctx context.Context, streams []Stream) error {
	if len(streams) == 0 {
		return nil
	}

	body, err := buildPushBody(streams)
	if err != nil {
		return err
	}

	requestBody, compressed, err := prepareRequestBody(body, c.compressRequest)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.pushURL, bytes.NewReader(requestBody))
	if err != nil {
		return fmt.Errorf("failed to create loki request: %w", err)
	}

	for key, value := range c.headers {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if c.tenantID != "" {
		req.Header.Set(tenantHeader, c.tenantID)
	}
	if c.basicAuth != nil {
		req.SetBasicAuth(c.basicAuth.Username, c.basicAuth.Password)
	}
	if c.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	}

	resp, err := c.doPost(req)
	if err != nil {
		return fmt.Errorf("failed to send loki request: %w", err)
	}
	defer resp.Body.Close()

	if !isSuccessStatus(resp.StatusCode) {
		body, truncated, readErr := readHTTPStatusErrorBody(resp.Body)
		if readErr != nil {
			// 本文の読み込みerrorは文言だけを残し、原因としては包みません。再試行の判定は
			// EOFやcontextなどの型付きerrorをHTTP statusより優先するため、包むとstatusによる判定が変わります。
			body = []byte(readErr.Error())
			truncated = false
		}
		return &HTTPStatusError{
			Code:               resp.StatusCode,
			Status:             resp.Status,
			Body:               body,
			BodyTruncated:      truncated,
			RetryAfterDuration: parseRetryAfter(resp),
		}
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// doPostは呼び出し元のredirect制限とnet/httpの既定の上限を保ちつつ、logのpayloadを
// 捨てるredirectを拒否します。最後のresponseを返すことで、PushはGETの成功ではなく、
// 元のHTTP statusを報告できます。
func (c *Client) doPost(req *http.Request) (*http.Response, error) {
	httpClient := *c.httpClient
	checkRedirect := httpClient.CheckRedirect
	httpClient.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if checkRedirect != nil {
			if err := checkRedirect(next, via); err != nil {
				return err
			}
		} else if len(via) >= 10 {
			return ErrTooManyRedirects
		}
		if next.Method != http.MethodPost || next.Body == nil || next.Body == http.NoBody {
			return http.ErrUseLastResponse
		}
		return nil
	}
	return httpClient.Do(req)
}

func prepareRequestBody(body []byte, compress bool) ([]byte, bool, error) {
	if !compress {
		return body, false, nil
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(body); err != nil {
		_ = zw.Close()
		return nil, false, fmt.Errorf("failed to gzip loki request body: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close gzip writer: %w", err)
	}

	return buf.Bytes(), true, nil
}

type pushRequest struct {
	Streams []pushStream `json:"streams"`
}

type pushStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

func buildPushBody(streams []Stream) ([]byte, error) {
	payload := pushRequest{
		Streams: make([]pushStream, 0, len(streams)),
	}

	for i, stream := range streams {
		if err := ValidateLabels(stream.Labels); err != nil {
			return nil, fmt.Errorf("invalid labels at stream %d: %w", i, err)
		}
		if len(stream.Entries) == 0 {
			return nil, newValidationError(ErrStreamWithoutEntries, nil, "stream %d has no entries", i)
		}

		values := make([][]string, 0, len(stream.Entries))
		for _, entry := range stream.Entries {
			values = append(values, []string{
				formatTimestamp(entry.Timestamp),
				entry.Line,
			})
		}

		payload.Streams = append(payload.Streams, pushStream{
			Stream: copyLabels(stream.Labels),
			Values: values,
		})
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal loki push body: %w", err)
	}

	return body, nil
}

// PushRequestSize returns the size in bytes, before compression, of the
// canonical JSON body that Push sends. The logger uses this value to split
// batches within the application's request limit before sending them over the
// network.
func PushRequestSize(streams []Stream) (int, error) {
	body, err := buildPushBody(streams)
	if err != nil {
		return 0, err
	}
	return len(body), nil
}

func normalizePushURL(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", ErrEndpointRequired
	}

	// 解析できないendpointのerrorにも、URLのuser info（パスワードなど）を含めません。
	parsed, err := endpointurl.Parse(endpoint)
	if err != nil {
		return "", newValidationError(ErrInvalidEndpoint, err, "invalid loki endpoint: %v", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", newValidationError(ErrInvalidEndpoint, nil, "loki endpoint must include scheme and host")
	}
	if parsed.User != nil {
		return "", newValidationError(ErrInvalidEndpoint, nil, "loki endpoint must not include user info")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", newValidationError(ErrInvalidEndpoint, nil, "loki endpoint scheme must be http or https")
	}
	if parsed.ForceQuery || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", newValidationError(ErrInvalidEndpoint, nil, "loki endpoint must not include query or fragment")
	}

	// decodeする前に区切り文字を除きます。%2Fはsegmentの一部であり、除去できるslashや、
	// Push API pathの要素を区切るslashではありません。
	path := strings.TrimRight(parsed.EscapedPath(), "/")
	switch {
	case path == "":
		path = pushPath
	case hasPushPathSuffix(path):
	default:
		return "", newValidationError(
			ErrInvalidEndpoint, nil, "loki endpoint path must be empty or end with %s", pushPath,
		)
	}
	parsed.RawPath = path
	parsed.Path, err = url.PathUnescape(path)
	if err != nil {
		return "", newValidationError(ErrInvalidEndpoint, err, "invalid loki endpoint path: %v", err)
	}

	return parsed.String(), nil
}

func hasPushPathSuffix(path string) bool {
	segments := strings.Split(path, "/")
	want := []string{"loki", "api", "v1", "push"}
	if len(segments) < len(want)+1 {
		return false
	}
	for i, expected := range want {
		segment, err := url.PathUnescape(segments[len(segments)-len(want)+i])
		if err != nil || segment != expected {
			return false
		}
	}
	return true
}

func isSuccessStatus(code int) bool {
	if IsIngestionBlockedStatus(code) {
		return false
	}
	return code >= http.StatusOK && code < http.StatusMultipleChoices
}

func IsIngestionBlockedStatus(code int) bool {
	return code == statusIngestionBlocked
}

func formatTimestamp(t time.Time) string {
	if t.IsZero() {
		t = time.Now()
	}
	return fmt.Sprintf("%d", t.UnixNano())
}

// ValidateLabels validates one set of stream labels to pass to the Loki Push
// API. It requires at least one name-value pair and limits label names to the
// Prometheus form `[a-zA-Z_:][a-zA-Z0-9_:]*` that Loki accepts. `__` is a
// convention for internal use, but it is not an invalid value that the server
// always rejects, so it is not excluded here. It reports ErrLabelsRequired when
// there are no labels and *InvalidLabelNameError for an invalid label name.
func ValidateLabels(labels map[string]string) error {
	if len(labels) == 0 {
		return newValidationError(ErrLabelsRequired, nil, "at least one label is required")
	}
	for name := range labels {
		if !isValidLabelName(name) {
			return &InvalidLabelNameError{Name: name}
		}
	}
	return nil
}

func validateTenantID(tenantID string) error {
	if tenantID == "" {
		return nil
	}
	if len(tenantID) > maxTenantIDBytes {
		return newValidationError(
			ErrInvalidTenantID, nil, "loki tenant id must be %d bytes or less", maxTenantIDBytes,
		)
	}
	if tenantID == "." || tenantID == ".." {
		return newValidationError(ErrInvalidTenantID, nil, "loki tenant id must not be %q", tenantID)
	}
	for _, r := range tenantID {
		if !isValidTenantIDRune(r) {
			return newValidationError(ErrInvalidTenantID, nil, "invalid loki tenant id %q", tenantID)
		}
	}
	return nil
}

func isValidTenantIDRune(r rune) bool {
	if isLetter(r) || isDigit(r) {
		return true
	}
	switch r {
	case '!', '-', '_', '.', '*', '\'', '(', ')':
		return true
	default:
		return false
	}
}

func isValidLabelName(name string) bool {
	if name == "" {
		return false
	}

	for i, r := range name {
		if i == 0 {
			if !isLetter(r) && r != '_' && r != ':' {
				return false
			}
			continue
		}
		if !isLetter(r) && !isDigit(r) && r != '_' && r != ':' {
			return false
		}
	}

	return true
}

func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func copyLabels(labels map[string]string) map[string]string {
	copied := make(map[string]string, len(labels))
	for key, value := range labels {
		copied[key] = value
	}
	return copied
}

func copyHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}

	copied := make(map[string]string, len(headers))
	for key, value := range headers {
		copied[key] = value
	}
	return copied
}

func copyBasicAuth(auth *BasicAuthConfig) *BasicAuthConfig {
	if auth == nil {
		return nil
	}
	copied := *auth
	return &copied
}

// HTTPStatusError is the error returned when Loki responds with an HTTP status
// that is treated as a failure.
type HTTPStatusError struct {
	Code               int
	Status             string
	Body               []byte
	BodyTruncated      bool
	RetryAfterDuration time.Duration
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return ""
	}
	if len(e.Body) == 0 {
		return fmt.Sprintf("loki http status error: code=%d status=%s", e.Code, e.Status)
	}
	return fmt.Sprintf("loki http status error: code=%d status=%s body=%s", e.Code, e.Status, formatHTTPStatusErrorBody(e.Body, e.BodyTruncated))
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
	return e.RetryAfterDuration
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
	if retryAt, err := http.ParseTime(value); err == nil {
		if delay := time.Until(retryAt); delay > 0 {
			return delay
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

func (e *HTTPStatusError) IsIngestionBlocked() bool {
	if e == nil {
		return false
	}
	return IsIngestionBlockedStatus(e.Code)
}

func readHTTPStatusErrorBody(reader io.Reader) ([]byte, bool, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxHTTPStatusErrorBodyBytes+1))
	if err != nil {
		return nil, false, err
	}
	if len(body) <= maxHTTPStatusErrorBodyBytes {
		return body, false, nil
	}

	return body[:maxHTTPStatusErrorBodyBytes], true, nil
}

func formatHTTPStatusErrorBody(body []byte, truncated bool) string {
	if len(body) > maxHTTPStatusErrorBodyBytes {
		body = body[:maxHTTPStatusErrorBodyBytes]
		truncated = true
	}

	formatted := fmt.Sprintf("%q", string(body))
	if !truncated {
		return formatted
	}

	return formatted + truncatedHTTPStatusErrorMessage
}
