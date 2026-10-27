package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	serviceName      = "s3"
	unsignedPayload  = "UNSIGNED-PAYLOAD"
	contentSHA256Key = "X-Amz-Content-Sha256"
	defaultTimeout   = 10 * time.Second

	// maxHTTPStatusErrorBodyBytesは、失敗responseをHTTPStatusErrorへ保持する上限です。
	// S3のXML errorは小さいため、これを超えるresponseは診断用prefixだけを保持します。
	maxHTTPStatusErrorBodyBytes int64 = 64 * 1024
)

// ErrorCode is the Code of an S3 XML error response. It is kept separately
// from the HTTP status code.
type ErrorCode string

const (
	// ErrorCodeRequestTimeout is the temporary request timeout that S3 returns
	// with HTTP 400.
	ErrorCodeRequestTimeout ErrorCode = "RequestTimeout"
	// ErrorCodeTemporaryRedirect is the temporary redirect returned while the
	// DNS of a bucket is being updated.
	ErrorCodeTemporaryRedirect ErrorCode = "TemporaryRedirect"
	// ErrorCodeOperationAborted reports a conflicting operation in progress on
	// the resource.
	ErrorCodeOperationAborted ErrorCode = "OperationAborted"
	// ErrorCodeConditionalRequestConflict reports a conflict of a conditional
	// PutObject.
	ErrorCodeConditionalRequestConflict ErrorCode = "ConditionalRequestConflict"
)

type Client struct {
	region          string
	endpoint        string
	bucket          string
	forcePathStyle  bool
	httpClient      *http.Client
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

type Config struct {
	objectstorage.S3Config
	// HTTPClient, when set, is used with its own settings, and S3Config.Timeout
	// is not applied. Upload does not follow redirects of the PUT and returns a
	// 3xx response as an HTTPStatusError. The CheckRedirect of the original
	// client is not changed.
	HTTPClient *http.Client
}

func New(cfg Config) (*Client, error) {
	if err := ValidateConfig(cfg.S3Config); err != nil {
		return nil, err
	}

	// SigV4のcredential scopeとdefault endpointには、入力されたregionを前後の空白だけ除いて
	// 使います。S3互換serviceのregion名を変えないよう、大文字小文字は正規化しません。
	region := strings.TrimSpace(cfg.Region)
	bucket := strings.TrimSpace(cfg.Bucket)
	endpoint, err := normalizeEndpoint(cfg.Endpoint, region)
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
		region:          region,
		endpoint:        endpoint,
		bucket:          bucket,
		forcePathStyle:  cfg.ForcePathStyle,
		httpClient:      httpClient,
		accessKeyID:     cfg.AccessKeyID,
		secretAccessKey: cfg.SecretAccessKey,
		sessionToken:    cfg.SessionToken,
	}, nil
}

// Upload sends the file with a single PutObject. It returns any response other
// than 2xx, including a redirect, as an HTTPStatusError, and leaves the
// decision to retry or retain the file to the caller's classification.
func (c *Client) Upload(ctx context.Context, input uploader.UploadInput) error {
	if err := input.Validate(); err != nil {
		return err
	}
	filePath := input.FilePath()
	// ファイル自身がリンク・ディレクトリ・特殊ファイルなら、開く前に拒否します。
	// 途中のディレクトリにあるリンクの検査は、このLstatでは行いません。
	pathInfo, err := os.Lstat(filePath)
	if err != nil {
		return fmt.Errorf("failed to inspect upload file %q: %w", filePath, err)
	}
	if !pathInfo.Mode().IsRegular() {
		return newValidationError(ErrNotRegularFile, nil, "upload file path is not a regular file: %q", filePath)
	}
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open upload file %q: %w", filePath, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat upload file %q: %w", filePath, err)
	}
	// 送信サイズは開いたファイルから取得し、直前に検査した実体と一致することも確認します。
	if !os.SameFile(pathInfo, info) {
		return fmt.Errorf("upload file changed while opening: %q", filePath)
	}

	reqURL, err := c.objectURL(input.ObjectKey())
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, file)
	if err != nil {
		return fmt.Errorf("failed to create s3 upload request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = info.Size()
	if info.Size() == 0 {
		// ContentLengthが0でnilではない一般のbodyは、net/httpでは長さ不明を意味します。
		// NoBodyにすると、空のPUTはContent-Length: 0を送ります。
		req.Body = http.NoBody
	}

	if err := c.signRequest(req); err != nil {
		return fmt.Errorf("failed to sign s3 upload request: %w", err)
	}

	resp, err := c.doPut(req)
	if err != nil {
		return fmt.Errorf("failed to send s3 upload request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, truncated, readErr := readHTTPStatusErrorBody(resp.Body)
		return newHTTPStatusError(resp, body, truncated, readErr)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// doPutは署名済みPUTを一度だけ送信し、redirectを追従せずにそのresponseを返します。
// net/httpは301/302/303ではPUTを本文なしのGETへ変えて追従し、本文が空なら307/308も
// 同じ署名のまま別URLへ再送します。追従先の成功をupload成功と誤認しないよう、
// 元のhttp.Clientは変更せず、shallow copyだけでredirectを停止します。
func (c *Client) doPut(req *http.Request) (*http.Response, error) {
	httpClient := *c.httpClient
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return httpClient.Do(req)
}

// HTTPStatusError holds a failure response returned by S3 PutObject. It keeps
// the HTTP status separate from the S3 XML error, and it retains bounded
// diagnostic information even when the body is malformed.
type HTTPStatusError struct {
	// Code is the HTTP status code. The Code of the S3 XML response is kept in
	// ErrorCode.
	Code   int
	Status string

	// Body is the response body read up to the limit, and BodyTruncated
	// reports whether it was truncated.
	Body          []byte
	BodyTruncated bool

	// ErrorCode and the fields after it are taken from the S3 XML error
	// response. RequestID and HostID use the corresponding X-Amz response
	// header when the XML has no value.
	ErrorCode ErrorCode
	Message   string
	Resource  string
	RequestID string
	HostID    string

	// ResponseReadError and ResponseParseError hold, as supplementary
	// information, a failure to read the response completely or to parse it as
	// XML, so that the HTTP status is not lost even in those cases. errors.Is
	// and errors.As do not reach them: the retry decision gives typed errors
	// such as EOF and context errors priority over the HTTP status, so if they
	// could be followed as causes, the decision based on the status would
	// change.
	ResponseReadError  error
	ResponseParseError error

	// RetryAfterDuration is the wait obtained from the Retry-After header.
	RetryAfterDuration time.Duration
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return "s3 upload failed"
	}
	parts := []string{fmt.Sprintf("s3 upload failed: status=%d", e.Code)}
	if e.Status != "" {
		parts = append(parts, "http_status="+e.Status)
	}
	if e.ErrorCode != "" {
		parts = append(parts, "code="+string(e.ErrorCode))
	}
	if e.Message != "" {
		parts = append(parts, "message="+e.Message)
	}
	if e.RequestID != "" {
		parts = append(parts, "request_id="+e.RequestID)
	}
	if len(e.Body) > 0 && e.ErrorCode == "" && e.Message == "" {
		body := string(e.Body)
		if e.BodyTruncated {
			body += "...(truncated)"
		}
		parts = append(parts, "body="+body)
	}
	if e.ResponseReadError != nil {
		parts = append(parts, "response_read_error="+e.ResponseReadError.Error())
	}
	if e.ResponseParseError != nil {
		parts = append(parts, "response_parse_error="+e.ResponseParseError.Error())
	}
	return strings.Join(parts, " ")
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

type s3ErrorResponse struct {
	XMLName   xml.Name  `xml:"Error"`
	Code      ErrorCode `xml:"Code"`
	Message   string    `xml:"Message"`
	Resource  string    `xml:"Resource"`
	RequestID string    `xml:"RequestId"`
	HostID    string    `xml:"HostId"`
}

func newHTTPStatusError(
	resp *http.Response,
	body []byte,
	truncated bool,
	readErr error,
) *HTTPStatusError {
	statusErr := &HTTPStatusError{
		Code:               resp.StatusCode,
		Status:             resp.Status,
		Body:               body,
		BodyTruncated:      truncated,
		RequestID:          strings.TrimSpace(resp.Header.Get("X-Amz-Request-Id")),
		HostID:             strings.TrimSpace(resp.Header.Get("X-Amz-Id-2")),
		ResponseReadError:  readErr,
		RetryAfterDuration: parseRetryAfter(resp.Header.Get("Retry-After")),
	}
	if readErr != nil || len(body) == 0 {
		return statusErr
	}

	var response s3ErrorResponse
	if err := xml.Unmarshal(body, &response); err != nil {
		statusErr.ResponseParseError = err
		return statusErr
	}
	statusErr.ErrorCode = ErrorCode(strings.TrimSpace(string(response.Code)))
	statusErr.Message = strings.TrimSpace(response.Message)
	statusErr.Resource = strings.TrimSpace(response.Resource)
	if requestID := strings.TrimSpace(response.RequestID); requestID != "" {
		statusErr.RequestID = requestID
	}
	if hostID := strings.TrimSpace(response.HostID); hostID != "" {
		statusErr.HostID = hostID
	}
	return statusErr
}

func readHTTPStatusErrorBody(body io.Reader) ([]byte, bool, error) {
	limited := io.LimitReader(body, maxHTTPStatusErrorBodyBytes+1)
	payload, err := io.ReadAll(limited)
	if int64(len(payload)) <= maxHTTPStatusErrorBodyBytes {
		return payload, false, err
	}
	return payload[:maxHTTPStatusErrorBodyBytes], true, err
}

func (c *Client) objectURL(objectKey string) (string, error) {
	if objectKey == "" {
		return "", fmt.Errorf("s3 object key is required")
	}
	parsed, err := url.Parse(c.endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid s3 endpoint: %w", err)
	}
	// escapeされたslashを区切りとして解釈せずに、endpointの各segment内のencodingを
	// 正規化します。endpointはproxyのprefixを含む場合があります。
	prefixParts := strings.Split(parsed.EscapedPath(), "/")
	for i, part := range prefixParts {
		decoded, err := url.PathUnescape(part)
		if err != nil {
			return "", fmt.Errorf("invalid s3 endpoint path: %w", err)
		}
		prefixParts[i] = escapePathSegment(decoded)
	}
	escapedPath := strings.Join(prefixParts, "/")
	if c.forcePathStyle {
		parsed.Path += "/" + c.bucket
		escapedPath += "/" + escapePathSegment(c.bucket)
	} else {
		parsed.Host = c.bucket + "." + parsed.Host
	}
	// object keyは不透明な名前です。特に、連続したslashやdot segmentを、filesystemの
	// pathのように整理してはいけません。
	parsed.Path += "/" + objectKey
	parsed.RawPath = escapedPath + "/" + escapeObjectKey(objectKey)
	return parsed.String(), nil
}

func (c *Client) signRequest(req *http.Request) error {
	creds := aws.Credentials{
		AccessKeyID:     c.accessKeyID,
		SecretAccessKey: c.secretAccessKey,
		SessionToken:    c.sessionToken,
		Source:          "static",
	}

	req.Header.Set(contentSHA256Key, unsignedPayload)
	signer := v4.NewSigner(func(options *v4.SignerOptions) {
		// objectURLはすでにS3のcanonical URI encodingを使っています。ほかのAWS serviceと
		// 異なり、S3は署名のためにそのURIをもう一度escapeしません。
		options.DisableURIPathEscaping = true
	})
	return signer.SignHTTP(req.Context(), creds, req, unsignedPayload, serviceName, c.region, time.Now())
}

func escapeObjectKey(objectKey string) string {
	parts := strings.Split(objectKey, "/")
	for i, part := range parts {
		parts[i] = escapePathSegment(part)
	}
	return strings.Join(parts, "/")
}

func escapePathSegment(segment string) string {
	// S3はsegment内で、RFC 3986のunreservedのbyteだけをescapeせずに残します。
	// QueryEscapeがescapeせずに残すのも同じ文字集合です。ただし、空白は+ではなく%20に
	// する必要があります。
	return strings.ReplaceAll(url.QueryEscape(segment), "+", "%20")
}

func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
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
	t, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	d := time.Until(t)
	if d < 0 {
		return 0
	}
	return d
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
