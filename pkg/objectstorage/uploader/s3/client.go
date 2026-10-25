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

// ErrorCodeはS3 XML error responseのCodeです。HTTP status codeとは独立して保持します。
type ErrorCode string

const (
	// ErrorCodeRequestTimeoutは、S3がHTTP 400で返す一時的なrequest timeoutです。
	ErrorCodeRequestTimeout ErrorCode = "RequestTimeout"
	// ErrorCodeTemporaryRedirectは、bucketのDNS更新中に返る一時redirectです。
	ErrorCodeTemporaryRedirect ErrorCode = "TemporaryRedirect"
	// ErrorCodeOperationAbortedは、resourceに対する競合中のoperationです。
	ErrorCodeOperationAborted ErrorCode = "OperationAborted"
	// ErrorCodeConditionalRequestConflictは、条件付きPutObjectの競合です。
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
	// HTTPClientを指定した場合はその設定を使い、S3Config.Timeoutは適用しません。
	// UploadはPUTのredirectを追従せず、3xx responseをHTTPStatusErrorとして返します。
	// 元のclientのCheckRedirectは変更しません。
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

// Uploadはfileを一回のPutObjectで送信します。2xx以外のresponseは、redirectを含めて
// HTTPStatusErrorとして返し、再送や保持の判断は呼び出し側の分類へ委ねます。
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
		// A non-nil generic body with ContentLength 0 means unknown length to
		// net/http. NoBody makes the empty PUT send Content-Length: 0.
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

// HTTPStatusErrorは、S3 PutObjectが返した失敗responseを保持します。
// HTTP statusとS3 XML errorを分離し、本文が壊れていても上限付きの診断情報を残します。
type HTTPStatusError struct {
	// CodeはHTTP status codeです。S3 XML responseのCodeはErrorCodeへ保持します。
	Code   int
	Status string

	// Bodyは上限まで読み込んだresponse body、BodyTruncatedは切り詰めの有無です。
	Body          []byte
	BodyTruncated bool

	// ErrorCode以降はS3 XML error responseから取得します。RequestIDとHostIDは
	// XMLに値がなければ対応するX-Amz response headerを使用します。
	ErrorCode ErrorCode
	Message   string
	Resource  string
	RequestID string
	HostID    string

	// responseを完全に読めない場合、またはXMLとして解釈できない場合もHTTP statusを
	// 失わないよう、解析失敗を補助情報として保持します。
	// これらはUnwrapで返しません。再試行の判定はEOFやcontextなどの型付きerrorをHTTP statusより
	// 優先するため、原因として辿れるとstatusによる判定が変わります。
	ResponseReadError  error
	ResponseParseError error

	// RetryAfterDurationはRetry-After headerから得た待機時間です。
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
	// Canonicalize encoding within each endpoint segment without interpreting
	// escaped slashes as separators. The endpoint may include a proxy prefix.
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
	// Object keys are opaque names. In particular, repeated slashes and dot
	// segments must not be cleaned as if this were a filesystem path.
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
		// objectURL already uses S3's canonical URI encoding. Unlike other
		// AWS services, S3 does not escape that URI a second time for signing.
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
	// S3 leaves only RFC 3986 unreserved bytes unescaped within a segment.
	// QueryEscape has that character set; spaces must use %20 rather than +.
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
