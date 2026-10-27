// Package cloudwatchlogs provides a lightweight client that calls the AWS
// CloudWatch Logs PutLogEvents and CreateLogStream APIs with SigV4-signed HTTP
// requests.
package cloudwatchlogs

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/go-jappe/heterolog/pkg/internal/endpointurl"
	"github.com/go-jappe/heterolog/pkg/utils/jsonutil"
)

// These are the defaults, target names, error-handling constants, and request
// limits of the CloudWatch Logs API.
const (
	defaultTimeout               = 10 * time.Second
	serviceName                  = "logs"
	amzTargetPrefix              = "Logs_20140328."
	targetPutLogEvents           = amzTargetPrefix + "PutLogEvents"
	targetCreateLogStream        = amzTargetPrefix + "CreateLogStream"
	contentType                  = "application/x-amz-json-1.1"
	maxHTTPStatusErrorBodyBytes  = 64 * 1024
	truncatedHTTPStatusErrorBody = "...(body truncated)"
	// maxPutLogEventsBatchBytes は、一回の PutLogEvents request に含める event size の合計上限です。
	maxPutLogEventsBatchBytes = 1_048_576
	// MaxPutLogEventsCount is the maximum number of events that one PutLogEvents
	// request can contain.
	MaxPutLogEventsCount = 10_000
	// MaxPutLogEventBytes is the maximum event size that CloudWatch Logs allows
	// for one event. Like the request size, the event size is counted as the
	// number of UTF-8 bytes in the message after JSON decoding plus an overhead
	// of 26 bytes per event. It is not a limit on the message itself, so use
	// MaxPutLogEventMessageBytes to check the length of a message before sending.
	MaxPutLogEventBytes = 1_048_576
	// MaxPutLogEventMessageBytes is the maximum number of UTF-8 bytes that can
	// be sent as the message of one event. It is 1,048,550 bytes because even a
	// request with only one event must fit within MaxPutLogEventBytes and the
	// request size limit, including the 26-byte overhead.
	MaxPutLogEventMessageBytes = MaxPutLogEventBytes - putLogEventOverheadBytes
	putLogEventOverheadBytes   = 26
)

// HTTPClient is a Client implementation that sends HTTP requests to the
// CloudWatch Logs API.
type HTTPClient struct {
	endpoint            string
	region              string
	logGroupName        string
	httpClient          *http.Client
	credentialsProvider aws.CredentialsProvider
	headers             map[string]string
	autoCreateLogStream bool
	compressRequest     bool
}

// Config holds the settings needed to create an HTTPClient.
type Config struct {
	// Region is the AWS Region of CloudWatch Logs. It is required. Set any
	// application-specific default before passing the Config to this client.
	Region string

	// Endpoint is the full URL of the destination, with an http or https scheme
	// and a host, and without user info, a query or a fragment. Surrounding
	// whitespace and trailing slashes are removed. If it is empty or only
	// whitespace, https://logs.<Region>.amazonaws.com is used, with
	// amazonaws.com.cn for a Region that starts with "cn-". Set it explicitly
	// for a partition that uses another domain.
	Endpoint string

	// LogGroupName is the name of the destination log group. It is required.
	LogGroupName string

	// Timeout is the timeout of the HTTP client that is created when HTTPClient
	// is nil. Zero is the unset sentinel that selects the default timeout, and
	// a negative value is invalid.
	Timeout time.Duration

	// AccessKeyID is the access key ID used for SigV4 signing. It is required
	// when CredentialsProvider is nil.
	AccessKeyID string

	// SecretAccessKey is the secret access key used for SigV4 signing. It is
	// required when CredentialsProvider is nil.
	SecretAccessKey string

	// SessionToken is the session token used with temporary credentials.
	SessionToken string

	// CredentialsProvider is the AWS credentials provider used for SigV4 signing.
	// If it is set, it takes precedence over AccessKeyID, SecretAccessKey, and
	// SessionToken.
	CredentialsProvider aws.CredentialsProvider

	// Headers holds the headers added to each HTTP request. Headers that the API
	// requires are overwritten with the values of this client.
	Headers map[string]string

	// AutoCreateLogStream, if true, creates the destination stream when it does
	// not exist and retries PutLogEvents only once.
	AutoCreateLogStream bool

	// CompressRequest, if true, compresses the JSON request body with gzip.
	CompressRequest bool

	// HTTPClient is the HTTP client used for API calls. If it is nil, a client
	// configured with Timeout is created.
	HTTPClient *http.Client
}

// PutLogEventsRequest represents one CloudWatch Logs submission that has
// already been routed. The sender fixes LogStreamName in advance from the time
// the events occurred and keeps AttemptedAt unchanged across retries. Events
// belong to one stream and must be sorted by Timestamp in ascending order.
type PutLogEventsRequest struct {
	LogStreamName string
	Events        []Event
	AttemptedAt   time.Time
}

// Client is the contract for sending a finalized PutLogEventsRequest to
// CloudWatch Logs. Retries and circuit breakers decorate this single I/O
// operation and take no part in resolving stream names.
type Client interface {
	PutLogEvents(ctx context.Context, request PutLogEventsRequest) (*PutLogEventsOutput, error)
}

// Event is one log event to send to CloudWatch Logs.
type Event struct {
	// Timestamp is the time when the event occurred. If it is the zero value,
	// HTTPClient.PutLogEvents uses PutLogEventsRequest.AttemptedAt instead.
	Timestamp time.Time

	// Message is the log text to send.
	Message string
}

// PutLogEventsOutput represents the successful result of PutLogEvents.
//
// CloudWatch Logs may reject only some of the log events in a batch even when
// it returns HTTP 200. Callers check RejectedLogEventsInfo and handle accepted
// events and rejected events separately.
type PutLogEventsOutput struct {
	// NextSequenceToken is a deprecated field that CloudWatch Logs returns.
	// PutLogEvents now ignores sequence tokens, but the field is kept for
	// compatibility.
	NextSequenceToken string `json:"nextSequenceToken,omitempty"`

	// RejectedLogEventsInfo is the index information of the log events that
	// were rejected within an HTTP 200 response.
	RejectedLogEventsInfo *RejectedLogEventsInfo `json:"rejectedLogEventsInfo,omitempty"`

	// RejectedEntityInfo is the information for the case where only the entity
	// was rejected. Even in this case, the log events may have been accepted.
	RejectedEntityInfo *RejectedEntityInfo `json:"rejectedEntityInfo,omitempty"`
}

// HasRejectedLogEvents reports whether the PutLogEvents response contains
// per-event rejection information.
func (o *PutLogEventsOutput) HasRejectedLogEvents() bool {
	return o != nil &&
		o.RejectedLogEventsInfo != nil &&
		o.RejectedLogEventsInfo.HasRejectedLogEvents()
}

// RejectedLogEventsInfo is the per-event rejection information that
// PutLogEvents returns with HTTP 200.
//
// The AWS indexes are positions in the logEvents array that was actually sent.
// If the caller reorders the events before sending them, it must keep that
// sending order together with the mapping to the original records and apply
// the response indexes to the same order.
type RejectedLogEventsInfo struct {
	ExpiredLogEventEndIndex  *int `json:"expiredLogEventEndIndex,omitempty"`
	TooNewLogEventStartIndex *int `json:"tooNewLogEventStartIndex,omitempty"`
	TooOldLogEventEndIndex   *int `json:"tooOldLogEventEndIndex,omitempty"`
}

// RejectedEntityInfo is the entity rejection information of PutLogEvents.
type RejectedEntityInfo struct {
	ErrorType string `json:"errorType,omitempty"`
}

// RejectedLogEventReason is the reason CloudWatch Logs rejected a log event.
type RejectedLogEventReason string

const (
	RejectedLogEventReasonExpired RejectedLogEventReason = "expired"
	RejectedLogEventReasonTooOld  RejectedLogEventReason = "too_old"
	RejectedLogEventReasonTooNew  RejectedLogEventReason = "too_new"
)

// RejectedLogEvent is the index and the reason of a rejected log event.
type RejectedLogEvent struct {
	Index  int
	Reason RejectedLogEventReason
}

// Retryable reports whether an event rejected for this reason may be sent
// again later.
//
// too_new is retryable because the event can become acceptable as time passes,
// and too_old and expired are permanent because sending the event again does
// not get it accepted. The sending code in the infrastructure layer classifies
// rejected events with a classifier dedicated to CloudWatch Logs.
func (r RejectedLogEvent) Retryable() bool {
	return r.Reason == RejectedLogEventReasonTooNew
}

// RejectedLogEventError reports that CloudWatch Logs rejected one log event
// within an HTTP 200 response. Extract it with errors.As to determine the
// position and the reason of the rejected event.
type RejectedLogEventError struct {
	// Index is the position of the rejected event in the sent request.
	Index int
	// Reason is the rejection reason that CloudWatch Logs returned.
	Reason RejectedLogEventReason
}

// Error returns the position and the reason of the rejected event in the form
// "cloudwatch logs rejected log event at index <Index>: <Reason>".
func (e *RejectedLogEventError) Error() string {
	return fmt.Sprintf("cloudwatch logs rejected log event at index %d: %s", e.Index, e.Reason)
}

// HasRejectedLogEvents reports whether per-event rejection indexes are present.
func (i *RejectedLogEventsInfo) HasRejectedLogEvents() bool {
	return i != nil &&
		(i.ExpiredLogEventEndIndex != nil ||
			i.TooNewLogEventStartIndex != nil ||
			i.TooOldLogEventEndIndex != nil)
}

// RejectedEvents expands the range representation of a CloudWatch Logs response
// into rejection information for each index.
//
// tooOldLogEventEndIndex is treated as an exclusive end, as the AWS API defines
// it. tooNewLogEventStartIndex is treated as an inclusive start, and the events
// from there to the end are treated as rejected. expiredLogEventEndIndex is
// also treated as an exclusive end that represents a rejected range at the
// beginning. Values outside the range of eventCount are clamped so that they
// do not break the caller's slice.
func (i *RejectedLogEventsInfo) RejectedEvents(eventCount int) []RejectedLogEvent {
	if i == nil || eventCount <= 0 {
		return nil
	}

	reasons := make([]RejectedLogEventReason, eventCount)
	if i.TooOldLogEventEndIndex != nil {
		end := clampIndex(*i.TooOldLogEventEndIndex, eventCount)
		for idx := 0; idx < end; idx++ {
			reasons[idx] = RejectedLogEventReasonTooOld
		}
	}
	if i.ExpiredLogEventEndIndex != nil {
		end := clampIndex(*i.ExpiredLogEventEndIndex, eventCount)
		for idx := 0; idx < end; idx++ {
			reasons[idx] = RejectedLogEventReasonExpired
		}
	}
	if i.TooNewLogEventStartIndex != nil {
		for idx := clampIndex(*i.TooNewLogEventStartIndex, eventCount); idx < eventCount; idx++ {
			reasons[idx] = RejectedLogEventReasonTooNew
		}
	}

	rejected := make([]RejectedLogEvent, 0)
	for idx, reason := range reasons {
		if reason == "" {
			continue
		}
		rejected = append(rejected, RejectedLogEvent{Index: idx, Reason: reason})
	}
	return rejected
}

func clampIndex(value int, eventCount int) int {
	if value < 0 {
		return 0
	}
	if value > eventCount {
		return eventCount
	}
	return value
}

// New validates the Config and creates a CloudWatch Logs client.
func New(cfg Config) (*HTTPClient, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}

	region := strings.TrimSpace(cfg.Region)
	endpoint, err := normalizeEndpoint(cfg.Endpoint, region)
	if err != nil {
		return nil, err
	}

	// HTTPClient が未指定の場合だけ、このクライアント専用の既定値を適用します。
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		timeout := cfg.Timeout
		if timeout == 0 {
			timeout = defaultTimeout
		}
		httpClient = &http.Client{Timeout: timeout}
	}

	credentialsProvider, err := resolveCredentialsProvider(cfg)
	if err != nil {
		return nil, err
	}

	client := &HTTPClient{
		endpoint:            endpoint,
		region:              region,
		logGroupName:        cfg.LogGroupName,
		httpClient:          httpClient,
		credentialsProvider: credentialsProvider,
		headers:             copyHeaders(cfg.Headers),
		autoCreateLogStream: cfg.AutoCreateLogStream,
		compressRequest:     cfg.CompressRequest,
	}
	return client, nil
}

// PutLogEvents sends one request that the caller has already routed. Even when
// it creates the stream after ResourceNotFound and sends the request again, it
// changes neither the stream name nor AttemptedAt fixed in the request.
func (c *HTTPClient) PutLogEvents(
	ctx context.Context,
	request PutLogEventsRequest,
) (*PutLogEventsOutput, error) {
	if len(request.Events) == 0 {
		return &PutLogEventsOutput{}, nil
	}
	if request.AttemptedAt.IsZero() {
		return nil, fmt.Errorf("cloudwatch logs send time is required")
	}
	if err := ValidateLogStreamName(request.LogStreamName); err != nil {
		return nil, err
	}
	logStreamName := request.LogStreamName

	output, err := c.putLogEventsOnce(ctx, logStreamName, request.Events, request.AttemptedAt)
	if err == nil || !c.autoCreateLogStream || !IsResourceNotFound(err) {
		return output, err
	}

	// 同時作成による AlreadyExists は成功扱いにして、送信を継続します。
	if createErr := c.createLogStream(ctx, logStreamName); createErr != nil && !IsResourceAlreadyExists(createErr) {
		return nil, &AutoCreateLogStreamError{PutError: err, CreateError: createErr}
	}
	output, retryErr := c.putLogEventsOnce(ctx, logStreamName, request.Events, request.AttemptedAt)
	if unsent, ok := retryErr.(*RequestNotSentError); ok {
		// 2回目のHTTP requestは開始されていませんが、1回目のrequestは開始され、明示的に拒否されました。
		// 配送結果の報告に使えるよう、両方の事実を保持します。
		withPrevious := *unsent
		withPrevious.PreviousPutError = err
		retryErr = &withPrevious
	}
	return output, retryErr
}

// CreateLogStream creates, in the configured log group, the stream that the
// caller has determined. It does not perform routing such as date suffixes.
func (c *HTTPClient) CreateLogStream(ctx context.Context, logStreamName string) error {
	if err := ValidateLogStreamName(logStreamName); err != nil {
		return err
	}
	return c.createLogStream(ctx, logStreamName)
}

func (c *HTTPClient) createLogStream(ctx context.Context, logStreamName string) error {
	req := createLogStreamRequest{
		LogGroupName:  c.logGroupName,
		LogStreamName: logStreamName,
	}
	return c.doJSON(ctx, targetCreateLogStream, req, nil)
}

// putLogEventsOnce は PutLogEvents API への 1 回分の送信処理を行います。
func (c *HTTPClient) putLogEventsOnce(
	ctx context.Context,
	logStreamName string,
	events []Event,
	sentAt time.Time,
) (*PutLogEventsOutput, error) {
	payload := putLogEventsRequest{
		LogGroupName:  c.logGroupName,
		LogStreamName: logStreamName,
		LogEvents:     make([]inputLogEvent, 0, len(events)),
	}

	preparedEvents, err := preparePutLogEvents(events, sentAt)
	if err != nil {
		return nil, err
	}

	for _, event := range preparedEvents {
		payload.LogEvents = append(payload.LogEvents, inputLogEvent{
			Timestamp: event.TimestampMillis,
			Message:   event.Message,
		})
	}

	var output *PutLogEventsOutput
	if err := c.doJSON(ctx, targetPutLogEvents, payload, &output); err != nil {
		return nil, err
	}
	if err := output.Validate(len(events)); err != nil {
		return nil, &ResponseDecodeError{Operation: targetPutLogEvents, Err: err}
	}
	return output, nil
}

// doJSON は JSON 化、任意の gzip 圧縮、SigV4 署名、HTTP 送信、応答デコードを行います。
func (c *HTTPClient) doJSON(ctx context.Context, target string, payload any, output any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal cloudwatch logs request: %w", err)
	}
	requestBody, compressed, err := prepareRequestBody(body, c.compressRequest)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return fmt.Errorf("failed to create cloudwatch logs request: %w", err)
	}
	for key, value := range c.headers {
		req.Header.Set(key, value)
	}
	// API 必須ヘッダーはユーザー指定ヘッダーより優先します。
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Amz-Target", target)
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}

	if err := c.signRequest(req, requestBody); err != nil {
		return &RequestNotSentError{Operation: target, Err: err}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send cloudwatch logs request: %w", err)
	}
	defer resp.Body.Close()

	if !isSuccessStatus(resp.StatusCode) {
		// 過大なエラー応答によるメモリ消費を避けるため、本文を上限付きで読み込みます。
		body, truncated, readErr := readHTTPStatusErrorBody(resp.Body)
		if readErr != nil {
			// 本文の読み込みerrorは文言だけを残し、原因としては包みません。再試行の判定は
			// EOFやcontextなどの型付きerrorをHTTP statusより優先するため、包むとstatusによる判定が変わります。
			body = []byte(readErr.Error())
			truncated = false
		}
		statusErr := newHTTPStatusError(resp, body, truncated)
		return statusErr
	}

	if output == nil {
		// 接続を再利用しやすくするため、不要な成功レスポンスも最後まで読み捨てます。
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(output); err != nil {
		return &ResponseDecodeError{Operation: target, Err: err}
	}
	// eventの結果を受け入れる前に、JSON値の後に届くgzip checksumやread errorも含めて、
	// response全体を検証します。
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("unexpected additional JSON value")
		}
		return &ResponseDecodeError{Operation: target, Err: err}
	}
	return nil
}

// prepareRequestBody は必要に応じて JSON 本文を gzip 圧縮します。
// 第 2 戻り値は圧縮の有無を示します。
func prepareRequestBody(body []byte, compress bool) ([]byte, bool, error) {
	if !compress {
		return body, false, nil
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(body); err != nil {
		_ = zw.Close()
		return nil, false, fmt.Errorf("failed to gzip cloudwatch logs request body: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close gzip writer: %w", err)
	}
	return buf.Bytes(), true, nil
}

// signRequest は本文の SHA-256 ハッシュを計算し、AWS SigV4 署名を付与します。
func (c *HTTPClient) signRequest(req *http.Request, body []byte) error {
	credentials, err := c.credentialsProvider.Retrieve(req.Context())
	if err != nil {
		return fmt.Errorf("failed to retrieve cloudwatch logs credentials: %w", err)
	}

	h := sha256.New()
	h.Write(body)
	payloadHash := hex.EncodeToString(h.Sum(nil))

	signer := v4.NewSigner()
	return signer.SignHTTP(req.Context(), credentials, req, payloadHash, serviceName, c.region, time.Now())
}

// putLogEventsRequest は PutLogEvents API の JSON リクエスト本文です。
type putLogEventsRequest struct {
	LogGroupName  string          `json:"logGroupName"`
	LogStreamName string          `json:"logStreamName"`
	LogEvents     []inputLogEvent `json:"logEvents"`
}

// createLogStreamRequest は CreateLogStream API の JSON リクエスト本文です。
type createLogStreamRequest struct {
	LogGroupName  string `json:"logGroupName"`
	LogStreamName string `json:"logStreamName"`
}

// inputLogEvent は CloudWatch Logs API が受け取るログイベント形式です。
type inputLogEvent struct {
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message"`
}

type preparedPutLogEvent struct {
	TimestampMillis int64
	Message         string
	MessageBytes    int
}

func preparePutLogEvents(events []Event, now time.Time) ([]preparedPutLogEvent, error) {
	prepared := make([]preparedPutLogEvent, 0, len(events))
	for _, event := range events {
		timestamp := event.Timestamp
		if timestamp.IsZero() {
			timestamp = now
		}
		prepared = append(prepared, preparedPutLogEvent{
			TimestampMillis: eventTimestampMillis(timestamp),
			Message:         event.Message,
			MessageBytes:    jsonutil.UTF8StringBytes(event.Message),
		})
	}

	if err := validatePutLogEventsBatch(prepared); err != nil {
		return nil, err
	}
	return prepared, nil
}

func validatePutLogEventsBatch(events []preparedPutLogEvent) error {
	if len(events) > MaxPutLogEventsCount {
		return &validationError{kind: ErrTooManyEvents, message: fmt.Sprintf(
			"invalid cloudwatch logs put batch: event count %d exceeds %d", len(events), MaxPutLogEventsCount,
		)}
	}

	batchBytes := 0
	var firstTimestamp int64
	for idx, event := range events {
		if event.Message == "" {
			return &validationError{kind: ErrEventMessageEmpty, message: fmt.Sprintf(
				"invalid cloudwatch logs put batch: event %d message is empty", idx,
			)}
		}
		if event.MessageBytes > MaxPutLogEventMessageBytes {
			return &EventTooLargeError{Index: idx, MessageBytes: event.MessageBytes, MaxBytes: MaxPutLogEventMessageBytes}
		}
		batchBytes += event.MessageBytes + putLogEventOverheadBytes
		if batchBytes > maxPutLogEventsBatchBytes {
			return &validationError{kind: ErrBatchTooLarge, message: fmt.Sprintf(
				"invalid cloudwatch logs put batch: batch size %d exceeds %d bytes", batchBytes, maxPutLogEventsBatchBytes,
			)}
		}
		if idx > 0 && events[idx-1].TimestampMillis > event.TimestampMillis {
			return unsortedEventsError(idx, event.TimestampMillis, events[idx-1].TimestampMillis)
		}
		if idx == 0 {
			firstTimestamp = event.TimestampMillis
			continue
		}
		if event.TimestampMillis-firstTimestamp > maxPutLogEventsTimeSpan.Milliseconds() {
			return &validationError{kind: ErrBatchTimeSpanTooLong, message: fmt.Sprintf(
				"invalid cloudwatch logs put batch: event time span exceeds %s",
				maxPutLogEventsTimeSpan,
			)}
		}
	}

	return nil
}

// eventTimestampMillis は時刻を Unix エポックからのミリ秒に変換します。
func eventTimestampMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func resolveCredentialsProvider(cfg Config) (aws.CredentialsProvider, error) {
	if cfg.CredentialsProvider != nil {
		return aws.NewCredentialsCache(cfg.CredentialsProvider), nil
	}
	if err := validateCredentialsConfig(cfg); err != nil {
		return nil, err
	}
	return aws.NewCredentialsCache(staticCredentialsProvider{credentials: aws.Credentials{
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		SessionToken:    cfg.SessionToken,
		Source:          "static",
	}}), nil
}

type staticCredentialsProvider struct {
	credentials aws.Credentials
}

func (p staticCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	_ = ctx
	return p.credentials, nil
}

// normalizeEndpoint はエンドポイントを補完・検証し、末尾のスラッシュを除去します。
func normalizeEndpoint(endpoint, region string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		domain := "amazonaws.com"
		if strings.HasPrefix(region, "cn-") {
			domain = "amazonaws.com.cn"
		}
		endpoint = fmt.Sprintf("https://logs.%s.%s", region, domain)
	}

	// 解析できないendpointのerrorにも、URLのuser info（パスワードなど）を含めません。
	parsed, err := endpointurl.Parse(endpoint)
	if err != nil {
		return "", &validationError{
			kind: ErrInvalidEndpoint, cause: err, message: fmt.Sprintf("invalid cloudwatch logs endpoint: %v", err),
		}
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", &validationError{kind: ErrInvalidEndpoint, message: "cloudwatch logs endpoint must include scheme and host"}
	}
	if parsed.User != nil {
		return "", &validationError{kind: ErrInvalidEndpoint, message: "cloudwatch logs endpoint must not include user info"}
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", &validationError{kind: ErrInvalidEndpoint, message: "cloudwatch logs endpoint scheme must be http or https"}
	}
	if parsed.ForceQuery || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", &validationError{
			kind: ErrInvalidEndpoint, message: "cloudwatch logs endpoint must not include query or fragment",
		}
	}

	// 末尾にあるliteralの区切り文字を取り除きます。proxy pathのsegment内でencodeされた区切り文字は、
	// 末尾のencodeされたslashも含めて変更しません。
	parsed.RawPath = strings.TrimRight(parsed.EscapedPath(), "/")
	parsed.Path, err = url.PathUnescape(parsed.RawPath)
	if err != nil {
		return "", &validationError{
			kind: ErrInvalidEndpoint, cause: err, message: fmt.Sprintf("invalid cloudwatch logs endpoint path: %v", err),
		}
	}
	return parsed.String(), nil
}

// isSuccessStatus は HTTP ステータスコードが 2xx かどうかを返します。
func isSuccessStatus(code int) bool {
	return code >= http.StatusOK && code < http.StatusMultipleChoices
}

// copyHeaders は呼び出し元による後続変更の影響を避けるため、マップを複製します。
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

// HTTPStatusError is the error returned when the CloudWatch Logs API returns a
// status other than 2xx. Its details can be obtained with errors.As or
// AsHTTPStatusError.
type HTTPStatusError struct {
	// Code is the HTTP status code.
	Code int

	// Status is the HTTP status string, such as "400 Bad Request".
	Status string

	// Body is the response body, read up to the limit. If reading the body
	// fails, Body holds the text of the read error instead.
	Body []byte

	// BodyTruncated indicates that the body exceeded the limit and was truncated.
	BodyTruncated bool

	// AwsErrorType is the type of the AWS error.
	AwsErrorType string

	// AwsMessage is the error message that AWS returned.
	AwsMessage string

	// RetryAfterDur is the wait duration calculated from the Retry-After header.
	RetryAfterDur time.Duration
}

// Error returns the HTTP error and the AWS error information that could be
// obtained, as a string.
func (e *HTTPStatusError) Error() string {
	if e == nil {
		return ""
	}

	parts := []string{fmt.Sprintf("cloudwatch logs http status error: code=%d status=%s", e.Code, e.Status)}
	if e.AwsErrorType != "" {
		parts = append(parts, "type="+e.AwsErrorType)
	}
	if e.AwsMessage != "" {
		parts = append(parts, "message="+e.AwsMessage)
	}
	if len(e.Body) > 0 {
		parts = append(parts, "body="+formatHTTPStatusErrorBody(e.Body, e.BodyTruncated))
	}
	return strings.Join(parts, " ")
}

// StatusCode returns the HTTP status code.
func (e *HTTPStatusError) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.Code
}

// RetryAfter returns the wait duration calculated from the Retry-After header.
func (e *HTTPStatusError) RetryAfter() time.Duration {
	if e == nil {
		return 0
	}
	return e.RetryAfterDur
}

// ErrorType returns the AWS error type with its namespace and suffix removed.
func (e *HTTPStatusError) ErrorType() string {
	if e == nil {
		return ""
	}
	return normalizeAWSErrorType(e.AwsErrorType)
}

// newHTTPStatusError は HTTP レスポンスと本文から HTTPStatusError を組み立てます。
//
// AWS error type は X-Amzn-Errortype header、JSON body の code、JSON body の __type の順で採用します。
// header は AWS JSON 1.1 の代表的な error source で、body よりも transport に近い情報として優先します。
func newHTTPStatusError(resp *http.Response, body []byte, truncated bool) *HTTPStatusError {
	statusErr := &HTTPStatusError{
		Code:          resp.StatusCode,
		Status:        resp.Status,
		Body:          body,
		BodyTruncated: truncated,
		RetryAfterDur: parseRetryAfter(resp),
	}

	statusErr.AwsErrorType = firstAWSErrorType(resp.Header.Get("X-Amzn-Errortype"))
	var awsErr awsErrorBody
	if err := json.Unmarshal(body, &awsErr); err == nil {
		statusErr.AwsErrorType = firstAWSErrorType(statusErr.AwsErrorType, awsErr.Code, awsErr.Type)
		statusErr.AwsMessage = awsErr.Message
	}
	if statusErr.AwsMessage == "" {
		var alt awsErrorBody
		if err := json.Unmarshal(body, &alt); err == nil {
			statusErr.AwsMessage = alt.Message
		}
	}
	return statusErr
}

type awsErrorBody struct {
	Type    string `json:"__type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// IsResourceNotFound reports whether err is a ResourceNotFoundException.
func IsResourceNotFound(err error) bool {
	return hasAWSErrorType(err, ErrorTypeResourceNotFoundException)
}

// IsResourceAlreadyExists reports whether err is a ResourceAlreadyExistsException.
func IsResourceAlreadyExists(err error) bool {
	return hasAWSErrorType(err, ErrorTypeResourceAlreadyExistsException)
}

// hasAWSErrorType はエラー連鎖内の AWS エラー種別が want と一致するか判定します。
func hasAWSErrorType(err error, want string) bool {
	var statusErr *HTTPStatusError
	if !AsHTTPStatusError(err, &statusErr) {
		return false
	}
	return statusErr.ErrorType() == want
}

// AsHTTPStatusError extracts an HTTPStatusError from the error chain.
func AsHTTPStatusError(err error, target **HTTPStatusError) bool {
	return errors.As(err, target)
}

// firstAWSErrorType は、優先順位どおりに最初に見つかった AWS error type を正規化して返します。
func firstAWSErrorType(values ...string) string {
	for _, value := range values {
		if normalized := normalizeAWSErrorType(value); normalized != "" {
			return normalized
		}
	}
	return ""
}

// normalizeAWSErrorType は AWS エラー種別から名前空間や URI 接尾辞を除去します。
//
// 正規化順序:
//  1. 前後の空白を除去する
//  2. 最初の ":" より前だけを残す
//  3. 最初の "#" より後だけを残す
//  4. 再度前後の空白を除去する
func normalizeAWSErrorType(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if idx := strings.Index(value, ":"); idx >= 0 {
		value = value[:idx]
	}
	if idx := strings.Index(value, "#"); idx >= 0 {
		value = value[idx+1:]
	}
	return strings.TrimSpace(value)
}

// readHTTPStatusErrorBody はエラー本文を上限付きで読み込み、切り詰めの有無を返します。
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

// formatHTTPStatusErrorBody は本文を最大長に整え、必要なら省略表示を付加します。
func formatHTTPStatusErrorBody(body []byte, truncated bool) string {
	if len(body) > maxHTTPStatusErrorBodyBytes {
		body = body[:maxHTTPStatusErrorBodyBytes]
		truncated = true
	}
	if !truncated {
		return string(body)
	}
	return string(body) + truncatedHTTPStatusErrorBody
}

func parseRetryAfter(resp *http.Response) time.Duration {
	v := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if v == "" {
		return 0
	}

	if secs, ok := parseRetryAfterSeconds(v); ok {
		// time.Durationで表せない秒数は最大値へ飽和させ、呼び出し側の待機上限を適用させます。
		// 0にすると、serverが長い待機を求めたのに即時再送になります。HTTP-dateも同じく飽和します。
		const maxDelay = time.Duration(1<<63 - 1)
		if secs > int64(maxDelay/time.Second) {
			return maxDelay
		}
		return time.Duration(secs) * time.Second
	}

	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			return 0
		}
		return d
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
