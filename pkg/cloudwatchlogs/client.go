// Package cloudwatchlogs は、AWS CloudWatch Logs の PutLogEvents API と
// CreateLogStream API を SigV4 署名付き HTTP リクエストで呼び出すための
// 軽量クライアントを提供します。
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

// CloudWatch Logs API の既定値、ターゲット名、エラー処理用定数です。
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
	// MaxPutLogEventsCount は、一回の PutLogEvents requestへ含められるevent件数の上限です。
	MaxPutLogEventsCount = 10_000
	// MaxPutLogEventBytes は、CloudWatch Logs が 1 event に許可する event size の上限です。
	// event size は request size と同じく、JSON decode後の message の UTF-8 byte数に
	// event ごとの付加サイズ 26 byte を加えて数えます。message 自体の上限ではないため、
	// 送信前の message 長の判定には MaxPutLogEventMessageBytes を使用してください。
	MaxPutLogEventBytes = 1_048_576
	// MaxPutLogEventMessageBytes は、1 event の message として送信できる最大 UTF-8 byte数です。
	// event が一件だけの request でも付加サイズ 26 byte を含めて MaxPutLogEventBytes と
	// request size の上限に収める必要があるため、1,048,550 byte です。
	MaxPutLogEventMessageBytes = MaxPutLogEventBytes - putLogEventOverheadBytes
	putLogEventOverheadBytes   = 26
)

// HTTPClientはCloudWatch Logs APIへHTTP requestを送るClient実装です。
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

// Config は HTTPClient の生成に必要な設定を保持します。
type Config struct {
	// Region はCloudWatch LogsのAWSリージョンです。必須です。
	// application固有の既定値は、このclientへ渡す前に設定してください。
	Region string

	// Endpoint は送信先の完全な URL です。
	// 空の場合は Region から AWS の標準エンドポイントを生成します。
	Endpoint string

	// LogGroupName は送信先のロググループ名です。必須です。
	LogGroupName string

	// Timeout は HTTPClient が nil の場合に生成する HTTP クライアントの
	// タイムアウトです。0はdefault timeoutを使う未指定sentinel、負値は不正です。
	Timeout time.Duration

	// AccessKeyID は SigV4 署名に使用するアクセスキー ID です。
	// CredentialsProvider が nil の場合は必須です。
	AccessKeyID string

	// SecretAccessKey は SigV4 署名に使用するシークレットアクセスキーです。
	// CredentialsProvider が nil の場合は必須です。
	SecretAccessKey string

	// SessionToken は一時認証情報を使う場合のセッショントークンです。
	SessionToken string

	// CredentialsProvider は SigV4 署名に使用する AWS 認証情報 provider です。
	// 設定されている場合、AccessKeyID / SecretAccessKey より優先します。
	CredentialsProvider aws.CredentialsProvider

	// Headers は各 HTTP リクエストに追加するヘッダーです。
	// API 必須ヘッダーは、このクライアント側の値で上書きされます。
	Headers map[string]string

	// AutoCreateLogStream が true の場合、送信先ストリームが存在しないときに
	// ストリームを作成し、PutLogEvents を 1 回だけ再実行します。
	AutoCreateLogStream bool

	// CompressRequest が true の場合、JSON リクエスト本文を gzip 圧縮します。
	CompressRequest bool

	// HTTPClient は API 呼び出しに使用する HTTP クライアントです。
	// nil の場合は Timeout を設定したクライアントを生成します。
	HTTPClient *http.Client
}

// PutLogEventsRequestは、routing済みの一回分のCloudWatch Logs送信を表します。
// LogStreamNameはevent発生日時から送信側が事前に確定し、AttemptedAtはretryを通じて
// 変更しません。Eventsは一つのstreamに属し、Timestamp昇順でなければなりません。
type PutLogEventsRequest struct {
	LogStreamName string
	Events        []Event
	AttemptedAt   time.Time
}

// Clientは、確定済みのPutLogEventsRequestをCloudWatch Logsへ送る契約です。
// retryやcircuit breakerはこの一つのI/O操作を装飾し、stream名の解決には関与しません。
type Client interface {
	PutLogEvents(ctx context.Context, request PutLogEventsRequest) (*PutLogEventsOutput, error)
}

// Event は CloudWatch Logs に送信する 1 件のログイベントです。
type Event struct {
	// Timestamp はイベントの発生時刻です。
	// ゼロ値の場合は送信処理時の現在時刻を使用します。
	Timestamp time.Time

	// Message は送信するログ本文です。
	Message string
}

// PutLogEventsOutput は PutLogEvents の成功結果を表します。
//
// CloudWatch Logs は HTTP 200 を返しても、batch 内の一部 log event だけを拒否することがあります。
// 呼び出し元は RejectedLogEventsInfo を見て、受理済み event と拒否 event を分けて扱います。
type PutLogEventsOutput struct {
	// NextSequenceToken は CloudWatch Logs が返す deprecated field です。
	// 現在 PutLogEvents では sequence token が無視されますが、互換性のため保持します。
	NextSequenceToken string `json:"nextSequenceToken,omitempty"`

	// RejectedLogEventsInfo は、HTTP 200 の中で拒否された log event の index 情報です。
	RejectedLogEventsInfo *RejectedLogEventsInfo `json:"rejectedLogEventsInfo,omitempty"`

	// RejectedEntityInfo は entity だけが拒否された場合の情報です。
	// この場合でも log event は受理されている可能性があります。
	RejectedEntityInfo *RejectedEntityInfo `json:"rejectedEntityInfo,omitempty"`
}

// HasRejectedLogEvents は、PutLogEvents response が event 単位の拒否情報を含むかを返します。
func (o *PutLogEventsOutput) HasRejectedLogEvents() bool {
	return o != nil &&
		o.RejectedLogEventsInfo != nil &&
		o.RejectedLogEventsInfo.HasRejectedLogEvents()
}

// RejectedLogEventsInfo は PutLogEvents が HTTP 200 で返す event 単位の拒否情報です。
//
// AWS の index は、実際に送信した logEvents 配列に対する位置です。
// 呼び出し元が送信前に並べ替えた場合は、その送信順を元recordとの対応情報と
// 一緒に保持し、responseのindexを同じ順序へ適用する必要があります。
type RejectedLogEventsInfo struct {
	ExpiredLogEventEndIndex  *int `json:"expiredLogEventEndIndex,omitempty"`
	TooNewLogEventStartIndex *int `json:"tooNewLogEventStartIndex,omitempty"`
	TooOldLogEventEndIndex   *int `json:"tooOldLogEventEndIndex,omitempty"`
}

// RejectedEntityInfo は PutLogEvents の entity 拒否情報です。
type RejectedEntityInfo struct {
	ErrorType string `json:"errorType,omitempty"`
}

// RejectedLogEventReason は、CloudWatch Logs が log event を拒否した理由です。
type RejectedLogEventReason string

const (
	RejectedLogEventReasonExpired RejectedLogEventReason = "expired"
	RejectedLogEventReasonTooOld  RejectedLogEventReason = "too_old"
	RejectedLogEventReasonTooNew  RejectedLogEventReason = "too_new"
)

// RejectedLogEvent は、拒否された log event の index と理由です。
type RejectedLogEvent struct {
	Index  int
	Reason RejectedLogEventReason
}

// Retryable は、この拒否理由を後で再送してよいかを返します。
//
// too_new は時間経過で受理可能になるため retryable とし、
// too_old / expired は再送しても受理されないため permanent とします。
// infrastructure 層の送信処理では CloudWatch Logs 専用 classifier を使って分類します。
func (r RejectedLogEvent) Retryable() bool {
	return r.Reason == RejectedLogEventReasonTooNew
}

// RejectedLogEventErrorは、CloudWatch LogsがHTTP 200の応答内で一件のlog eventを
// 拒否したことを表します。errors.Asで取り出すと、拒否されたeventの位置と理由を判別できます。
type RejectedLogEventError struct {
	// Indexは、拒否されたeventの送信request内での位置です。
	Index int
	// Reasonは、CloudWatch Logsが返した拒否理由です。
	Reason RejectedLogEventReason
}

// Errorは、拒否されたeventの位置と理由を"cloudwatch logs rejected log event at index
// <Index>: <Reason>"の形式で返します。
func (e *RejectedLogEventError) Error() string {
	return fmt.Sprintf("cloudwatch logs rejected log event at index %d: %s", e.Index, e.Reason)
}

// HasRejectedLogEvents は、event 単位の拒否 index が含まれているかを返します。
func (i *RejectedLogEventsInfo) HasRejectedLogEvents() bool {
	return i != nil &&
		(i.ExpiredLogEventEndIndex != nil ||
			i.TooNewLogEventStartIndex != nil ||
			i.TooOldLogEventEndIndex != nil)
}

// RejectedEvents は、CloudWatch Logs response の範囲表現を index ごとの拒否情報へ展開します。
//
// tooOldLogEventEndIndex は AWS API 定義どおり exclusive end として扱います。
// tooNewLogEventStartIndex は inclusive start として扱い、そこから末尾までを拒否扱いにします。
// expiredLogEventEndIndex も先頭側の拒否範囲を表すexclusive endとして扱います。
// eventCount の範囲外に出る値は、呼び出し元の配列を壊さないよう clamp します。
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

// New は Config を検証し、CloudWatch Logs クライアントを生成します。
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

// PutLogEventsは、呼び出し側でrouting済みの一回分のrequestを送信します。
// ResourceNotFound後のstream作成と再送でも、requestに確定済みのstream名と
// AttemptedAtを変更しません。
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
		// The second HTTP request never started, but the first request did and
		// was explicitly refused. Preserve both facts for delivery reporting.
		withPrevious := *unsent
		withPrevious.PreviousPutError = err
		retryErr = &withPrevious
	}
	return output, retryErr
}

// CreateLogStreamは、設定済みlog group内に呼び出し側で確定したstreamを作成します。
// 日付suffixなどのroutingは行いません。
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

// putLogEvents は PutLogEvents API への 1 回分の送信処理を行います。
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
	// Verify the entire response before accepting event outcomes, including
	// gzip checksums and read errors delivered after the JSON value.
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

	// Remove literal trailing delimiters without changing encoded separators
	// within a proxy path segment, including an encoded terminal slash.
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

// HTTPStatusError は CloudWatch Logs API が 2xx 以外を返した場合のエラーです。
// errors.As または AsHTTPStatusError で詳細を取得できます。
type HTTPStatusError struct {
	// Code は HTTP ステータスコードです。
	Code int

	// Status は "400 Bad Request" のような HTTP ステータス文字列です。
	Status string

	// Body は上限まで読み込んだレスポンス本文です。
	Body []byte

	// BodyTruncated は本文が上限を超えて切り詰められたことを示します。
	BodyTruncated bool

	// AwsErrorType は AWS エラーの種類です。
	AwsErrorType string

	// AwsMessage は AWS が返したエラーメッセージです。
	AwsMessage string

	// RetryAfterDur は Retry-After ヘッダーから算出した待機時間です。
	RetryAfterDur time.Duration
}

// Error は HTTP エラーと、取得できた AWS エラー情報を文字列で返します。
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

// StatusCode は HTTP ステータスコードを返します。
func (e *HTTPStatusError) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.Code
}

// RetryAfter は Retry-After ヘッダーから算出した待機時間を返します。
func (e *HTTPStatusError) RetryAfter() time.Duration {
	if e == nil {
		return 0
	}
	return e.RetryAfterDur
}

// ErrorType は名前空間や接尾辞を除去した AWS エラー種別を返します。
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

// IsResourceNotFound は err が ResourceNotFoundException かどうかを返します。
func IsResourceNotFound(err error) bool {
	return hasAWSErrorType(err, ErrorTypeResourceNotFoundException)
}

// IsResourceAlreadyExists は err が ResourceAlreadyExistsException かどうかを返します。
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

// AsHTTPStatusError はエラー連鎖から HTTPStatusError を取り出します。
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
