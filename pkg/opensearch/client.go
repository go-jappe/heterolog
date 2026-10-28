package opensearch

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/go-jappe/heterolog/pkg/opaqueid"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// AuthType is an enumerated type that represents an authentication type.
type AuthType string

const (
	// AuthTypeBasic represents Basic authentication.
	AuthTypeBasic AuthType = "basic"
	// AuthTypeAPIKey represents API key authentication.
	AuthTypeAPIKey AuthType = "apikey"
	// AuthTypeAWSIAM represents AWS IAM authentication.
	AuthTypeAWSIAM AuthType = "awsiam"
	// AuthTypeSAML represents SAML authentication.
	AuthTypeSAML AuthType = "saml"
	// AuthTypeOIDC represents OpenID Connect authentication.
	AuthTypeOIDC AuthType = "oidc"
	// AuthTypeCertificate represents client certificate authentication.
	AuthTypeCertificate AuthType = "certificate"
	// AuthTypeNone represents no authentication.
	AuthTypeNone AuthType = "none"
)

// IndexResponse represents the response of an OpenSearch index operation for a
// single document.
type IndexResponse struct {
	Index   string `json:"_index"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
	Result  string `json:"result"`
	Status  int    `json:"status"`
}

// BulkResponse represents the response of an OpenSearch bulk operation.
type BulkResponse struct {
	Took   int                         `json:"took"`
	Errors bool                        `json:"errors"`
	Items  []map[string]BulkItemResult `json:"items"`
}
type BulkItemResult struct {
	Index   string       `json:"_index"`
	ID      string       `json:"_id"`
	Status  int          `json:"status"`
	Result  string       `json:"result,omitempty"`
	Version int          `json:"_version,omitempty"`
	Error   *ErrorDetail `json:"error,omitempty"`
}

// Client is the interface for communicating with OpenSearch.
type Client interface {
	// IndexDocument adds a single document to an index.
	// On success, it returns the OpenSearch response.
	IndexDocument(ctx context.Context, doc DocumentWriteRequest) (*IndexResponse, error)

	// IndexDocuments adds multiple documents to indexes with a bulk operation.
	// On success, it returns the OpenSearch bulk response.
	IndexDocuments(ctx context.Context, docs []DocumentWriteRequest) (*BulkResponse, error)
}

// realClient はClient インターフェースの実装
type realClient struct {
	endpoint   string
	httpClient *http.Client
	authConfig AuthConfig

	compressRequest bool //　圧縮送信可否
}

// AuthConfig is a struct that holds authentication settings.
type AuthConfig struct {
	Type   AuthType
	Config any // the actual type differs for each authentication method
}

type BasicAuthConfig struct {
	Username string
	Password string
}

type APIKeyAuthConfig struct {
	ID     string
	Secret string
}

type AWSIAMAuthConfig struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type OIDCAuthConfig struct {
	Token string
}

type CertificateAuthConfig struct {
	CertFile string
	KeyFile  string
	// CAFile, when set, is a PEM file whose certificates are trusted for
	// verifying the server instead of the system roots.
	CAFile string
}

type SAMLAuthConfig struct {
	Token string
}

// Config is the configuration of an OpenSearch client.
type Config struct {
	Endpoint string // "https://example.com:9200"
	// Timeout is the time limit of an HTTP request. Zero is the unset sentinel
	// that selects the default timeout, and a negative value is invalid.
	Timeout time.Duration
	// SkipVerifySSL, when true, disables verification of the server certificate
	// and host name entirely, not only for self-signed certificates. It has no
	// effect on an http endpoint. To keep verification against a private CA,
	// use CertificateAuthConfig.CAFile with certificate authentication.
	SkipVerifySSL bool
	AuthConfig    AuthConfig

	CompressRequest bool // whether to send requests compressed
}

// DocumentWriteRequest holds the document and operation to write to OpenSearch.
type DocumentWriteRequest struct {
	Index string // must be non-empty and have no surrounding whitespace
	ID    string // must be non-empty
	Doc   map[string]any
	Op    WriteOp // WriteOpIndex or WriteOpCreate
}

// ActionPath returns the OpenSearch endpoint for the write operation.
func (d DocumentWriteRequest) ActionPath() (string, error) {
	switch d.Op {
	case WriteOpIndex:
		return endpointDoc, nil
	case WriteOpCreate:
		return endpointCreate, nil
	default:
		return "", fmt.Errorf("unknown op: %s", d.Op)
	}
}

// BulkActionKey returns the OpenSearch bulk action for the write operation.
func (d DocumentWriteRequest) BulkActionKey() (string, error) {
	switch d.Op {
	case WriteOpIndex:
		return bulkActionIndex, nil
	case WriteOpCreate:
		return bulkActionCreate, nil
	default:
		return "", fmt.Errorf("unknown op: %s", d.Op)
	}
}

const (
	endpointDoc    = "_doc"
	endpointCreate = "_create"

	bulkActionIndex  = "index"
	bulkActionCreate = "create"

	defaultTimeout = 10 * time.Second
)

type WriteOp string

const (
	WriteOpIndex  WriteOp = "index"
	WriteOpCreate WriteOp = "create"
)

// New creates a new OpenSearch client. Its requests do not follow redirects
// and return a 3xx response as an HTTPStatusError.
func New(cfg Config) (Client, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}

	// HTTPクライアントの設定
	transport := &http.Transport{}

	// スキーマからTLSが必要かどうか判定
	if strings.HasPrefix(strings.ToLower(cfg.Endpoint), "https://") {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: cfg.SkipVerifySSL,
		}

		if cfg.AuthConfig.Type == AuthTypeCertificate {
			if err := setupClientCertificates(tlsConfig, cfg.AuthConfig); err != nil {
				return nil, fmt.Errorf("failed to setup client certificates: %w", err)
			}
		}

		transport.TLSClientConfig = tlsConfig
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	// redirectは、設定した送信先が本来の送信先ではないことを表します。追従すると、301/302/303
	// では本文なしのGETへ変わり、307/308では文書を別URLへ再送するため、どれも追従せず、
	// 元のHTTP statusを報告します。
	httpClient := &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	// fmt.Printf("AuthConfig.Type=%q\n", cfg.AuthConfig.Type)

	return &realClient{
		endpoint:   strings.TrimRight(cfg.Endpoint, "/"),
		httpClient: httpClient,
		authConfig: AuthConfig{
			Type:   cfg.AuthConfig.Type,
			Config: cfg.AuthConfig.Config,
		},
		compressRequest: cfg.CompressRequest,
	}, nil
}

// setupClientCertificates はクライアント証明書認証のためのTLS設定を行います
func setupClientCertificates(tlsConfig *tls.Config, authConfig AuthConfig) error {
	conf, ok := authConfig.Config.(CertificateAuthConfig)
	if !ok {
		return fmt.Errorf("invalid CertificateAuthConfig")
	}

	cert, err := tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
	if err != nil {
		return fmt.Errorf("failed to load client certificate: %w", err)
	}

	if conf.CAFile != "" {
		caCert, err := os.ReadFile(conf.CAFile)
		if err != nil {
			return fmt.Errorf("failed to read CA certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return fmt.Errorf("failed to append CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	tlsConfig.Certificates = []tls.Certificate{cert}
	return nil
}

// IndexDocument は単一のドキュメントをインデックスに追加します。
// NOTE: このメソッドはリトライしません（1回だけ送信します）。
// リトライ制御は呼び出し側（RetryingClient 等）で行ってください。
func (r *realClient) IndexDocument(ctx context.Context, doc DocumentWriteRequest) (*IndexResponse, error) {
	if doc.ID == "" {
		return nil, fmt.Errorf("document id is required")
	}
	if err := ValidateIndex(doc.Index); err != nil {
		return nil, err
	}

	action, err := doc.ActionPath()
	if err != nil {
		return nil, err
	}

	// date mathのindex式では、PathEscapeが変更しないtime zoneの'+'や':'も含め、すべての予約文字を
	// encodeする必要があります。QueryEscapeがそのencodeを行います。path内の空白は'+'ではなく
	// %20にする必要があります。
	indexPath := strings.ReplaceAll(url.QueryEscape(doc.Index), "+", "%20")
	urlStr := fmt.Sprintf("%s/%s/%s/%s", r.endpoint, indexPath, action, url.PathEscape(doc.ID))

	bodyBytes, err := json.Marshal(doc.Doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	sendBody, compressed, err := r.prepareRequestBody(bodyBytes)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, urlStr, bytes.NewReader(sendBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}

	if opaqueID, ok := opaqueid.OpaqueID(ctx); ok && opaqueID != "" {
		req.Header.Set("X-Opaque-Id", opaqueID)
	}

	// 認証
	if r.authConfig.Type == AuthTypeAWSIAM {
		if err := r.signRequest(req, sendBody); err != nil {
			return nil, fmt.Errorf("failed to sign request: %w", err)
		}
	} else {
		if err := r.setAuthHeader(req); err != nil {
			return nil, fmt.Errorf("failed to set auth header: %w", err)
		}
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(resp.Body)

	if resp.StatusCode >= 300 {
		if readErr != nil {
			// 本文の読み込みerrorは文言だけを残し、原因としては包みません。再試行の判定は
			// EOFやcontextなどの型付きerrorをHTTP statusより優先するため、包むとstatusによる判定が変わります。
			respBody = []byte(readErr.Error())
		}
		// status付きで返す（呼び出し側が code を見て retry 判断できる）
		return nil, &HTTPStatusError{
			Code:   resp.StatusCode,
			Status: resp.Status,
			Body:   respBody,
			After:  parseRetryAfter(resp),
		}
	}
	if readErr != nil {
		return nil, &ResponseDecodeError{Operation: "index", Err: readErr}
	}

	var indexResponse IndexResponse
	if err := json.Unmarshal(respBody, &indexResponse); err != nil {
		return nil, &ResponseDecodeError{Operation: "index", Err: err}
	}
	// ingest pipelineは、index/create requestを正常にdropできます。そのnoopはHTTP 200を返します。
	// 矛盾するstatusでは、この結果を確認できません。
	if indexResponse.Result == "noop" && resp.StatusCode != http.StatusOK {
		return nil, &ResponseDecodeError{Operation: "index", Err: fmt.Errorf("unexpected HTTP status for noop result: %d", resp.StatusCode)}
	}

	if err := indexResponse.Validate(doc); err != nil {
		return nil, &ResponseDecodeError{Operation: "index", Err: err}
	}

	return &indexResponse, nil
}

// IndexDocuments は複数のドキュメントをバルク操作でインデックスに追加します
func (r *realClient) IndexDocuments(ctx context.Context, docs []DocumentWriteRequest) (*BulkResponse, error) {
	if len(docs) == 0 {
		return &BulkResponse{}, nil
	}

	body, err := bulkRequestBody(docs)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/_bulk", r.endpoint)
	reader := bytes.NewReader(body)

	return r.sendBulkRequest(ctx, "POST", url, "application/x-ndjson", reader)
}

// sendBulkRequest はバルクリクエストを送信し、レスポンスを処理します
func (r *realClient) sendBulkRequest(ctx context.Context, method, url, contentType string, body io.Reader) (*BulkResponse, error) {
	var bodyBytes []byte
	var err error

	// ボディはリセットできるようにバイト配列化
	if body != nil {
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body for signing: %w", err)
		}
	}

	sendBody, compressed, err := r.prepareRequestBody(bodyBytes)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(sendBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Content-Typeはここで先にセット
	req.Header.Set("Content-Type", contentType)
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}

	// X-Opaque-Idヘッダーの設定
	if opaqueID, ok := opaqueid.OpaqueID(ctx); ok && opaqueID != "" {
		req.Header.Set("X-Opaque-Id", opaqueID)
	}

	// 認証ヘッダーの追加
	if r.authConfig.Type == AuthTypeAWSIAM {
		if err := r.signRequest(req, sendBody); err != nil {
			return nil, fmt.Errorf("failed to sign request with AWS IAM: %w", err)
		}
	} else {
		if err := r.setAuthHeader(req); err != nil {
			return nil, fmt.Errorf("failed to set authentication header: %w", err)
		}
	}

	// 実行
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		retryAfter := parseRetryAfter(resp)
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			// body読めない場合も status は保持したい
			// 読み込みerrorは文言だけを残し、原因としては包みません。再試行の判定は
			// EOFやcontextなどの型付きerrorをHTTP statusより優先するため、包むとstatusによる判定が変わります。
			return nil, &HTTPStatusError{Code: resp.StatusCode, Status: resp.Status, Body: []byte(readErr.Error()), After: retryAfter}
		}
		return nil, &HTTPStatusError{Code: resp.StatusCode, Status: resp.Status, Body: body, After: retryAfter}
	}

	var bulkResponse BulkResponse
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&bulkResponse); err != nil {
		return nil, &ResponseDecodeError{Operation: "bulk", Err: err}
	}
	// 完全なJSON値を読めただけでは、responseを検証したことになりません。gzip checksumの誤りや
	// 途中で切れたHTTP bodyは、streamの最後で初めて現れることがあるためです。
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("unexpected additional JSON value")
		}
		return nil, &ResponseDecodeError{Operation: "bulk", Err: err}
	}

	return &bulkResponse, nil
}

func (r *realClient) prepareRequestBody(body []byte) ([]byte, bool, error) {
	if !r.compressRequest {
		return body, false, nil
	}

	var compressed bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressed)
	if _, err := gzipWriter.Write(body); err != nil {
		_ = gzipWriter.Close()
		return nil, false, fmt.Errorf("failed to gzip request body: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to finish gzip request body: %w", err)
	}

	return compressed.Bytes(), true, nil
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

// setAuthHeader は認証タイプに応じた認証ヘッダーを設定します
func (r *realClient) setAuthHeader(req *http.Request) error {
	switch r.authConfig.Type {
	case AuthTypeBasic:
		conf, ok := r.authConfig.Config.(BasicAuthConfig)
		if !ok {
			return fmt.Errorf("invalid BasicAuthConfig")
		}
		req.SetBasicAuth(conf.Username, conf.Password)

	case AuthTypeAPIKey:
		conf, ok := r.authConfig.Config.(APIKeyAuthConfig)
		if !ok {
			return fmt.Errorf("invalid APIKeyAuthConfig")
		}
		raw := fmt.Sprintf("%s:%s", conf.ID, conf.Secret)
		encoded := base64.StdEncoding.EncodeToString([]byte(raw))
		req.Header.Set("Authorization", "ApiKey "+encoded)

	case AuthTypeSAML:
		conf, ok := r.authConfig.Config.(SAMLAuthConfig)
		if !ok {
			return fmt.Errorf("invalid SAMLAuthConfig")
		}
		req.Header.Set("Authorization", "Bearer "+conf.Token)

	case AuthTypeOIDC:
		conf, ok := r.authConfig.Config.(OIDCAuthConfig)
		if !ok {
			return fmt.Errorf("invalid OIDCAuthConfig")
		}
		req.Header.Set("Authorization", "Bearer "+conf.Token)

	case AuthTypeNone, AuthTypeCertificate:
		// 何もしない
		return nil

	default:
		return fmt.Errorf("unsupported auth type: %s", r.authConfig.Type)
	}

	return nil
}

// signRequest は AWS IAM 認証用にリクエストへ SigV4 署名を付与します
func (r *realClient) signRequest(req *http.Request, body []byte) error {
	conf, ok := r.authConfig.Config.(AWSIAMAuthConfig)
	if !ok {
		return fmt.Errorf("invalid AWSIAMAuthConfig")
	}

	creds := aws.Credentials{
		AccessKeyID:     conf.AccessKeyID,
		SecretAccessKey: conf.SecretAccessKey,
		SessionToken:    conf.SessionToken,
		Source:          "static",
	}

	h := sha256.New()
	h.Write(body)
	payloadHash := hex.EncodeToString(h.Sum(nil))

	signer := v4.NewSigner()
	return signer.SignHTTP(req.Context(), creds, req, payloadHash, "es", conf.Region, time.Now())

}
