package victoriametrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/pkg/internal/endpointurl"
)

const defaultBatchSize = 1000

// MetricsClient はメトリクス送信のインターフェース
type MetricsClient interface {
	Send(ctx context.Context, metrics []Metric) error
	SendAsync(metrics []Metric) error
	HealthCheck(ctx context.Context) error
	Close() error
}

// ClientOption はクライアントの設定オプション
type ClientOption func(*Client)

// Client はVictoriaMetricsにメトリクスを送信するためのHTTPクライアント
type Client struct {
	endpoint   string
	httpClient *http.Client
	formatter  MetricFormatter

	// 設定可能なオプション
	timeout      time.Duration
	maxRetries   int
	retryDelay   time.Duration
	batchSize    int
	maxBatchWait time.Duration

	// 非同期送信用
	asyncCh     chan []Metric
	workerCount int
	wg          sync.WaitGroup
	closed      bool
	mu          sync.RWMutex

	// エラーハンドラー
	onError func(error)
}

// NewClient は新しいVictoriaMetricsクライアントを作成します
func NewClient(endpoint string, opts ...ClientOption) *Client {
	c := &Client{
		endpoint:     endpoint,
		formatter:    NewPrometheusTextFormatter(),
		timeout:      10 * time.Second,
		maxRetries:   3,
		retryDelay:   time.Second,
		batchSize:    defaultBatchSize,
		maxBatchWait: 5 * time.Second,
		workerCount:  2,
		asyncCh:      make(chan []Metric, 100),
		onError:      func(err error) {}, // デフォルトは何もしない
	}

	// カスタムHTTPクライアントを作成
	c.httpClient = &http.Client{
		Timeout: c.timeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// オプションを適用
	for _, opt := range opts {
		opt(c)
	}
	if c.batchSize == 0 {
		c.batchSize = defaultBatchSize
	}

	// 呼び出し元のHTTPクライアント設定を変更せず、このClientの期限を設定します。
	httpClient := *c.httpClient
	httpClient.Timeout = c.timeout
	c.httpClient = &httpClient

	// 非同期ワーカーを起動
	c.startAsyncWorkers()

	return c
}

// WithTimeout は同期・非同期送信それぞれのHTTPリクエストのタイムアウトを設定します。
// 再送にも毎回同じ期限を適用します。0は期限を設けません。
// WithHTTPClientで渡されたクライアントのTimeoutより優先します。
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithRetry はリトライ設定を行います
// 通信エラーと一時的なHTTPエラーだけを再送し、metricsの検証・変換や
// request生成の失敗はrequestを送らずにすぐ返します。
func WithRetry(maxRetries int, retryDelay time.Duration) ClientOption {
	return func(c *Client) {
		c.maxRetries = maxRetries
		c.retryDelay = retryDelay
	}
}

// WithBatchSize はバッチサイズを設定します
// 0は既定値1000を使用します。負数ではSendとSendAsyncがエラーを返し、
// 非同期ワーカーは起動しません。
func WithBatchSize(size int) ClientOption {
	return func(c *Client) {
		c.batchSize = size
	}
}

// WithWorkerCount は非同期ワーカーの数を設定します
// 0以下の場合は非同期ワーカーを起動せず、SendAsyncはエラーを返します。
// 同期送信のSendは引き続き使用できます。
func WithWorkerCount(count int) ClientOption {
	return func(c *Client) {
		c.workerCount = count
	}
}

// WithErrorHandler はエラーハンドラーを設定します
func WithErrorHandler(handler func(error)) ClientOption {
	return func(c *Client) {
		c.onError = handler
	}
}

// WithFormatter はカスタムフォーマッターを設定します
func WithFormatter(formatter MetricFormatter) ClientOption {
	return func(c *Client) {
		c.formatter = formatter
	}
}

// WithHTTPClient はカスタムHTTPクライアントを設定します。
// 設定はコピーし、Transport、Jar、CheckRedirectを引き継ぎます。
// TimeoutにはWithTimeoutの値（未指定なら既定値）を使い、元の設定は変更しません。
// メトリクス送信では、POSTのメソッドや本文を失うリダイレクトを停止します。
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// Send はメトリクスを同期的にVictoriaMetricsに送信します
// batch sizeが0以下ならErrInvalidBatchSizeを返します。
func (c *Client) Send(ctx context.Context, metrics []Metric) error {
	if c.batchSize <= 0 {
		return ErrInvalidBatchSize
	}
	if len(metrics) == 0 {
		return nil
	}

	// バッチ処理
	for i := 0; i < len(metrics); {
		end := len(metrics)
		if c.batchSize < end-i {
			end = i + c.batchSize
		}

		batch := metrics[i:end]
		if err := c.sendBatchWithRetry(ctx, batch); err != nil {
			return fmt.Errorf("failed to send batch %d-%d: %w", i, end, err)
		}
		i = end
	}

	return nil
}

// SendAsync はメトリクスを非同期的に送信します
// 非同期ワーカーがない場合は、データを受理せずエラーを返します。
// メトリクスと各Labelsをコピーするため、戻った後に呼び出し元は入力を再利用できます。
// 呼び出し中は入力を変更しないでください。受け付けなかった理由はErrClientClosed、
// ErrInvalidBatchSize、ErrInvalidWorkerCount、ErrAsyncChannelFullとerrors.Isで判別できます。
func (c *Client) SendAsync(metrics []Metric) error {
	c.mu.RLock()
	// 受付が終わるまでCloseによるasyncChのcloseを待たせます。
	defer c.mu.RUnlock()
	if c.closed {
		return ErrClientClosed
	}
	if c.batchSize <= 0 {
		return ErrInvalidBatchSize
	}
	if c.workerCount <= 0 {
		return ErrInvalidWorkerCount
	}

	accepted := slices.Clone(metrics)
	for i := range accepted {
		accepted[i].Labels = maps.Clone(accepted[i].Labels)
	}

	select {
	case c.asyncCh <- accepted:
		return nil
	default:
		return ErrAsyncChannelFull
	}
}

// newEndpointRequestはendpointへのrequestを作ります。endpointを解析できない場合も、errorの文面に
// URLのuser info（パスワードなど）を含めません。
func (c *Client) newEndpointRequest(ctx context.Context, method string, body io.Reader) (*http.Request, error) {
	if _, err := endpointurl.Parse(c.endpoint); err != nil {
		return nil, err
	}
	return http.NewRequestWithContext(ctx, method, c.endpoint, body)
}

// HealthCheck はVictoriaMetricsエンドポイントの可用性を確認します
func (c *Client) HealthCheck(ctx context.Context) error {
	req, err := c.newEndpointRequest(ctx, http.MethodGet, nil)
	if err != nil {
		return fmt.Errorf("failed to create health check request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("health check returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Closeは非同期送信の受付を停止し、受理済みqueue・worker内bufferの送信と
// エラー通知を待ちます。複数の呼び出し元も同じworkerの終了を待ちます。
// 直接呼び出したSendの終了待機は、その呼び出し元が管理します。
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.wg.Wait()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	// channelを閉じ、受理済み要素をworkerが取り出し終わってから終了させます。
	// 別の停止channelをselectすると、queueを残したまま終了する可能性があります。
	close(c.asyncCh)

	// すべてのワーカーが終了するのを待つ
	c.wg.Wait()

	return nil
}

// sendBatchWithRetry はリトライ機能付きでバッチを送信します
func (c *Client) sendBatchWithRetry(ctx context.Context, metrics []Metric) error {
	var lastErr error

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			// リトライ前に待機
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.retryDelay * time.Duration(attempt)):
			}
		}

		err := c.sendBatch(ctx, metrics)
		if err == nil {
			return nil
		}

		lastErr = err

		// リトライ可能なエラーかチェック
		if !isRetryableError(err) {
			return err
		}
	}

	return fmt.Errorf("failed after %d retries: %w", c.maxRetries, lastErr)
}

// sendBatch は単一のバッチを送信します
func (c *Client) sendBatch(ctx context.Context, metrics []Metric) error {
	body, err := c.formatter.Format(metrics)
	if err != nil {
		return &requestPreparationError{err: fmt.Errorf("failed to format metrics: %w", err)}
	}

	req, err := c.newEndpointRequest(ctx, http.MethodPost, bytes.NewBufferString(body))
	if err != nil {
		return &requestPreparationError{err: fmt.Errorf("failed to create request: %w", err)}
	}

	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("User-Agent", "VictoriaMetrics-Go-Client/1.0")

	resp, err := c.doPost(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return &HTTPError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Body:       string(body),
		}
	}

	// レスポンスボディを読み切る（接続の再利用のため）
	_, _ = io.Copy(io.Discard, resp.Body)

	return nil
}

// doPost keeps the caller's redirect policy while refusing redirects that drop
// the submitted metrics. Returning the last response preserves its HTTP error.
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

// startAsyncWorkers は非同期ワーカーを起動します
func (c *Client) startAsyncWorkers() {
	if c.batchSize <= 0 {
		return
	}
	for i := 0; i < c.workerCount; i++ {
		c.wg.Add(1)
		go c.asyncWorker()
	}
}

// asyncWorker は非同期でメトリクスを送信するワーカー
func (c *Client) asyncWorker() {
	defer c.wg.Done()

	buffer := make([]Metric, 0, c.batchSize)
	timer := time.NewTimer(c.maxBatchWait)
	defer timer.Stop()

	for {
		select {
		case metrics, ok := <-c.asyncCh:
			if !ok {
				// チャネルが閉じられた場合、残りのバッファを送信
				if len(buffer) > 0 {
					c.sendBufferedMetrics(buffer)
				}
				return
			}

			buffer = append(buffer, metrics...)

			// バッファがいっぱいになったら送信
			if len(buffer) >= c.batchSize {
				c.sendBufferedMetrics(buffer)
				buffer = buffer[:0]
				timer.Reset(c.maxBatchWait)
			}

		case <-timer.C:
			// タイムアウトしたらバッファを送信
			if len(buffer) > 0 {
				c.sendBufferedMetrics(buffer)
				buffer = buffer[:0]
			}
			timer.Reset(c.maxBatchWait)

		}
	}
}

// sendBufferedMetrics はバッファされたメトリクスを送信します
func (c *Client) sendBufferedMetrics(metrics []Metric) {
	// HTTPClientが各requestの期限を管理します。バッファ全体へ同じ期限を
	// 課すと、分割requestや再送待機が増えたときに受理済みの後続を打ち切ります。
	if err := c.Send(context.Background(), metrics); err != nil {
		c.onError(fmt.Errorf("failed to send buffered metrics: %w", err))
	}
}

// HTTPError はHTTPエラーの詳細情報を含むエラー
type HTTPError struct {
	StatusCode int
	Status     string
	Body       string
}

// Error はエラーメッセージを返します
func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d %s: %s", e.StatusCode, e.Status, e.Body)
}

// IsRetryable はリトライ可能なエラーかどうかを返します
func (e *HTTPError) IsRetryable() bool {
	// 5xx エラーと一部の4xxエラーはリトライ可能
	return e.StatusCode >= 500 || e.StatusCode == 429 || e.StatusCode == 408
}

// requestPreparationErrorは、HTTP requestを送る前に、metricsの検証・変換または
// requestの生成が失敗したことを表します。失敗は入力と設定だけで決まるため、
// 待機して再試行しても解消しません。
type requestPreparationError struct {
	err error
}

func (e *requestPreparationError) Error() string {
	return e.err.Error()
}

func (e *requestPreparationError) Unwrap() error {
	return e.err
}

// isRetryableError はエラーがリトライ可能かどうかを判定します。
// 送信前の検証・変換・request生成の失敗は、requestを送らないまま待機と再試行を
// 繰り返さないよう、リトライ不可として扱います。
func isRetryableError(err error) bool {
	var preparationErr *requestPreparationError
	if errors.As(err, &preparationErr) {
		return false
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.IsRetryable()
	}

	// ネットワークエラーなどはリトライ可能
	return true
}
