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

// MetricsClient is the interface for sending metrics.
type MetricsClient interface {
	Send(ctx context.Context, metrics []Metric) error
	SendAsync(metrics []Metric) error
	HealthCheck(ctx context.Context) error
	Close() error
}

// ClientOption is a configuration option for the client.
type ClientOption func(*Client)

// Client is an HTTP client that sends metrics to VictoriaMetrics.
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

// NewClient creates a new VictoriaMetrics client.
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

// WithTimeout sets the timeout of each HTTP request for both synchronous and
// asynchronous sends. Each retry gets a new timeout of the same length. Zero
// sets no deadline. It takes precedence over the Timeout of a client passed
// with WithHTTPClient.
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithRetry sets how many times a failed request is retried and the delay
// between retries. maxRetries counts the retries after the first request:
// with 1, a batch is sent at most twice; with 0, it is sent once and not
// retried. A negative maxRetries makes Send and SendAsync return
// ErrInvalidRetryCount. The wait before the nth retry is n times retryDelay.
// Only communication errors and temporary HTTP errors are retried; failures to
// validate or convert the metrics or to build the request return immediately
// without sending a request.
func WithRetry(maxRetries int, retryDelay time.Duration) ClientOption {
	return func(c *Client) {
		c.maxRetries = maxRetries
		c.retryDelay = retryDelay
	}
}

// WithBatchSize sets the batch size. Zero uses the default of 1000. With a
// negative size, Send and SendAsync return an error and the asynchronous
// workers are not started.
func WithBatchSize(size int) ClientOption {
	return func(c *Client) {
		c.batchSize = size
	}
}

// WithWorkerCount sets the number of asynchronous workers. If it is 0 or less,
// no asynchronous workers are started and SendAsync returns an error. The
// synchronous Send can still be used.
func WithWorkerCount(count int) ClientOption {
	return func(c *Client) {
		c.workerCount = count
	}
}

// WithErrorHandler sets the error handler.
func WithErrorHandler(handler func(error)) ClientOption {
	return func(c *Client) {
		c.onError = handler
	}
}

// WithFormatter sets a custom formatter.
func WithFormatter(formatter MetricFormatter) ClientOption {
	return func(c *Client) {
		c.formatter = formatter
	}
}

// WithHTTPClient sets a custom HTTP client. Its settings are copied, keeping
// Transport, Jar and CheckRedirect. Timeout is set to the WithTimeout value
// (the default if it is not specified), and the original settings are not
// modified. When sending metrics, redirects that would lose the POST method or
// body are stopped.
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// Send sends metrics to VictoriaMetrics synchronously. It returns
// ErrInvalidBatchSize if the batch size given with WithBatchSize is negative,
// and ErrInvalidRetryCount if the retry count given with WithRetry is
// negative.
func (c *Client) Send(ctx context.Context, metrics []Metric) error {
	if c.batchSize <= 0 {
		return ErrInvalidBatchSize
	}
	if c.maxRetries < 0 {
		return ErrInvalidRetryCount
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

// SendAsync sends metrics asynchronously. If there are no asynchronous workers,
// it returns an error without accepting the data. It copies the metrics and
// each Labels map, so the caller can reuse the input after it returns. Do not
// modify the input during the call. Use errors.Is with ErrClientClosed,
// ErrInvalidBatchSize, ErrInvalidRetryCount, ErrInvalidWorkerCount or
// ErrAsyncChannelFull to tell why the metrics were not accepted.
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
	if c.maxRetries < 0 {
		return ErrInvalidRetryCount
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

// HealthCheck checks the availability of the VictoriaMetrics endpoint.
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

// Close stops accepting asynchronous sends and waits until the accepted queue
// and the buffers in the workers are sent and errors are reported. Multiple
// callers all wait for the same workers to finish. Waiting for a Send called
// directly to finish is up to its caller.
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

// doPostは呼び出し元のredirect policyを保ちつつ、送信するmetricsを落とすredirectを拒否します。
// 最後のresponseを返すことで、そのHTTP errorを保ちます。
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

// HTTPError is an error that contains the details of an HTTP error.
type HTTPError struct {
	StatusCode int
	Status     string
	Body       string
}

// Error returns the error message.
func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d %s: %s", e.StatusCode, e.Status, e.Body)
}

// IsRetryable reports whether the error is retryable.
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
