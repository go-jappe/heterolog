package victoriametrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// TestNewClient は新しいクライアントの作成をテストします
func TestNewClient(t *testing.T) {
	endpoint := "http://localhost:8428/api/v1/import/prometheus"

	t.Run("デフォルト設定", func(t *testing.T) {
		client := NewClient(endpoint)
		defer client.Close()

		if client.endpoint != endpoint {
			t.Errorf("expected endpoint %s, got %s", endpoint, client.endpoint)
		}

		if client.timeout != 10*time.Second {
			t.Errorf("expected timeout 10s, got %v", client.timeout)
		}

		if client.maxRetries != 3 {
			t.Errorf("expected maxRetries 3, got %d", client.maxRetries)
		}

		if client.batchSize != 1000 {
			t.Errorf("expected batchSize 1000, got %d", client.batchSize)
		}
	})

	t.Run("カスタム設定", func(t *testing.T) {
		client := NewClient(endpoint,
			WithTimeout(5*time.Second),
			WithRetry(5, 2*time.Second),
			WithBatchSize(500),
			WithWorkerCount(4),
		)
		defer client.Close()

		if client.timeout != 5*time.Second {
			t.Errorf("expected timeout 5s, got %v", client.timeout)
		}

		if client.maxRetries != 5 {
			t.Errorf("expected maxRetries 5, got %d", client.maxRetries)
		}

		if client.retryDelay != 2*time.Second {
			t.Errorf("expected retryDelay 2s, got %v", client.retryDelay)
		}

		if client.batchSize != 500 {
			t.Errorf("expected batchSize 500, got %d", client.batchSize)
		}

		if client.workerCount != 4 {
			t.Errorf("expected workerCount 4, got %d", client.workerCount)
		}
	})
}

// TestSend は同期送信をテストします
func TestSend(t *testing.T) {
	t.Run("正常な送信", func(t *testing.T) {
		var receivedBody string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := make([]byte, r.ContentLength)
			r.Body.Read(body)
			receivedBody = string(body)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := NewClient(server.URL)
		defer client.Close()

		metrics := []Metric{
			{
				Name:  "test_metric",
				Value: 42.0,
				Labels: map[string]string{
					"env": "test",
				},
			},
		}

		ctx := context.Background()
		err := client.Send(ctx, metrics)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expectedBody := `test_metric{env="test"} 42`
		if receivedBody != expectedBody+"\n" {
			t.Errorf("expected body %q, got %q", expectedBody, receivedBody)
		}
	})

	t.Run("空のメトリクス", func(t *testing.T) {
		client := NewClient("http://localhost:8428")
		defer client.Close()

		ctx := context.Background()
		err := client.Send(ctx, []Metric{})
		if err != nil {
			t.Errorf("expected no error for empty metrics, got %v", err)
		}
	})

	t.Run("バッチ処理", func(t *testing.T) {
		var mu sync.Mutex
		var bodies []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read batch body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			bodies = append(bodies, string(body))
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := NewClient(server.URL, WithBatchSize(10))
		defer client.Close()

		// 25個のメトリクスを送信（3バッチになるはず）
		metrics := make([]Metric, 25)
		for i := range metrics {
			metrics[i] = Metric{
				Name:  fmt.Sprintf("metric_%d", i),
				Value: float64(i),
			}
		}

		ctx := context.Background()
		err := client.Send(ctx, metrics)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(bodies) != 3 {
			t.Errorf("expected 3 requests, got %d", len(bodies))
		}
		counts := make(map[string]int)
		for index, body := range bodies {
			lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
			if len(lines) > 10 {
				t.Errorf("batch %d contains %d metrics, limit is 10", index, len(lines))
			}
			for _, line := range lines {
				counts[line]++
			}
		}
		for i := range metrics {
			line := fmt.Sprintf("metric_%d %d", i, i)
			if counts[line] != 1 {
				t.Errorf("metric %q sent %d times, want exactly once", line, counts[line])
			}
			delete(counts, line)
		}
		if len(counts) != 0 {
			t.Errorf("unexpected metrics sent: %v", counts)
		}
	})

	t.Run("コンテキストキャンセル", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := NewClient(server.URL)
		defer client.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // すぐにキャンセル

		metrics := []Metric{{Name: "test", Value: 1}}
		err := client.Send(ctx, metrics)
		if err == nil {
			t.Error("expected error for cancelled context")
		}
	})
}

// TestSendWithRetry はリトライ機能をテストします
func TestSendWithRetry(t *testing.T) {
	t.Run("リトライで成功", func(t *testing.T) {
		attemptCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attemptCount++
			if attemptCount < 3 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := NewClient(server.URL,
			WithRetry(3, 10*time.Millisecond),
		)
		defer client.Close()

		metrics := []Metric{{Name: "test", Value: 1}}
		ctx := context.Background()
		err := client.Send(ctx, metrics)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if attemptCount != 3 {
			t.Errorf("expected 3 attempts, got %d", attemptCount)
		}
	})

	t.Run("リトライ上限に達する", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := NewClient(server.URL,
			WithRetry(2, 10*time.Millisecond),
		)
		defer client.Close()

		metrics := []Metric{{Name: "test", Value: 1}}
		ctx := context.Background()
		err := client.Send(ctx, metrics)
		if err == nil {
			t.Error("expected error after max retries")
		}
	})

	t.Run("リトライ不可能なエラー", func(t *testing.T) {
		attemptCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attemptCount++
			w.WriteHeader(http.StatusBadRequest) // 400はリトライ不可
		}))
		defer server.Close()

		client := NewClient(server.URL,
			WithRetry(3, 10*time.Millisecond),
		)
		defer client.Close()

		metrics := []Metric{{Name: "test", Value: 1}}
		ctx := context.Background()
		err := client.Send(ctx, metrics)
		if err == nil {
			t.Error("expected error for non-retryable status")
		}

		if attemptCount != 1 {
			t.Errorf("expected 1 attempt for non-retryable error, got %d", attemptCount)
		}
	})

	// 検証・変換やrequest生成の失敗は入力と設定だけで決まるため、requestを送らないまま
	// 再試行の待機を繰り返さず、最初の失敗を返します。
	t.Run("送信前の検証エラーはリトライ不可", func(t *testing.T) {
		for _, tt := range []struct {
			name     string
			endpoint string
			metrics  []Metric
			// wantは、最初の失敗として返るerrorを判定します。
			want func(error) bool
		}{
			{
				name:     "invalid metric",
				endpoint: "http://metrics.invalid/write",
				metrics:  []Metric{{Name: "123_invalid", Value: 1}},
				want:     func(err error) bool { return errors.Is(err, ErrInvalidMetricName) },
			},
			{
				name:     "invalid endpoint",
				endpoint: "http://metrics.invalid/\x7f",
				metrics:  []Metric{{Name: "valid_metric", Value: 1}},
				// request生成の失敗は、net/urlのparse errorです。
				want: func(err error) bool {
					var urlErr *url.Error
					return errors.As(err, &urlErr)
				},
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					formatCalls := 0
					formatter := formatterFunc(func(metrics []Metric) (string, error) {
						formatCalls++
						return NewPrometheusTextFormatter().Format(metrics)
					})
					transport := &countingTransport{}
					client := NewClient(tt.endpoint,
						WithRetry(3, time.Second),
						WithWorkerCount(0),
						WithFormatter(formatter),
						WithHTTPClient(&http.Client{Transport: transport}),
					)
					defer client.Close()

					start := time.Now()
					err := client.Send(context.Background(), tt.metrics)
					if err == nil || !tt.want(err) {
						t.Fatalf("Send() error = %v, want first %s error without retries", err, tt.name)
					}
					// synctestの時刻は全goroutineが待機した場合だけ進むため、0なら再試行を待っていません。
					if elapsed := time.Since(start); elapsed != 0 || formatCalls != 1 || transport.requests != 0 {
						t.Fatalf(
							"Send() waited %s, formatted %d times, sent %d requests; want one attempt without waiting or sending",
							elapsed,
							formatCalls,
							transport.requests,
						)
					}
				})
			})
		}
	})
}

// formatterFuncは、テストでFormatの呼び出しを観測するMetricFormatterです。
type formatterFunc func([]Metric) (string, error)

func (f formatterFunc) Format(metrics []Metric) (string, error) {
	return f(metrics)
}

// countingTransportは、送信されたHTTP requestを数えて空の成功responseを返します。
type countingTransport struct {
	requests int
}

func (t *countingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.requests++
	if request.Body != nil {
		_ = request.Body.Close()
	}
	return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody, Request: request}, nil
}

// TestSendAsync は非同期送信をテストします
func TestSendAsync(t *testing.T) {
	t.Run("正常な非同期送信", func(t *testing.T) {
		var mu sync.Mutex
		var requests [][]string
		var sendErrors []error

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read request body: %v", err)
			}

			mu.Lock()
			requests = append(requests, strings.Split(strings.TrimSuffix(string(body), "\n"), "\n"))
			mu.Unlock()

			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := NewClient(server.URL,
			WithBatchSize(2),
			WithWorkerCount(1),
			WithErrorHandler(func(err error) {
				mu.Lock()
				defer mu.Unlock()
				sendErrors = append(sendErrors, err)
			}),
		)
		defer client.Close()

		// 5個のメトリクスを非同期送信
		for i := 0; i < 5; i++ {
			metric := []Metric{{
				Name:  fmt.Sprintf("async_metric_%d", i),
				Value: float64(i),
			}}
			err := client.SendAsync(metric)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		// Closeは受理済みqueueとworker内bufferを送信し終えるまで待ちます。
		if err := client.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(sendErrors) != 0 {
			t.Fatalf("async send errors = %v, want none", sendErrors)
		}
		received := make(map[string]int)
		for _, lines := range requests {
			if len(lines) > 2 {
				t.Errorf("request contains %d metrics, want at most batch size 2: %q", len(lines), lines)
			}
			for _, line := range lines {
				received[line]++
			}
		}
		// 受理した各メトリクスは、Close後までに重複も欠落もなく一度だけ届きます。
		for i := 0; i < 5; i++ {
			line := fmt.Sprintf("async_metric_%d %d", i, i)
			if received[line] != 1 {
				t.Errorf("metric %q received %d times, want exactly once", line, received[line])
			}
			delete(received, line)
		}
		if len(received) != 0 {
			t.Errorf("unexpected metrics received: %v", received)
		}
	})

	t.Run("チャネルフル", func(t *testing.T) {
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		// 最初の送信が処理中になってからチャネルを満杯にします。
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := NewClient(server.URL,
			WithWorkerCount(1),
			WithBatchSize(1),
		)
		defer client.Close()
		defer close(release)

		if err := client.SendAsync([]Metric{{Name: "in_flight", Value: 1}}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("worker did not start sending")
		}

		// チャネルをいっぱいにする
		for i := 0; i < cap(client.asyncCh); i++ {
			if err := client.SendAsync([]Metric{{Name: "test", Value: float64(i)}}); err != nil {
				t.Fatalf("send %d failed before the channel was full: %v", i, err)
			}
		}

		// チャネルがフルの状態で送信を試みる
		err := client.SendAsync([]Metric{{Name: "test", Value: 1}})
		if !errors.Is(err, ErrAsyncChannelFull) {
			t.Errorf("expected channel full error, got %v", err)
		}
		// READMEのトラブルシューティングはこの文言で対処を案内しているため、文言自体も固定します。
		if err != nil && err.Error() != "async channel is full" {
			t.Errorf("channel full error text = %q, want %q", err.Error(), "async channel is full")
		}
	})

	t.Run("クライアントクローズ後", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := NewClient(server.URL)
		client.Close()

		// Closeは戻る前に受付を止めるため、待たずに拒否されます。
		err := client.SendAsync([]Metric{{Name: "test", Value: 1}})
		if !errors.Is(err, ErrClientClosed) {
			t.Errorf("expected client closed error, got %v", err)
		}
	})
}

// TestHealthCheck はヘルスチェック機能をテストします
func TestHealthCheck(t *testing.T) {
	t.Run("正常なヘルスチェック", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("expected GET method, got %s", r.Method)
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := NewClient(server.URL)
		defer client.Close()

		ctx := context.Background()
		err := client.HealthCheck(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ヘルスチェック失敗", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("service unavailable"))
		}))
		defer server.Close()

		client := NewClient(server.URL)
		defer client.Close()

		ctx := context.Background()
		err := client.HealthCheck(ctx)
		if err == nil {
			t.Error("expected error for failed health check")
		}
	})
}

// TestErrorHandler はエラーハンドラーをテストします
func TestErrorHandler(t *testing.T) {
	capturedErrors := make(chan error, 1)
	errorHandler := func(err error) {
		capturedErrors <- err
	}

	// 常にエラーを返すサーバー
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewClient(server.URL,
		WithErrorHandler(errorHandler),
		WithRetry(0, 0), // リトライなし
		WithBatchSize(1),
	)
	defer client.Close()

	// 非同期送信でエラーを発生させる
	if err := client.SendAsync([]Metric{{Name: "test", Value: 1}}); err != nil {
		t.Fatal(err)
	}

	// エラーハンドラーが呼ばれるまで待つ
	select {
	case err := <-capturedErrors:
		if err == nil {
			t.Error("error handler received nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("expected error handler to be called")
	}
}

// TestHTTPError はHTTPErrorの動作をテストします
func TestHTTPError(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		retryable  bool
	}{
		{"5xx error", 500, true},
		{"503 error", 503, true},
		{"429 error", 429, true},
		{"408 error", 408, true},
		{"400 error", 400, false},
		{"401 error", 401, false},
		{"404 error", 404, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &HTTPError{
				StatusCode: tt.statusCode,
				Status:     fmt.Sprintf("%d", tt.statusCode),
				Body:       "error body",
			}

			if err.IsRetryable() != tt.retryable {
				t.Errorf("expected IsRetryable()=%v for status %d", tt.retryable, tt.statusCode)
			}
		})
	}
}

// TestConcurrentSend は並行送信をテストします
func TestConcurrentSend(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	defer client.Close()

	var wg sync.WaitGroup
	goroutines := 10
	metricsPerGoroutine := 5

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < metricsPerGoroutine; j++ {
				metrics := []Metric{{
					Name:  fmt.Sprintf("concurrent_metric_%d_%d", id, j),
					Value: float64(j),
				}}

				ctx := context.Background()
				if err := client.Send(ctx, metrics); err != nil {
					t.Errorf("goroutine %d: unexpected error: %v", id, err)
				}
			}
		}(i)
	}

	wg.Wait()

	expectedRequests := int32(goroutines * metricsPerGoroutine)
	if atomic.LoadInt32(&requestCount) != expectedRequests {
		t.Errorf("expected %d requests, got %d", expectedRequests, requestCount)
	}
}

// BenchmarkSend はSendメソッドのベンチマークを実行します
func BenchmarkSend(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	defer client.Close()

	metrics := []Metric{
		{Name: "benchmark_metric", Value: 42.0, Labels: map[string]string{"test": "true"}},
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Send(ctx, metrics); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkSendAsync は非同期送信のベンチマークを実行します
func BenchmarkSendAsync(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, WithWorkerCount(4))
	defer client.Close()

	metrics := []Metric{
		{Name: "async_benchmark_metric", Value: 42.0},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.SendAsync(metrics); err != nil {
			// チャネルフルの場合は少し待つ
			time.Sleep(time.Microsecond)
			i--
		}
	}
}

// endpointのURLにuser info（ユーザー名・パスワード）があり解析できない場合も、request生成のerrorの文面と、
// 原因として残すnet/urlの*url.Errorにuser infoを含めず、requestも送りません。資格情報を文面へ出さない
// ことが要件なので、文面を直接確認します。
func TestClientRequestErrorsDoNotExposeEndpointUserInfo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const user, password = "endpoint-user", "S3cret-Passw0rd"
		for _, endpoint := range []string{
			// pathのescapeが不正です。
			"https://" + user + ":" + password + "@metrics.example.test/%zz",
			// url.Parseは"/"でauthorityを区切るため、パスワードの一部をportとして報告します。
			"https://" + user + ":S3cret/Passw0rd@metrics.example.test/",
		} {
			transport := &countingTransport{}
			client := NewClient(endpoint,
				WithRetry(3, time.Second),
				WithWorkerCount(0),
				WithHTTPClient(&http.Client{Transport: transport}),
			)
			sendErr := client.Send(context.Background(), []Metric{{Name: "valid_metric", Value: 1}})
			healthErr := client.HealthCheck(context.Background())
			if err := client.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			for name, err := range map[string]error{"Send": sendErr, "HealthCheck": healthErr} {
				var urlErr *url.Error
				if !errors.As(err, &urlErr) {
					t.Fatalf("%s() error = %v, want the URL parse error", name, err)
				}
				for _, secret := range []string{user, "S3cret", "Passw0rd"} {
					if strings.Contains(err.Error(), secret) || strings.Contains(urlErr.URL, secret) {
						t.Fatalf("%s() error %q exposes %q from the endpoint user info", name, err, secret)
					}
				}
			}
			if transport.requests != 0 {
				t.Fatalf("requests = %d, want none for an endpoint that cannot be parsed", transport.requests)
			}
		}
	})
}
