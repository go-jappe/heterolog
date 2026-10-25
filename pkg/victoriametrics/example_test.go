package victoriametrics_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/pkg/victoriametrics"
)

// exampleServerは、例を実行できるようVictoriaMetricsのimport endpointの代わりに
// requestを記録するローカルHTTP serverです。実際のアプリケーションでは
// "http://localhost:8428/api/v1/import/prometheus" などのURLを指定します。
type exampleServer struct {
	*httptest.Server

	mu       sync.Mutex
	requests int
	lines    []string
}

// newExampleServerは、すべてのrequestへstatusを返すexampleServerを起動します。
func newExampleServer(status int) *exampleServer {
	server := &exampleServer{}
	server.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		server.mu.Lock()
		server.requests++
		for _, line := range strings.Split(string(body), "\n") {
			if line != "" {
				server.lines = append(server.lines, line)
			}
		}
		server.mu.Unlock()
		w.WriteHeader(status)
	}))
	return server
}

// receivedは、受信したrequest数とメトリクスの行を受信順に返します。
func (s *exampleServer) received() (int, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests, slices.Clone(s.lines)
}

// ExampleClient_Send は同期的なメトリクス送信の例を示します
func ExampleClient_Send() {
	server := newExampleServer(http.StatusNoContent)
	defer server.Close()

	// VictoriaMetricsクライアントを作成
	client := victoriametrics.NewClient(server.URL)
	defer client.Close()

	// 送信するメトリクスを準備
	metrics := []victoriametrics.Metric{
		{
			Name: "http_requests_total",
			Labels: map[string]string{
				"method": "GET",
				"status": "200",
			},
			Value: 1234,
		},
		{
			Name:  "cpu_usage_percent",
			Value: 85.5,
		},
	}

	// メトリクスを送信
	ctx := context.Background()
	if err := client.Send(ctx, metrics); err != nil {
		log.Printf("Failed to send metrics: %v", err)
	}

	// Sendは送信の完了を待つため、戻った時点でserverが受信しています。
	_, lines := server.received()
	for _, line := range lines {
		fmt.Println(line)
	}
	// Output:
	// http_requests_total{method="GET",status="200"} 1234
	// cpu_usage_percent 85.5
}

// ExampleClient_SendAsync は非同期メトリクス送信の例を示します
func ExampleClient_SendAsync() {
	server := newExampleServer(http.StatusNoContent)
	defer server.Close()

	// エラーハンドラーを定義
	errorHandler := func(err error) {
		log.Printf("Async send error: %v", err)
	}

	// カスタム設定でクライアントを作成
	client := victoriametrics.NewClient(server.URL,
		victoriametrics.WithErrorHandler(errorHandler),
		victoriametrics.WithBatchSize(100),
		victoriametrics.WithWorkerCount(4),
	)

	// 非同期でメトリクスを送信
	for i := 0; i < 10; i++ {
		metrics := []victoriametrics.Metric{
			{
				Name: "async_counter",
				Labels: map[string]string{
					"job": "example",
				},
				Value: float64(i),
			},
		}

		if err := client.SendAsync(metrics); err != nil {
			log.Printf("Failed to queue metrics: %v", err)
		}
	}

	// Closeは受理済みのメトリクスを送信し終えるまで待ちます。
	if err := client.Close(); err != nil {
		log.Printf("Failed to close client: %v", err)
	}

	// 複数のワーカーが並行して送信するため、受信順は一定ではありません。
	_, lines := server.received()
	slices.Sort(lines)
	for _, line := range lines {
		fmt.Println(line)
	}
	// Output:
	// async_counter{job="example"} 0
	// async_counter{job="example"} 1
	// async_counter{job="example"} 2
	// async_counter{job="example"} 3
	// async_counter{job="example"} 4
	// async_counter{job="example"} 5
	// async_counter{job="example"} 6
	// async_counter{job="example"} 7
	// async_counter{job="example"} 8
	// async_counter{job="example"} 9
}

// ExampleNewClient_withOptions はカスタムオプションを使用したクライアント作成の例を示します
func ExampleNewClient_withOptions() {
	server := newExampleServer(http.StatusNoContent)
	defer server.Close()

	client := victoriametrics.NewClient(server.URL,
		// タイムアウトを設定
		victoriametrics.WithTimeout(30*time.Second),
		// リトライ設定
		victoriametrics.WithRetry(5, 2*time.Second),
		// バッチサイズを設定
		victoriametrics.WithBatchSize(500),
		// ワーカー数を設定
		victoriametrics.WithWorkerCount(8),
		// エラーハンドラーを設定
		victoriametrics.WithErrorHandler(func(err error) {
			log.Printf("Metrics error: %v", err)
		}),
	)
	defer client.Close()

	// クライアントを使用...
	ctx := context.Background()
	if err := client.Send(ctx, []victoriametrics.Metric{{Name: "configured_metric", Value: 1}}); err != nil {
		log.Printf("Failed to send metrics: %v", err)
	}

	requests, lines := server.received()
	fmt.Printf("requests: %d, metrics: %q\n", requests, lines)
	// Output:
	// requests: 1, metrics: ["configured_metric 1"]
}

// ExampleClient_HealthCheck はヘルスチェックの使用例を示します
func ExampleClient_HealthCheck() {
	server := newExampleServer(http.StatusOK)
	defer server.Close()

	client := victoriametrics.NewClient(server.URL)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.HealthCheck(ctx); err != nil {
		log.Printf("VictoriaMetrics is not healthy: %v", err)
	} else {
		fmt.Println("VictoriaMetrics is healthy")
	}
	// Output:
	// VictoriaMetrics is healthy
}

// ExampleClient_batchProcessing は大量のメトリクスをバッチ処理する例を示します
func ExampleClient_batchProcessing() {
	server := newExampleServer(http.StatusNoContent)
	defer server.Close()

	// バッチサイズを小さく設定してバッチ処理を確認
	client := victoriametrics.NewClient(server.URL,
		victoriametrics.WithBatchSize(10),
	)
	defer client.Close()

	// 100個のメトリクスを生成（10バッチに分割される）
	var metrics []victoriametrics.Metric
	for i := 0; i < 100; i++ {
		metrics = append(metrics, victoriametrics.Metric{
			Name: "batch_metric",
			Labels: map[string]string{
				"index": fmt.Sprintf("%d", i),
			},
			Value: float64(i),
		})
	}

	ctx := context.Background()
	if err := client.Send(ctx, metrics); err != nil {
		log.Printf("Failed to send batch: %v", err)
	}

	requests, lines := server.received()
	fmt.Printf("requests: %d, metrics: %d\n", requests, len(lines))
	// Output:
	// requests: 10, metrics: 100
}

// ExamplePrometheusTextFormatter_Format はフォーマッターの使用例を示します
func ExamplePrometheusTextFormatter_Format() {
	// フォーマッターを作成
	formatter := victoriametrics.NewPrometheusTextFormatter()

	// メトリクスを定義
	metrics := []victoriametrics.Metric{
		{
			Name: "http_requests_total",
			Labels: map[string]string{
				"method": "GET",
				"status": "200",
			},
			Value: 1234,
		},
		{
			Name:  "cpu_usage_percent",
			Value: 85.5,
		},
		{
			Name: "memory_usage_bytes",
			Labels: map[string]string{
				"host": "server1",
				"type": "used",
			},
			Value: 1073741824, // 1GB
		},
	}

	// メトリクスをPrometheus形式にフォーマット
	output, err := formatter.Format(metrics)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(output)
	// Output:
	// http_requests_total{method="GET",status="200"} 1234
	// cpu_usage_percent 85.5
	// memory_usage_bytes{host="server1",type="used"} 1.073741824e+09
}

// ExampleMetric_Validate はメトリクスバリデーションの例を示します
func ExampleMetric_Validate() {
	// 有効なメトリクス
	validMetric := victoriametrics.Metric{
		Name: "valid_metric_name",
		Labels: map[string]string{
			"env": "production",
		},
		Value: 42,
	}

	if err := validMetric.Validate(); err != nil {
		fmt.Printf("Valid metric validation failed: %v\n", err)
	} else {
		fmt.Println("Valid metric passed validation")
	}

	// 無効なメトリクス（名前が数字で始まる）
	invalidMetric := victoriametrics.Metric{
		Name:  "123_invalid_name",
		Value: 42,
	}

	if err := invalidMetric.Validate(); err != nil {
		fmt.Printf("Invalid metric validation failed: %v\n", err)
	}

	// Output:
	// Valid metric passed validation
	// Invalid metric validation failed: invalid metric name: 123_invalid_name
}

// customFormatter はカスタムフォーマッターの実装例です
type customFormatter struct{}

func (f *customFormatter) Format(metrics []victoriametrics.Metric) (string, error) {
	// カスタムフォーマット実装
	var result string
	for _, m := range metrics {
		result += fmt.Sprintf("CUSTOM: %s = %f\n", m.Name, m.Value)
	}
	return result, nil
}

// ExampleClient_withCustomFormatter はカスタムフォーマッターの使用例を示します
func ExampleClient_withCustomFormatter() {
	server := newExampleServer(http.StatusNoContent)
	defer server.Close()

	// カスタムフォーマッターを使用してクライアントを作成
	client := victoriametrics.NewClient(server.URL,
		victoriametrics.WithFormatter(&customFormatter{}),
	)
	defer client.Close()

	// メトリクスを送信
	metrics := []victoriametrics.Metric{
		{Name: "custom_metric", Value: 42},
	}

	ctx := context.Background()
	if err := client.Send(ctx, metrics); err != nil {
		log.Printf("Failed to send metrics: %v", err)
	}

	_, lines := server.received()
	for _, line := range lines {
		fmt.Println(line)
	}
	// Output:
	// CUSTOM: custom_metric = 42.000000
}

// ExampleClient_errorHandling はエラーハンドリングの例を示します
func ExampleClient_errorHandling() {
	// 送信が失敗する例として、すべてのrequestへ400 Bad Requestを返します。
	server := newExampleServer(http.StatusBadRequest)
	defer server.Close()

	// エラーを収集するチャネル
	errorCh := make(chan error, 100)

	// エラーハンドラーを定義
	errorHandler := func(err error) {
		select {
		case errorCh <- err:
		default:
			// チャネルがフルの場合はログに記録
			log.Printf("Error channel full, dropping error: %v", err)
		}
	}

	// クライアントを作成
	client := victoriametrics.NewClient(server.URL,
		victoriametrics.WithErrorHandler(errorHandler),
		victoriametrics.WithRetry(3, time.Second),
	)

	// 別のゴルーチンでエラーを処理
	done := make(chan struct{})
	go func() {
		defer close(done)
		for err := range errorCh {
			// エラーに応じた処理を実装
			var httpErr *victoriametrics.HTTPError
			if errors.As(err, &httpErr) {
				fmt.Printf("Received HTTP error: status=%d retryable=%t\n", httpErr.StatusCode, httpErr.IsRetryable())
				continue
			}
			log.Printf("Received error: %v", err)
		}
	}()

	// メトリクスを送信
	metrics := []victoriametrics.Metric{
		{Name: "error_example", Value: 1},
	}

	// 非同期送信（エラーはerrorHandlerで処理される）
	if err := client.SendAsync(metrics); err != nil {
		log.Printf("Failed to queue metrics: %v", err)
	}

	// Closeは受理済みメトリクスの送信とエラー通知を待つため、その後にチャネルを閉じられます。
	if err := client.Close(); err != nil {
		log.Printf("Failed to close client: %v", err)
	}
	close(errorCh)
	<-done
	// Output:
	// Received HTTP error: status=400 retryable=false
}

// 後方互換性のための使用例
func ExampleFormatPrometheusText() {
	metrics := []victoriametrics.Metric{
		{
			Name:   "deprecated_function_example",
			Labels: map[string]string{"version": "legacy"},
			Value:  1,
		},
	}

	// 非推奨の関数を使用（後方互換性のため）
	output := victoriametrics.FormatPrometheusText(metrics)
	fmt.Print(output)
	// Output:
	// deprecated_function_example{version="legacy"} 1
}
