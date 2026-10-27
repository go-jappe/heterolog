package victoriametrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestClientZeroBatchSizeUsesDefaultForSyncAndAsyncDelivery(t *testing.T) {
	for _, mode := range []string{"sync", "async"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			var batches [][]string
			transport := shutdownRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(request.Body)
				if err != nil {
					return nil, err
				}
				if len(body) == 0 {
					// size 0の不正なloopは、testをhangさせずに止まる必要があります。
					t.Error("nonempty metrics produced an empty request")
					return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader("empty batch"))}, nil
				}
				mu.Lock()
				batches = append(batches, strings.Split(strings.TrimSuffix(string(body), "\n"), "\n"))
				mu.Unlock()
				return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader(""))}, nil
			})
			client := NewClient("http://metrics.invalid", WithBatchSize(0), WithWorkerCount(1),
				WithHTTPClient(&http.Client{Transport: transport}),
				WithErrorHandler(func(err error) { t.Errorf("async send failed: %v", err) }))
			defer client.Close()
			metrics := make([]Metric, 1001)
			for i := range metrics {
				metrics[i] = Metric{Name: fmt.Sprintf("batch_metric_%d", i), Value: float64(i)}
			}
			var err error
			if mode == "sync" {
				err = client.Send(context.Background(), metrics)
			} else {
				err = client.SendAsync(metrics)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(batches) != 2 {
				t.Fatalf("batch count = %d, want 2 for the documented default of 1000", len(batches))
			}
			counts := make(map[string]int)
			for _, batch := range batches {
				if len(batch) > 1000 {
					t.Errorf("batch contains %d metrics, limit is 1000", len(batch))
				}
				for _, line := range batch {
					counts[line]++
				}
			}
			for i := range metrics {
				line := fmt.Sprintf("batch_metric_%d %d", i, i)
				if counts[line] != 1 {
					t.Errorf("metric %q delivered %d times, want 1", line, counts[line])
				}
				delete(counts, line)
			}
			if len(counts) != 0 {
				t.Errorf("unexpected metrics: %v", counts)
			}
		})
	}
}

func TestClientNegativeBatchSizeRejectsSendingBeforeTransport(t *testing.T) {
	var calls atomic.Int32
	client := NewClient("http://metrics.invalid", WithBatchSize(-1),
		WithHTTPClient(&http.Client{Transport: shutdownRoundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader(""))}, nil
		})}))
	defer client.Close()
	metrics := []Metric{{Name: "invalid_batch", Value: 1}}
	if err := client.Send(context.Background(), metrics); !errors.Is(err, ErrInvalidBatchSize) {
		t.Fatalf("Send error = %v, want %v", err, ErrInvalidBatchSize)
	}
	if err := client.SendAsync(metrics); !errors.Is(err, ErrInvalidBatchSize) {
		t.Fatalf("SendAsync error = %v, want %v", err, ErrInvalidBatchSize)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("invalid configuration sent %d requests", got)
	}
}

func TestClientNegativeRetryCountRejectsSendingBeforeTransport(t *testing.T) {
	var calls atomic.Int32
	client := NewClient("http://metrics.invalid", WithRetry(-1, 0),
		WithHTTPClient(&http.Client{Transport: shutdownRoundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader(""))}, nil
		})}))
	defer client.Close()
	metrics := []Metric{{Name: "invalid_retry", Value: 1}}
	if err := client.Send(context.Background(), metrics); !errors.Is(err, ErrInvalidRetryCount) {
		t.Fatalf("Send error = %v, want %v", err, ErrInvalidRetryCount)
	}
	if err := client.SendAsync(metrics); !errors.Is(err, ErrInvalidRetryCount) {
		t.Fatalf("SendAsync error = %v, want %v", err, ErrInvalidRetryCount)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("invalid configuration sent %d requests", got)
	}
}

// WithRetryの回数は初回の送信に加える再試行の回数です。0は不正ではなく、1回だけ送ります。
func TestClientRetryCountExcludesFirstRequest(t *testing.T) {
	for _, tc := range []struct {
		retries  int
		requests int32
	}{
		{retries: 0, requests: 1},
		{retries: 1, requests: 2},
		{retries: 2, requests: 3},
	} {
		t.Run(fmt.Sprintf("retries=%d", tc.retries), func(t *testing.T) {
			var calls atomic.Int32
			client := NewClient("http://metrics.invalid", WithRetry(tc.retries, 0),
				WithHTTPClient(&http.Client{Transport: shutdownRoundTripFunc(func(*http.Request) (*http.Response, error) {
					calls.Add(1)
					return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("busy"))}, nil
				})}))
			defer client.Close()
			if err := client.Send(context.Background(), []Metric{{Name: "retry_count", Value: 1}}); err == nil {
				t.Fatal("Send error = nil, want the last 503 error")
			}
			if got := calls.Load(); got != tc.requests {
				t.Fatalf("requests = %d, want %d", got, tc.requests)
			}
		})
	}
}
