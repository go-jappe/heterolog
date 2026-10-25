package victoriametrics_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	vm "github.com/go-jappe/heterolog/pkg/victoriametrics"
)

func TestRequestTimeoutAppliesToEverySyncAndAsyncBatch(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Second} {
		for _, mode := range []string{"sync", "async_full", "async_close", "async_timer"} {
			t.Run(fmt.Sprintf("timeout=%s/%s", timeout, mode), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var bodies []string
					var failures []error
					transport := ownershipRoundTripFunc(func(r *http.Request) (*http.Response, error) {
						if _, ok := r.Context().Deadline(); ok != (timeout > 0) {
							t.Errorf("request has deadline = %v, configured timeout = %s", ok, timeout)
						}
						select {
						case <-r.Context().Done():
							return nil, r.Context().Err()
						case <-time.After(750 * time.Millisecond):
						}
						body, err := io.ReadAll(r.Body)
						if err != nil {
							return nil, err
						}
						bodies = append(bodies, string(body))
						return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
					})
					batchSize := 1
					want := []string{"a 1\n", "b 2\n", "c 3\n", "d 4\n"}
					if mode == "async_close" || mode == "async_timer" {
						batchSize = 5
						want = []string{"a 1\nb 2\nc 3\nd 4\n"}
					}
					client := vm.NewClient("http://metrics.invalid/write", vm.WithTimeout(timeout),
						vm.WithBatchSize(batchSize), vm.WithWorkerCount(1), vm.WithRetry(0, 0),
						vm.WithHTTPClient(&http.Client{Transport: transport}),
						vm.WithErrorHandler(func(err error) { failures = append(failures, err) }))
					defer client.Close()
					metrics := []vm.Metric{{Name: "a", Value: 1}, {Name: "b", Value: 2}, {Name: "c", Value: 3}, {Name: "d", Value: 4}}
					var err error
					if mode == "sync" {
						err = client.Send(context.Background(), metrics)
					} else {
						err = client.SendAsync(metrics)
					}
					if err != nil {
						t.Fatal(err)
					}
					if mode == "async_timer" {
						time.Sleep(6 * time.Second)
						synctest.Wait()
						if !reflect.DeepEqual(bodies, want) {
							t.Errorf("timer flush = %q, want %q", bodies, want)
						}
					}
					if err := client.Close(); err != nil {
						t.Fatal(err)
					}
					if len(failures) != 0 || !reflect.DeepEqual(bodies, want) {
						t.Fatalf("delivered = %q, failures = %v; want %q", bodies, failures, want)
					}
				})
			})
		}
	}
}

func TestAsyncRetryHasFreshRequestTimeoutAfterBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var bodies []string
		var failures []error
		transport := ownershipRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			select {
			case <-r.Context().Done():
				return nil, r.Context().Err()
			case <-time.After(750 * time.Millisecond):
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			bodies = append(bodies, string(body))
			status := http.StatusServiceUnavailable
			if len(bodies) == 3 {
				status = http.StatusNoContent
			}
			return &http.Response{StatusCode: status, Body: http.NoBody}, nil
		})
		client := vm.NewClient("http://metrics.invalid", vm.WithTimeout(time.Second), vm.WithBatchSize(1),
			vm.WithWorkerCount(1), vm.WithRetry(2, time.Second), vm.WithHTTPClient(&http.Client{Transport: transport}),
			vm.WithErrorHandler(func(err error) { failures = append(failures, err) }))
		defer client.Close()
		if err := client.SendAsync([]vm.Metric{{Name: "retried", Value: 1}}); err != nil {
			t.Fatal(err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if want := []string{"retried 1\n", "retried 1\n", "retried 1\n"}; !reflect.DeepEqual(bodies, want) || len(failures) != 0 {
			t.Fatalf("attempted bodies = %q, failures = %v; want %q", bodies, failures, want)
		}
	})
}

func TestAsyncRequestStillTimesOutAndReportsError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var failures []error
		requests := 0
		transport := ownershipRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			requests++
			<-r.Context().Done()
			return nil, r.Context().Err()
		})
		client := vm.NewClient("http://metrics.invalid", vm.WithTimeout(time.Second), vm.WithBatchSize(1),
			vm.WithWorkerCount(1), vm.WithRetry(0, 0), vm.WithHTTPClient(&http.Client{Transport: transport}),
			vm.WithErrorHandler(func(err error) { failures = append(failures, err) }))
		defer client.Close()
		start := time.Now()
		if err := client.SendAsync([]vm.Metric{{Name: "timeout", Value: 1}}); err != nil {
			t.Fatal(err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if requests != 1 || len(failures) != 1 || !errors.Is(failures[0], context.DeadlineExceeded) {
			t.Fatalf("requests = %d, failures = %v; want one timed out request", requests, failures)
		}
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Fatalf("elapsed = %s, want 1s request timeout", elapsed)
		}
	})
}
