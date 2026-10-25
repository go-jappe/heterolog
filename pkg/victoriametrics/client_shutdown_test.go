package victoriametrics

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
)

type shutdownRoundTripFunc func(*http.Request) (*http.Response, error)

func (f shutdownRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestClientCloseFlushesWorkerBuffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var bodies []string
		transport := shutdownRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			if err := request.Context().Err(); err != nil {
				t.Errorf("shutdown flush context is canceled: %v", err)
			}
			if _, ok := request.Context().Deadline(); !ok {
				t.Error("shutdown flush must retain its timeout")
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			bodies = append(bodies, string(body))
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}, nil
		})
		client := NewClient("http://metrics.invalid", WithWorkerCount(1), WithBatchSize(2),
			WithHTTPClient(&http.Client{Transport: transport}))
		defer client.Close()
		if err := client.SendAsync([]Metric{{Name: "shutdown_buffer_metric", Value: 1}}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(bodies) != 0 {
			t.Fatal("worker buffer was sent before Close")
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if len(bodies) != 1 || !strings.Contains(bodies[0], "shutdown_buffer_metric 1") {
			t.Fatalf("shutdown requests = %v, want buffered metric", bodies)
		}
		if err := client.SendAsync([]Metric{{Name: "after_close", Value: 1}}); err == nil {
			t.Fatal("closed client accepted an asynchronous send")
		}
	})
}

func TestClientCloseWaitsForInFlightAsyncSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		transport := shutdownRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			close(started)
			<-release
			if err := request.Context().Err(); err != nil {
				t.Errorf("Close canceled an in-flight send: %v", err)
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}, nil
		})
		client := NewClient("http://metrics.invalid", WithWorkerCount(1), WithBatchSize(1),
			WithHTTPClient(&http.Client{Transport: transport}))
		if err := client.SendAsync([]Metric{{Name: "in_flight", Value: 1}}); err != nil {
			t.Fatal(err)
		}
		<-started
		closed := make(chan error, 1)
		go func() { closed <- client.Close() }()
		synctest.Wait()
		concurrentClose := make(chan error, 1)
		go func() { concurrentClose <- client.Close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Close returned before the send completed: %v", err)
		default:
		}
		select {
		case err := <-concurrentClose:
			t.Fatalf("concurrent Close returned before the send completed: %v", err)
		default:
		}
		close(release)
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if err := <-concurrentClose; err != nil {
			t.Fatal(err)
		}
	})
}

func TestClientCloseSendsAllAcceptedAsyncQueueEntries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var bodies []string
		transport := shutdownRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			if len(bodies) == 0 {
				close(entered)
				<-release
			}
			if err := request.Context().Err(); err != nil {
				t.Errorf("Close canceled accepted queue entry: %v", err)
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			bodies = append(bodies, string(body))
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}, nil
		})
		client := NewClient("http://metrics.invalid", WithWorkerCount(1), WithBatchSize(1),
			WithHTTPClient(&http.Client{Transport: transport}))
		if err := client.SendAsync([]Metric{{Name: "first_accepted", Value: 1}}); err != nil {
			t.Fatal(err)
		}
		<-entered
		for _, name := range []string{"second_accepted", "third_accepted"} {
			if err := client.SendAsync([]Metric{{Name: name, Value: 1}}); err != nil {
				t.Fatal(err)
			}
		}
		closed := make(chan error, 1)
		go func() { closed <- client.Close() }()
		synctest.Wait()
		if err := client.SendAsync([]Metric{{Name: "late", Value: 1}}); err == nil {
			t.Fatal("Close did not stop new async requests")
		}
		close(release)
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if len(bodies) != 3 {
			t.Fatalf("requests = %v, want every accepted queue entry", bodies)
		}
		for index, name := range []string{"first_accepted", "second_accepted", "third_accepted"} {
			if !strings.Contains(bodies[index], name+" 1") {
				t.Errorf("request %d = %q, want %q", index, bodies[index], name)
			}
		}
	})
}

func TestClientSendAsyncRejectsNonPositiveWorkerCount(t *testing.T) {
	for _, count := range []int{0, -1} {
		t.Run(fmt.Sprintf("workers_%d", count), func(t *testing.T) {
			var bodies []string
			transport := shutdownRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(request.Body)
				if err != nil {
					return nil, err
				}
				bodies = append(bodies, string(body))
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}, nil
			})
			client := NewClient("http://metrics.invalid", WithWorkerCount(count),
				WithHTTPClient(&http.Client{Transport: transport}))
			defer client.Close()
			if err := client.SendAsync([]Metric{{Name: "unserviceable", Value: 1}}); err == nil {
				t.Fatal("SendAsync accepted data without any workers to deliver it")
			}
			// Disabling asynchronous workers must leave synchronous sending available.
			if err := client.Send(context.Background(), []Metric{{Name: "synchronous", Value: 2}}); err != nil {
				t.Fatal(err)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			if len(bodies) != 1 || bodies[0] != "synchronous 2\n" {
				t.Fatalf("requests = %q, want only the synchronous metric", bodies)
			}
		})
	}
}

func TestClientSendAsyncConcurrentClose(t *testing.T) {
	for attempt := 0; attempt < 50; attempt++ {
		var mu sync.Mutex
		delivered := make(map[string]int)
		accepted := make(map[string]int)
		transport := shutdownRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			mu.Lock()
			for _, line := range strings.Split(strings.TrimSuffix(string(body), "\n"), "\n") {
				delivered[line]++
			}
			mu.Unlock()
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}, nil
		})
		client := NewClient("http://metrics.invalid", WithWorkerCount(2), WithBatchSize(10),
			WithHTTPClient(&http.Client{Transport: transport}),
			WithErrorHandler(func(err error) { t.Errorf("async send: %v", err) }))
		defer client.Close()
		// Ensure the test checks delivery even when Close wins every subsequent race.
		if err := client.SendAsync([]Metric{{Name: "before_close", Value: 1}}); err != nil {
			t.Fatal(err)
		}
		accepted["before_close 1"] = 1
		start := make(chan struct{})
		var workers sync.WaitGroup
		for sender := 0; sender < 4; sender++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				for i := 0; i < 100; i++ {
					name := fmt.Sprintf("concurrent_%d_%d", sender, i)
					if err := client.SendAsync([]Metric{{Name: name, Value: float64(i)}}); err == nil {
						mu.Lock()
						accepted[fmt.Sprintf("%s %d", name, i)]++
						mu.Unlock()
					}
				}
			}()
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			if err := client.Close(); err != nil {
				t.Error(err)
			}
		}()
		close(start)
		workers.Wait()
		if !reflect.DeepEqual(delivered, accepted) {
			t.Errorf("attempt %d: delivered metrics = %v, want accepted metrics %v", attempt, delivered, accepted)
		}
		if err := client.SendAsync(nil); err == nil {
			t.Fatalf("attempt %d: closed client accepted a send", attempt)
		}
	}
}
