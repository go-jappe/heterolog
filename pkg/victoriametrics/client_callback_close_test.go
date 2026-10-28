package victoriametrics

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
)

func TestErrorHandlerCloseDrainsAcceptedQueueAndExternalCloseWaits(t *testing.T) {
	for _, externalFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("external_close_first_%v", externalFirst), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				requestStarted, releaseRequest := make(chan struct{}), make(chan struct{})
				releaseHandler := make(chan struct{})
				handlerClosed := make(chan error, 2)
				externalClosed := make(chan error, 2)
				requests, callbacks := 0, 0
				transport := shutdownRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					_, _ = io.Copy(io.Discard, r.Body)
					_ = r.Body.Close()
					requests++
					if requests == 1 {
						close(requestStarted)
						<-releaseRequest
					}
					return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader("invalid metric"))}, nil
				})
				var client *Client
				client = NewClient("http://metrics.invalid", WithWorkerCount(1), WithBatchSize(1), WithRetry(0, 0),
					WithHTTPClient(&http.Client{Transport: transport}), WithErrorHandler(func(error) {
						callbacks++
						handlerClosed <- client.Close()
						if callbacks == 1 {
							<-releaseHandler
						}
					}))
				if err := client.SendAsync([]Metric{{Name: "first", Value: 1}}); err != nil {
					t.Fatal(err)
				}
				<-requestStarted
				if err := client.SendAsync([]Metric{{Name: "already_accepted", Value: 2}}); err != nil {
					t.Fatal(err)
				}
				if externalFirst {
					go func() { externalClosed <- client.Close() }()
					synctest.Wait()
				}
				close(releaseRequest)
				if err := <-handlerClosed; err != nil {
					t.Fatalf("Close from handler: %v", err)
				}
				if err := client.SendAsync([]Metric{{Name: "after_close", Value: 3}}); err != ErrClientClosed {
					t.Fatalf("SendAsync after handler Close = %v, want ErrClientClosed", err)
				}
				go func() { externalClosed <- client.Close() }()
				if !externalFirst {
					go func() { externalClosed <- client.Close() }()
				}
				synctest.Wait()
				select {
				case err := <-externalClosed:
					t.Fatalf("external Close returned before the handler and queue finished: %v", err)
				default:
				}
				close(releaseHandler)
				for i := 0; i < 2; i++ {
					if err := <-externalClosed; err != nil {
						t.Fatal(err)
					}
				}
				if err := <-handlerClosed; err != nil {
					t.Fatalf("Close from the remaining queued error handler: %v", err)
				}
				if requests != 2 || callbacks != 2 {
					t.Fatalf("requests=%d callbacks=%d, want both accepted entries sent and reported", requests, callbacks)
				}
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestConcurrentErrorHandlersCanCloseWithoutReleasingExternalClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		requestsStarted := make(chan struct{}, 2)
		releaseRequests, releaseHandlers := make(chan struct{}), make(chan struct{})
		handlersClosed := make(chan error, 2)
		transport := shutdownRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			_ = r.Body.Close()
			requestsStarted <- struct{}{}
			<-releaseRequests
			return &http.Response{StatusCode: http.StatusBadRequest, Body: http.NoBody}, nil
		})
		var client *Client
		client = NewClient("http://metrics.invalid", WithWorkerCount(2), WithBatchSize(1), WithRetry(0, 0),
			WithHTTPClient(&http.Client{Transport: transport}), WithErrorHandler(func(error) {
				handlersClosed <- client.Close()
				<-releaseHandlers
			}))
		for i := 0; i < 2; i++ {
			if err := client.SendAsync([]Metric{{Name: "metric", Value: float64(i)}}); err != nil {
				t.Fatal(err)
			}
		}
		<-requestsStarted
		<-requestsStarted
		close(releaseRequests)
		for i := 0; i < 2; i++ {
			if err := <-handlersClosed; err != nil {
				t.Fatal(err)
			}
		}
		externalClosed := make(chan error, 1)
		go func() { externalClosed <- client.Close() }()
		synctest.Wait()
		select {
		case err := <-externalClosed:
			t.Fatalf("external Close returned with handlers running: %v", err)
		default:
		}
		close(releaseHandlers)
		if err := <-externalClosed; err != nil {
			t.Fatal(err)
		}
	})
}
