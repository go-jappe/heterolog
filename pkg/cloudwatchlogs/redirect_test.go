package cloudwatchlogs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 301/302/303ではnet/httpがPOSTを本文なしのGETへ変えて追従し、307/308では同じ署名と
// 本文を別URLへ再送します。PutLogEventsの成功応答は{}のため、転送先が{}を返すと、
// 届いていないlogを配送の成功と誤認します。どのredirectも追従せず、元のPOST一回の
// responseをHTTPStatusErrorとして返し、呼び出し元のclientのredirect方針も変えない
// 必要があります。
func TestAPICallsReturnRedirectWithoutFollowingOrChangingHTTPClientPolicy(t *testing.T) {
	statuses := []int{
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	}
	operations := []struct {
		name   string
		target string
		call   func(*HTTPClient) error
	}{
		{
			name:   "PutLogEvents",
			target: targetPutLogEvents,
			call: func(client *HTTPClient) error {
				sentAt := time.Unix(1700000000, 0)
				_, err := client.PutLogEvents(context.Background(), PutLogEventsRequest{
					LogStreamName: "app-stream",
					Events:        []Event{{Timestamp: sentAt, Message: "preserve this log"}},
					AttemptedAt:   sentAt,
				})
				return err
			},
		},
		{
			name:   "CreateLogStream",
			target: targetCreateLogStream,
			call: func(client *HTTPClient) error {
				return client.CreateLogStream(context.Background(), "app-stream")
			},
		},
	}
	for _, operation := range operations {
		for _, status := range statuses {
			t.Run(fmt.Sprintf("%s/%d", operation.name, status), func(t *testing.T) {
				var mu sync.Mutex
				var requests []string
				var policyCalls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/check-client-policy":
						w.Header().Set("Location", "/client-policy-preserved")
						w.WriteHeader(http.StatusTemporaryRedirect)
						return
					case "/client-policy-preserved":
						w.WriteHeader(http.StatusNoContent)
						return
					}
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Errorf("read request body: %v", err)
					}
					mu.Lock()
					requests = append(requests, r.Method+" "+r.URL.Path+" "+r.Header.Get("X-Amz-Target"))
					mu.Unlock()
					if r.URL.Path != "/" {
						// proxyやSSO pageのように、転送先は本文なしのGETにも、PutLogEventsの成功と
						// 同じ{}を返します。
						w.Header().Set("Content-Type", contentType)
						_, _ = io.WriteString(w, "{}")
						return
					}
					w.Header().Set("Location", "/redirected")
					w.WriteHeader(status)
					_, _ = io.WriteString(w, "redirected by gateway")
				}))
				defer server.Close()
				httpClient := server.Client()
				httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
					policyCalls.Add(1)
					return nil
				}
				client, err := New(Config{
					Region: "ap-northeast-1", Endpoint: server.URL, LogGroupName: "/app/test",
					AccessKeyID: "access-key", SecretAccessKey: "secret-key", HTTPClient: httpClient,
				})
				if err != nil {
					t.Fatal(err)
				}

				err = operation.call(client)
				var statusErr *HTTPStatusError
				if !errors.As(err, &statusErr) || statusErr.StatusCode() != status {
					t.Fatalf("%s() error = %v, want HTTPStatusError with status %d returned for classification", operation.name, err, status)
				}
				mu.Lock()
				gotRequests := slices.Clone(requests)
				mu.Unlock()
				if want := []string{"POST / " + operation.target}; !slices.Equal(gotRequests, want) || policyCalls.Load() != 0 {
					t.Fatalf("requests=%q policy calls=%d, want only %q without following the redirect", gotRequests, policyCalls.Load(), want)
				}

				// 共有するclientは、API呼び出しの後も呼び出し元のredirect方針を保っている必要があります。
				resp, err := httpClient.Get(server.URL + "/check-client-policy")
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusNoContent || policyCalls.Load() != 1 {
					t.Errorf("shared client status=%d policy calls=%d, want 204 and original policy invoked once", resp.StatusCode, policyCalls.Load())
				}
			})
		}
	}
}

// Configにclientを渡さない場合に作る既定のclientも、redirectを追従してはいけません。
func TestDefaultHTTPClientDoesNotFollowRedirects(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/" {
			_, _ = io.WriteString(w, "{}")
			return
		}
		w.Header().Set("Location", "/redirected")
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()
	client, err := New(Config{
		Region: "ap-northeast-1", Endpoint: server.URL, LogGroupName: "/app/test",
		AccessKeyID: "access-key", SecretAccessKey: "secret-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	sentAt := time.Unix(1700000000, 0)
	_, err = client.PutLogEvents(context.Background(), PutLogEventsRequest{
		LogStreamName: "app-stream",
		Events:        []Event{{Timestamp: sentAt, Message: "preserve this log"}},
		AttemptedAt:   sentAt,
	})
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusFound || requests.Load() != 1 {
		t.Fatalf("PutLogEvents() error = %v after %d requests, want HTTPStatusError 302 after the original POST only", err, requests.Load())
	}
}
