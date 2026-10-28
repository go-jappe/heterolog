package loki

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type redirectTestTransport func(*http.Request) (*http.Response, error)

func (f redirectTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newRedirectTestSend(t *testing.T, httpClient *http.Client, compress bool) func() error {
	t.Helper()
	client, err := New(Config{Endpoint: "https://logging.test", HTTPClient: httpClient, CompressRequest: compress})
	if err != nil {
		t.Fatal(err)
	}
	return func() error {
		return client.PushEntry(context.Background(), map[string]string{"app": "test"}, Entry{Timestamp: time.Unix(1700000000, 0), Line: "preserve this log"})
	}
}

func redirectTestResponse(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}
}

// redirectは、設定した送信先が本来の送信先ではないことを表します。301/302/303を追従すると
// 本文なしのGETへ変わり、転送先のlogin pageなどの成功を配送の成功と取り違えます。307/308を
// 追従すると、logの本文を別URLへ再送します。どのredirectも追従せず、元のPOST
// 一回のresponseを、本文とRetry-Afterを保ったHTTPStatusErrorとして返し、呼び出し元の
// redirect方針も使わない必要があります。
func TestPostReturnsEveryRedirectWithoutFollowing(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308} {
		for _, compress := range []bool{false, true} {
			t.Run(fmt.Sprintf("status_%d/gzip_%v", status, compress), func(t *testing.T) {
				type sentRequest struct {
					method, encoding string
					body             []byte
				}
				var requests []sentRequest
				policyCalls := 0
				transport := redirectTestTransport(func(r *http.Request) (*http.Response, error) {
					var body []byte
					if r.Body != nil {
						var err error
						body, err = io.ReadAll(r.Body)
						_ = r.Body.Close()
						if err != nil {
							return nil, err
						}
					}
					requests = append(requests, sentRequest{r.Method, r.Header.Get("Content-Encoding"), body})
					if r.URL.Path != "/accepted" {
						response := redirectTestResponse(r, status, "redirected by gateway")
						response.Header.Set("Location", "/accepted")
						response.Header.Set("Retry-After", "2")
						return response, nil
					}
					// 転送先は、本文なしのGETにも本文付きのPOSTにも成功を返します。
					return redirectTestResponse(r, http.StatusNoContent, ""), nil
				})
				client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
					policyCalls++
					return nil
				}}
				err := newRedirectTestSend(t, client, compress)()
				var statusErr *HTTPStatusError
				if !errors.As(err, &statusErr) || statusErr.Code != status {
					t.Fatalf("send error = %v, want original redirect status %d", err, status)
				}
				if string(statusErr.Body) != "redirected by gateway" || statusErr.RetryAfter() != 2*time.Second {
					t.Errorf("redirect response lost body or Retry-After: %#v", statusErr)
				}
				if len(requests) != 1 || policyCalls != 0 {
					t.Fatalf("requests=%d policy calls=%d; want only the original POST without consulting the redirect policy", len(requests), policyCalls)
				}
				first := requests[0]
				if first.method != http.MethodPost {
					t.Fatalf("initial method=%s", first.method)
				}
				payload := first.body
				if compress {
					reader, err := gzip.NewReader(bytes.NewReader(payload))
					if err != nil {
						t.Fatal(err)
					}
					payload, err = io.ReadAll(reader)
					_ = reader.Close()
					if err != nil {
						t.Fatal(err)
					}
					if first.encoding != "gzip" {
						t.Fatalf("Content-Encoding=%q", first.encoding)
					}
				}
				if !bytes.Contains(payload, []byte("preserve this log")) {
					t.Fatalf("missing log in body: %q", payload)
				}
			})
		}
	}
}

// 送信のためにredirectを止めても、呼び出し元のclientは元のredirect方針のまま動作します。
// libraryのguardがclientに残っていると、呼び出し元の別の要求のredirectまで止まります。
func TestPostDoesNotChangeTheRedirectPolicyOfTheSharedClient(t *testing.T) {
	requests, policyCalls := 0, 0
	client := &http.Client{Transport: redirectTestTransport(func(r *http.Request) (*http.Response, error) {
		requests++
		if r.Body != nil {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		}
		if r.URL.Path != "/next" {
			response := redirectTestResponse(r, http.StatusTemporaryRedirect, "")
			response.Header.Set("Location", "/next")
			return response, nil
		}
		return redirectTestResponse(r, http.StatusOK, ""), nil
	}), CheckRedirect: func(*http.Request, []*http.Request) error {
		policyCalls++
		return nil
	}}
	var statusErr *HTTPStatusError
	if err := newRedirectTestSend(t, client, false)(); !errors.As(err, &statusErr) || statusErr.Code != http.StatusTemporaryRedirect ||
		requests != 1 || policyCalls != 0 {
		t.Fatalf("send err=%v requests=%d policy calls=%d; want the 307 returned without following it", err, requests, policyCalls)
	}
	req, err := http.NewRequest(http.MethodPost, "https://logging.test/original", strings.NewReader("other request"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || requests != 3 || policyCalls != 1 {
		t.Fatalf("shared client changed: status=%d requests=%d policy calls=%d", response.StatusCode, requests, policyCalls)
	}
}
