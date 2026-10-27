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

func TestPostRedirectsRejectBodyDroppingStatusesAndPreserve307And308(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308} {
		for _, compress := range []bool{false, true} {
			t.Run(fmt.Sprintf("status_%d/gzip_%v", status, compress), func(t *testing.T) {
				type sentRequest struct {
					method, encoding string
					body             []byte
				}
				var requests []sentRequest
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
					if len(requests) == 1 {
						response := redirectTestResponse(r, status, "redirected by gateway")
						response.Header.Set("Location", "/accepted")
						response.Header.Set("Retry-After", "2")
						return response, nil
					}
					// login pageも200を返します。空のGETでそこへ到達しても、
					// 受理されたlog payloadと取り違えてはいけません。
					bodyText := ""
					if r.Method != http.MethodPost || len(body) == 0 {
						bodyText = "<html>Sign in</html>"
					}
					return redirectTestResponse(r, http.StatusOK, bodyText), nil
				})
				send := newRedirectTestSend(t, &http.Client{Transport: transport}, compress)
				err := send()
				if status < 307 {
					var statusErr *HTTPStatusError
					if !errors.As(err, &statusErr) || statusErr.Code != status {
						t.Fatalf("send error = %v, want original redirect status %d", err, status)
					}
					if string(statusErr.Body) != "redirected by gateway" || statusErr.RetryAfter() != 2*time.Second {
						t.Errorf("redirect response lost body or Retry-After: %#v", statusErr)
					}
					if len(requests) != 1 {
						t.Fatalf("sent %d requests; must stop before sending a bodyless GET", len(requests))
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if len(requests) != 2 {
						t.Fatalf("requests=%d, want original and redirected POST", len(requests))
					}
					if requests[1].method != http.MethodPost || !bytes.Equal(requests[0].body, requests[1].body) || requests[0].encoding != requests[1].encoding {
						t.Fatalf("307/308 changed POST method, body, or encoding: %#v", requests)
					}
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

func TestPostRedirectPreservesCallerLimitsAndDefaultTenHopLimit(t *testing.T) {
	sentinel := errors.New("caller redirect limit")
	for _, test := range []struct {
		name         string
		customLimit  int
		wantRequests int
	}{
		{"default limit", 0, 10}, {"stricter caller limit", 2, 2}, {"caller permits more redirects", 12, 12},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests, policyCalls := 0, 0
			client := &http.Client{Transport: redirectTestTransport(func(r *http.Request) (*http.Response, error) {
				requests++
				if r.Body != nil {
					_, _ = io.Copy(io.Discard, r.Body)
					_ = r.Body.Close()
				}
				response := redirectTestResponse(r, http.StatusTemporaryRedirect, "")
				response.Header.Set("Location", fmt.Sprintf("/redirect/%d", requests))
				return response, nil
			})}
			if test.customLimit > 0 {
				client.CheckRedirect = func(_ *http.Request, via []*http.Request) error {
					policyCalls++
					if len(via) >= test.customLimit {
						return sentinel
					}
					return nil
				}
			}
			err := newRedirectTestSend(t, client, false)()
			if err == nil {
				t.Fatal("redirect loop unexpectedly succeeded")
			}
			if requests != test.wantRequests {
				t.Fatalf("requests=%d, want %d", requests, test.wantRequests)
			}
			if test.customLimit > 0 {
				if !errors.Is(err, sentinel) || policyCalls != test.customLimit {
					t.Fatalf("caller policy err=%v calls=%d", err, policyCalls)
				}
			} else if !errors.Is(err, ErrTooManyRedirects) {
				t.Fatalf("default limit error=%v, want %v", err, ErrTooManyRedirects)
			}
		})
	}
}

func TestPostRedirectPreservesCallerErrorAndLastResponse(t *testing.T) {
	sentinel := errors.New("caller rejects redirect")
	for _, status := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		for _, policyErr := range []error{sentinel, http.ErrUseLastResponse} {
			t.Run(fmt.Sprintf("%d/%s", status, policyErr), func(t *testing.T) {
				requests, policyCalls := 0, 0
				client := &http.Client{Transport: redirectTestTransport(func(r *http.Request) (*http.Response, error) {
					requests++
					if r.Body != nil {
						_, _ = io.Copy(io.Discard, r.Body)
						_ = r.Body.Close()
					}
					response := redirectTestResponse(r, status, "caller stopped this redirect")
					response.Header.Set("Location", "/next")
					return response, nil
				}), CheckRedirect: func(*http.Request, []*http.Request) error { policyCalls++; return policyErr }}
				err := newRedirectTestSend(t, client, false)()
				if policyErr == http.ErrUseLastResponse {
					var statusErr *HTTPStatusError
					if !errors.As(err, &statusErr) || statusErr.Code != status {
						t.Fatalf("last response error=%v", err)
					}
				} else if !errors.Is(err, sentinel) {
					t.Fatalf("caller policy error was lost: %v", err)
				}
				if requests != 1 || policyCalls != 1 {
					t.Fatalf("requests=%d policy calls=%d", requests, policyCalls)
				}
			})
		}
	}
}

func TestPostRedirectGuardDoesNotMutateSharedClientAndRejectsEmptyPOST(t *testing.T) {
	for _, restorePOST := range []bool{false, true} {
		t.Run(fmt.Sprintf("callback_restores_POST_%v", restorePOST), func(t *testing.T) {
			requests, policyCalls := 0, 0
			client := &http.Client{Transport: redirectTestTransport(func(r *http.Request) (*http.Response, error) {
				requests++
				if r.Body != nil {
					_, _ = io.Copy(io.Discard, r.Body)
					_ = r.Body.Close()
				}
				if r.URL.Path != "/next" {
					response := redirectTestResponse(r, http.StatusFound, "")
					response.Header.Set("Location", "/next")
					return response, nil
				}
				return redirectTestResponse(r, http.StatusOK, "<html>Sign in</html>"), nil
			}), CheckRedirect: func(next *http.Request, _ []*http.Request) error {
				policyCalls++
				if restorePOST {
					next.Method = http.MethodPost
				} // Bodyはnilのままです。
				return nil
			}}
			err := newRedirectTestSend(t, client, false)()
			var statusErr *HTTPStatusError
			if !errors.As(err, &statusErr) || statusErr.Code != http.StatusFound || requests != 1 || policyCalls != 1 {
				t.Fatalf("send err=%v requests=%d policy calls=%d; bodyless redirected request must be stopped", err, requests, policyCalls)
			}
			// 呼び出し元のclientを直接再利用しても、元のpolicyのまま動作します。
			// libraryのguardがclientに残っていると、この送信は止められてしまいます。
			req, err := http.NewRequest(http.MethodPost, "https://logging.test/original", strings.NewReader("other request"))
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK || requests != 3 || policyCalls != 2 {
				t.Fatalf("shared client changed: status=%d requests=%d policy calls=%d", response.StatusCode, requests, policyCalls)
			}
		})
	}
}
