package victoriametrics_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	vm "github.com/go-jappe/heterolog/pkg/victoriametrics"
)

func newRedirectSend(t *testing.T, httpClient *http.Client) func() error {
	t.Helper()
	client := vm.NewClient("http://metrics.invalid/write", vm.WithWorkerCount(0), vm.WithRetry(0, 0), vm.WithHTTPClient(httpClient))
	t.Cleanup(func() { _ = client.Close() })
	return func() error { return client.Send(context.Background(), []vm.Metric{{Name: "metric", Value: 1}}) }
}

func redirectResponse(r *http.Request, code int) *http.Response {
	return &http.Response{StatusCode: code, Status: fmt.Sprint(code), Request: r,
		Header: http.Header{"Location": []string{"/next"}}, Body: io.NopCloser(strings.NewReader("gateway redirect"))}
}

func TestMetricRedirectsPreserveSubmissionOrReturnOriginalError(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var methods, bodies []string
			transport := ownershipRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				var body []byte
				if r.Body != nil {
					var err error
					body, err = io.ReadAll(r.Body)
					_ = r.Body.Close()
					if err != nil {
						return nil, err
					}
				}
				methods = append(methods, r.Method)
				bodies = append(bodies, string(body))
				if len(methods) == 1 {
					return redirectResponse(r, code), nil
				}
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			})
			err := newRedirectSend(t, &http.Client{Transport: transport})()
			if code < 307 {
				var statusErr *vm.HTTPError
				if !errors.As(err, &statusErr) || statusErr.StatusCode != code || statusErr.Body != "gateway redirect" {
					t.Fatalf("error = %v, want original redirect status %d and body", err, code)
				}
				if len(methods) != 1 {
					t.Fatalf("bodyless redirect sent: methods=%v bodies=%q", methods, bodies)
				}
			} else if err != nil || len(methods) != 2 || methods[1] != http.MethodPost || bodies[1] != "metric 1\n" {
				t.Fatalf("redirect did not preserve metric: methods=%v bodies=%q error=%v", methods, bodies, err)
			}
			if methods[0] != http.MethodPost || bodies[0] != "metric 1\n" {
				t.Fatalf("original request = %s %q", methods[0], bodies[0])
			}
		})
	}
}

func TestMetricRedirectKeepsDefaultAndCallerLimits(t *testing.T) {
	for _, limit := range []int{0, 2, 12} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			requests, callbacks := 0, 0
			sentinel := errors.New("caller redirect limit")
			httpClient := &http.Client{Transport: ownershipRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests++
				_ = r.Body.Close()
				return redirectResponse(r, http.StatusTemporaryRedirect), nil
			})}
			want := 10
			if limit > 0 {
				want = limit
				httpClient.CheckRedirect = func(_ *http.Request, via []*http.Request) error {
					callbacks++
					if len(via) >= limit {
						return sentinel
					}
					return nil
				}
			}
			err := newRedirectSend(t, httpClient)()
			if err == nil || requests != want {
				t.Fatalf("requests=%d error=%v, want %d requests followed by redirect error", requests, err, want)
			}
			if limit > 0 && (!errors.Is(err, sentinel) || callbacks != limit) {
				t.Fatalf("caller policy changed: callbacks=%d error=%v", callbacks, err)
			}
			if limit == 0 && !errors.Is(err, vm.ErrTooManyRedirects) {
				t.Fatalf("default policy error = %v, want %v", err, vm.ErrTooManyRedirects)
			}
		})
	}
}

func TestMetricRedirectHonorsCallerErrorsAndRejectsEmptyPOST(t *testing.T) {
	sentinel := errors.New("caller rejects redirect")
	for _, policyErr := range []error{nil, sentinel, http.ErrUseLastResponse} {
		t.Run(fmt.Sprint(policyErr), func(t *testing.T) {
			requests, callbacks := 0, 0
			httpClient := &http.Client{Transport: ownershipRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests++
				if r.Body != nil {
					_ = r.Body.Close()
				}
				if r.URL.Path == "/next" {
					return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
				}
				return redirectResponse(r, http.StatusFound), nil
			}), CheckRedirect: func(r *http.Request, _ []*http.Request) error {
				callbacks++
				r.Method = http.MethodPost // methodだけを戻しても、失われたbodyが隠れてはいけません。
				return policyErr
			}}
			err := newRedirectSend(t, httpClient)()
			if policyErr == sentinel {
				if !errors.Is(err, sentinel) {
					t.Fatalf("caller error lost: %v", err)
				}
			} else {
				var statusErr *vm.HTTPError
				if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusFound {
					t.Fatalf("error=%v, want original 302", err)
				}
			}
			if requests != 1 || callbacks != 1 {
				t.Fatalf("requests=%d callbacks=%d, want 1 each", requests, callbacks)
			}
			if policyErr == nil {
				// libraryのguardは、所有者が直接送るrequestを変えてはいけません。
				req, err := http.NewRequest(http.MethodPost, "http://metrics.invalid/write", strings.NewReader("other payload"))
				if err != nil {
					t.Fatal(err)
				}
				resp, err := httpClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_ = resp.Body.Close()
				if requests != 3 || callbacks != 2 || resp.StatusCode != http.StatusOK {
					t.Fatalf("owner's client changed: requests=%d callbacks=%d status=%d", requests, callbacks, resp.StatusCode)
				}
			}
		})
	}
}
