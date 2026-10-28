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

// redirectは、設定した送信先が本来の送信先ではないことを表します。301/302/303を追従すると
// 本文なしのGETへ変わり、307/308を追従するとmetricsを別URLへ再送します。どのredirectも
// 追従せず、元のPOST一回のresponseをHTTPErrorとして返し、呼び出し元のredirect方針も
// 使わない必要があります。
func TestMetricRedirectsReturnTheOriginalErrorWithoutFollowing(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var methods, bodies []string
			callbacks := 0
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
				if r.URL.Path != "/next" {
					return redirectResponse(r, code), nil
				}
				// 転送先は、本文なしのGETにも本文付きのPOSTにも成功を返します。
				return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
			})
			err := newRedirectSend(t, &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
				callbacks++
				return nil
			}})()
			var statusErr *vm.HTTPError
			if !errors.As(err, &statusErr) || statusErr.StatusCode != code || statusErr.Body != "gateway redirect" {
				t.Fatalf("error = %v, want original redirect status %d and body", err, code)
			}
			if len(methods) != 1 || callbacks != 0 {
				t.Fatalf("methods=%v bodies=%q callbacks=%d; want only the original POST without consulting the redirect policy", methods, bodies, callbacks)
			}
			if methods[0] != http.MethodPost || bodies[0] != "metric 1\n" {
				t.Fatalf("original request = %s %q", methods[0], bodies[0])
			}
		})
	}
}

// 送信のためにredirectを止めても、所有者が直接送るrequestは、元のredirect方針のまま
// 動作します。
func TestMetricRedirectGuardDoesNotChangeTheOwnersClient(t *testing.T) {
	requests, callbacks := 0, 0
	httpClient := &http.Client{Transport: ownershipRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		if r.Body != nil {
			_ = r.Body.Close()
		}
		if r.URL.Path == "/next" {
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}
		return redirectResponse(r, http.StatusTemporaryRedirect), nil
	}), CheckRedirect: func(*http.Request, []*http.Request) error {
		callbacks++
		return nil
	}}
	var statusErr *vm.HTTPError
	if err := newRedirectSend(t, httpClient)(); !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusTemporaryRedirect ||
		requests != 1 || callbacks != 0 {
		t.Fatalf("error=%v requests=%d callbacks=%d; want the 307 returned without following it", err, requests, callbacks)
	}
	req, err := http.NewRequest(http.MethodPost, "http://metrics.invalid/write", strings.NewReader("other payload"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if requests != 3 || callbacks != 1 || resp.StatusCode != http.StatusOK {
		t.Fatalf("owner's client changed: requests=%d callbacks=%d status=%d", requests, callbacks, resp.StatusCode)
	}
}
