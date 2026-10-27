package victoriametrics_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	vm "github.com/go-jappe/heterolog/pkg/victoriametrics"
)

type ownershipRoundTripFunc func(*http.Request) (*http.Response, error)

func (f ownershipRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type ownershipFormatterFunc func([]vm.Metric) (string, error)

func (f ownershipFormatterFunc) Format(metrics []vm.Metric) (string, error) {
	return f(metrics)
}

func TestSendAsyncPreservesAcceptedValuesAfterInputReuse(t *testing.T) {
	for _, change := range []string{"slice", "labels"} {
		t.Run(change, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			var bodies []string
			var sendErrors []error
			transport := ownershipRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				if len(bodies) == 0 {
					close(entered)
					select {
					case <-release:
					case <-request.Context().Done():
						return nil, request.Context().Err()
					}
				}
				body, err := io.ReadAll(request.Body)
				if err != nil {
					return nil, err
				}
				bodies = append(bodies, string(body))
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			})
			client := vm.NewClient("http://metrics.invalid", vm.WithWorkerCount(1), vm.WithBatchSize(1),
				vm.WithRetry(0, 0), vm.WithHTTPClient(&http.Client{Transport: transport}),
				vm.WithErrorHandler(func(err error) { sendErrors = append(sendErrors, err) }))
			defer func() {
				releaseOnce.Do(func() { close(release) })
				client.Close()
			}()
			if err := client.SendAsync([]vm.Metric{{Name: "blocker", Value: 1}}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("worker did not start the blocking request")
			}

			metrics := []vm.Metric{
				{Name: "first", Labels: map[string]string{"owner": "one"}, Value: 2},
				{Name: "second", Labels: map[string]string{"owner": "two"}, Value: 3},
				{Name: "nil_labels", Value: 4},
				{Name: "empty_labels", Labels: map[string]string{}, Value: 5},
			}
			if err := client.SendAsync(metrics); err != nil {
				t.Fatal(err)
			}
			// 唯一のworkerはまだblockerを送信中です。ここで受理済みの入力を再利用しても、
			// 後で行われるformatやrequest生成と競合しません。
			for i := range metrics {
				if change == "slice" {
					metrics[i] = vm.Metric{Name: "reused", Value: 99}
				} else if metrics[i].Labels != nil {
					delete(metrics[i].Labels, "owner")
					metrics[i].Labels["reused"] = "changed"
				}
			}
			releaseOnce.Do(func() { close(release) })
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			if len(sendErrors) != 0 {
				t.Fatalf("asynchronous send errors: %v", sendErrors)
			}
			want := []string{"blocker 1\n", "first{owner=\"one\"} 2\n", "second{owner=\"two\"} 3\n", "nil_labels 4\n", "empty_labels 5\n"}
			if !reflect.DeepEqual(bodies, want) {
				t.Fatalf("requests after caller reused input = %q, want %q", bodies, want)
			}
		})
	}
}

func TestSendAsyncPreservesNilAndEmptyLabelsForCustomFormatter(t *testing.T) {
	var nilLabels []bool
	var bodies []string
	formatter := ownershipFormatterFunc(func(metrics []vm.Metric) (string, error) {
		for _, metric := range metrics {
			nilLabels = append(nilLabels, metric.Labels == nil)
		}
		return vm.NewPrometheusTextFormatter().Format(metrics)
	})
	transport := ownershipRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		bodies = append(bodies, string(body))
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})
	client := vm.NewClient("http://metrics.invalid", vm.WithWorkerCount(1), vm.WithBatchSize(2),
		vm.WithRetry(0, 0), vm.WithFormatter(formatter), vm.WithHTTPClient(&http.Client{Transport: transport}))
	defer client.Close()
	for _, metrics := range [][]vm.Metric{nil, {}, {
		{Name: "nil_labels", Value: 1},
		{Name: "empty_labels", Labels: map[string]string{}, Value: 2},
	}} {
		if err := client.SendAsync(metrics); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nilLabels, []bool{true, false}) {
		t.Fatalf("formatter observed nil labels = %v, want [true false]", nilLabels)
	}
	if want := []string{"nil_labels 1\nempty_labels 2\n"}; !reflect.DeepEqual(bodies, want) {
		t.Fatalf("requests = %q, want %q", bodies, want)
	}
}

func TestSharedHTTPClientKeepsIndependentRequestTimeouts(t *testing.T) {
	var deadline, observedAt time.Time
	var hasDeadline bool
	transport := ownershipRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		deadline, hasDeadline = request.Context().Deadline()
		observedAt = time.Now()
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})
	shared := &http.Client{Timeout: 37 * time.Second, Transport: transport}
	cases := []struct {
		name    string
		client  *vm.Client
		timeout time.Duration
	}{
		{"first", vm.NewClient("http://metrics.invalid/first", vm.WithWorkerCount(0),
			vm.WithTimeout(2*time.Second), vm.WithHTTPClient(shared), vm.WithTimeout(time.Second)), time.Second},
		{"second", vm.NewClient("http://metrics.invalid/second", vm.WithWorkerCount(0),
			vm.WithTimeout(time.Minute), vm.WithHTTPClient(shared)), time.Minute},
		{"default", vm.NewClient("http://metrics.invalid/default", vm.WithWorkerCount(0),
			vm.WithHTTPClient(shared)), 10 * time.Second},
		{"unlimited", vm.NewClient("http://metrics.invalid/unlimited", vm.WithWorkerCount(0),
			vm.WithHTTPClient(shared), vm.WithTimeout(0)), 0},
	}
	for _, tc := range cases {
		defer tc.client.Close()
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := time.Now()
			if err := tc.client.Send(context.Background(), []vm.Metric{{Name: "metric", Value: 1}}); err != nil {
				t.Fatal(err)
			}
			if hasDeadline != (tc.timeout != 0) {
				t.Fatalf("request has deadline = %v, configured timeout = %s", hasDeadline, tc.timeout)
			}
			// HTTPはSendの間、つまりこれらの観測時刻の間にdeadlineを計算します。
			// timeoutまでsleepせずに、実際のrequest contextを確認します。
			if hasDeadline && (deadline.Before(before.Add(tc.timeout)) || deadline.After(observedAt.Add(tc.timeout))) {
				t.Errorf("request deadline = %s, want between %s and %s", deadline, before.Add(tc.timeout), observedAt.Add(tc.timeout))
			}
		})
	}
	if shared.Timeout != 37*time.Second {
		t.Errorf("caller HTTP client timeout changed to %s, want 37s", shared.Timeout)
	}
}

func TestHTTPClientCopyPreservesTransportJarAndRedirectPolicy(t *testing.T) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := url.Parse("http://metrics.invalid")
	if err != nil {
		t.Fatal(err)
	}
	jar.SetCookies(endpoint, []*http.Cookie{{Name: "session", Value: "original"}})
	var requests, redirects int
	transport := ownershipRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		if cookie, err := request.Cookie("session"); err != nil || cookie.Value != "original" {
			t.Errorf("request cookie = %v, error = %v", cookie, err)
		}
		return &http.Response{
			StatusCode: http.StatusTemporaryRedirect,
			Header: http.Header{
				"Location":   []string{"/redirected"},
				"Set-Cookie": []string{"session=updated"},
			},
			Body: http.NoBody,
		}, nil
	})
	shared := &http.Client{Timeout: 37 * time.Second, Transport: transport, Jar: jar,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			redirects++
			return http.ErrUseLastResponse
		}}
	client := vm.NewClient(endpoint.String(), vm.WithWorkerCount(0), vm.WithRetry(0, 0),
		vm.WithHTTPClient(shared), vm.WithTimeout(time.Second))
	defer client.Close()
	err = client.Send(context.Background(), []vm.Metric{{Name: "metric", Value: 1}})
	var httpErr *vm.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("Send error = %v, want redirect response rejected by caller policy", err)
	}
	if requests != 1 || redirects != 1 {
		t.Errorf("requests = %d, redirect policy calls = %d; want 1 each", requests, redirects)
	}
	if cookies := jar.Cookies(endpoint); len(cookies) != 1 || cookies[0].Value != "updated" {
		t.Errorf("shared cookie jar after response = %v, want session=updated", cookies)
	}
	if shared.Timeout != 37*time.Second {
		t.Errorf("caller HTTP client timeout changed to %s, want 37s", shared.Timeout)
	}
}
