package loki

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

type retryWaitTransport func(*http.Request) (*http.Response, error)

func (f retryWaitTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestKnownRefusalSurvivesCanceledRetryWait(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{pkglogger.RetryClientModeRetry, pkglogger.RetryClientModeRetryPolicy} {
		for _, when := range []string{"before return", "during backoff"} {
			t.Run(string(mode)+"/"+when, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					calls := 0
					rawcfg := pkgloki.Config{Endpoint: "https://loki.example.test", HTTPClient: &http.Client{Transport: retryWaitTransport(func(r *http.Request) (*http.Response, error) {
						calls++
						body, err := io.ReadAll(r.Body)
						if err != nil || !strings.Contains(string(body), "preserve refusal") {
							t.Fatalf("body=%q err=%v", body, err)
						}
						if when == "before return" {
							cancel()
						} else {
							go func() { time.Sleep(10 * time.Millisecond); cancel() }()
						}
						return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("temporarily unavailable"))}, nil
					})}}
					client, err := infraloki.NewClient(rawcfg, NewRetryConfigProvider(&pkglogger.LokiRetryableClientCaseTransportErrorConfig{RetryClientMode: mode, MaxRetries: 2, BaseDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond}), nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					saved := &cancellationLokiHandler{}
					composite := mustNewLokiReportingFailureComposite(t, []flushfailure.FailureHandlerWithReport{saved})
					fm := newTestLokiFlushManagerWithReportingFailureHandler(client, composite)
					report, err := fm.PushWithReport(ctx, newTestLokiLogEntry(pkglogger.LogLevelInfo, "preserve refusal"))
					if err == nil {
						t.Fatal("expected failure")
					}
					if closeErr := fm.Close(context.Background()); closeErr != nil {
						t.Fatal(closeErr)
					}
					if calls != 1 {
						t.Fatalf("calls=%d want=1", calls)
					}
					if len(saved.results) != 1 {
						t.Fatalf("saved=%d", len(saved.results))
					}
					got := saved.results[0]
					if len(got.RetryableRecords) != 1 || len(got.QuarantineRecords) != 0 {
						t.Errorf("503 with no further request: retryable=%d quarantine=%d; report=%+v err=%v", len(got.RetryableRecords), len(got.QuarantineRecords), report, err)
					}
					if len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliveryFailed {
						t.Errorf("target=%+v want known Failed", report.Targets)
					}
					var httpErr *pkgloki.HTTPStatusError
					if !errors.As(err, &httpErr) || httpErr.StatusCode() != 503 {
						t.Errorf("original known 503 lost: %v", err)
					}
					if when == "during backoff" && !errors.Is(err, context.Canceled) {
						t.Errorf("cancel cause lost: %v", err)
					}
				})
			})
		}
	}
}
