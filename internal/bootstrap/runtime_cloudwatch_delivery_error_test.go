package bootstrap_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	cw "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestCloudWatchDeliveryPreservesErrorCauses(t *testing.T) {
	connectionErr := errors.New("connection lost after sending log")
	for _, tc := range []struct {
		name         string
		code         int
		body         string
		transportErr error
		wantStatus   pkglogger.DeliveryStatus
		wantReason   cw.RejectedLogEventReason
		wantDecode   bool
	}{
		{name: "success", code: 200, body: `{}`, wantStatus: pkglogger.DeliverySucceeded},
		{name: "HTTP refusal", code: 403, body: `{"__type":"AccessDeniedException","message":"permission denied for test stream"}`, wantStatus: pkglogger.DeliveryFailed},
		{name: "transport failure", transportErr: connectionErr, wantStatus: pkglogger.DeliveryUnconfirmed},
		{name: "client deadline", transportErr: context.DeadlineExceeded, wantStatus: pkglogger.DeliveryUnconfirmed},
		{name: "invalid response", code: 200, body: `{"incomplete":`, wantStatus: pkglogger.DeliveryUnconfirmed, wantDecode: true},
		{name: "event rejection", code: 200, body: `{"rejectedLogEventsInfo":{"tooNewLogEventStartIndex":0}}`, wantStatus: pkglogger.DeliveryFailed, wantReason: cw.RejectedLogEventReasonTooNew},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			cfg := deliveryEvidenceConfig("cloudwatchlogs", "https://logs.example.test")
			cfg.CloudWatchLogsClient.HTTPClient = &http.Client{Transport: cloudWatchCauseTransport(func(req *http.Request) (*http.Response, error) {
				calls++
				defer req.Body.Close()
				body, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}
				if !strings.Contains(string(body), "delivery-error-cause") {
					t.Error("request did not contain the log message")
				}
				if tc.transportErr != nil {
					return nil, tc.transportErr
				}
				return &http.Response{
					StatusCode: tc.code, Status: http.StatusText(tc.code), Header: make(http.Header),
					Body: io.NopCloser(strings.NewReader(tc.body)), Request: req,
				}, nil
			})}
			logger, err := bootstrap.NewDelivery(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := logger.Close(); err != nil {
					t.Error(err)
				}
			})
			report, err := logger.Info(context.Background(), "delivery-error-cause")
			if calls != 1 || len(report.Targets) != 1 {
				t.Fatalf("calls=%d report=%+v error=%v", calls, report, err)
			}
			target := report.Targets[0]
			if target.Backend != "cloudwatchlogs" || target.Status != tc.wantStatus {
				t.Errorf("target=%+v, want cloudwatchlogs/%s", target, tc.wantStatus)
			}
			for _, cause := range []error{err, target.Err} {
				if tc.wantStatus == pkglogger.DeliverySucceeded {
					if cause != nil {
						t.Errorf("successful delivery returned %v", cause)
					}
					continue
				}
				if cause == nil {
					t.Fatal("failed delivery lost its error")
				}
				if tc.transportErr != nil && !errors.Is(cause, tc.transportErr) {
					t.Errorf("error=%v does not wrap %v", cause, tc.transportErr)
				}
				if tc.code == http.StatusForbidden {
					var statusErr *cw.HTTPStatusError
					if !errors.As(cause, &statusErr) {
						t.Errorf("error=%v does not retain HTTPStatusError", cause)
					} else if statusErr.Code != tc.code || statusErr.AwsErrorType != "AccessDeniedException" || statusErr.AwsMessage != "permission denied for test stream" {
						t.Errorf("backend details changed: %+v", statusErr)
					}
				}
				if tc.wantDecode {
					var decodeErr *cw.ResponseDecodeError
					if !errors.As(cause, &decodeErr) {
						t.Errorf("error=%v does not retain ResponseDecodeError", cause)
					}
				}
				if tc.wantReason != "" {
					var rejected *cw.RejectedLogEventError
					if !errors.As(cause, &rejected) || rejected.Reason != tc.wantReason {
						t.Errorf("error=%v lost rejection reason %q", cause, tc.wantReason)
					}
				}
			}
		})
	}
}

type cloudWatchCauseTransport func(*http.Request) (*http.Response, error)

func (f cloudWatchCauseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
