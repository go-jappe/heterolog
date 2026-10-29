package bootstrap_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// この試験では、実際のconstructorとSlack SDKを使います。transportは、Slackへ接続
// せずに要求を読み取り、応答の証拠を返します。
func TestSlackDeliveryPreservesAcceptanceEvidence(t *testing.T) {
	const success = `{"ok":true,"channel":"C123","ts":"123.456"}`
	for _, tc := range []struct {
		name         string
		code         int
		body         string
		transportErr error
		bodyErr      error
		cancelBefore bool
		waitTimeout  bool
		want         pkglogger.DeliveryStatus
		wantCause    error
	}{
		{name: "success", body: success, want: pkglogger.DeliverySucceeded},
		{name: "success with whitespace", body: success + "\n\t ", want: pkglogger.DeliverySucceeded},
		{name: "HTTP refusal", code: http.StatusServiceUnavailable, want: pkglogger.DeliveryFailed},
		{name: "HTTP refusal with unreadable body", code: http.StatusServiceUnavailable, bodyErr: io.ErrUnexpectedEOF, want: pkglogger.DeliveryFailed},
		{name: "API refusal", body: `{"ok":false,"error":"invalid_auth"}`, want: pkglogger.DeliveryFailed},
		// Delivery loggerのAPIは、logを受け付けた後に自分のtimeoutを使います。
		{name: "caller cancellation does not cancel delivery", cancelBefore: true, body: success, want: pkglogger.DeliverySucceeded},
		{name: "response lost after request", transportErr: io.ErrUnexpectedEOF, want: pkglogger.DeliveryUnconfirmed, wantCause: io.ErrUnexpectedEOF},
		{name: "transport canceled after request", transportErr: context.Canceled, want: pkglogger.DeliveryUnconfirmed, wantCause: context.Canceled},
		{name: "Slack HTTP timeout after request", waitTimeout: true, want: pkglogger.DeliveryUnconfirmed, wantCause: context.DeadlineExceeded},
		{name: "invalid JSON", body: `{"ok":`, want: pkglogger.DeliveryUnconfirmed},
		{name: "second JSON", body: success + `{"ok":false,"error":"invalid_auth"}`, want: pkglogger.DeliveryUnconfirmed},
		{name: "trailing garbage", body: success + "garbage", want: pkglogger.DeliveryUnconfirmed},
		{name: "body failure after JSON", body: success, bodyErr: io.ErrUnexpectedEOF, want: pkglogger.DeliveryUnconfirmed, wantCause: io.ErrUnexpectedEOF},
		{name: "invalid refusal response", body: `{"ok":false,"error":"invalid_auth"}garbage`, want: pkglogger.DeliveryUnconfirmed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			var responseBody *slackEvidenceBody
			transport := slackEvidenceTransport(func(req *http.Request) (*http.Response, error) {
				if req.URL.Host != "slack.com" || req.URL.Path != "/api/chat.postMessage" {
					return nil, errors.New("unexpected request blocked by test transport")
				}
				calls++
				defer req.Body.Close()
				body, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}
				if !strings.Contains(string(body), "slack-delivery-evidence") {
					t.Error("request did not contain the log message")
				}
				if tc.waitTimeout {
					<-req.Context().Done()
					return nil, req.Context().Err()
				}
				if tc.transportErr != nil {
					return nil, tc.transportErr
				}
				code := tc.code
				if code == 0 {
					code = http.StatusOK
				}
				responseBody = &slackEvidenceBody{reader: strings.NewReader(tc.body), err: tc.bodyErr}
				return &http.Response{
					StatusCode: code, Status: http.StatusText(code),
					Header: http.Header{"Content-Type": []string{"application/json"}},
					Body:   responseBody, Request: req,
				}, nil
			})

			cfg := loggerruntime.DefaultDeliveryConfig()
			cfg.Stdout = nil
			cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputSlack}
			cfg.Slack = &pkglogger.DeliverySlackLoggerConfig{
				RuntimeConfig: pkglogger.RuntimeConfig{AppName: "delivery-test", LogLevel: pkglogger.LogLevelInfo},
				LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
				Timeout:       time.Second, Token: "test-only-token", Channel: "C123", MaxMessageLength: 4000,
				HTTPClient: &http.Client{Transport: transport},
			}
			if tc.waitTimeout {
				// 外側のDeliveryのtimeoutより前に、SlackのHTTP clientの中で終わらせます。一般的な
				// timeoutの報告で、backendの誤りが隠れないようにするためです。
				cfg.Slack.Timeout = 20 * time.Millisecond
			}
			logger, err := bootstrap.NewDelivery(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := logger.Close(); err != nil {
					t.Error(err)
				}
			}()
			if tc.cancelBefore {
				cancel()
			}
			report, err := logger.Info(ctx, "slack-delivery-evidence")
			if calls != 1 {
				t.Errorf("HTTP calls=%d, want 1", calls)
			}
			if len(report.Targets) != 1 {
				t.Fatalf("report=%+v error=%v", report, err)
			}
			if len(report.Durables) != 0 {
				t.Errorf("Slack must not report persistence it does not provide: %+v", report.Durables)
			}
			target := report.Targets[0]
			if target.Backend != "slack" || target.Status != tc.want {
				t.Errorf("target=%+v, want slack/%s", target, tc.want)
			}
			if (err == nil) != (tc.want == pkglogger.DeliverySucceeded) || (target.Err == nil) != (err == nil) {
				t.Errorf("status=%s error=%v target error=%v", target.Status, err, target.Err)
			}
			if tc.wantCause != nil && (!errors.Is(err, tc.wantCause) || !errors.Is(target.Err, tc.wantCause)) {
				t.Errorf("error=%v target error=%v, want cause %v", err, target.Err, tc.wantCause)
			}
			if report.HasTargetFailure() != tc.want.IsFailed() || len(report.FailedTargets()) != boolCount(tc.want.IsFailed()) {
				t.Errorf("failure helpers disagree with %s: %+v", tc.want, report)
			}
			if report.AllTargetsRegistered() != tc.want.IsRegistered() || report.HasUnregisteredTarget() == tc.want.IsRegistered() {
				t.Errorf("registration helpers disagree with %s: %+v", tc.want, report)
			}
			if responseBody != nil && !responseBody.closed {
				t.Error("response body was not closed")
			}
		})
	}
}

type slackEvidenceTransport func(*http.Request) (*http.Response, error)

func (f slackEvidenceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type slackEvidenceBody struct {
	reader *strings.Reader
	err    error
	closed bool
}

func (b *slackEvidenceBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if n == 0 && err == io.EOF && b.err != nil {
		return 0, b.err
	}
	return n, err
}

func (b *slackEvidenceBody) Close() error {
	b.closed = true
	return nil
}
