package slack

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	slackgo "github.com/slack-go/slack"
	"github.com/stretchr/testify/require"
)

func TestSlackDeliveryRequiresCompleteResponseEvidence(t *testing.T) {
	const success = `{"ok":true,"channel":"C-resolved","ts":"123.456"}`
	bodyFailure := errors.New("response read failed after complete JSON")
	for _, test := range []struct {
		name       string
		body       string
		code       int
		readErr    error
		closeErr   error
		sendErr    error
		cancel     bool
		want       pkglogger.DeliveryStatus
		wantCause  error
		apiRefusal bool
	}{
		{name: "valid success with resolved channel", body: success, want: pkglogger.DeliverySucceeded},
		{name: "whitespace and future fields", body: `{"ok":true,"channel":"C-resolved","ts":"opaque-ts","future":{"x":1}}` + " \n\t", want: pkglogger.DeliverySucceeded},
		{name: "confirmed success despite cancellation", body: success, cancel: true, want: pkglogger.DeliverySucceeded},
		{name: "HTTP refusal", code: 503, readErr: bodyFailure, want: pkglogger.DeliveryFailed},
		{name: "HTTP refusal despite cancellation", code: 403, cancel: true, want: pkglogger.DeliveryFailed},
		{name: "429 without retry header", code: 429, want: pkglogger.DeliveryFailed},
		{name: "API refusal", body: `{"ok":false,"error":"invalid_auth"}`, want: pkglogger.DeliveryFailed, apiRefusal: true},
		{name: "network response lost", sendErr: io.ErrUnexpectedEOF, want: pkglogger.DeliveryUnconfirmed, wantCause: io.ErrUnexpectedEOF},
		{name: "canceled after send", sendErr: context.Canceled, cancel: true, want: pkglogger.DeliveryUnconfirmed, wantCause: context.Canceled},
		{name: "body read error", body: success, readErr: bodyFailure, want: pkglogger.DeliveryUnconfirmed, wantCause: bodyFailure},
		{name: "body close error", body: success, closeErr: bodyFailure, want: pkglogger.DeliveryUnconfirmed, wantCause: bodyFailure},
		{name: "malformed JSON", body: `{"ok":`, want: pkglogger.DeliveryUnconfirmed},
		{name: "trailing garbage", body: success + "garbage", want: pkglogger.DeliveryUnconfirmed},
		{name: "second JSON value", body: success + `{"ok":false,"error":"invalid_auth"}`, want: pkglogger.DeliveryUnconfirmed},
		{name: "incomplete refusal", body: `{"ok":false,"error":"invalid_auth"}garbage`, want: pkglogger.DeliveryUnconfirmed},
		{name: "missing ok", body: `{"channel":"C","ts":"1"}`, want: pkglogger.DeliveryUnconfirmed},
		{name: "null ok", body: `{"ok":null,"channel":"C","ts":"1"}`, want: pkglogger.DeliveryUnconfirmed},
		{name: "string ok", body: `{"ok":"true","channel":"C","ts":"1"}`, want: pkglogger.DeliveryUnconfirmed},
		{name: "null response", body: `null`, want: pkglogger.DeliveryUnconfirmed},
		{name: "array response", body: `[]`, want: pkglogger.DeliveryUnconfirmed},
		{name: "missing channel", body: `{"ok":true,"ts":"1"}`, want: pkglogger.DeliveryUnconfirmed},
		{name: "missing timestamp", body: `{"ok":true,"channel":"C"}`, want: pkglogger.DeliveryUnconfirmed},
		{name: "timestamp wrong type", body: `{"ok":true,"channel":"C","ts":123}`, want: pkglogger.DeliveryUnconfirmed},
		{name: "contradictory success", body: `{"ok":true,"channel":"C","ts":"1","error":"invalid_auth"}`, want: pkglogger.DeliveryUnconfirmed},
		{name: "refusal without error", body: `{"ok":false}`, want: pkglogger.DeliveryUnconfirmed},
		{name: "invalid UTF-8", body: "{\"ok\":true,\"channel\":\"C\",\"ts\":\"\xff\"}", want: pkglogger.DeliveryUnconfirmed},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			body := &slackOutcomeBody{Reader: strings.NewReader(test.body), readErr: test.readErr, closeErr: test.closeErr}
			client := slackResponseHTTPClient{client: slackOutcomeHTTPFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				requestBody, err := io.ReadAll(request.Body)
				require.NoError(t, err)
				require.NoError(t, request.Body.Close())
				require.Contains(t, string(requestBody), "response-evidence")
				if test.cancel {
					cancel()
				}
				if test.sendErr != nil {
					return nil, test.sendErr
				}
				code := test.code
				if code == 0 {
					code = http.StatusOK
				}
				return &http.Response{StatusCode: code, Status: http.StatusText(code), Header: make(http.Header), Body: body, Request: request}, nil
			})}
			logger, _ := newDeliverySlackLoggerForLevelTest(t, ctx, pkglogger.LogLevelDebug)
			logger.slackAPI = slackgo.New("test-only-token", slackgo.OptionHTTPClient(client))
			report, err := logger.testInfo(ctx, "response-evidence")
			require.Equal(t, 1, calls)
			result := requireSingleSlackDeliveryTarget(t, report)
			require.Equal(t, test.want, result.Status)
			if test.want == pkglogger.DeliverySucceeded {
				require.NoError(t, err)
				require.NoError(t, result.Err)
			} else {
				require.Error(t, err)
				require.Same(t, err, result.Err)
			}
			if test.wantCause != nil {
				require.ErrorIs(t, err, test.wantCause)
			}
			if test.code == 503 || test.code == 403 {
				var statusErr slackgo.StatusCodeError
				require.ErrorAs(t, err, &statusErr)
				require.Equal(t, test.code, statusErr.Code)
			}
			if test.apiRefusal {
				var refusal slackgo.SlackErrorResponse
				require.ErrorAs(t, err, &refusal)
				require.Equal(t, "invalid_auth", refusal.Err)
			}
			if test.sendErr == nil {
				require.Equal(t, 1, body.closes)
			}
		})
	}
}

func TestSlackInvalidResponseRetainsBufferedEntriesUntilConfirmed(t *testing.T) {
	for _, suffix := range []string{"garbage", `{"ok":false,"error":"invalid_auth"}`} {
		t.Run(suffix, func(t *testing.T) {
			calls := 0
			var sent []string
			client := slackResponseHTTPClient{client: slackOutcomeHTTPFunc(func(request *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(request.Body)
				require.NoError(t, err)
				require.NoError(t, request.Body.Close())
				sent = append(sent, string(body))
				calls++
				response := `{"ok":true,"channel":"C","ts":"123.456"}`
				if calls == 1 {
					response += suffix
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response)), Request: request}, nil
			})}
			logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Hour}, client)
			occurredAt := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
			first := &logging.LogEntry{Message: "first-kept-record", Level: pkglogger.LogLevelInfo, OccurredAt: occurredAt}
			second := &logging.LogEntry{Message: "second-kept-record", Level: pkglogger.LogLevelInfo, OccurredAt: occurredAt.Add(time.Second)}
			logger.logs = []*logging.LogEntry{first, second}
			require.Error(t, logger.sendBatch(context.Background()))
			require.Equal(t, []*logging.LogEntry{first, second}, logger.logs)
			require.NoError(t, logger.Close())
			require.Empty(t, logger.logs)
			require.Equal(t, 2, calls)
			require.Equal(t, sent[0], sent[1], "retry must preserve both original entries")
			require.Contains(t, sent[0], "first-kept-record")
			require.Contains(t, sent[0], "second-kept-record")
		})
	}
}

type slackOutcomeHTTPFunc func(*http.Request) (*http.Response, error)

func (f slackOutcomeHTTPFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

type slackOutcomeBody struct {
	*strings.Reader
	readErr  error
	closeErr error
	closes   int
}

func (b *slackOutcomeBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if err == io.EOF && b.readErr != nil {
		return n, b.readErr
	}
	return n, err
}

func (b *slackOutcomeBody) Close() error {
	b.closes++
	return b.closeErr
}
