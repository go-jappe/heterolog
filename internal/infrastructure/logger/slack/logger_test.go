package slack

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	slackgo "github.com/slack-go/slack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeliverySlackLoggerLevelFilteringControlsPosting(t *testing.T) {
	ctx := context.Background()

	logCalls := []struct {
		name    string
		level   pkglogger.LogLevel
		channel string
		call    func(*DeliverySlackLogger, context.Context) (pkglogger.DeliveryReport, error)
	}{
		{
			name:    "debug",
			level:   pkglogger.LogLevelDebug,
			channel: "dummy-debug-channel",
			call: func(logger *DeliverySlackLogger, ctx context.Context) (pkglogger.DeliveryReport, error) {
				return logger.testDebug(ctx, "debug slack delivery")
			},
		},
		{
			name:    "info",
			level:   pkglogger.LogLevelInfo,
			channel: "dummy-info-channel",
			call: func(logger *DeliverySlackLogger, ctx context.Context) (pkglogger.DeliveryReport, error) {
				return logger.testInfo(ctx, "info slack delivery")
			},
		},
		{
			name:    "warn",
			level:   pkglogger.LogLevelWarn,
			channel: "dummy-warn-channel",
			call: func(logger *DeliverySlackLogger, ctx context.Context) (pkglogger.DeliveryReport, error) {
				return logger.testWarn(ctx, "warn slack delivery")
			},
		},
		{
			name:    "error",
			level:   pkglogger.LogLevelError,
			channel: "dummy-error-channel",
			call: func(logger *DeliverySlackLogger, ctx context.Context) (pkglogger.DeliveryReport, error) {
				return logger.testError(ctx, "error slack delivery")
			},
		},
	}

	thresholds := []pkglogger.LogLevel{
		pkglogger.LogLevelDebug,
		pkglogger.LogLevelInfo,
		pkglogger.LogLevelWarn,
		pkglogger.LogLevelError,
	}

	for _, threshold := range thresholds {
		for _, logCall := range logCalls {
			t.Run(fmt.Sprintf("threshold_%s_call_%s", threshold, logCall.name), func(t *testing.T) {
				logger, client := newDeliverySlackLoggerForLevelTest(t, ctx, threshold)

				report, err := logCall.call(logger, ctx)
				require.NoError(t, err)
				result := requireSingleSlackDeliveryTarget(t, report)
				assert.Equal(t, BackendServices, result.Backend)
				assert.NoError(t, result.Err)

				if logCall.level < threshold {
					assert.Equal(t, pkglogger.DeliverySkipped, result.Status)
					assert.Empty(t, client.calls)
					return
				}

				assert.Equal(t, pkglogger.DeliverySucceeded, result.Status)
				require.Len(t, client.calls, 1)
				assert.Equal(t, logCall.channel, client.calls[0].channel)
			})
		}
	}
}

// errorを渡さないErrorWithStackも拒否せず、要求されたstackを持つERRORとして一件投稿します。
// errorの欄は投稿本文に含めません。
func TestDeliverySlackLoggerErrorWithStackWithoutErrorPostsStack(t *testing.T) {
	cfg := deliverySlackLevelTestConfig(pkglogger.LogLevelDebug)
	// stack traceを切り詰めずに検証できるよう、本文の上限を広げます。
	cfg.MaxMessageLength = 10_000
	logger, client := newDeliverySlackLoggerWithConfigForTest(t, context.Background(), cfg)

	report, err := logger.testErrorWithStack(context.Background(), "missing error", nil)

	require.NoError(t, err)
	result := requireSingleSlackDeliveryTarget(t, report)
	assert.Equal(t, pkglogger.DeliverySucceeded, result.Status)
	assert.NoError(t, result.Err)
	requireSingleSlackStackPostWithoutError(t, client, cfg.ErrorChannel, "missing error")
}

// 通常APIでもerrorを渡さないErrorWithStackを拒否せず、stack付きのERRORとして投稿します。
func TestSlackLoggerErrorWithStackWithoutErrorPostsStack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &recordingSlackHTTPClient{}
		logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Hour}, client)
		t.Cleanup(func() { require.NoError(t, logger.Close()) })

		require.NoError(t, logger.testErrorWithStack(context.Background(), "missing error", nil))
		// startup後の最初の通知は即時送信のため、Closeを待たずに投稿されます。
		synctest.Wait()
		requireSingleSlackStackPostWithoutError(
			t,
			client,
			normalSlackLifecycleConfig(nil).ErrorChannel,
			"missing error",
		)
	})
}

// requireSingleSlackStackPostWithoutErrorは、errorのないErrorWithStackがERROR channelへ
// 一件だけstack付きで投稿され、error欄を含まないことを確認します。
func requireSingleSlackStackPostWithoutError(
	t *testing.T,
	client *recordingSlackHTTPClient,
	channel string,
	message string,
) {
	t.Helper()

	client.mu.Lock()
	calls := append([]recordingSlackPost(nil), client.calls...)
	client.mu.Unlock()
	require.Len(t, calls, 1)
	assert.Equal(t, channel, calls[0].channel)
	assert.Contains(t, calls[0].text, "[ERROR] ")
	assert.Contains(t, calls[0].text, " - "+message)
	assert.Contains(t, calls[0].text, " (stacktrace): (1) at: (File: ")
	assert.NotContains(t, calls[0].text, " - err: ")
}

func TestDeliverySlackLoggerPostMessageFailureReportsFailed(t *testing.T) {
	logger, _ := newDeliverySlackLoggerForLevelTest(t, context.Background(), pkglogger.LogLevelDebug)
	logger.slackAPI = slackgo.New("dummy-token", slackgo.OptionHTTPClient(slackFailureHTTPClient{
		statusCode: http.StatusServiceUnavailable,
	}))

	report, err := logger.testInfo(context.Background(), "slack unavailable")

	require.Error(t, err)
	result := requireSingleSlackDeliveryTarget(t, report)
	require.Equal(t, err, result.Err)
	assert.Equal(t, pkglogger.DeliveryFailed, result.Status)
}

func TestDeliverySlackLoggerCanceledBeforeSendIsRejectedWithoutHTTP(t *testing.T) {
	logger, _ := newDeliverySlackLoggerForLevelTest(t, context.Background(), pkglogger.LogLevelDebug)
	calls := 0
	logger.slackAPI = slackgo.New("dummy-token", slackgo.OptionHTTPClient(slackCallerContextHTTPClient{calls: &calls}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report, err := logger.testInfo(ctx, "canceled delivery")

	require.ErrorIs(t, err, context.Canceled)
	result := requireSingleSlackDeliveryTarget(t, report)
	require.ErrorIs(t, result.Err, context.Canceled)
	assert.Equal(t, pkglogger.DeliveryRejected, result.Status)
	assert.Zero(t, calls)
}

type slackFailureHTTPClient struct {
	statusCode int
}

type slackCallerContextHTTPClient struct{ calls *int }

func requireSingleSlackDeliveryTarget(t *testing.T, report pkglogger.DeliveryReport) pkglogger.DeliveryResult {
	t.Helper()

	require.Len(t, report.Targets, 1)
	return report.Targets[0]
}

func (c slackFailureHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: c.statusCode,
		Status:     http.StatusText(c.statusCode),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("")),
		Request:    req,
	}, nil
}

func (c slackCallerContextHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if c.calls != nil {
		*c.calls++
	}
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("Slack request did not inherit the caller context")
}

type recordingSlackHTTPClient struct {
	mu    sync.Mutex
	calls []recordingSlackPost
}

type recordingSlackPost struct {
	channel string
	text    string
}

func (c *recordingSlackHTTPClient) Do(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	values, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}

	channel := values.Get("channel")
	text := values.Get("text")
	c.mu.Lock()
	c.calls = append(c.calls, recordingSlackPost{channel: channel, text: text})
	c.mu.Unlock()

	response := fmt.Sprintf(`{"ok":true,"channel":%q,"ts":"dummy-timestamp"}`, channel)
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(response)),
		Request:    req,
	}, nil
}

func (c *recordingSlackHTTPClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func (c *recordingSlackHTTPClient) requireTextContains(t *testing.T, want string) {
	t.Helper()

	c.mu.Lock()
	calls := append([]recordingSlackPost(nil), c.calls...)
	c.mu.Unlock()

	require.NotEmpty(t, calls)
	for _, call := range calls {
		if strings.Contains(call.text, want) {
			return
		}
	}
	t.Fatalf("slack post text does not contain %q; calls=%+v", want, calls)
}

func TestSlackLoggerCloseFlushesBufferedBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &recordingSlackHTTPClient{}
		logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Hour, time.Hour}, client)
		t.Cleanup(func() { require.NoError(t, logger.Close()) })
		ctx := context.Background()
		require.NoError(t, logger.testInfo(ctx, "first immediate notification"))
		synctest.Wait()
		require.Equal(t, 1, client.callCount())

		message := "slack close should flush buffered batch"
		require.NoError(t, logger.testError(ctx, message))
		synctest.Wait()
		require.Equal(t, 1, client.callCount(), "second notification must still be buffered")
		require.NoError(t, logger.Close())
		require.Equal(t, 2, client.callCount())
		client.requireTextContains(t, message)
		require.NoError(t, logger.Close())
		require.Equal(t, 2, client.callCount(), "repeated Close must not resend accepted messages")
	})
}

func newDeliverySlackLoggerForLevelTest(t *testing.T, ctx context.Context, level pkglogger.LogLevel) (*DeliverySlackLogger, *recordingSlackHTTPClient) {
	t.Helper()

	return newDeliverySlackLoggerWithConfigForTest(t, ctx, deliverySlackLevelTestConfig(level))
}

func deliverySlackLevelTestConfig(level pkglogger.LogLevel) pkglogger.DeliverySlackLoggerConfig {
	return pkglogger.DeliverySlackLoggerConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: false,
			TimeZone:           "UTC",
		},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "delivery-slack-test",
			LogLevel: level,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		Timeout:          time.Second,
		Token:            "dummy-token",
		Channel:          "dummy-channel",
		ErrorChannel:     "dummy-error-channel",
		WarnChannel:      "dummy-warn-channel",
		InfoChannel:      "dummy-info-channel",
		DebugChannel:     "dummy-debug-channel",
		MaxMessageLength: 100,
	}
}

func newDeliverySlackLoggerWithConfigForTest(
	t *testing.T,
	ctx context.Context,
	cfg pkglogger.DeliverySlackLoggerConfig,
) (*DeliverySlackLogger, *recordingSlackHTTPClient) {
	t.Helper()

	logger, err := NewDeliverySlackLogger(
		ctx,
		NewDeliveryConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)

	client := &recordingSlackHTTPClient{}
	logger.slackAPI = slackgo.New(cfg.Token, slackgo.OptionHTTPClient(client))

	return logger, client
}

// An HTTP 200 response can still mean Slack rejected the message.
func TestDeliverySlackLoggerAPIRejectionReportsFailed(t *testing.T) {
	logger, _ := newDeliverySlackLoggerForLevelTest(t, context.Background(), pkglogger.LogLevelDebug)
	logger.slackAPI = slackgo.New("dummy-token", slackgo.OptionHTTPClient(slackAPIRejectionClient{}))
	report, err := logger.testInfo(context.Background(), "rejected by Slack")
	requireSlackAPIError(t, err, slackAPIRejectionCode)
	result := requireSingleSlackDeliveryTarget(t, report)
	require.Equal(t, pkglogger.DeliveryFailed, result.Status)
	requireSlackAPIError(t, result.Err, slackAPIRejectionCode)
}

func TestSlackLoggerAPIRejectionNotifiesAndCloseCanRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Hour}, slackAPIRejectionClient{})
		// 途中で失敗した場合もbuffer送信の予約を止めます。送信先が拒否し続けるため、
		// cleanupのClose結果は検証しません。Closeは完了後の再呼び出しで何もしません。
		t.Cleanup(func() { _ = logger.Close() })
		errors := make(chan error, 4)
		logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			errors <- err
		}
		const message = "retain this message after API rejection"
		require.NoError(t, logger.testError(context.Background(), message), "enqueue succeeds before asynchronous delivery")
		synctest.Wait()
		select {
		case err := <-errors:
			requireSlackAPIError(t, err, slackAPIRejectionCode)
		default:
			t.Fatal("asynchronous Slack failure was not reported to OnError")
		}
		// Closeは最後の送信失敗を返します。
		requireSlackAPIError(t, logger.Close(), slackAPIRejectionCode)

		client := &recordingSlackHTTPClient{}
		logger.slackAPI = slackgo.New("dummy-token", slackgo.OptionHTTPClient(client))
		require.NoError(t, logger.Close())
		require.Equal(t, 1, client.callCount())
		client.requireTextContains(t, message)
		require.NoError(t, logger.Close())
		require.Equal(t, 1, client.callCount())
	})
}

// slackAPIRejectionCodeは、slackAPIRejectionClientがHTTP 200の応答で返すSlack API error codeです。
const slackAPIRejectionCode = "invalid_auth"

// requireSlackAPIErrorは、errがSlack APIの拒否応答をerror codeとともに保持することを検証します。
func requireSlackAPIError(t *testing.T, err error, code string) {
	t.Helper()
	var apiErr slackgo.SlackErrorResponse
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, code, apiErr.Err)
}

type slackAPIRejectionClient struct{}

func (slackAPIRejectionClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"ok":false,"error":"invalid_auth"}`)),
		Request:    req,
	}, nil
}

func TestSlackLoggerIdlePeriodResetsNotificationDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &recordingSlackHTTPClient{}
		logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Second, 2 * time.Second}, client)
		t.Cleanup(func() { require.NoError(t, logger.Close()) })
		ctx := context.Background()
		require.NoError(t, logger.testInfo(ctx, "first"))
		synctest.Wait()
		require.Equal(t, 1, client.callCount())
		require.NoError(t, logger.testInfo(ctx, "delayed"))
		synctest.Wait()
		require.Equal(t, 1, client.callCount())
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, 1, client.callCount(), "do not send before the configured interval")
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, 2, client.callCount(), "the second notification uses the first configured interval")
		client.requireTextContains(t, "delayed")
		require.NoError(t, logger.testInfo(ctx, "longer interval"))
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, 2, client.callCount())
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, 3, client.callCount(), "subsequent notifications advance to the next interval")

		time.Sleep(time.Hour + time.Second)
		require.NoError(t, logger.testInfo(ctx, "immediate after idle"))
		synctest.Wait()
		require.Equal(t, 4, client.callCount(), "a new burst after the idle threshold must start immediately")
		client.requireTextContains(t, "immediate after idle")
	})
}
