package slack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	slackgo "github.com/slack-go/slack"
	"github.com/stretchr/testify/require"
)

func TestSlackLoggerRejectsEmptyIntervals(t *testing.T) {
	cfg := normalSlackLifecycleConfig(nil)
	logger, err := NewSlackLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	requireSlackConfigFieldError(t, err, pkglogger.ErrEmptySetting, pkglogger.ConfigFieldIntervals)
	require.Nil(t, logger)
}

func TestSlackLoggerTimerSendDoesNotInheritCallerCancellation(t *testing.T) {
	client := &callerCancellationCheckingSlackHTTPClient{
		called: make(chan error, 1),
	}
	logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Hour}, client)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := logger.testInfo(ctx, "caller is already canceled"); err != nil {
		t.Fatalf("Info() error: %v", err)
	}

	select {
	case err := <-client.called:
		if err != nil {
			t.Fatalf("timer Slack request inherited caller cancellation: %v", err)
		}
	case <-time.After(slackHangTimeout):
		t.Fatal("timer Slack request was not executed")
	}
	if err := logger.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestSlackLoggerNormalizesIntervalIndexPastLastEntry(t *testing.T) {
	client := &recordingSlackHTTPClient{}
	logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Hour, 2 * time.Hour}, client)
	// 起動後の最初の通知はindexと無関係にdelay 0で予約され、確認より先にcallbackがtimerを
	// 外すことがあります。即時送信を終えた状態にして、補正したindexの間隔で予約させます。
	// その間隔は試験中に経過しないため、Infoの後も予約が残ります。
	logger.mu.Lock()
	logger.intervalIndex = 100
	logger.nextNotificationImmediate = false
	logger.mu.Unlock()

	if err := logger.testInfo(context.Background(), "terminal interval"); err != nil {
		t.Fatalf("Info() error: %v", err)
	}
	logger.mu.Lock()
	intervalIndex := logger.intervalIndex
	timerCreated := logger.timer != nil
	logger.mu.Unlock()
	if intervalIndex != 1 {
		t.Fatalf("normalized interval index = %d, want the last index 1", intervalIndex)
	}
	if !timerCreated {
		t.Fatal("timer was not created")
	}

	if err := logger.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if got := client.callCount(); got != 1 {
		t.Fatalf("Slack calls = %d, want 1", got)
	}
}

func TestSlackLoggerEnqueueDoesNotWaitForSendAndCloseJoinsCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newBlockingNormalSlackHTTPClient()
		logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Hour}, client)
		// cleanupは登録と逆順に実行するため、失敗時も先にSlack送信を解放してからCloseし、
		// bubble内のtimer callbackをdeadlockではなく停止させます。Closeは冪等です。
		t.Cleanup(func() { _ = logger.Close() })
		t.Cleanup(client.releaseRequests)

		if err := logger.testInfo(context.Background(), "first"); err != nil {
			t.Fatalf("first Info() error: %v", err)
		}
		waitSlackLifecycleSignal(t, client.started, "timer callback did not start Slack send")

		enqueueDone := make(chan error, 1)
		go func() {
			enqueueDone <- logger.testInfo(context.Background(), "second")
		}()
		// 送信lockの待機はbubble内で停止状態として扱われるため、enqueueが進める所まで
		// 進んだ後に、送信中のSlack requestを待たずに戻ったことを確認できます。
		synctest.Wait()
		select {
		case err := <-enqueueDone:
			if err != nil {
				t.Fatalf("second Info() error: %v", err)
			}
		default:
			t.Fatal("enqueue waited for the in-flight Slack request")
		}

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- logger.Close()
		}()
		// Closeは新規受付を止め、実行中のtimer callbackの完了を待ちます。
		synctest.Wait()
		logger.mu.Lock()
		closeState := logger.closeState
		logger.mu.Unlock()
		if closeState != common.LoggerCloseStateStarted {
			t.Fatalf("close state while waiting for the callback = %v, want started", closeState)
		}
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before the in-flight callback completed: %v", err)
		default:
		}
		if err := logger.testInfo(context.Background(), "during close"); !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("Info() during Close error = %v, want ErrLoggerClosed", err)
		}

		client.releaseRequests()
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("Close() error: %v", err)
			}
		case <-time.After(slackHangTimeout):
			t.Fatal("Close did not finish after releasing the Slack request")
		}

		calls, maxActive := client.snapshot()
		if calls != 2 {
			t.Fatalf("Slack calls = %d, want 2", calls)
		}
		if maxActive != 1 {
			t.Fatalf("max concurrent Slack calls = %d, want 1", maxActive)
		}

		if err := logger.testInfo(context.Background(), "after close"); !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("Info() after Close error = %v, want ErrLoggerClosed", err)
		}
		if err := logger.Close(); err != nil {
			t.Fatalf("second Close() error: %v", err)
		}
		calls, _ = client.snapshot()
		if calls != 2 {
			t.Fatalf("Slack calls after successful Close = %d, want 2", calls)
		}
	})
}

func TestSlackLoggerRejectsEveryPublicWriteAfterClose(t *testing.T) {
	client := &recordingSlackHTTPClient{}
	logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Hour}, client)
	require.NoError(t, logger.Close())

	checks := []struct {
		name string
		log  func() error
	}{
		{name: "debug", log: func() error { return logger.testDebug(context.Background(), "after close") }},
		{name: "info", log: func() error { return logger.testInfo(context.Background(), "after close") }},
		{name: "warn", log: func() error { return logger.testWarn(context.Background(), "after close") }},
		{name: "error", log: func() error { return logger.testError(context.Background(), "after close") }},
		{name: "error with stack", log: func() error {
			return logger.testErrorWithStack(context.Background(), "after close", errors.New("cause"))
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			require.ErrorIs(t, check.log(), pkglogger.ErrLoggerClosed)
		})
	}
	require.Equal(t, 0, client.callCount())
}

func TestSlackLoggerCloseWaitsForTimerNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &channelFailOnceSlackHTTPClient{}
		logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Hour}, client)
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		releaseNotification := func() { releaseOnce.Do(func() { close(release) }) }
		// cleanupは登録と逆順に実行するため、失敗時も先に通知を解放してからCloseし、
		// bubble内のtimer callbackをdeadlockではなく終了させます。
		t.Cleanup(func() { _ = logger.Close() })
		t.Cleanup(releaseNotification)
		var first sync.Once
		var notificationCtx context.Context
		logger.OnInfo = func(ctx context.Context, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
			first.Do(func() {
				notificationCtx = ctx
				close(entered)
				<-release
				if err := ctx.Err(); err != nil {
					t.Errorf("normal Close canceled timer notification: %v", err)
				}
			})
		}
		if err := logger.testInfo(context.Background(), "accepted before close"); err != nil {
			t.Fatal(err)
		}
		<-entered
		closed := make(chan error, 1)
		go func() { closed <- logger.Close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Close returned before timer notification completed: %v", err)
		default:
		}
		if err := notificationCtx.Err(); err != nil {
			t.Fatal(err)
		}
		if err := logger.testInfo(context.Background(), "late"); !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("late write = %v, want closed", err)
		}
		releaseNotification()
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if calls := client.channels(); len(calls) != 1 {
			t.Fatalf("Slack requests = %v, want only accepted entry", calls)
		}
	})
}

func TestSlackLoggerCloseRetriesOnlyFailedAndUnsentChannels(t *testing.T) {
	client := &channelFailOnceSlackHTTPClient{failChannel: "warn-channel"}
	logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Hour}, client)
	// この試験はCloseによるbuffer送信と再試行だけを検証するため、起動直後の
	// 即時通知を消費済みの状態にします。timer callbackとCloseの競合に試験結果を
	// 依存させず、設定済みの1時間後timerはCloseが停止します。
	logger.mu.Lock()
	logger.nextNotificationImmediate = false
	logger.mu.Unlock()

	// slackChannelBatchesは新しいentryのchannelから送るため、errorが先に成功し、
	// warnだけが最初のCloseで失敗する順序にします。
	if err := logger.testWarn(context.Background(), "retry warn"); err != nil {
		t.Fatalf("Warn() error: %v", err)
	}
	if err := logger.testError(context.Background(), "send error once"); err != nil {
		t.Fatalf("Error() error: %v", err)
	}

	if err := logger.Close(); err == nil {
		t.Fatal("first Close() error = nil, want Slack send failure")
	}
	logger.mu.Lock()
	require.Equal(t, common.LoggerCloseStateStarted, logger.closeState)
	logger.mu.Unlock()
	if got := client.channels(); !slices.Equal(got, []string{"error-channel", "warn-channel"}) {
		t.Fatalf("channels after first Close = %v, want [error-channel warn-channel]", got)
	}
	logger.mu.Lock()
	remainingCount := len(logger.logs)
	remainingLevel := pkglogger.LogLevelDebug
	if remainingCount == 1 {
		remainingLevel = logger.logs[0].Level
	}
	logger.mu.Unlock()
	if remainingCount != 1 || remainingLevel != pkglogger.LogLevelWarn {
		t.Fatalf("remaining logs = %d level=%s, want one warn log", remainingCount, remainingLevel)
	}

	if err := logger.Close(); err != nil {
		t.Fatalf("second Close() error: %v", err)
	}
	logger.mu.Lock()
	require.Equal(t, common.LoggerCloseStateCompleted, logger.closeState)
	logger.mu.Unlock()
	if got := client.channels(); !slices.Equal(got, []string{"error-channel", "warn-channel", "warn-channel"}) {
		t.Fatalf("channels after Close retry = %v, want no duplicate error-channel send", got)
	}
	if err := logger.Close(); err != nil {
		t.Fatalf("third Close() error: %v", err)
	}
	if got := len(client.channels()); got != 3 {
		t.Fatalf("Slack calls after successful Close = %d, want 3", got)
	}
}

func newNormalSlackLifecycleLogger(
	t *testing.T,
	intervals []time.Duration,
	httpClient interface {
		Do(*http.Request) (*http.Response, error)
	},
) *SlackLogger {
	t.Helper()

	cfg := normalSlackLifecycleConfig(intervals)
	logger, err := NewSlackLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("NewSlackLogger() error: %v", err)
	}
	logger.slackAPI = slackgo.New(
		cfg.Token,
		slackgo.OptionHTTPClient(httpClient),
	)
	return logger
}

func normalSlackLifecycleConfig(intervals []time.Duration) pkglogger.SlackLoggerConfig {
	return pkglogger.SlackLoggerConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: false,
			TimeZone:           "UTC",
		},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "slack-lifecycle-test",
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		Timeout:                         time.Second,
		Token:                           "dummy-token",
		Channel:                         "normal-channel",
		ErrorChannel:                    "error-channel",
		WarnChannel:                     "warn-channel",
		InfoChannel:                     "info-channel",
		DebugChannel:                    "debug-channel",
		MaxLogsPerNotification:          10,
		MaxRestThreshold:                time.Hour,
		Intervals:                       intervals,
		MaxMessageLength:                10_000,
		SlackPostWarnThresholdDuration:  time.Hour,
		SlackPostErrorThresholdDuration: time.Hour,
	}
}

// slackHangTimeoutは、完了するはずの処理がhangしたことを検出するための時間の上限です。
// 処理の完了は通知で待ち、この上限は失敗の検出だけに使います。
const slackHangTimeout = 10 * time.Second

func waitSlackLifecycleSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(slackHangTimeout):
		t.Fatal(message)
	}
}

type blockingNormalSlackHTTPClient struct {
	started chan struct{}
	release chan struct{}

	startOnce   sync.Once
	releaseOnce sync.Once
	mu          sync.Mutex
	calls       int
	active      int
	maxActive   int
}

type callerCancellationCheckingSlackHTTPClient struct {
	called chan error
}

func (c *callerCancellationCheckingSlackHTTPClient) Do(req *http.Request) (*http.Response, error) {
	err := req.Context().Err()
	c.called <- err
	if err != nil {
		return nil, err
	}
	return successfulSlackLifecycleResponse(req, requestChannel(req)), nil
}

func newBlockingNormalSlackHTTPClient() *blockingNormalSlackHTTPClient {
	return &blockingNormalSlackHTTPClient{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

// releaseRequestsは、待機中と以後のSlack requestを一度だけ解放します。
func (c *blockingNormalSlackHTTPClient) releaseRequests() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func (c *blockingNormalSlackHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.calls++
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	c.mu.Unlock()
	c.startOnce.Do(func() {
		close(c.started)
	})

	<-c.release

	c.mu.Lock()
	c.active--
	c.mu.Unlock()
	return successfulSlackLifecycleResponse(req, requestChannel(req)), nil
}

func (c *blockingNormalSlackHTTPClient) snapshot() (calls int, maxActive int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.maxActive
}

type channelFailOnceSlackHTTPClient struct {
	mu          sync.Mutex
	failChannel string
	failed      bool
	calls       []string
}

func (c *channelFailOnceSlackHTTPClient) Do(req *http.Request) (*http.Response, error) {
	channel := requestChannel(req)
	c.mu.Lock()
	c.calls = append(c.calls, channel)
	shouldFail := channel == c.failChannel && !c.failed
	if shouldFail {
		c.failed = true
	}
	c.mu.Unlock()

	if shouldFail {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("unavailable")),
			Request:    req,
		}, nil
	}
	return successfulSlackLifecycleResponse(req, channel), nil
}

func (c *channelFailOnceSlackHTTPClient) channels() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func requestChannel(req *http.Request) string {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return ""
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}
	return values.Get("channel")
}

func successfulSlackLifecycleResponse(req *http.Request, channel string) *http.Response {
	body := fmt.Sprintf(`{"ok":true,"channel":%q,"ts":"lifecycle-test"}`, channel)
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}
