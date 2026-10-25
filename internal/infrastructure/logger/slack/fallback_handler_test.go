package slack

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	slackgo "github.com/slack-go/slack"
	"github.com/stretchr/testify/require"
)

func TestFallbackLoggerUsesConfiguredSDKDebug(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		for _, debug := range []bool{false, true} {
			t.Run(fmt.Sprintf("delivery=%t/debug=%t", delivery, debug), func(t *testing.T) {
				cfg := fallbackSlackTestConfig([]time.Duration{time.Hour})
				cfg.RuntimeConfig.IsDebug = debug
				adapter := NewConfigAdapter(cfg)
				if delivery {
					deliveryCfg := deliveryFallbackSlackTestConfig()
					deliveryCfg.RuntimeConfig.IsDebug = debug
					adapter = NewDeliveryFallbackConfigAdapter(deliveryCfg, pkglogger.DeliveryInternalLogSlackBatchConfig{
						Intervals:              []time.Duration{time.Hour},
						MaxLogsPerNotification: 10,
						MaxRestThreshold:       time.Hour,
					})
				}
				fallback, err := newHandleSlackLogger(context.Background(), adapter, observer.NopTracer{}, observer.NopMeter{})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, fallback.Close()) })
				require.Equal(t, debug, fallback.slackAPI.Debug(), "fallback SDK debug must follow Runtime.IsDebug")
			})
		}
	}
}

func TestFallbackLoggerIntervalIndexPastEndUsesLastInterval(t *testing.T) {
	tests := []struct {
		name          string
		intervals     []time.Duration
		intervalIndex int
		wantIndex     int
	}{
		{
			name:          "index past end uses last interval",
			intervals:     []time.Duration{time.Hour},
			intervalIndex: 10,
			wantIndex:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := fallbackSlackTestConfig(tt.intervals)
			fallback := newFallbackLoggerForTest(t, cfg)
			client := newFallbackRecordingHTTPClient()
			fallback.slackAPI = slackgo.New(
				cfg.Token,
				slackgo.OptionHTTPClient(client),
			)
			fallback.intervalIndex = tt.intervalIndex

			fallback.enqueue(fallbackSlackTestEntry(
				pkglogger.LogLevelError,
				tt.name,
			))

			client.waitForCalls(t, 1)
			require.Equal(t, tt.wantIndex, snapshotFallbackState(fallback).intervalIndex)
		})
	}
}

func TestDeliveryFallbackHandlerBatchesQueuedEntriesUntilClose(t *testing.T) {
	batchCfg := pkglogger.DeliveryInternalLogSlackBatchConfig{
		Intervals:              []time.Duration{time.Hour},
		MaxLogsPerNotification: 4,
		MaxRestThreshold:       time.Hour,
	}
	named := NewNamedInfoFallbackHandler(NewDeliveryFallbackConfigAdapter(
		deliveryFallbackSlackTestConfig(),
		batchCfg,
	))
	controller, ok := named.Closer.(*fallbackHandlerController)
	require.True(t, ok)

	fallback, acquired := controller.acquire(
		context.Background(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.True(t, acquired)
	client := newFallbackRecordingHTTPClient()
	fallback.slackAPI = slackgo.New(
		fallback.config.Token(),
		slackgo.OptionHTTPClient(client),
	)
	// Closeまで二件を同じbufferへ保持し、Delivery fallbackが一件固定でないことを確認します。
	fallback.nextNotificationImmediate = false
	controller.release()

	for _, message := range []string{"delivery fallback first", "delivery fallback second"} {
		named.Handler(
			context.Background(),
			fallbackSlackTestEntry(pkglogger.LogLevelInfo, message),
			observer.NopTracer{},
			observer.NopMeter{},
		)
	}
	require.Empty(t, client.callsSnapshot())

	require.NoError(t, named.Closer.Close())
	calls := client.callsSnapshot()
	require.Len(t, calls, 1)
	require.Contains(t, calls[0].text, "delivery fallback first")
	require.Contains(t, calls[0].text, "delivery fallback second")
}

func TestDeliveryFallbackHandlerReturnsAfterEnqueueWhileSlackSendIsBlocked(t *testing.T) {
	batchCfg := pkglogger.DeliveryInternalLogSlackBatchConfig{
		Intervals:              []time.Duration{time.Hour},
		MaxLogsPerNotification: 4,
		MaxRestThreshold:       time.Hour,
	}
	named := NewNamedInfoFallbackHandler(NewDeliveryFallbackConfigAdapter(
		deliveryFallbackSlackTestConfig(),
		batchCfg,
	))
	controller, ok := named.Closer.(*fallbackHandlerController)
	require.True(t, ok)
	fallback, acquired := controller.acquire(
		context.Background(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.True(t, acquired)
	client := newFallbackBlockingHTTPClient()
	t.Cleanup(client.releaseFirstCall)
	fallback.slackAPI = slackgo.New(
		fallback.config.Token(),
		slackgo.OptionHTTPClient(client),
	)
	controller.release()

	handlerDone := make(chan struct{})
	go func() {
		named.Handler(
			context.Background(),
			fallbackSlackTestEntry(pkglogger.LogLevelInfo, "asynchronous delivery fallback"),
			observer.NopTracer{},
			observer.NopMeter{},
		)
		close(handlerDone)
	}()

	select {
	case <-handlerDone:
	case <-time.After(slackHangTimeout):
		t.Fatal("Delivery Slack fallback handler waited for the HTTP request")
	}
	client.waitForFirstCall(t)
	client.releaseFirstCall()
	client.waitForCalls(t, 1)
	require.NoError(t, named.Closer.Close())
}

func TestFallbackHandlerControllerDoesNotRetryInvalidMaxLogsInitialization(t *testing.T) {
	controller := newFallbackHandlerController(NewConfigAdapter(pkglogger.SlackLoggerConfig{
		Token:                  "xoxb-test-token",
		Channel:                "test-channel",
		Timeout:                time.Second,
		MaxLogsPerNotification: 0,
	}))

	for attempt := 1; attempt <= 2; attempt++ {
		fallback, ok := controller.acquire(
			context.Background(),
			observer.NopTracer{},
			observer.NopMeter{},
		)
		if ok || fallback != nil {
			t.Fatalf("attempt %d acquire = (%T, %t), want (nil, false)", attempt, fallback, ok)
		}
	}
	if !controller.initializationAttempted {
		t.Fatal("invalid fallback logger initialization was not recorded")
	}
	if controller.logger != nil {
		t.Fatalf("invalid fallback logger = %T, want nil", controller.logger)
	}
}

func TestNamedFallbackHandlerCloserFlushesBufferedEntry(t *testing.T) {
	cfg := fallbackSlackTestConfig([]time.Duration{time.Hour})
	named := NewNamedInfoFallbackHandler(NewConfigAdapter(cfg))
	require.NotNil(t, named.Closer)

	controller, ok := named.Closer.(*fallbackHandlerController)
	require.True(t, ok)
	fallback, acquired := controller.acquire(
		context.Background(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.True(t, acquired)
	require.NotNil(t, fallback)

	client := newFallbackRecordingHTTPClient()
	fallback.slackAPI = slackgo.New(
		cfg.Token,
		slackgo.OptionHTTPClient(client),
	)
	// この試験はCloseが予約済みtimerを止め、bufferをflushすることを検証します。
	// 初回即時通知を止めてからhandlerを呼び、Closeまでentryをbufferへ残します。
	fallback.nextNotificationImmediate = false
	controller.release()

	named.Handler(
		context.Background(),
		fallbackSlackTestEntry(pkglogger.LogLevelInfo, "flush from handler closer close"),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Empty(t, client.callsSnapshot(), "entry must remain buffered before Close")

	require.NoError(t, named.Closer.Close())
	calls := client.callsSnapshot()
	require.Len(t, calls, 1)
	require.Equal(t, cfg.InfoChannel, calls[0].channel)
	require.Contains(t, calls[0].text, "flush from handler closer close")

	// CloserのClose後はhandlerが新しいtimerやbufferを作りません。
	named.Handler(
		context.Background(),
		fallbackSlackTestEntry(pkglogger.LogLevelInfo, "ignored after close"),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, named.Closer.Close())
	require.Len(t, client.callsSnapshot(), 1)
}

// levelが混在する一回分のbufferは、entryごとの送信先channelで分割します。
// 同じchannelのentryは一投稿へまとめ、従来どおり新しいentryから並べます。
func TestFallbackLoggerMixedLevelsAreBatchedByResolvedChannel(t *testing.T) {
	cfg := fallbackSlackTestConfig([]time.Duration{time.Hour})
	fallback := newFallbackLoggerForTest(t, cfg)
	client := newFallbackRecordingHTTPClient()
	fallback.slackAPI = slackgo.New(
		cfg.Token,
		slackgo.OptionHTTPClient(client),
	)

	fallback.logs = []*logging.LogEntry{
		fallbackSlackTestEntry(pkglogger.LogLevelError, "error-old"),
		fallbackSlackTestEntry(pkglogger.LogLevelInfo, "info-old"),
		fallbackSlackTestEntry(pkglogger.LogLevelWarn, "warn"),
		fallbackSlackTestEntry(pkglogger.LogLevelInfo, "info-new"),
		fallbackSlackTestEntry(pkglogger.LogLevelDebug, "debug-newest"),
	}

	fallback.sendBatch()

	calls := client.callsSnapshot()
	require.Len(t, calls, 4)
	require.Equal(t, []string{
		cfg.DebugChannel,
		cfg.InfoChannel,
		cfg.WarnChannel,
		cfg.ErrorChannel,
	}, fallbackCallChannels(calls))
	require.Contains(t, calls[0].text, "debug-newest")
	require.Contains(t, calls[1].text, "info-new")
	require.Contains(t, calls[1].text, "info-old")
	require.Less(
		t,
		strings.Index(calls[1].text, "info-new"),
		strings.Index(calls[1].text, "info-old"),
	)
	require.Contains(t, calls[2].text, "warn")
	require.Contains(t, calls[3].text, "error-old")
}

// timer callbackがSlack送信で停止している間にentryを追加しても、callbackは送信前に
// timer所有権を解放済みなので、追加entryには次回timerが必ず設定されます。
func TestFallbackLoggerEnqueueDuringTimerSendSchedulesNextBatch(t *testing.T) {
	cfg := fallbackSlackTestConfig([]time.Duration{
		time.Hour,
		time.Hour,
	})
	fallback := newFallbackLoggerForTest(t, cfg)
	client := newFallbackBlockingHTTPClient()
	// cleanupは登録と逆順に実行するため、失敗時も先に一件目の送信を解放してから
	// fallback loggerを閉じます（Closeは実行中のtimer callbackの完了を待ちます）。
	t.Cleanup(client.releaseFirstCall)
	fallback.slackAPI = slackgo.New(
		cfg.Token,
		slackgo.OptionHTTPClient(client),
	)

	fallback.enqueue(fallbackSlackTestEntry(
		pkglogger.LogLevelError,
		"first batch",
	))
	client.waitForFirstCall(t)

	fallback.enqueue(fallbackSlackTestEntry(
		pkglogger.LogLevelError,
		"second batch",
	))

	state := snapshotFallbackState(fallback)
	require.Equal(t, []string{"second batch"}, state.messages)
	require.True(t, state.timerScheduled, "entry added during send must own the next timer")
	require.Equal(t, 0, state.intervalIndex)
	require.False(t, state.nextNotificationImmediate)

	client.releaseFirstCall()
	client.waitForCalls(t, 1)

	stopFallbackTimerForTest(t, fallback)
	fallback.sendBatch()

	client.waitForCalls(t, 1)
	calls := client.callsSnapshot()
	require.Len(t, calls, 2)
	require.Contains(t, calls[0].text, "first batch")
	require.Contains(t, calls[1].text, "second batch")
}

// timer callbackなどからsendBatchが重なっても、同じfallback loggerの
// PostMessageは並行実行せず、先行送信の完了後に次のbatchを送ります。
func TestFallbackLoggerSerializesConcurrentBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := fallbackSlackTestConfig([]time.Duration{
			time.Hour,
			time.Hour,
		})
		fallback := newFallbackLoggerForTest(t, cfg)
		client := newFallbackBlockingHTTPClient()
		// cleanupは登録と逆順に実行するため、失敗時も先に一件目の送信を解放してから
		// fallback loggerのtimer callbackを待ちます。
		t.Cleanup(client.releaseFirstCall)
		fallback.slackAPI = slackgo.New(
			cfg.Token,
			slackgo.OptionHTTPClient(client),
		)

		fallback.enqueue(fallbackSlackTestEntry(
			pkglogger.LogLevelError,
			"first batch",
		))
		client.waitForFirstCall(t)

		fallback.enqueue(fallbackSlackTestEntry(
			pkglogger.LogLevelError,
			"second batch",
		))
		stopFallbackTimerForTest(t, fallback)

		secondDone := make(chan struct{})
		go func() {
			fallback.sendBatch()
			close(secondDone)
		}()

		// 送信lockの待機はbubble内で停止状態として扱われるため、二つ目のbatchが
		// 進める所まで進んだ後に、送信を始めていないことを確認できます。
		synctest.Wait()
		select {
		case <-secondDone:
			t.Fatal("second batch completed while the first request was still active")
		default:
		}
		require.Len(
			t,
			client.callsSnapshot(),
			1,
			"second Slack request must wait until the first request completes",
		)

		client.releaseFirstCall()
		client.waitForCalls(t, 2)
		select {
		case <-secondDone:
		case <-time.After(slackHangTimeout):
			t.Fatal("second batch did not complete after the first request was released")
		}

		require.False(t, client.overlappedRequests(), "Slack requests must not overlap")
		calls := client.callsSnapshot()
		require.Len(t, calls, 2)
		require.Contains(t, calls[0].text, "first batch")
		require.Contains(t, calls[1].text, "second batch")
	})
}

// fallbackの送信失敗は、通知の再帰を避けるためhandlerへ通知せず、他の通知先のfallback loggerと
// 同じく、失敗したSlack API呼び出しごとに標準loggerへ一行残します。成功した送信は残しません。
func TestFallbackLoggerLogsEachFailedPostWithoutInvokingInternalHandler(t *testing.T) {
	standardLog := captureSlackStandardLog(t)
	cfg := fallbackSlackTestConfig([]time.Duration{time.Hour})
	fallback := newFallbackLoggerForTest(t, cfg)
	fallback.slackAPI = slackgo.New(
		cfg.Token,
		slackgo.OptionHTTPClient(fallbackFailureHTTPClient{}),
	)

	var handlerCalls atomic.Int32
	fallback.OnError = func(
		context.Context,
		error,
		*logging.LogEntry,
		observer.Tracer,
		observer.Meter,
	) {
		handlerCalls.Add(1)
	}
	// levelごとのchannelへ、二回のSlack API呼び出しで送ります。
	fallback.logs = []*logging.LogEntry{
		fallbackSlackTestEntry(pkglogger.LogLevelError, "post failure"),
		fallbackSlackTestEntry(pkglogger.LogLevelWarn, "post failure on another channel"),
	}

	fallback.sendBatch()

	require.Zero(t, handlerCalls.Load())
	const failureLine = "failed to flush Slack fallback logger"
	require.Equal(t, 2, standardLog.count(failureLine), "standard logger output:\n%s", standardLog)

	client := newFallbackRecordingHTTPClient()
	fallback.slackAPI = slackgo.New(cfg.Token, slackgo.OptionHTTPClient(client))
	fallback.logs = []*logging.LogEntry{fallbackSlackTestEntry(pkglogger.LogLevelError, "post success")}
	fallback.sendBatch()
	client.waitForCalls(t, 1)
	require.Equal(t, 2, standardLog.count(failureLine), "a successful post must not be logged:\n%s", standardLog)
}

// slackStandardLogは、試験の間だけ標準loggerの出力を記録します。標準loggerはprocess全体で
// 共有されるため、t.Parallelの試験では使いません。
type slackStandardLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func captureSlackStandardLog(t *testing.T) *slackStandardLog {
	t.Helper()
	capture := &slackStandardLog{}
	previous := log.Writer()
	log.SetOutput(capture)
	t.Cleanup(func() { log.SetOutput(previous) })
	return capture
}

func (l *slackStandardLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// countは、substrが出力に現れる回数を返します。
func (l *slackStandardLog) count(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Count(l.buf.String(), substr)
}

func (l *slackStandardLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func fallbackSlackTestConfig(intervals []time.Duration) pkglogger.SlackLoggerConfig {
	return pkglogger.SlackLoggerConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: false,
			TimeZone:           "UTC",
		},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "fallback-slack-test",
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		Timeout:                time.Second,
		Token:                  "dummy-token",
		Channel:                "normal-channel",
		ErrorChannel:           "error-channel",
		WarnChannel:            "warn-channel",
		InfoChannel:            "info-channel",
		DebugChannel:           "debug-channel",
		MaxLogsPerNotification: 10,
		MaxRestThreshold:       time.Hour,
		Intervals:              intervals,
		MaxMessageLength:       10_000,
	}
}

func deliveryFallbackSlackTestConfig() pkglogger.DeliverySlackLoggerConfig {
	return pkglogger.DeliverySlackLoggerConfig{
		TimeConfig: pkglogger.TimeConfig{
			TimeZone: "UTC",
		},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "delivery-fallback-slack-test",
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		Timeout:          time.Second,
		Token:            "dummy-token",
		Channel:          "normal-channel",
		ErrorChannel:     "error-channel",
		WarnChannel:      "warn-channel",
		InfoChannel:      "info-channel",
		DebugChannel:     "debug-channel",
		MaxMessageLength: 10_000,
	}
}

func newFallbackLoggerForTest(
	t *testing.T,
	cfg pkglogger.SlackLoggerConfig,
) *FallbackLogger {
	t.Helper()

	fallback, err := newHandleSlackLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		// 試験が送信を確認しなかったentryは送らずに捨て、productionの終了処理（Close）で
		// 予約済みのtimerを止め、実行中のcallbackの完了を待ちます。送信を止めるfakeの解放は、
		// この後に登録したcleanupが先に行います。Closeが戻らない場合もhangせず失敗にします。
		fallback.mu.Lock()
		clear(fallback.logs)
		fallback.logs = fallback.logs[:0]
		fallback.mu.Unlock()
		closed := make(chan error, 1)
		go func() { closed <- fallback.Close() }()
		select {
		case err := <-closed:
			if err != nil {
				t.Errorf("FallbackLogger.Close() error = %v", err)
			}
		case <-time.After(slackHangTimeout):
			t.Errorf("FallbackLogger.Close() did not return within %s", slackHangTimeout)
		}
	})
	return fallback
}

// fallbackTestStateは、試験がFallbackLoggerの状態をmuの外で確認するためのcopyです。
// muを保持したままassertionが失敗するとmuが解放されず、後片付けのCloseが待ち続けるため、
// 状態はsnapshotFallbackStateでcopyしてから確認します。
type fallbackTestState struct {
	messages                  []string
	timerScheduled            bool
	intervalIndex             int
	nextNotificationImmediate bool
}

func snapshotFallbackState(fallback *FallbackLogger) fallbackTestState {
	fallback.mu.Lock()
	defer fallback.mu.Unlock()
	state := fallbackTestState{
		timerScheduled:            fallback.timer != nil,
		intervalIndex:             fallback.intervalIndex,
		nextNotificationImmediate: fallback.nextNotificationImmediate,
	}
	for _, entry := range fallback.logs {
		state.messages = append(state.messages, entry.Message)
	}
	return state
}

// stopFallbackTimerForTestは、予約済みの次回送信をproductionと同じ停止処理
// （stopBatchTimerLocked）で取り消します。試験の間に発火しない長さの予約が残っている
// 状態で呼びます。
func stopFallbackTimerForTest(t *testing.T, fallback *FallbackLogger) {
	t.Helper()

	fallback.mu.Lock()
	defer fallback.mu.Unlock()
	require.NotNil(t, fallback.timer, "a scheduled batch must remain to be stopped")
	fallback.stopBatchTimerLocked()
}

func fallbackSlackTestEntry(
	level pkglogger.LogLevel,
	message string,
) *logging.LogEntry {
	return &logging.LogEntry{
		Level:   level,
		Message: message,
	}
}

type fallbackSlackPost struct {
	channel string
	text    string
}

type fallbackRecordingHTTPClient struct {
	mu     sync.Mutex
	calls  []fallbackSlackPost
	called chan struct{}
}

func newFallbackRecordingHTTPClient() *fallbackRecordingHTTPClient {
	return &fallbackRecordingHTTPClient{called: make(chan struct{}, 16)}
}

func (c *fallbackRecordingHTTPClient) Do(
	req *http.Request,
) (*http.Response, error) {
	call, err := fallbackSlackPostFromRequest(req)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.calls = append(c.calls, call)
	c.mu.Unlock()
	c.called <- struct{}{}
	return fallbackSlackSuccessResponse(req, call.channel), nil
}

func (c *fallbackRecordingHTTPClient) waitForCalls(t *testing.T, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		select {
		case <-c.called:
		case <-time.After(slackHangTimeout):
			t.Fatalf("timed out waiting for Slack call %d", i+1)
		}
	}
}

func (c *fallbackRecordingHTTPClient) callsSnapshot() []fallbackSlackPost {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]fallbackSlackPost(nil), c.calls...)
}

type fallbackBlockingHTTPClient struct {
	*fallbackRecordingHTTPClient
	firstStarted chan struct{}
	releaseFirst chan struct{}
	releaseOnce  sync.Once

	// 以下はfallbackRecordingHTTPClient.muで保護します。
	inFlight int
	// overlappedは、別のrequestの送信中に次のrequestが開始されたことを表します。
	overlapped bool
}

func newFallbackBlockingHTTPClient() *fallbackBlockingHTTPClient {
	return &fallbackBlockingHTTPClient{
		fallbackRecordingHTTPClient: newFallbackRecordingHTTPClient(),
		firstStarted:                make(chan struct{}),
		releaseFirst:                make(chan struct{}),
	}
}

func (c *fallbackBlockingHTTPClient) Do(
	req *http.Request,
) (*http.Response, error) {
	call, err := fallbackSlackPostFromRequest(req)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	callIndex := len(c.calls)
	c.calls = append(c.calls, call)
	if c.inFlight > 0 {
		c.overlapped = true
	}
	c.inFlight++
	c.mu.Unlock()
	if callIndex == 0 {
		close(c.firstStarted)
		<-c.releaseFirst
	}
	c.mu.Lock()
	c.inFlight--
	c.mu.Unlock()
	c.called <- struct{}{}
	return fallbackSlackSuccessResponse(req, call.channel), nil
}

// overlappedRequestsは、送信中に別のrequestが開始されたかを返します。
func (c *fallbackBlockingHTTPClient) overlappedRequests() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.overlapped
}

func (c *fallbackBlockingHTTPClient) waitForFirstCall(t *testing.T) {
	t.Helper()

	select {
	case <-c.firstStarted:
	case <-time.After(slackHangTimeout):
		t.Fatal("timed out waiting for the first Slack call")
	}
}

func (c *fallbackBlockingHTTPClient) releaseFirstCall() {
	c.releaseOnce.Do(func() {
		close(c.releaseFirst)
	})
}

type fallbackFailureHTTPClient struct{}

func (fallbackFailureHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Status:     http.StatusText(http.StatusServiceUnavailable),
		Header:     make(http.Header),
		Body: io.NopCloser(strings.NewReader(
			`{"ok":false,"error":"service_unavailable"}`,
		)),
		Request: req,
	}, nil
}

func fallbackSlackPostFromRequest(
	req *http.Request,
) (fallbackSlackPost, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return fallbackSlackPost{}, err
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return fallbackSlackPost{}, err
	}
	return fallbackSlackPost{
		channel: values.Get("channel"),
		text:    values.Get("text"),
	}, nil
}

func fallbackSlackSuccessResponse(
	req *http.Request,
	channel string,
) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     http.StatusText(http.StatusOK),
		Header:     make(http.Header),
		Body: io.NopCloser(strings.NewReader(fmt.Sprintf(
			`{"ok":true,"channel":%q,"ts":"test-timestamp"}`,
			channel,
		))),
		Request: req,
	}
}

func fallbackCallChannels(calls []fallbackSlackPost) []string {
	channels := make([]string, 0, len(calls))
	for _, call := range calls {
		channels = append(channels, call.channel)
	}
	return channels
}
