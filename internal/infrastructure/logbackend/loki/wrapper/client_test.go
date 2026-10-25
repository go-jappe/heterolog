package wrapper

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/pkg/loki"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type traceSpanKey struct{}

type recordingLokiClient struct {
	push      func(context.Context, []loki.Stream) error
	pushCalls []pushCall
}

type pushCall struct {
	ctx     context.Context
	streams []loki.Stream
}

func (c *recordingLokiClient) Push(ctx context.Context, streams []loki.Stream) error {
	c.pushCalls = append(c.pushCalls, pushCall{ctx: ctx, streams: append([]loki.Stream(nil), streams...)})
	if c.push == nil {
		return errors.New("unexpected Push call")
	}
	return c.push(ctx, streams)
}

type recordingTracer struct {
	spans []*recordingSpan
}

func (t *recordingTracer) StartSpan(ctx context.Context, name string, attrs ...observer.Attr) (context.Context, observer.Span) {
	span := &recordingSpan{name: name, attrs: append([]observer.Attr(nil), attrs...)}
	t.spans = append(t.spans, span)
	ctx = context.WithValue(ctx, traceSpanKey{}, span)
	ctx = observer.WithSpan(ctx, span)
	return ctx, span
}

// markは、これまでに開始したspanの数を返します。spansSinceと組み合わせ、
// 一回の呼び出しで開始したspanだけを、位置や総数に依存せずに調べます。
func (t *recordingTracer) mark() int { return len(t.spans) }

// spansSinceは、markの後に開始したspanを返します。
func (t *recordingTracer) spansSince(mark int) []*recordingSpan { return t.spans[mark:] }

type recordingSpan struct {
	name   string
	attrs  []observer.Attr
	events []recordedEvent
	ended  bool
}

type recordedEvent struct {
	name  string
	attrs []observer.Attr
}

func (s *recordingSpan) TraceID() observer.TraceID {
	return ""
}

func (s *recordingSpan) End() {
	s.ended = true
}

func (s *recordingSpan) AddEvent(name string, attrs ...observer.Attr) {
	s.events = append(s.events, recordedEvent{name: name, attrs: append([]observer.Attr(nil), attrs...)})
}

func (s *recordingSpan) SetAttributes(attrs ...observer.Attr) {
	s.attrs = append(s.attrs, attrs...)
}

func (s *recordingSpan) RecordError(err error, attrs ...observer.Attr) {}

type staticRetryConfig struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
}

func (c staticRetryConfig) MaxAttempts() int {
	return c.maxAttempts
}

func (c staticRetryConfig) BaseDelay() time.Duration {
	return c.baseDelay
}

func (c staticRetryConfig) MaxDelay() time.Duration {
	return c.maxDelay
}

func TestNewRetryingClientKeepsValidatedConfigAndDefaultsTracer(t *testing.T) {
	t.Parallel()

	client := newRetryingClient(&recordingLokiClient{}, staticRetryConfig{
		maxAttempts: 2,
		baseDelay:   3 * time.Millisecond,
		maxDelay:    5 * time.Millisecond,
	}, nil)

	assert.Equal(t, 2, client.maxAttempts())
	assert.Equal(t, 3*time.Millisecond, client.baseDelay())
	assert.Equal(t, 5*time.Millisecond, client.maxDelay())
	assert.NotNil(t, client.tracer)
}

func TestNewRetryingClientAcceptsCircuitBreakerClientChild(t *testing.T) {
	t.Parallel()

	circuitClient := newCircuitBreakerClient(&recordingLokiClient{}, nil, nil)

	retrying := newRetryingClient(circuitClient, staticRetryConfig{}, nil)

	assert.Same(t, circuitClient, retrying.next)
}

// backoff はランダムに揺らさず、再試行ごとに基準遅延ぶんずつ長くする。
func TestBackoffIncreasesLinearlyAndStopsAtMaxDelay(t *testing.T) {
	t.Parallel()

	client := newRetryingClient(
		&recordingLokiClient{},
		staticRetryConfig{
			maxAttempts: 5,
			baseDelay:   100 * time.Millisecond,
			maxDelay:    250 * time.Millisecond,
		},
		nil,
	)

	assert.Equal(t, 100*time.Millisecond, client.backoff(1))
	assert.Equal(t, 200*time.Millisecond, client.backoff(2))
	assert.Equal(t, 250*time.Millisecond, client.backoff(3))
	assert.Equal(t, 250*time.Millisecond, client.backoff(4))
}

// Loki の一時的な HTTP status error ではリトライし、span context と retry event を残す。
func TestPushRetriesHTTPStatusErrorAndReturnsNil(t *testing.T) {
	t.Parallel()

	tracer := &recordingTracer{}
	streams := []loki.Stream{{
		Labels:  map[string]string{"app": "service"},
		Entries: []loki.Entry{{Timestamp: time.Unix(1_700_000_000, 0), Line: "hello"}},
	}}
	client := &recordingLokiClient{}
	client.push = func(ctx context.Context, got []loki.Stream) error {
		assert.Equal(t, streams, got)
		if len(client.pushCalls) == 1 {
			return &loki.HTTPStatusError{Code: http.StatusServiceUnavailable, Status: "503 Service Unavailable"}
		}
		return nil
	}
	retrying := newRetryingClient(client, fastRetryConfig(2), tracer)

	err := retrying.Push(context.Background(), streams)

	require.NoError(t, err)
	require.Len(t, client.pushCalls, 2)
	failed, retried := spanFromContext(t, client.pushCalls[0].ctx), spanFromContext(t, client.pushCalls[1].ctx)
	assert.NotSame(t, failed, retried, "each attempt runs in its own span")
	assert.Equal(t, SpanNamePush, failed.name)
	assert.Equal(t, SpanNamePush, retried.name)
	requireSpansEnded(t, tracer.spans)
	require.Len(t, failed.events, 1)
	assertRetryEvent(t, failed.events[0], retryable.RetryReasonHTTPStatus, 1, http.StatusServiceUnavailable)
	assert.Empty(t, retried.events)
}

func TestPushDoesNotImmediatelyRetryLokiIngestionBlocked(t *testing.T) {
	t.Parallel()

	tracer := &recordingTracer{}
	streams := []loki.Stream{{
		Labels:  map[string]string{"app": "service"},
		Entries: []loki.Entry{{Timestamp: time.Unix(1_700_000_000, 0), Line: "hello"}},
	}}
	client := &recordingLokiClient{}
	client.push = func(ctx context.Context, got []loki.Stream) error {
		assert.Equal(t, streams, got)
		return &loki.HTTPStatusError{Code: 260, Status: "260 Ingestion Blocked"}
	}
	retrying := newRetryingClient(client, fastRetryConfig(3), tracer)

	err := retrying.Push(context.Background(), streams)

	require.Error(t, err)
	require.Len(t, client.pushCalls, 1)
	attempt := spanFromContext(t, client.pushCalls[0].ctx)
	assert.Equal(t, SpanNamePush, attempt.name)
	requireSpansEnded(t, tracer.spans)
	assert.Empty(t, eventsWith(tracer.spans), "a failure without retry records no retry decision")
}

func TestRetryDelayUsesRetryAfterOnlyForThrottleAndServiceUnavailable(t *testing.T) {
	t.Parallel()

	client := newRetryingClient(
		&recordingLokiClient{},
		staticRetryConfig{
			maxAttempts: 3,
			baseDelay:   100 * time.Millisecond,
			maxDelay:    250 * time.Millisecond,
		},
		nil,
	)

	// 2回目の通常backoff(200ms)とも上限(250ms)とも異なる値にし、Retry-Afterを
	// 無視してbackoffを返す実装を検出します。
	assert.Equal(t, 150*time.Millisecond, client.retryDelay(2, retryable.RetryDecision{
		Retry:      true,
		Reason:     retryable.RetryReasonHTTPThrottle,
		Code:       http.StatusTooManyRequests,
		RetryAfter: 150 * time.Millisecond,
	}))
	assert.Equal(t, 230*time.Millisecond, client.retryDelay(2, retryable.RetryDecision{
		Retry:      true,
		Reason:     retryable.RetryReasonHTTPThrottle,
		Code:       http.StatusTooManyRequests,
		RetryAfter: 230 * time.Millisecond,
	}))
	assert.Equal(t, 250*time.Millisecond, client.retryDelay(1, retryable.RetryDecision{
		Retry:      true,
		Reason:     retryable.RetryReasonHTTPStatus,
		Code:       http.StatusServiceUnavailable,
		RetryAfter: time.Second,
	}))
	assert.Equal(t, 100*time.Millisecond, client.retryDelay(1, retryable.RetryDecision{
		Retry:      true,
		Reason:     retryable.RetryReasonHTTPStatus,
		Code:       http.StatusBadGateway,
		RetryAfter: 200 * time.Millisecond,
	}))
}

// 最大試行回数まで retryable error が続く場合は、最後の error を wrap して返す。
func TestPushWrapsLastErrorWhenAttemptsExhausted(t *testing.T) {
	t.Parallel()

	client := &recordingLokiClient{}
	client.push = func(ctx context.Context, streams []loki.Stream) error {
		return io.EOF
	}
	tracer := &recordingTracer{}
	retrying := newRetryingClient(client, fastRetryConfig(2), tracer)

	err := retrying.Push(context.Background(), []loki.Stream{{Labels: map[string]string{"app": "service"}, Entries: []loki.Entry{{Line: "hello"}}}})

	require.ErrorIs(t, err, io.EOF)
	var exhausted *retryable.RetryAttemptsExhaustedError
	require.ErrorAs(t, err, &exhausted)
	assert.False(t, exhausted.Policy)
	assert.Equal(t, 2, exhausted.Attempts)
	assert.ErrorIs(t, exhausted.LastErr, io.EOF)
	assert.Len(t, client.pushCalls, 2)
	require.Len(t, spanFromContext(t, client.pushCalls[0].ctx).events, 1, "the first failure schedules the retry")
	assert.Empty(t, spanFromContext(t, client.pushCalls[1].ctx).events, "the last failure schedules no further retry")
	requireSpansEnded(t, tracer.spans)
}

// backoff 中に呼び出し元 context が終わった場合は、追加送信せず context error を優先する。
func TestPushStopsWhenContextCanceledDuringBackoff(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		client := &recordingLokiClient{}
		client.push = func(ctx context.Context, streams []loki.Stream) error {
			return io.EOF
		}
		tracer := &recordingTracer{}
		retrying := newRetryingClient(
			client,
			staticRetryConfig{
				maxAttempts: 3,
				baseDelay:   time.Hour,
				maxDelay:    time.Hour,
			},
			tracer,
		)
		done := make(chan error, 1)
		go func() {
			done <- retrying.Push(ctx, []loki.Stream{{Labels: map[string]string{"app": "service"}, Entries: []loki.Entry{{Line: "hello"}}}})
		}()

		// 一回目の失敗後のbackoffで待機してから、呼び出し元が停止します。
		synctest.Wait()
		cancel()
		err := <-done

		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, client.pushCalls, 1)
		require.Len(t, spanFromContext(t, client.pushCalls[0].ctx).events, 1, "the failure that started the backoff is recorded")
		requireSpansEnded(t, tracer.spans)
	})
}

func TestRetryPolicyClientTimeoutRetriesOnceWithShortAttemptTimeout(t *testing.T) {
	t.Parallel()

	tracer := &recordingTracer{}
	streams := []loki.Stream{{
		Labels:  map[string]string{"app": "service"},
		Entries: []loki.Entry{{Timestamp: time.Unix(1_700_000_000, 0), Line: "hello"}},
	}}
	client := &recordingLokiClient{}
	client.push = func(ctx context.Context, got []loki.Stream) error {
		assert.Equal(t, streams, got)
		if len(client.pushCalls) == 1 {
			return &net.DNSError{IsTimeout: true}
		}
		assertPolicyRetryTimeout(t, ctx)
		return nil
	}
	retrying := newRetryPolicyClient(client, fastRetryConfig(3), tracer)

	err := retrying.Push(context.Background(), streams)

	require.NoError(t, err)
	require.Len(t, client.pushCalls, 2)
	failed := spanFromContext(t, client.pushCalls[0].ctx)
	require.Len(t, failed.events, 1)
	assert.Equal(t, true, attrValue(failed.events[0].attrs, "retry_policy_client"))
}

func TestRetryPolicyClientCircuitBreakerSuppressesRetryAfterRepeatedFailure(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	tracer := &recordingTracer{}
	streams := []loki.Stream{{
		Labels:  map[string]string{"app": "service"},
		Entries: []loki.Entry{{Timestamp: now, Line: "hello"}},
	}}
	client := &recordingLokiClient{}
	client.push = func(context.Context, []loki.Stream) error {
		return &net.DNSError{IsTimeout: true}
	}
	circuitClient := newCircuitBreakerClientWithCircuitBreaker(
		client,
		tracer,
		observer.NopMeter{},
		circuitBreaker,
	)
	retrying := newRetryPolicyClient(circuitClient, fastCircuitBreakerConfig(3), tracer)

	err := retrying.Push(context.Background(), streams)
	require.Error(t, err)
	assert.Len(t, client.pushCalls, 2)

	now = now.Add(time.Second)
	opening := tracer.mark()
	err = retrying.Push(context.Background(), streams)
	require.Error(t, err)
	assert.Len(t, client.pushCalls, 3)
	// breakerを開いた失敗はsuppressedとして記録し、その後の再試行はbackendへ送らない。
	opened := eventsWith(
		tracer.spansSince(opening),
		observer.Attr{Key: "circuit_breaker_client", Value: true},
		observer.Attr{Key: "circuit_breaker_suppressed", Value: true},
		observer.Attr{Key: "reason", Value: string(retryable.RetryReasonNetTimeout)},
	)
	require.Len(t, opened, 1)
	assert.Equal(t, false, attrValue(opened[0].attrs, "retry"))
	require.Len(t, eventsWith(
		tracer.spansSince(opening),
		observer.Attr{Key: "reason", Value: string(retryable.RetryReasonCircuitBreakerOpen)},
	), 1, "the retry after opening is rejected by the breaker")

	suppressed := tracer.mark()
	err = retrying.Push(context.Background(), streams)
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.pushCalls, 3)
	require.Len(t, eventsWith(
		tracer.spansSince(suppressed),
		observer.Attr{Key: "reason", Value: string(retryable.RetryReasonCircuitBreakerOpen)},
	), 1)
	requireSpansEnded(t, tracer.spans)
}

func TestRetryPolicyClientCircuitBreakerLifecycleIntegration(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	tracer := &recordingTracer{}
	streams := []loki.Stream{{
		Labels:  map[string]string{"app": "service"},
		Entries: []loki.Entry{{Timestamp: now, Line: "hello"}},
	}}
	client := &recordingLokiClient{}
	client.push = func(context.Context, []loki.Stream) error {
		if len(client.pushCalls) <= 3 {
			return &net.DNSError{IsTimeout: true}
		}
		return nil
	}
	circuitClient := newCircuitBreakerClientWithCircuitBreaker(
		client,
		tracer,
		observer.NopMeter{},
		circuitBreaker,
	)
	retrying := newRetryPolicyClient(circuitClient, fastCircuitBreakerConfig(3), tracer)

	err := retrying.Push(context.Background(), streams)
	require.Error(t, err)
	assert.Len(t, client.pushCalls, 2)

	now = now.Add(time.Second)
	err = retrying.Push(context.Background(), streams)
	require.Error(t, err)
	assert.Len(t, client.pushCalls, 3)

	err = retrying.Push(context.Background(), streams)
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.pushCalls, 3)

	now = circuitBreakerErr.OpenUntil.Add(time.Nanosecond)
	err = retrying.Push(context.Background(), streams)
	require.NoError(t, err)
	assert.Len(t, client.pushCalls, 4)

	err = retrying.Push(context.Background(), streams)
	require.NoError(t, err)
	assert.Len(t, client.pushCalls, 5)
}

func TestCircuitBreakerClientSuppressesBackendRequestAfterRepeatedFailure(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	tracer := &recordingTracer{}
	streams := []loki.Stream{{Labels: map[string]string{"app": "test"}, Entries: []loki.Entry{{Timestamp: now, Line: "line"}}}}
	client := &recordingLokiClient{}
	client.push = func(context.Context, []loki.Stream) error {
		return &net.DNSError{IsTimeout: true}
	}
	circuitClient := newCircuitBreakerClientWithCircuitBreaker(client, tracer, observer.NopMeter{}, circuitBreaker)

	err := circuitClient.Push(context.Background(), streams)
	require.Error(t, err)
	assert.Len(t, client.pushCalls, 1)

	now = now.Add(time.Second)
	err = circuitClient.Push(context.Background(), streams)
	require.Error(t, err)
	assert.Len(t, client.pushCalls, 2)
	opening := spanFromContext(t, client.pushCalls[1].ctx)
	require.Len(t, opening.events, 1)
	assert.Equal(t, true, attrValue(opening.events[0].attrs, "circuit_breaker_client"))
	assert.Equal(t, true, attrValue(opening.events[0].attrs, "circuit_breaker_suppressed"))

	suppressed := tracer.mark()
	err = circuitClient.Push(context.Background(), streams)
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.pushCalls, 2)
	require.Len(t, eventsWith(
		tracer.spansSince(suppressed),
		observer.Attr{Key: "reason", Value: string(retryable.RetryReasonCircuitBreakerOpen)},
	), 1)
	requireSpansEnded(t, tracer.spans)
}

func TestCircuitBreakerClientDoesNotCountIngestionBlockedAsImmediateFailure(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Nanosecond,
		MaxFailureIntervalForConsecutive: time.Minute,
		MinConsecutiveFailuresToOpen:     1,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	streams := []loki.Stream{{
		Labels:  map[string]string{"app": "test"},
		Entries: []loki.Entry{{Timestamp: now, Line: "line"}},
	}}
	client := &recordingLokiClient{}
	client.push = func(context.Context, []loki.Stream) error {
		return &loki.HTTPStatusError{Code: 260, Status: "260 Ingestion Blocked"}
	}
	circuitClient := newCircuitBreakerClientWithCircuitBreaker(
		client,
		nil,
		observer.NopMeter{},
		circuitBreaker,
	)

	for attempt := 0; attempt < 3; attempt++ {
		err := circuitClient.Push(context.Background(), streams)
		var statusErr *loki.HTTPStatusError
		require.ErrorAs(t, err, &statusErr)
	}

	assert.Len(t, client.pushCalls, 3)
}

func fastRetryConfig(maxAttempts int) retryBackoffConfigProvider {
	return staticRetryConfig{
		maxAttempts: maxAttempts,
		baseDelay:   time.Nanosecond,
		maxDelay:    time.Nanosecond,
	}
}

func fastCircuitBreakerConfig(maxAttempts int) retryBackoffConfigProvider {
	return staticRetryConfig{
		maxAttempts: maxAttempts,
		baseDelay:   time.Nanosecond,
		maxDelay:    time.Nanosecond,
	}
}

func assertPolicyRetryTimeout(t *testing.T, ctx context.Context) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	require.True(t, ok, "retry_policy_with_breaker timeout retry attempt should have a deadline")
	remaining := time.Until(deadline)
	assert.Positive(t, remaining)
	assert.LessOrEqual(t, remaining, retryable.DefaultPolicyTimeoutRetryAttemptTimeout)
}

func assertRetryEvent(t *testing.T, event recordedEvent, wantReason retryable.RetryReason, wantAttempt, wantStatus int) {
	t.Helper()

	assert.Equal(t, "loki.retry_decision", event.name)
	assert.Equal(t, string(wantReason), attrValue(event.attrs, "reason"))
	assert.Equal(t, wantAttempt, attrValue(event.attrs, "attempt"))
	assert.Equal(t, true, attrValue(event.attrs, "retry"))
	assert.Equal(t, wantStatus, attrValue(event.attrs, "http_status"))
	assert.NotEmpty(t, attrValue(event.attrs, "error"))
}

func attrValue(attrs []observer.Attr, key string) any {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value
		}
	}
	return nil
}

// spanFromContextは、backendへ渡されたcontextを開始したspanを返します。
func spanFromContext(t *testing.T, ctx context.Context) *recordingSpan {
	t.Helper()

	span, ok := ctx.Value(traceSpanKey{}).(*recordingSpan)
	require.True(t, ok, "backend call context was not started by the recording tracer")
	return span
}

// eventsWithは、spansに記録されたeventのうち、wantの属性をすべて持つものを返します。
func eventsWith(spans []*recordingSpan, want ...observer.Attr) []recordedEvent {
	var matched []recordedEvent
	for _, span := range spans {
	events:
		for _, event := range span.events {
			for _, attr := range want {
				if attrValue(event.attrs, attr.Key) != attr.Value {
					continue events
				}
			}
			matched = append(matched, event)
		}
	}
	return matched
}

// requireSpansEndedは、spansがすべて終了したことを確認します。
func requireSpansEnded(t *testing.T, spans []*recordingSpan) {
	t.Helper()

	for _, span := range spans {
		require.True(t, span.ended, "span %q was not ended", span.name)
	}
}
