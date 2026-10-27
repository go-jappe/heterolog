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
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type traceSpanKey struct{}

type recordingClient struct {
	send  func(context.Context, cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error)
	calls []sendCall
}

type sendCall struct {
	ctx     context.Context
	request cloudwatchlogs.PutLogEventsRequest
}

func (c *recordingClient) PutLogEvents(
	ctx context.Context,
	request cloudwatchlogs.PutLogEventsRequest,
) (*cloudwatchlogs.PutLogEventsOutput, error) {
	recordedRequest := request
	recordedRequest.Events = append([]cloudwatchlogs.Event(nil), request.Events...)
	c.calls = append(c.calls, sendCall{ctx: ctx, request: recordedRequest})
	if c.send == nil {
		return nil, errors.New("unexpected Send call")
	}
	return c.send(ctx, request)
}

func testPutLogEventsRequest(events []cloudwatchlogs.Event) cloudwatchlogs.PutLogEventsRequest {
	return cloudwatchlogs.PutLogEventsRequest{
		LogStreamName: "stream/2026/08/14",
		Events:        events,
		AttemptedAt:   time.Date(2026, time.August, 14, 12, 34, 56, 0, time.UTC),
	}
}

type recordingTracer struct {
	spans []*recordingSpan
}

func (t *recordingTracer) StartSpan(ctx context.Context, name string, attrs ...pkgobserver.Attr) (context.Context, pkgobserver.Span) {
	span := &recordingSpan{name: name, attrs: append([]pkgobserver.Attr(nil), attrs...)}
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
	attrs  []pkgobserver.Attr
	events []recordedEvent
	ended  bool
}

type recordedEvent struct {
	name  string
	attrs []pkgobserver.Attr
}

func (s *recordingSpan) TraceID() pkgobserver.TraceID {
	return ""
}

func (s *recordingSpan) End() {
	s.ended = true
}

func (s *recordingSpan) AddEvent(name string, attrs ...pkgobserver.Attr) {
	s.events = append(s.events, recordedEvent{name: name, attrs: append([]pkgobserver.Attr(nil), attrs...)})
}

func (s *recordingSpan) SetAttributes(attrs ...pkgobserver.Attr) {
	s.attrs = append(s.attrs, attrs...)
}

func (s *recordingSpan) RecordError(err error, attrs ...pkgobserver.Attr) {}

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

	client := newRetryingClient(&recordingClient{}, staticRetryConfig{
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

	circuitClient := newCircuitBreakerClient(&recordingClient{}, nil, nil)

	retrying := newRetryingClient(circuitClient, staticRetryConfig{}, nil)

	assert.Same(t, circuitClient, retrying.next)
}

func TestBackoffIncreasesLinearlyAndStopsAtMaxDelay(t *testing.T) {
	t.Parallel()

	client := newRetryingClient(
		&recordingClient{},
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

func TestSendRetriesCloudWatchThrottleErrorType(t *testing.T) {
	t.Parallel()

	tracer := &recordingTracer{}
	wantOutput := &cloudwatchlogs.PutLogEventsOutput{NextSequenceToken: "token-2"}
	request := testPutLogEventsRequest([]cloudwatchlogs.Event{{Timestamp: time.Unix(10, 0), Message: "retry me"}})
	client := &recordingClient{}
	client.send = func(ctx context.Context, got cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error) {
		assert.Equal(t, request, got)
		if len(client.calls) == 1 {
			return nil, &cloudwatchlogs.HTTPStatusError{
				Code:         http.StatusBadRequest,
				Status:       "400 Bad Request",
				AwsErrorType: "ThrottlingException:http://internal.example/",
			}
		}
		return wantOutput, nil
	}
	retrying := newRetryingClient(client, fastRetryConfig(2), tracer)

	output, err := retrying.PutLogEvents(context.Background(), request)

	require.NoError(t, err)
	assert.Same(t, wantOutput, output)
	require.Len(t, client.calls, 2)
	failed, retried := spanFromContext(t, client.calls[0].ctx), spanFromContext(t, client.calls[1].ctx)
	assert.NotSame(t, failed, retried, "each attempt runs in its own span")
	assert.Equal(t, spanNamePutLogEvents, failed.name)
	assert.Equal(t, spanNamePutLogEvents, retried.name)
	requireSpansEnded(t, tracer.spans)
	require.Len(t, failed.events, 1)
	assertRetryEvent(t, failed.events[0], retryable.RetryReasonHTTPThrottle, 1, http.StatusBadRequest)
	assert.Empty(t, retried.events)
}

func TestRetryingClientPreservesCanonicalRequestAcrossAttempts(t *testing.T) {
	t.Parallel()

	request := cloudwatchlogs.PutLogEventsRequest{
		LogStreamName: "container/task-1/2026/08/13/10",
		Events: []cloudwatchlogs.Event{
			{Timestamp: time.Date(2026, time.August, 13, 10, 0, 0, 0, time.UTC), Message: "first"},
			{Timestamp: time.Date(2026, time.August, 13, 10, 0, 1, 0, time.UTC), Message: "second"},
		},
		AttemptedAt: time.Date(2026, time.August, 13, 23, 59, 59, 0, time.UTC),
	}
	client := &recordingClient{}
	client.send = func(
		context.Context,
		cloudwatchlogs.PutLogEventsRequest,
	) (*cloudwatchlogs.PutLogEventsOutput, error) {
		if len(client.calls) == 1 {
			return nil, &cloudwatchlogs.HTTPStatusError{
				Code:         http.StatusBadRequest,
				AwsErrorType: "ThrottlingException",
			}
		}
		return &cloudwatchlogs.PutLogEventsOutput{}, nil
	}
	retrying := newRetryingClient(client, fastRetryConfig(2), nil)

	_, err := retrying.PutLogEvents(context.Background(), request)

	require.NoError(t, err)
	require.Len(t, client.calls, 2)
	assert.Equal(t, request, client.calls[0].request)
	assert.Equal(t, request, client.calls[1].request)
}

func TestSendReturnsNonRetryableErrorWithoutRetry(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("invalid event")
	client := &recordingClient{}
	client.send = func(ctx context.Context, request cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error) {
		return nil, wantErr
	}
	tracer := &recordingTracer{}
	retrying := newRetryingClient(client, fastRetryConfig(3), tracer)

	output, err := retrying.PutLogEvents(context.Background(), testPutLogEventsRequest(
		[]cloudwatchlogs.Event{{Timestamp: time.Unix(10, 0), Message: "do not retry"}},
	))

	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, output)
	assert.Len(t, client.calls, 1)
	attempt := spanFromContext(t, client.calls[0].ctx)
	assert.Equal(t, spanNamePutLogEvents, attempt.name)
	requireSpansEnded(t, tracer.spans)
	assert.Empty(t, eventsWith(tracer.spans), "a failure without retry records no retry decision")
}

func TestSendWrapsLastErrorWhenAttemptsExhausted(t *testing.T) {
	t.Parallel()

	client := &recordingClient{}
	client.send = func(ctx context.Context, request cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error) {
		return nil, io.EOF
	}
	tracer := &recordingTracer{}
	retrying := newRetryingClient(client, fastRetryConfig(2), tracer)

	output, err := retrying.PutLogEvents(context.Background(), testPutLogEventsRequest(
		[]cloudwatchlogs.Event{{Timestamp: time.Unix(10, 0), Message: "retry me"}},
	))

	require.ErrorIs(t, err, io.EOF)
	var exhausted *retryable.RetryAttemptsExhaustedError
	require.ErrorAs(t, err, &exhausted)
	assert.False(t, exhausted.Policy)
	assert.Equal(t, 2, exhausted.Attempts)
	assert.ErrorIs(t, exhausted.LastErr, io.EOF)
	assert.Nil(t, output)
	assert.Len(t, client.calls, 2)
	require.Len(t, spanFromContext(t, client.calls[0].ctx).events, 1, "the first failure schedules the retry")
	assert.Empty(t, spanFromContext(t, client.calls[1].ctx).events, "the last failure schedules no further retry")
	requireSpansEnded(t, tracer.spans)
}

func TestSendStopsWhenContextCanceledDuringBackoff(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		client := &recordingClient{}
		client.send = func(ctx context.Context, request cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error) {
			return nil, io.EOF
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
		type result struct {
			output *cloudwatchlogs.PutLogEventsOutput
			err    error
		}
		done := make(chan result, 1)
		go func() {
			output, err := retrying.PutLogEvents(ctx, testPutLogEventsRequest(
				[]cloudwatchlogs.Event{{Timestamp: time.Unix(10, 0), Message: "cancel before retry"}},
			))
			done <- result{output: output, err: err}
		}()

		// 一回目の失敗後のbackoffで待機してから、呼び出し元が停止します。
		synctest.Wait()
		cancel()
		got := <-done

		require.ErrorIs(t, got.err, context.Canceled)
		assert.Nil(t, got.output)
		require.Len(t, client.calls, 1)
		require.Len(t, spanFromContext(t, client.calls[0].ctx).events, 1, "the failure that started the backoff is recorded")
		requireSpansEnded(t, tracer.spans)
	})
}

func TestRetryPolicyClientTimeoutRetriesOnceWithShortAttemptTimeout(t *testing.T) {
	t.Parallel()

	tracer := &recordingTracer{}
	wantOutput := &cloudwatchlogs.PutLogEventsOutput{NextSequenceToken: "token-2"}
	request := testPutLogEventsRequest([]cloudwatchlogs.Event{{Timestamp: time.Unix(10, 0), Message: "retry me"}})
	client := &recordingClient{}
	client.send = func(ctx context.Context, got cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error) {
		assert.Equal(t, request, got)
		if len(client.calls) == 1 {
			return nil, &net.DNSError{IsTimeout: true}
		}
		assertPolicyRetryTimeout(t, ctx)
		return wantOutput, nil
	}
	retrying := newRetryPolicyClient(client, fastRetryConfig(3), tracer)

	output, err := retrying.PutLogEvents(context.Background(), request)

	require.NoError(t, err)
	assert.Same(t, wantOutput, output)
	require.Len(t, client.calls, 2)
	assert.Equal(t, request, client.calls[0].request)
	assert.Equal(t, request, client.calls[1].request)
	failed := spanFromContext(t, client.calls[0].ctx)
	require.Len(t, failed.events, 1)
	assert.Equal(t, true, attrValue(failed.events[0].attrs, "retry_policy"))
}

func TestRetryPolicyClientRequestAbortedUsesSingleProbe(t *testing.T) {
	t.Parallel()

	client := &recordingClient{}
	client.send = func(
		context.Context,
		cloudwatchlogs.PutLogEventsRequest,
	) (*cloudwatchlogs.PutLogEventsOutput, error) {
		return nil, &cloudwatchlogs.HTTPStatusError{
			Code:         http.StatusBadRequest,
			Status:       "400 Bad Request",
			AwsErrorType: cloudwatchlogs.ErrorTypeRequestAbortedException,
		}
	}
	retrying := newRetryPolicyClient(client, fastRetryConfig(5), nil)

	_, err := retrying.PutLogEvents(context.Background(), testPutLogEventsRequest(
		[]cloudwatchlogs.Event{{Timestamp: time.Unix(10, 0), Message: "probe once"}},
	))

	var exhausted *retryable.RetryAttemptsExhaustedError
	require.ErrorAs(t, err, &exhausted)
	assert.True(t, exhausted.Policy)
	assert.Equal(t, retryable.DefaultPolicySingleProbeMaxAttempts, exhausted.Attempts)
	var aborted *cloudwatchlogs.HTTPStatusError
	require.ErrorAs(t, exhausted.LastErr, &aborted)
	assert.Equal(t, cloudwatchlogs.ErrorTypeRequestAbortedException, aborted.ErrorType())
	assert.Len(t, client.calls, retryable.DefaultPolicySingleProbeMaxAttempts)
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
	request := testPutLogEventsRequest([]cloudwatchlogs.Event{{Timestamp: now, Message: "hello"}})
	client := &recordingClient{}
	client.send = func(context.Context, cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error) {
		return nil, &net.DNSError{IsTimeout: true}
	}
	child := newCircuitBreakerClientWithCircuitBreaker(client, tracer, observer.NopMeter{}, circuitBreaker)
	retrying := newRetryPolicyClient(child, fastCircuitBreakerConfig(3), tracer)

	_, err := retrying.PutLogEvents(context.Background(), request)
	require.Error(t, err)
	assert.Len(t, client.calls, 2)

	now = now.Add(time.Second)
	opening := tracer.mark()
	_, err = retrying.PutLogEvents(context.Background(), request)
	require.Error(t, err)
	assert.Len(t, client.calls, 3)
	// breakerを開いた失敗はsuppressedとして記録し、その後の再試行はbackendへ送らない。
	opened := eventsWith(
		tracer.spansSince(opening),
		pkgobserver.Attr{Key: "circuit_breaker_client", Value: true},
		pkgobserver.Attr{Key: "circuit_breaker_suppressed", Value: true},
		pkgobserver.Attr{Key: "reason", Value: string(retryable.RetryReasonNetTimeout)},
	)
	require.Len(t, opened, 1)
	assert.Equal(t, false, attrValue(opened[0].attrs, "retry"))
	require.Len(t, eventsWith(
		tracer.spansSince(opening),
		pkgobserver.Attr{Key: "reason", Value: string(retryable.RetryReasonCircuitBreakerOpen)},
	), 1, "the retry after opening is rejected by the breaker")

	suppressed := tracer.mark()
	_, err = retrying.PutLogEvents(context.Background(), request)
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.calls, 3)
	require.Len(t, eventsWith(
		tracer.spansSince(suppressed),
		pkgobserver.Attr{Key: "reason", Value: string(retryable.RetryReasonCircuitBreakerOpen)},
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
	request := testPutLogEventsRequest([]cloudwatchlogs.Event{{Timestamp: now, Message: "hello"}})
	wantOutput := &cloudwatchlogs.PutLogEventsOutput{}
	client := &recordingClient{}
	client.send = func(context.Context, cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error) {
		if len(client.calls) <= 3 {
			return nil, &net.DNSError{IsTimeout: true}
		}
		return wantOutput, nil
	}
	child := newCircuitBreakerClientWithCircuitBreaker(client, tracer, observer.NopMeter{}, circuitBreaker)
	retrying := newRetryPolicyClient(child, fastCircuitBreakerConfig(3), tracer)

	_, err := retrying.PutLogEvents(context.Background(), request)
	require.Error(t, err)
	assert.Len(t, client.calls, 2)

	now = now.Add(time.Second)
	_, err = retrying.PutLogEvents(context.Background(), request)
	require.Error(t, err)
	assert.Len(t, client.calls, 3)

	_, err = retrying.PutLogEvents(context.Background(), request)
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.calls, 3)

	now = circuitBreakerErr.OpenUntil.Add(time.Nanosecond)
	output, err := retrying.PutLogEvents(context.Background(), request)
	require.NoError(t, err)
	assert.Same(t, wantOutput, output)
	assert.Len(t, client.calls, 4)

	output, err = retrying.PutLogEvents(context.Background(), request)
	require.NoError(t, err)
	assert.Same(t, wantOutput, output)
	assert.Len(t, client.calls, 5)
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
	request := testPutLogEventsRequest([]cloudwatchlogs.Event{{Timestamp: now, Message: "line"}})
	client := &recordingClient{}
	client.send = func(context.Context, cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error) {
		return nil, &net.DNSError{IsTimeout: true}
	}
	circuitClient := newCircuitBreakerClientWithCircuitBreaker(client, tracer, observer.NopMeter{}, circuitBreaker)

	_, err := circuitClient.PutLogEvents(context.Background(), request)
	require.Error(t, err)
	assert.Len(t, client.calls, 1)
	assert.Equal(t, request, client.calls[0].request)

	now = now.Add(time.Second)
	_, err = circuitClient.PutLogEvents(context.Background(), request)
	require.Error(t, err)
	assert.Len(t, client.calls, 2)
	assert.Equal(t, request, client.calls[1].request)
	opening := spanFromContext(t, client.calls[1].ctx)
	require.Len(t, opening.events, 1)
	assert.Equal(t, true, attrValue(opening.events[0].attrs, "circuit_breaker_client"))
	assert.Equal(t, true, attrValue(opening.events[0].attrs, "circuit_breaker_suppressed"))

	suppressed := tracer.mark()
	_, err = circuitClient.PutLogEvents(context.Background(), request)
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.calls, 2)
	require.Len(t, eventsWith(
		tracer.spansSince(suppressed),
		pkgobserver.Attr{Key: "reason", Value: string(retryable.RetryReasonCircuitBreakerOpen)},
	), 1)
	requireSpansEnded(t, tracer.spans)
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

	assert.Equal(t, "cloudwatchlogs.retry_decision", event.name)
	assert.Equal(t, string(wantReason), attrValue(event.attrs, "reason"))
	assert.Equal(t, wantAttempt, attrValue(event.attrs, "attempt"))
	assert.Equal(t, true, attrValue(event.attrs, "retry"))
	assert.Equal(t, wantStatus, attrValue(event.attrs, "http_status"))
	assert.NotEmpty(t, attrValue(event.attrs, "error"))
}

func attrValue(attrs []pkgobserver.Attr, key string) any {
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
func eventsWith(spans []*recordingSpan, want ...pkgobserver.Attr) []recordedEvent {
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
