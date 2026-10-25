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
	"github.com/go-jappe/heterolog/pkg/clickhouse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type traceSpanKey struct{}

type recordingClickHouseClient struct {
	insertLogsWithOptions func(context.Context, []clickhouse.LogRecord, clickhouse.InsertOptions) (*clickhouse.InsertResponse, error)
	calls                 []insertLogsWithOptionsCall
}

type insertLogsWithOptionsCall struct {
	ctx     context.Context
	records []clickhouse.LogRecord
	opts    clickhouse.InsertOptions
}

func (c *recordingClickHouseClient) InsertLogsWithOptions(
	ctx context.Context,
	records []clickhouse.LogRecord,
	opts clickhouse.InsertOptions,
) (*clickhouse.InsertResponse, error) {
	c.calls = append(c.calls, insertLogsWithOptionsCall{
		ctx:     ctx,
		records: append([]clickhouse.LogRecord(nil), records...),
		opts:    opts,
	})
	if c.insertLogsWithOptions == nil {
		return nil, errors.New("unexpected InsertLogsWithOptions call")
	}
	return c.insertLogsWithOptions(ctx, records, opts)
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

	client := newRetryingClient(&recordingClickHouseClient{}, staticRetryConfig{
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

	circuitClient := newCircuitBreakerClient(&recordingClickHouseClient{}, nil, nil)

	retrying := newRetryingClient(circuitClient, staticRetryConfig{}, nil)

	assert.Same(t, circuitClient, retrying.next)
}

func TestBackoffIncreasesLinearlyAndStopsAtMaxDelay(t *testing.T) {
	t.Parallel()

	client := newRetryingClient(
		&recordingClickHouseClient{},
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

func TestRetryDelayUsesRetryAfterForThrottlingAndServiceUnavailable(t *testing.T) {
	t.Parallel()

	client := newRetryingClient(
		&recordingClickHouseClient{},
		staticRetryConfig{
			maxAttempts: 3,
			baseDelay:   100 * time.Millisecond,
			maxDelay:    250 * time.Millisecond,
		},
		nil,
	)

	for _, decision := range []retryable.RetryDecision{
		{
			Retry:      true,
			Reason:     retryable.RetryReasonHTTPThrottle,
			Code:       http.StatusTooManyRequests,
			RetryAfter: 300 * time.Millisecond,
		},
		{
			Retry:      true,
			Reason:     retryable.RetryReasonHTTPStatus,
			Code:       http.StatusServiceUnavailable,
			RetryAfter: 300 * time.Millisecond,
		},
	} {
		assert.Equal(t, 250*time.Millisecond, client.retryDelay(1, decision))
	}

	assert.Equal(t, 100*time.Millisecond, client.retryDelay(1, retryable.RetryDecision{
		Retry:      true,
		Reason:     retryable.RetryReasonHTTPStatus,
		Code:       http.StatusInternalServerError,
		RetryAfter: 300 * time.Millisecond,
	}))
}

func TestInsertLogsWithOptionsRetriesHTTPStatusErrorAndReturnsResponse(t *testing.T) {
	t.Parallel()

	tracer := &recordingTracer{}
	wantResp := &clickhouse.InsertResponse{StatusCode: http.StatusOK}
	records := []clickhouse.LogRecord{{Timestamp: time.Unix(10, 0), Level: "ERROR", Message: "retry me"}}
	opts := clickhouse.InsertOptions{InsertDeduplicationToken: "dedup-token"}
	client := &recordingClickHouseClient{}
	client.insertLogsWithOptions = func(ctx context.Context, got []clickhouse.LogRecord, gotOpts clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
		assert.Equal(t, records, got)
		assert.Equal(t, opts, gotOpts)
		if len(client.calls) == 1 {
			return nil, &clickhouse.HTTPStatusError{Code: http.StatusServiceUnavailable, Status: "503 Service Unavailable"}
		}
		return wantResp, nil
	}
	retrying := newRetryingClient(client, fastRetryConfig(2), tracer)

	resp, err := retrying.InsertLogsWithOptions(context.Background(), records, opts)

	require.NoError(t, err)
	assert.Same(t, wantResp, resp)
	require.Len(t, client.calls, 2)
	assert.Equal(t, opts, client.calls[0].opts)
	assert.Equal(t, opts, client.calls[1].opts)
	failed, retried := spanFromContext(t, client.calls[0].ctx), spanFromContext(t, client.calls[1].ctx)
	assert.NotSame(t, failed, retried, "each attempt runs in its own span")
	assert.Equal(t, spanNameInsertLogs, failed.name)
	assert.Equal(t, spanNameInsertLogs, retried.name)
	requireSpansEnded(t, tracer.spans)
	require.Len(t, failed.events, 1)
	assertRetryEvent(t, failed.events[0], retryable.RetryReasonHTTPStatus, 1, http.StatusServiceUnavailable)
	assert.Empty(t, retried.events)
}

func TestInsertLogsWithOptionsPreservesSingleRecordAndOptions(t *testing.T) {
	t.Parallel()

	record := clickhouse.LogRecord{Timestamp: time.Unix(10, 0), Level: "INFO", Message: "one row"}
	opts := clickhouse.InsertOptions{InsertDeduplicationToken: "single-token"}
	wantResp := &clickhouse.InsertResponse{StatusCode: http.StatusOK}
	client := &recordingClickHouseClient{}
	client.insertLogsWithOptions = func(ctx context.Context, got []clickhouse.LogRecord, gotOpts clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
		assert.Equal(t, []clickhouse.LogRecord{record}, got)
		assert.Equal(t, opts, gotOpts)
		return wantResp, nil
	}
	retrying := newRetryingClient(client, fastRetryConfig(2), observer.NopTracer{})

	resp, err := retrying.InsertLogsWithOptions(context.Background(), []clickhouse.LogRecord{record}, opts)

	require.NoError(t, err)
	assert.Same(t, wantResp, resp)
	require.Len(t, client.calls, 1)
	assert.Equal(t, []clickhouse.LogRecord{record}, client.calls[0].records)
	assert.Equal(t, opts, client.calls[0].opts)
}

func TestInsertLogsReturnsPermanentClickHouseErrorWithoutRetry(t *testing.T) {
	t.Parallel()

	wantErr := &clickhouse.HTTPStatusError{
		Code:   http.StatusInternalServerError,
		Status: "500 Internal Server Error",
		Body:   []byte("DB::Exception: unknown setting (UNKNOWN_SETTING)"),
	}
	client := &recordingClickHouseClient{}
	client.insertLogsWithOptions = func(ctx context.Context, records []clickhouse.LogRecord, opts clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
		return nil, wantErr
	}
	tracer := &recordingTracer{}
	retrying := newRetryingClient(client, fastRetryConfig(3), tracer)

	resp, err := retrying.InsertLogsWithOptions(context.Background(), []clickhouse.LogRecord{{Timestamp: time.Unix(10, 0), Message: "do not retry"}}, clickhouse.InsertOptions{})

	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, resp)
	assert.Len(t, client.calls, 1)
	attempt := spanFromContext(t, client.calls[0].ctx)
	assert.Equal(t, spanNameInsertLogs, attempt.name)
	requireSpansEnded(t, tracer.spans)
	assert.Empty(t, eventsWith(tracer.spans), "a failure without retry records no retry decision")
}

func TestInsertLogsDoesNotRetryKnownInputExceptionFromSuccessfulHTTPResponse(t *testing.T) {
	t.Parallel()

	wantErr := &clickhouse.HTTPStatusError{
		Code:          http.StatusOK,
		Status:        "200 OK",
		ExceptionCode: 27,
		ExceptionType: clickhouse.ErrorTypeCannotParseUUID,
	}
	client := &recordingClickHouseClient{}
	client.insertLogsWithOptions = func(context.Context, []clickhouse.LogRecord, clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
		return nil, wantErr
	}
	retrying := newRetryingClient(client, fastRetryConfig(3), observer.NopTracer{})

	response, err := retrying.InsertLogsWithOptions(context.Background(), []clickhouse.LogRecord{{Message: "invalid"}}, clickhouse.InsertOptions{})

	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, response)
	assert.Len(t, client.calls, 1)
}

func TestInsertLogsDoesNotRetryUnknownExceptionFromSuccessfulHTTPResponse(t *testing.T) {
	t.Parallel()

	wantResponse := &clickhouse.InsertResponse{StatusCode: http.StatusOK}
	client := &recordingClickHouseClient{}
	client.insertLogsWithOptions = func(context.Context, []clickhouse.LogRecord, clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
		if len(client.calls) == 1 {
			return nil, &clickhouse.HTTPStatusError{
				Code:          http.StatusOK,
				Status:        "200 OK",
				ExceptionCode: 999,
				ExceptionType: clickhouse.ErrorType("SOME_TRANSIENT_EXCEPTION"),
			}
		}
		return wantResponse, nil
	}
	retrying := newRetryingClient(client, fastRetryConfig(2), observer.NopTracer{})

	response, err := retrying.InsertLogsWithOptions(context.Background(), []clickhouse.LogRecord{{Message: "retry"}}, clickhouse.InsertOptions{})

	var statusErr *clickhouse.HTTPStatusError
	require.ErrorAs(t, err, &statusErr)
	assert.Equal(t, clickhouse.ErrorType("SOME_TRANSIENT_EXCEPTION"), statusErr.ClickHouseErrorType())
	assert.Nil(t, response)
	assert.Len(t, client.calls, 1)
}

func TestInsertLogsWrapsLastErrorWhenAttemptsExhausted(t *testing.T) {
	t.Parallel()

	client := &recordingClickHouseClient{}
	client.insertLogsWithOptions = func(ctx context.Context, records []clickhouse.LogRecord, opts clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
		return nil, io.EOF
	}
	tracer := &recordingTracer{}
	retrying := newRetryingClient(client, fastRetryConfig(2), tracer)

	resp, err := retrying.InsertLogsWithOptions(context.Background(), []clickhouse.LogRecord{{Timestamp: time.Unix(10, 0), Message: "retry me"}}, clickhouse.InsertOptions{})

	require.ErrorIs(t, err, io.EOF)
	var exhausted *retryable.RetryAttemptsExhaustedError
	require.ErrorAs(t, err, &exhausted)
	assert.False(t, exhausted.Policy)
	assert.Equal(t, 2, exhausted.Attempts)
	assert.ErrorIs(t, exhausted.LastErr, io.EOF)
	assert.Nil(t, resp)
	assert.Len(t, client.calls, 2)
	require.Len(t, spanFromContext(t, client.calls[0].ctx).events, 1, "the first failure schedules the retry")
	assert.Empty(t, spanFromContext(t, client.calls[1].ctx).events, "the last failure schedules no further retry")
	requireSpansEnded(t, tracer.spans)
}

func TestInsertLogsStopsWhenContextCanceledDuringBackoff(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		client := &recordingClickHouseClient{}
		client.insertLogsWithOptions = func(ctx context.Context, records []clickhouse.LogRecord, opts clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
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
			resp *clickhouse.InsertResponse
			err  error
		}
		done := make(chan result, 1)
		go func() {
			resp, err := retrying.InsertLogsWithOptions(ctx, []clickhouse.LogRecord{{Timestamp: time.Unix(10, 0), Message: "cancel before retry"}}, clickhouse.InsertOptions{})
			done <- result{resp: resp, err: err}
		}()

		// 一回目の失敗後のbackoffで待機してから、呼び出し元が停止します。
		synctest.Wait()
		cancel()
		got := <-done

		require.ErrorIs(t, got.err, context.Canceled)
		assert.Nil(t, got.resp)
		require.Len(t, client.calls, 1)
		require.Len(t, spanFromContext(t, client.calls[0].ctx).events, 1, "the failure that started the backoff is recorded")
		requireSpansEnded(t, tracer.spans)
	})
}

func TestRetryPolicyClientTimeoutRetriesOnceWithShortAttemptTimeout(t *testing.T) {
	t.Parallel()

	tracer := &recordingTracer{}
	wantResp := &clickhouse.InsertResponse{StatusCode: http.StatusOK}
	records := []clickhouse.LogRecord{{Timestamp: time.Unix(10, 0), Level: "ERROR", Message: "retry me"}}
	client := &recordingClickHouseClient{}
	client.insertLogsWithOptions = func(ctx context.Context, got []clickhouse.LogRecord, opts clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
		assert.Equal(t, records, got)
		if len(client.calls) == 1 {
			return nil, &net.DNSError{IsTimeout: true}
		}
		assertPolicyRetryTimeout(t, ctx)
		return wantResp, nil
	}
	retrying := newRetryPolicyClient(client, fastRetryConfig(3), tracer)

	resp, err := retrying.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})

	require.NoError(t, err)
	assert.Same(t, wantResp, resp)
	require.Len(t, client.calls, 2)
	require.NotEmpty(t, client.calls[0].opts.InsertDeduplicationToken)
	assert.Equal(t, client.calls[0].opts.InsertDeduplicationToken, client.calls[1].opts.InsertDeduplicationToken)
	failed := spanFromContext(t, client.calls[0].ctx)
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
	records := []clickhouse.LogRecord{{Timestamp: now, Level: "INFO", Message: "hello"}}
	client := &recordingClickHouseClient{}
	client.insertLogsWithOptions = func(context.Context, []clickhouse.LogRecord, clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
		return nil, &net.DNSError{IsTimeout: true}
	}
	child := newCircuitBreakerClientWithCircuitBreaker(client, tracer, observer.NopMeter{}, circuitBreaker)
	retrying := newRetryPolicyClient(child, fastCircuitBreakerConfig(3), tracer)

	_, err := retrying.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})
	require.Error(t, err)
	assert.Len(t, client.calls, 2)

	now = now.Add(time.Second)
	opening := tracer.mark()
	_, err = retrying.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})
	require.Error(t, err)
	assert.Len(t, client.calls, 3)
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
	_, err = retrying.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.calls, 3)
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
	records := []clickhouse.LogRecord{{Timestamp: now, Level: "INFO", Message: "hello"}}
	wantResp := &clickhouse.InsertResponse{StatusCode: http.StatusOK}
	client := &recordingClickHouseClient{}
	client.insertLogsWithOptions = func(context.Context, []clickhouse.LogRecord, clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
		if len(client.calls) <= 3 {
			return nil, &net.DNSError{IsTimeout: true}
		}
		return wantResp, nil
	}
	child := newCircuitBreakerClientWithCircuitBreaker(client, tracer, observer.NopMeter{}, circuitBreaker)
	retrying := newRetryPolicyClient(child, fastCircuitBreakerConfig(3), tracer)

	_, err := retrying.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})
	require.Error(t, err)
	assert.Len(t, client.calls, 2)

	now = now.Add(time.Second)
	_, err = retrying.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})
	require.Error(t, err)
	assert.Len(t, client.calls, 3)

	_, err = retrying.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.calls, 3)

	now = circuitBreakerErr.OpenUntil.Add(time.Nanosecond)
	resp, err := retrying.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})
	require.NoError(t, err)
	assert.Same(t, wantResp, resp)
	assert.Len(t, client.calls, 4)

	resp, err = retrying.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})
	require.NoError(t, err)
	assert.Same(t, wantResp, resp)
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
	records := []clickhouse.LogRecord{{Timestamp: now, Level: "info", Message: "line"}}
	client := &recordingClickHouseClient{}
	client.insertLogsWithOptions = func(context.Context, []clickhouse.LogRecord, clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
		return nil, &net.DNSError{IsTimeout: true}
	}
	circuitClient := newCircuitBreakerClientWithCircuitBreaker(client, tracer, observer.NopMeter{}, circuitBreaker)

	_, err := circuitClient.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})
	require.Error(t, err)
	assert.Len(t, client.calls, 1)

	now = now.Add(time.Second)
	_, err = circuitClient.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})
	require.Error(t, err)
	assert.Len(t, client.calls, 2)
	opening := spanFromContext(t, client.calls[1].ctx)
	require.Len(t, opening.events, 1)
	assert.Equal(t, true, attrValue(opening.events[0].attrs, "circuit_breaker_client"))
	assert.Equal(t, true, attrValue(opening.events[0].attrs, "circuit_breaker_suppressed"))

	suppressed := tracer.mark()
	_, err = circuitClient.InsertLogsWithOptions(context.Background(), records, clickhouse.InsertOptions{})
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.calls, 2)
	require.Len(t, eventsWith(
		tracer.spansSince(suppressed),
		observer.Attr{Key: "reason", Value: string(retryable.RetryReasonCircuitBreakerOpen)},
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

	assert.Equal(t, "clickhouse.retry_decision", event.name)
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
