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
	"github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type traceSpanKey struct{}

type recordingOpenSearchClient struct {
	indexDocument  func(context.Context, opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error)
	indexDocuments func(context.Context, []opensearch.DocumentWriteRequest) (*opensearch.BulkResponse, error)

	documentCalls []documentCall
	bulkCalls     []bulkCall
}

type documentCall struct {
	ctx context.Context
	doc opensearch.DocumentWriteRequest
}

type bulkCall struct {
	ctx  context.Context
	docs []opensearch.DocumentWriteRequest
}

func (c *recordingOpenSearchClient) IndexDocument(
	ctx context.Context,
	doc opensearch.DocumentWriteRequest,
) (*opensearch.IndexResponse, error) {
	c.documentCalls = append(c.documentCalls, documentCall{ctx: ctx, doc: doc})
	if c.indexDocument == nil {
		return nil, errors.New("unexpected IndexDocument call")
	}
	return c.indexDocument(ctx, doc)
}

func (c *recordingOpenSearchClient) IndexDocuments(
	ctx context.Context,
	docs []opensearch.DocumentWriteRequest,
) (*opensearch.BulkResponse, error) {
	c.bulkCalls = append(c.bulkCalls, bulkCall{ctx: ctx, docs: append([]opensearch.DocumentWriteRequest(nil), docs...)})
	if c.indexDocuments == nil {
		return nil, errors.New("unexpected IndexDocuments call")
	}
	return c.indexDocuments(ctx, docs)
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

func TestNewRetryingClientKeepsValidatedConfigAndDefaultsObserver(t *testing.T) {
	t.Parallel()

	client := newRetryingClient(
		&recordingOpenSearchClient{},
		retryConfigProvider(logger.OpenSearchRetryableClientCaseTransportErrorConfig{
			MaxAttempts: 2,
			BaseDelay:   3 * time.Millisecond,
			MaxDelay:    5 * time.Millisecond,
		}),
		nil,
	)

	assert.Equal(t, 2, client.maxAttempts())
	assert.Equal(t, 3*time.Millisecond, client.baseDelay())
	assert.Equal(t, 5*time.Millisecond, client.maxDelay())
	assert.NotNil(t, client.tracer)
}

func TestNewRetryingClientAcceptsCircuitBreakerClientChild(t *testing.T) {
	t.Parallel()

	circuitClient := newCircuitBreakerClient(&recordingOpenSearchClient{}, nil, nil)

	retrying := newRetryingClient(
		circuitClient,
		retryConfigProvider(logger.OpenSearchRetryableClientCaseTransportErrorConfig{}),
		nil,
	)

	assert.Same(t, circuitClient, retrying.next)
}

// backoff はランダムに揺らさず、再試行ごとに基準遅延ぶんずつ長くする。
func TestBackoffIncreasesLinearlyAndStopsAtMaxDelay(t *testing.T) {
	t.Parallel()

	client := newRetryingClient(
		&recordingOpenSearchClient{},
		retryConfigProvider(logger.OpenSearchRetryableClientCaseTransportErrorConfig{
			MaxAttempts: 5,
			BaseDelay:   100 * time.Millisecond,
			MaxDelay:    250 * time.Millisecond,
		}),
		nil,
	)

	assert.Equal(t, 100*time.Millisecond, client.backoff(1))
	assert.Equal(t, 200*time.Millisecond, client.backoff(2))
	assert.Equal(t, 250*time.Millisecond, client.backoff(3))
	assert.Equal(t, 250*time.Millisecond, client.backoff(4))
}

// OpenSearch の一時的な HTTP status error ではリトライし、span context と retry event を残す。
func TestIndexDocumentRetriesHTTPStatusErrorAndReturnsResponse(t *testing.T) {
	t.Parallel()

	tracer := &recordingTracer{}
	wantResp := &opensearch.IndexResponse{Index: "logs", ID: "doc-1", Status: http.StatusCreated}
	doc := opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1", Op: opensearch.WriteOpIndex}
	client := &recordingOpenSearchClient{}
	client.indexDocument = func(ctx context.Context, got opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
		assert.Equal(t, doc, got)
		if len(client.documentCalls) == 1 {
			return nil, &opensearch.HTTPStatusError{Code: http.StatusServiceUnavailable, Status: "503 Service Unavailable"}
		}
		return wantResp, nil
	}
	retrying := newRetryingClient(client, fastRetryConfig(2), tracer)

	resp, err := retrying.IndexDocument(context.Background(), doc)

	require.NoError(t, err)
	assert.Same(t, wantResp, resp)
	require.Len(t, client.documentCalls, 2)
	failed, retried := spanFromContext(t, client.documentCalls[0].ctx), spanFromContext(t, client.documentCalls[1].ctx)
	assert.NotSame(t, failed, retried, "each attempt runs in its own span")
	assert.Equal(t, spanNameIndexDocument, failed.name)
	assert.Equal(t, spanNameIndexDocument, retried.name)
	requireSpansEnded(t, tracer.spans)
	require.Len(t, failed.events, 1)
	assertRetryEvent(t, failed.events[0], retryable.RetryReasonHTTPStatus, 1, http.StatusServiceUnavailable)
	assert.Empty(t, retried.events)
}

func TestIndexDocumentDoesNotTreatIndexConflictAsIdempotentSuccess(t *testing.T) {
	t.Parallel()

	doc := opensearch.DocumentWriteRequest{
		Index: "logs",
		ID:    "fixed-id",
		Op:    opensearch.WriteOpIndex,
	}
	client := &recordingOpenSearchClient{
		indexDocument: func(
			context.Context,
			opensearch.DocumentWriteRequest,
		) (*opensearch.IndexResponse, error) {
			return nil, &opensearch.HTTPStatusError{Code: http.StatusConflict}
		},
	}
	retrying := newRetryingClient(client, fastRetryConfig(2), observer.NopTracer{})

	response, err := retrying.IndexDocument(context.Background(), doc)

	require.Error(t, err)
	assert.Nil(t, response)
	assert.Len(t, client.documentCalls, 1)
}

// bulk 登録でも retryable error の後に成功したレスポンスを返し、flush 用 span 名を使う。
func TestIndexDocumentsRetriesAndReturnsBulkResponse(t *testing.T) {
	t.Parallel()

	tracer := &recordingTracer{}
	wantResp := &opensearch.BulkResponse{Took: 12}
	docs := []opensearch.DocumentWriteRequest{
		{Index: "logs", ID: "doc-1", Op: opensearch.WriteOpIndex},
		{Index: "logs", ID: "doc-2", Op: opensearch.WriteOpCreate},
	}
	client := &recordingOpenSearchClient{}
	client.indexDocuments = func(ctx context.Context, got []opensearch.DocumentWriteRequest) (*opensearch.BulkResponse, error) {
		assert.Equal(t, docs, got)
		if len(client.bulkCalls) == 1 {
			return nil, io.ErrUnexpectedEOF
		}
		return wantResp, nil
	}
	retrying := newRetryingClient(client, fastRetryConfig(2), tracer)

	resp, err := retrying.IndexDocuments(context.Background(), docs)

	require.NoError(t, err)
	assert.Same(t, wantResp, resp)
	require.Len(t, client.bulkCalls, 2)
	failed, retried := spanFromContext(t, client.bulkCalls[0].ctx), spanFromContext(t, client.bulkCalls[1].ctx)
	assert.NotSame(t, failed, retried, "each attempt runs in its own span")
	assert.Equal(t, spanNameFlush, failed.name)
	assert.Equal(t, spanNameFlush, retried.name)
	requireSpansEnded(t, tracer.spans)
	require.Len(t, failed.events, 1)
	assertRetryEvent(t, failed.events[0], retryable.RetryReasonEOF, 1, 0)
	assert.Empty(t, retried.events)
}

// retryable ではない error は追加試行せず、そのまま呼び出し元に返す。
func TestIndexDocumentReturnsNonRetryableErrorWithoutRetry(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("mapping rejected")
	client := &recordingOpenSearchClient{}
	client.indexDocument = func(ctx context.Context, doc opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
		return nil, wantErr
	}
	tracer := &recordingTracer{}
	retrying := newRetryingClient(client, fastRetryConfig(3), tracer)

	resp, err := retrying.IndexDocument(context.Background(), opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1"})

	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, resp)
	assert.Len(t, client.documentCalls, 1)
	attempt := spanFromContext(t, client.documentCalls[0].ctx)
	assert.Equal(t, spanNameIndexDocument, attempt.name)
	requireSpansEnded(t, tracer.spans)
	assert.Empty(t, eventsWith(tracer.spans), "a failure without retry records no retry decision")
}

// 最大試行回数まで retryable error が続く場合は、最後の error を wrap して返す。
func TestIndexDocumentWrapsLastErrorWhenAttemptsExhausted(t *testing.T) {
	t.Parallel()

	client := &recordingOpenSearchClient{}
	client.indexDocument = func(ctx context.Context, doc opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
		return nil, io.EOF
	}
	tracer := &recordingTracer{}
	retrying := newRetryingClient(client, fastRetryConfig(2), tracer)

	resp, err := retrying.IndexDocument(context.Background(), opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1"})

	require.ErrorIs(t, err, io.EOF)
	var exhausted *retryable.RetryAttemptsExhaustedError
	require.ErrorAs(t, err, &exhausted)
	assert.False(t, exhausted.Policy)
	assert.Equal(t, 2, exhausted.Attempts)
	assert.ErrorIs(t, exhausted.LastErr, io.EOF)
	assert.Nil(t, resp)
	assert.Len(t, client.documentCalls, 2)
	require.Len(t, spanFromContext(t, client.documentCalls[0].ctx).events, 1, "the first failure schedules the retry")
	assert.Empty(t, spanFromContext(t, client.documentCalls[1].ctx).events, "the last failure schedules no further retry")
	requireSpansEnded(t, tracer.spans)
}

// backoff 中に呼び出し元 context が終わった場合は、追加送信せず context error を優先する。
func TestIndexDocumentStopsWhenContextCanceledDuringBackoff(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		client := &recordingOpenSearchClient{}
		client.indexDocument = func(ctx context.Context, doc opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
			return nil, io.EOF
		}
		tracer := &recordingTracer{}
		retrying := newRetryingClient(
			client,
			retryConfigProvider(logger.OpenSearchRetryableClientCaseTransportErrorConfig{
				MaxAttempts: 3,
				BaseDelay:   time.Hour,
				MaxDelay:    time.Hour,
			}),
			tracer,
		)
		type result struct {
			resp *opensearch.IndexResponse
			err  error
		}
		done := make(chan result, 1)
		go func() {
			resp, err := retrying.IndexDocument(ctx, opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1"})
			done <- result{resp: resp, err: err}
		}()

		// 一回目の失敗後のbackoffで待機してから、呼び出し元が停止します。
		synctest.Wait()
		cancel()
		got := <-done

		require.ErrorIs(t, got.err, context.Canceled)
		assert.Nil(t, got.resp)
		require.Len(t, client.documentCalls, 1)
		require.Len(t, spanFromContext(t, client.documentCalls[0].ctx).events, 1, "the failure that started the backoff is recorded")
		requireSpansEnded(t, tracer.spans)
	})
}

func TestRetryPolicyClientTimeoutRetriesOnceWithShortAttemptTimeout(t *testing.T) {
	t.Parallel()

	tracer := &recordingTracer{}
	wantResp := &opensearch.IndexResponse{Index: "logs", ID: "doc-1", Status: http.StatusCreated}
	doc := opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1", Op: opensearch.WriteOpIndex}
	client := &recordingOpenSearchClient{}
	client.indexDocument = func(ctx context.Context, got opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
		assert.Equal(t, doc, got)
		if len(client.documentCalls) == 1 {
			return nil, &net.DNSError{IsTimeout: true}
		}
		assertPolicyRetryTimeout(t, ctx)
		return wantResp, nil
	}
	retrying := newRetryPolicyClient(
		client,
		retryConfigProvider(logger.OpenSearchRetryableClientCaseTransportErrorConfig{
			MaxAttempts: 3,
			BaseDelay:   time.Nanosecond,
			MaxDelay:    time.Nanosecond,
		}),
		tracer,
	)

	resp, err := retrying.IndexDocument(context.Background(), doc)

	require.NoError(t, err)
	assert.Same(t, wantResp, resp)
	require.Len(t, client.documentCalls, 2)
	failed := spanFromContext(t, client.documentCalls[0].ctx)
	require.Len(t, failed.events, 1)
	assert.Equal(t, true, attrValue(failed.events[0].attrs, "retry_policy_client"))
}

func TestRetryPolicyClientTransientErrorUsesConfiguredMaxAttempts(t *testing.T) {
	t.Parallel()

	doc := opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1", Op: opensearch.WriteOpCreate}
	client := &recordingOpenSearchClient{}
	client.indexDocument = func(context.Context, opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
		if len(client.documentCalls) < 3 {
			return nil, &opensearch.HTTPStatusError{Code: http.StatusServiceUnavailable, Status: "503 Service Unavailable"}
		}
		return &opensearch.IndexResponse{Index: "logs", ID: "doc-1", Status: http.StatusCreated}, nil
	}
	retrying := newRetryPolicyClient(client, fastRetryConfig(3), observer.NopTracer{})

	_, err := retrying.IndexDocument(context.Background(), doc)

	require.NoError(t, err)
	require.Len(t, client.documentCalls, 3)
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
	doc := opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1", Op: opensearch.WriteOpIndex}
	client := &recordingOpenSearchClient{}
	client.indexDocument = func(context.Context, opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
		return nil, &net.DNSError{IsTimeout: true}
	}
	retrying := newRetryPolicyClient(
		newCircuitBreakerClientWithCircuitBreaker(client, tracer, observer.NopMeter{}, circuitBreaker),
		fastCircuitBreakerConfig(3),
		tracer,
	)

	_, err := retrying.IndexDocument(context.Background(), doc)
	require.Error(t, err)
	assert.Len(t, client.documentCalls, 2)

	now = now.Add(time.Second)
	opening := tracer.mark()
	_, err = retrying.IndexDocument(context.Background(), doc)
	require.Error(t, err)
	assert.Len(t, client.documentCalls, 3)
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
	_, err = retrying.IndexDocument(context.Background(), doc)
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.documentCalls, 3)
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
	wantResp := &opensearch.IndexResponse{Index: "logs", ID: "doc-1", Status: http.StatusCreated}
	doc := opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1", Op: opensearch.WriteOpIndex}
	client := &recordingOpenSearchClient{}
	client.indexDocument = func(context.Context, opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
		if len(client.documentCalls) <= 3 {
			return nil, &net.DNSError{IsTimeout: true}
		}
		return wantResp, nil
	}
	retrying := newRetryPolicyClient(
		newCircuitBreakerClientWithCircuitBreaker(client, tracer, observer.NopMeter{}, circuitBreaker),
		fastCircuitBreakerConfig(3),
		tracer,
	)

	_, err := retrying.IndexDocument(context.Background(), doc)
	require.Error(t, err)
	assert.Len(t, client.documentCalls, 2)

	now = now.Add(time.Second)
	_, err = retrying.IndexDocument(context.Background(), doc)
	require.Error(t, err)
	assert.Len(t, client.documentCalls, 3)

	_, err = retrying.IndexDocument(context.Background(), doc)
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.documentCalls, 3)

	now = circuitBreakerErr.OpenUntil.Add(time.Nanosecond)
	resp, err := retrying.IndexDocument(context.Background(), doc)
	require.NoError(t, err)
	assert.Same(t, wantResp, resp)
	assert.Len(t, client.documentCalls, 4)

	resp, err = retrying.IndexDocument(context.Background(), doc)
	require.NoError(t, err)
	assert.Same(t, wantResp, resp)
	assert.Len(t, client.documentCalls, 5)
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
	doc := opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1", Op: opensearch.WriteOpIndex}
	client := &recordingOpenSearchClient{}
	client.indexDocument = func(context.Context, opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
		return nil, &net.DNSError{IsTimeout: true}
	}
	circuitClient := newCircuitBreakerClientWithCircuitBreaker(client, tracer, observer.NopMeter{}, circuitBreaker)

	_, err := circuitClient.IndexDocument(context.Background(), doc)
	require.Error(t, err)
	assert.Len(t, client.documentCalls, 1)

	now = now.Add(time.Second)
	_, err = circuitClient.IndexDocument(context.Background(), doc)
	require.Error(t, err)
	assert.Len(t, client.documentCalls, 2)
	opening := spanFromContext(t, client.documentCalls[1].ctx)
	require.Len(t, opening.events, 1)
	assert.Equal(t, true, attrValue(opening.events[0].attrs, "circuit_breaker_client"))
	assert.Equal(t, true, attrValue(opening.events[0].attrs, "circuit_breaker_suppressed"))

	suppressed := tracer.mark()
	_, err = circuitClient.IndexDocument(context.Background(), doc)
	require.Error(t, err)
	var circuitBreakerErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &circuitBreakerErr)
	assert.Len(t, client.documentCalls, 2)
	require.Len(t, eventsWith(
		tracer.spansSince(suppressed),
		observer.Attr{Key: "reason", Value: string(retryable.RetryReasonCircuitBreakerOpen)},
	), 1)
	requireSpansEnded(t, tracer.spans)
}

func TestCircuitBreakerClientSharesOpenStateAcrossOperations(t *testing.T) {
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
	doc := opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1", Op: opensearch.WriteOpIndex}
	client := &recordingOpenSearchClient{
		indexDocument: func(context.Context, opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
			return nil, &net.DNSError{IsTimeout: true}
		},
		indexDocuments: func(context.Context, []opensearch.DocumentWriteRequest) (*opensearch.BulkResponse, error) {
			return &opensearch.BulkResponse{}, nil
		},
	}
	circuitClient := newCircuitBreakerClientWithCircuitBreaker(
		client,
		observer.NopTracer{},
		observer.NopMeter{},
		circuitBreaker,
	)

	_, err := circuitClient.IndexDocument(context.Background(), doc)
	require.Error(t, err)
	now = now.Add(time.Second)
	_, err = circuitClient.IndexDocument(context.Background(), doc)
	require.Error(t, err)

	_, err = circuitClient.IndexDocuments(context.Background(), []opensearch.DocumentWriteRequest{doc})
	var openErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &openErr)
	assert.Equal(t, spanNameFlush, openErr.Operation)
	assert.Empty(t, client.bulkCalls, "bulk operation must be suppressed by the backend-wide open circuit breaker")
}

func fastRetryConfig(maxAttempts int) retryBackoffConfigProvider {
	return retryConfigProvider(logger.OpenSearchRetryableClientCaseTransportErrorConfig{
		MaxAttempts: maxAttempts,
		BaseDelay:   time.Nanosecond,
		MaxDelay:    time.Nanosecond,
	})
}

func fastCircuitBreakerConfig(maxAttempts int) retryBackoffConfigProvider {
	return retryConfigProvider(logger.OpenSearchRetryableClientCaseTransportErrorConfig{
		RetryClientMode: logger.RetryClientModeRetryPolicyWithBreaker,
		MaxAttempts:     maxAttempts,
		BaseDelay:       time.Nanosecond,
		MaxDelay:        time.Nanosecond,
	})
}

func assertPolicyRetryTimeout(t *testing.T, ctx context.Context) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	require.True(t, ok, "retry_policy_with_breaker timeout retry attempt should have a deadline")
	remaining := time.Until(deadline)
	assert.Positive(t, remaining)
	assert.LessOrEqual(t, remaining, retryable.DefaultPolicyTimeoutRetryAttemptTimeout)
}

func retryConfigProvider(cfg logger.OpenSearchRetryableClientCaseTransportErrorConfig) retryBackoffConfigProvider {
	return staticRetryBackoffConfig{
		maxAttempts: cfg.MaxAttempts,
		baseDelay:   cfg.BaseDelay,
		maxDelay:    cfg.MaxDelay,
	}
}

type staticRetryBackoffConfig struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
}

func (c staticRetryBackoffConfig) MaxAttempts() int         { return c.maxAttempts }
func (c staticRetryBackoffConfig) BaseDelay() time.Duration { return c.baseDelay }
func (c staticRetryBackoffConfig) MaxDelay() time.Duration  { return c.maxDelay }

func assertRetryEvent(t *testing.T, event recordedEvent, wantReason retryable.RetryReason, wantAttempt, wantStatus int) {
	t.Helper()

	assert.Equal(t, "opensearch.retry_decision", event.name)
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
