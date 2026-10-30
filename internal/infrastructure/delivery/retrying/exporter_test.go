package retrying

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
)

// errTemporaryとerrPermanentは、試験の送り先が返す、送り直すerrorと送り直さないerrorです。
var (
	errTemporary = errors.New("temporary")
	errPermanent = errors.New("permanent")
	errTooLarge  = errors.New("too large")
)

// fakeBackendは、要求ごとに決めた結果を返す送り先です。
type fakeBackend struct {
	mu       sync.Mutex
	requests [][]int
	// respondは、送られたitemsへの結果を返します。
	respond func(items []int) error
}

func (b *fakeBackend) send(_ context.Context, items []int) error {
	b.mu.Lock()
	b.requests = append(b.requests, slices.Clone(items))
	b.mu.Unlock()
	return b.respond(items)
}

func classifyTest(_ context.Context, err error) retryable.RetryDecision {
	if errors.Is(err, errTemporary) {
		return retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonHTTPStatus, Code: 503}
	}
	return retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: 400}
}

func failTest(ctx context.Context, err error) delivery.Failure {
	if classifyTest(ctx, err).Retry {
		return delivery.Failure{Disposition: delivery.DispositionRetry, Err: err}
	}
	return delivery.Failure{Disposition: delivery.DispositionDead, Err: err}
}

func newTestExporter(b *fakeBackend, retries int) (*Exporter[int], *observertest.Meter, *[]time.Duration) {
	meter := observertest.NewMeter()
	var waits []time.Duration
	e := New(Config{
		Retries: retries, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Second,
		Sleep: func(ctx context.Context, d time.Duration) bool {
			waits = append(waits, d)
			return ctx.Err() == nil
		},
		Jitter: func(n time.Duration) time.Duration { return n },
	}, Backend[int]{
		Name:          "test",
		BackendName:   "test_traces",
		RetriesMetric: telemetry.TraceExportRetries,
		Send:          b.send,
		Classify:      classifyTest,
		Split: func(_ context.Context, err error) retryable.RetryReason {
			if errors.Is(err, errTooLarge) {
				return retryable.RetryReasonRequestTooLarge
			}
			return ""
		},
		Fail: failTest,
	}, meter)
	return e, meter, &waits
}

// rangesは、failuresの範囲と扱いを並べます。
func ranges(t *testing.T, err error) []delivery.Failure {
	t.Helper()
	var exportErr *delivery.ExportError
	if !errors.As(err, &exportErr) {
		t.Fatalf("Export = %v, want an *ExportError", err)
	}
	out := make([]delivery.Failure, len(exportErr.Failures))
	for i, f := range exportErr.Failures {
		out[i] = delivery.Failure{Start: f.Start, End: f.End, Disposition: f.Disposition}
	}
	return out
}

func TestExportResendsOnlyTheItemsThatFailedTemporarily(t *testing.T) {
	b := &fakeBackend{}
	b.respond = func(items []int) error {
		if len(b.requests) > 1 {
			return nil
		}
		// 1件目は送り直し、3件目は拒否、ほかは受け付けます。
		return &ItemErrors{Errs: []ItemError{{Index: 1, Err: errTemporary}, {Index: 3, Err: errPermanent}}}
	}
	e, meter, waits := newTestExporter(b, 2)
	err := e.Export(context.Background(), []int{10, 11, 12, 13, 14})
	if want := []delivery.Failure{{Start: 3, End: 4, Disposition: delivery.DispositionDead}}; !slices.Equal(ranges(t, err), want) {
		t.Errorf("failures = %+v, want %+v", ranges(t, err), want)
	}
	if want := [][]int{{10, 11, 12, 13, 14}, {11}}; !slices.EqualFunc(b.requests, want, slices.Equal) {
		t.Errorf("requests = %v, want %v", b.requests, want)
	}
	if len(*waits) != 1 || meter.Count(observertest.Key(telemetry.TraceExportRetries, telemetry.AttrExporter, "test", telemetry.AttrReason, "http_status")) != 1 {
		t.Errorf("waits %v, counters %v; want one retry", *waits, meter.Counters())
	}
}

func TestExportKeepsTheItemsThatStillFailAfterTheRetries(t *testing.T) {
	b := &fakeBackend{}
	b.respond = func(items []int) error {
		var errs ItemErrors
		for i, item := range items {
			if item%2 == 1 {
				errs.Errs = append(errs.Errs, ItemError{Index: i, Err: errTemporary})
			}
		}
		return &errs
	}
	e, _, _ := newTestExporter(b, 1)
	err := e.Export(context.Background(), []int{0, 1, 2, 3, 5, 6})
	// 送り直しても届かなかったitemsを、連続した位置ごとの範囲で返します。
	want := []delivery.Failure{
		{Start: 1, End: 2, Disposition: delivery.DispositionRetry},
		{Start: 3, End: 5, Disposition: delivery.DispositionRetry},
	}
	if got := ranges(t, err); !slices.Equal(got, want) {
		t.Errorf("failures = %+v, want %+v", got, want)
	}
	if want := [][]int{{0, 1, 2, 3, 5, 6}, {1, 3, 5}}; !slices.EqualFunc(b.requests, want, slices.Equal) {
		t.Errorf("requests = %v, want %v", b.requests, want)
	}
}

func TestExportSplitsRequestsAndStopsAfterATemporaryFailure(t *testing.T) {
	b := &fakeBackend{}
	b.respond = func(items []int) error {
		switch {
		case len(items) > 2:
			return errTooLarge
		case items[0] == 0:
			return errPermanent
		default:
			return errTemporary
		}
	}
	e, meter, _ := newTestExporter(b, 0)
	err := e.Export(context.Background(), []int{0, 1, 2, 3, 4, 5, 6, 7})
	// [0 1]は拒否、[2 3]は一時的な失敗で諦め、残りは送りません。
	want := []delivery.Failure{
		{Start: 0, End: 2, Disposition: delivery.DispositionDead},
		{Start: 2, End: 4, Disposition: delivery.DispositionRetry},
		{Start: 4, End: 8, Disposition: delivery.DispositionRetry},
	}
	if got := ranges(t, err); !slices.Equal(got, want) {
		t.Errorf("failures = %+v, want %+v", got, want)
	}
	if want := [][]int{{0, 1, 2, 3, 4, 5, 6, 7}, {0, 1, 2, 3}, {0, 1}, {2, 3}}; !slices.EqualFunc(b.requests, want, slices.Equal) {
		t.Errorf("requests = %v, want %v", b.requests, want)
	}
	if got := meter.Count(observertest.Key(telemetry.TraceExportRetries, telemetry.AttrExporter, "test", telemetry.AttrReason, "request_too_large")); got != 2 {
		t.Errorf("splits = %d, want 2", got)
	}
}

func TestExportDoesNotSplitASingleItem(t *testing.T) {
	b := &fakeBackend{respond: func([]int) error { return errTooLarge }}
	e, _, _ := newTestExporter(b, 3)
	err := e.Export(context.Background(), []int{7})
	if want := []delivery.Failure{{Start: 0, End: 1, Disposition: delivery.DispositionDead}}; !slices.Equal(ranges(t, err), want) {
		t.Errorf("failures = %+v, want %+v", ranges(t, err), want)
	}
	if len(b.requests) != 1 {
		t.Errorf("requests = %v, want one", b.requests)
	}
}

func TestRetryDelayGrowsExponentiallyWithJitterUpToTheMaximum(t *testing.T) {
	e := New(Config{RetryBaseDelay: 100 * time.Millisecond, RetryMaxDelay: time.Second}, Backend[int]{}, nil)
	for _, tc := range []struct {
		retry    int
		low, top time.Duration
	}{
		{1, 50 * time.Millisecond, 100 * time.Millisecond},
		{2, 100 * time.Millisecond, 200 * time.Millisecond},
		{4, 400 * time.Millisecond, 800 * time.Millisecond},
		{5, 500 * time.Millisecond, time.Second},
		{100, 500 * time.Millisecond, time.Second},
	} {
		e.cfg.Jitter = func(time.Duration) time.Duration { return 0 }
		if got := e.retryDelay(tc.retry, retryable.RetryDecision{Retry: true}); got != tc.low {
			t.Errorf("retry %d: lowest delay = %v, want %v", tc.retry, got, tc.low)
		}
		e.cfg.Jitter = func(n time.Duration) time.Duration { return n }
		if got := e.retryDelay(tc.retry, retryable.RetryDecision{Retry: true}); got != tc.top {
			t.Errorf("retry %d: highest delay = %v, want %v", tc.retry, got, tc.top)
		}
	}
	// 本来の無作為な値も、範囲に収まります。
	e.cfg.Jitter = randomDuration
	for range 100 {
		if got := e.retryDelay(2, retryable.RetryDecision{Retry: true}); got < 100*time.Millisecond || got > 200*time.Millisecond {
			t.Fatalf("random delay = %v, want from 100ms to 200ms", got)
		}
	}
	// Retry-Afterがあれば、それを使います。
	if got := e.retryDelay(1, retryable.RetryDecision{Retry: true, RetryAfter: 3 * time.Second}); got != 3*time.Second {
		t.Errorf("delay with Retry-After = %v, want 3s", got)
	}
}

func TestCircuitBreakerCountsItemErrorsAsSuccess(t *testing.T) {
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	b := &fakeBackend{respond: func([]int) error {
		return &ItemErrors{Errs: []ItemError{{Index: 0, Err: errTemporary}}}
	}}
	e := New(Config{CircuitBreaker: true, Now: func() time.Time { return now }}, Backend[int]{
		Name: "test", BackendName: "test_traces", RetriesMetric: telemetry.TraceExportRetries,
		Send: b.send, Classify: classifyTest, Fail: failTest,
	}, nil)
	// itemの失敗は一時的でも、要求そのものは処理されているため、何回続いてもbreakerは
	// 開きません。
	for range 20 {
		err := e.Export(context.Background(), []int{1})
		if !errors.Is(err, errTemporary) {
			t.Fatalf("Export = %v, want the item error", err)
		}
		now = now.Add(10 * time.Second)
	}
	if len(b.requests) != 20 {
		t.Errorf("requests = %d, want 20", len(b.requests))
	}
}
