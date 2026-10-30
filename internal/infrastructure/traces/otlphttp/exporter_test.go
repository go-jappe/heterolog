package otlphttp

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/circuitbreaker"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// receiverは、OTLP/HTTPの受信側です。responsesの応答を順に返し、尽きたら200を返します。
type receiver struct {
	t         *testing.T
	responses []func(http.ResponseWriter)

	mu       sync.Mutex
	requests []*http.Request
	bodies   []*collectortrace.ExportTraceServiceRequest
}

func (r *receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var body io.Reader = req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(req.Body)
		if err != nil {
			r.t.Errorf("gzip body: %v", err)
			return
		}
		body = zr
	}
	raw, err := io.ReadAll(body)
	if err != nil {
		r.t.Errorf("read body: %v", err)
	}
	var decoded collectortrace.ExportTraceServiceRequest
	if err := proto.Unmarshal(raw, &decoded); err != nil {
		r.t.Errorf("unmarshal body: %v", err)
	}
	r.mu.Lock()
	n := len(r.requests)
	r.requests = append(r.requests, req)
	r.bodies = append(r.bodies, &decoded)
	r.mu.Unlock()
	if n < len(r.responses) {
		r.responses[n](w)
	}
}

func (r *receiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

func status(code int, header ...string) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		for i := 0; i+1 < len(header); i += 2 {
			w.Header().Set(header[i], header[i+1])
		}
		w.WriteHeader(code)
	}
}

func newTestExporter(t *testing.T, r *receiver, cfg Config) (*Exporter, *observertest.Meter, *[]time.Duration) {
	t.Helper()
	server := httptest.NewServer(r)
	t.Cleanup(server.Close)
	cfg.Endpoint = server.URL + "/v1/traces"
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	meter := observertest.NewMeter()
	e := New(cfg, &tracing.Resource{}, meter)
	t.Cleanup(func() { _ = e.Close() })
	var waits []time.Duration
	e.sleep = func(ctx context.Context, d time.Duration) bool {
		waits = append(waits, d)
		return ctx.Err() == nil
	}
	// 待ち時間を確かめられるよう、ばらつきの範囲の上限を選びます。
	e.jitter = func(n time.Duration) time.Duration { return n }
	return e, meter, &waits
}

var testBatch = []tracing.SpanData{{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{1}, Name: "op"}}

func TestExportPostsCompressedProtobufWithHeaders(t *testing.T) {
	r := &receiver{t: t}
	e, _, _ := newTestExporter(t, r, Config{Compression: "gzip", Headers: map[string]string{"Authorization": "Bearer x"}})
	if err := e.Export(context.Background(), testBatch); err != nil {
		t.Fatal(err)
	}
	req := r.requests[0]
	if req.Method != http.MethodPost || req.URL.Path != "/v1/traces" || req.Header.Get("Content-Type") != "application/x-protobuf" ||
		req.Header.Get("Content-Encoding") != "gzip" || req.Header.Get("Authorization") != "Bearer x" {
		t.Errorf("request %s %s headers %v", req.Method, req.URL.Path, req.Header)
	}
	if spans := r.bodies[0].ResourceSpans[0].ScopeSpans[0].Spans; len(spans) != 1 || spans[0].Name != "op" {
		t.Errorf("body spans = %v", spans)
	}

	r = &receiver{t: t}
	e, _, _ = newTestExporter(t, r, Config{Compression: "none"})
	if err := e.Export(context.Background(), testBatch); err != nil {
		t.Fatal(err)
	}
	if got := r.requests[0].Header.Get("Content-Encoding"); got != "" {
		t.Errorf("Content-Encoding = %q, want none", got)
	}
}

func TestExportRetriesTemporaryFailures(t *testing.T) {
	r := &receiver{t: t, responses: []func(http.ResponseWriter){
		status(http.StatusServiceUnavailable, "Retry-After", "1"),
		status(http.StatusBadGateway),
		status(http.StatusGatewayTimeout),
		status(http.StatusTooManyRequests),
	}}
	e, meter, waits := newTestExporter(t, r, Config{Retries: 4, RetryBaseDelay: 100 * time.Millisecond, RetryMaxDelay: time.Second})
	if err := e.Export(context.Background(), testBatch); err != nil {
		t.Fatal(err)
	}
	// Retry-Afterに従います。ほかは、基準を1回ごとに2倍にします。
	want := []time.Duration{time.Second, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}
	if r.count() != 5 || !slices.Equal(*waits, want) {
		t.Errorf("requests %d, waits %v; want 5 requests and waits %v", r.count(), *waits, want)
	}
	if got := meter.Count(observertest.Key(telemetry.TraceExportRetries, telemetry.AttrExporter, ExporterName, telemetry.AttrReason, "http_status")); got != 3 {
		t.Errorf("retries for http_status = %d, want 3", got)
	}
	if got := meter.Count(observertest.Key(telemetry.TraceExportRetries, telemetry.AttrExporter, ExporterName, telemetry.AttrReason, "http_throttling")); got != 1 {
		t.Errorf("retries for http_throttling = %d, want 1", got)
	}
}

func TestRetryDelayGrowsExponentiallyWithJitterUpToTheMaximum(t *testing.T) {
	e := New(Config{RetryBaseDelay: 100 * time.Millisecond, RetryMaxDelay: time.Second}, &tracing.Resource{}, nil)
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
		e.jitter = func(time.Duration) time.Duration { return 0 }
		if got := e.retryDelay(tc.retry, retryable.RetryDecision{Retry: true}); got != tc.low {
			t.Errorf("retry %d: lowest delay = %v, want %v", tc.retry, got, tc.low)
		}
		e.jitter = func(n time.Duration) time.Duration { return n }
		if got := e.retryDelay(tc.retry, retryable.RetryDecision{Retry: true}); got != tc.top {
			t.Errorf("retry %d: highest delay = %v, want %v", tc.retry, got, tc.top)
		}
	}
	// 本来の無作為な値も、範囲に収まります。
	e.jitter = randomDuration
	for range 100 {
		if got := e.retryDelay(2, retryable.RetryDecision{Retry: true}); got < 100*time.Millisecond || got > 200*time.Millisecond {
			t.Fatalf("random delay = %v, want from 100ms to 200ms", got)
		}
	}
}

func TestExportStopsAfterTheRetries(t *testing.T) {
	r := &receiver{t: t, responses: []func(http.ResponseWriter){
		status(http.StatusServiceUnavailable), status(http.StatusServiceUnavailable), status(http.StatusServiceUnavailable),
	}}
	e, _, _ := newTestExporter(t, r, Config{Retries: 1, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond})
	err := e.Export(context.Background(), testBatch)
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.Code != http.StatusServiceUnavailable || r.count() != 2 {
		t.Errorf("Export = %v after %d requests, want the 503 after one retry", err, r.count())
	}
	// 一時的な失敗のspanは、failure storeで送り直す扱いです。
	want := traces.SpanFailure{Start: 0, End: 1, Disposition: traces.DispositionRetry, StatusCode: 503, Reason: "http_status"}
	if got := onlyFailure(t, err); !sameFailure(got, want) {
		t.Errorf("failure = %+v, want %+v", got, want)
	}
}

// onlyFailureは、Exportのerrorがただ1つの範囲の失敗であることを確かめて返します。
func onlyFailure(t *testing.T, err error) traces.SpanFailure {
	t.Helper()
	var exportErr *traces.ExportError
	if !errors.As(err, &exportErr) || len(exportErr.Failures) != 1 {
		t.Fatalf("Export = %v, want an ExportError with one failure", err)
	}
	return exportErr.Failures[0]
}

// sameFailureは、Errを除いて失敗を比べます。
func sameFailure(got, want traces.SpanFailure) bool {
	got.Err, want.Err = nil, nil
	return got == want
}

func TestExportDoesNotRetryPermanentFailuresOrFollowRedirects(t *testing.T) {
	// OTLPの仕様は、429・502・503・504のほかの4xx・5xxの送り直しを禁じています。
	for _, code := range []int{
		http.StatusBadRequest, http.StatusUnauthorized, http.StatusRequestTimeout, http.StatusInternalServerError,
		http.StatusTemporaryRedirect,
	} {
		r := &receiver{t: t, responses: []func(http.ResponseWriter){status(code, "Location", "/elsewhere")}}
		e, _, _ := newTestExporter(t, r, Config{Retries: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond})
		err := e.Export(context.Background(), testBatch)
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || statusErr.Code != code || r.count() != 1 {
			t.Errorf("status %d: Export = %v after %d requests, want the status without a retry", code, err, r.count())
		}
		// 受信側が拒否したspanは、送り直さずにdeadとして残す扱いです。
		if got := onlyFailure(t, err); got.Disposition != traces.DispositionDead || got.StatusCode != code {
			t.Errorf("status %d: failure = %+v, want dead with the status", code, got)
		}
	}
}

func TestExportSplitsRequestsThatAreTooLarge(t *testing.T) {
	// 4件のbatchは413で拒否され、2件ずつに分けた前半も413、1件ずつに分けた前半の1件目も
	// 413で拒否されます。1件だけで大きすぎるspanはdeadにし、ほかは送ります。
	r := &receiver{t: t, responses: []func(http.ResponseWriter){
		status(http.StatusRequestEntityTooLarge),
		status(http.StatusRequestEntityTooLarge),
		status(http.StatusRequestEntityTooLarge),
	}}
	e, meter, _ := newTestExporter(t, r, Config{Retries: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond})
	batch := []tracing.SpanData{
		{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{1}, Name: "a"},
		{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{2}, Name: "b"},
		{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{3}, Name: "c"},
		{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{4}, Name: "d"},
	}
	err := e.Export(context.Background(), batch)
	want := traces.SpanFailure{Start: 0, End: 1, Disposition: traces.DispositionDead, StatusCode: 413, Reason: "request_too_large"}
	if got := onlyFailure(t, err); !sameFailure(got, want) {
		t.Errorf("failure = %+v, want %+v", got, want)
	}
	var sent []string
	for _, body := range r.bodies[3:] {
		for _, span := range body.ResourceSpans[0].ScopeSpans[0].Spans {
			sent = append(sent, span.Name)
		}
	}
	if r.count() != 5 || !slices.Equal(sent, []string{"b", "c", "d"}) {
		t.Errorf("%d requests sent %v after the rejections, want b, then c and d", r.count(), sent)
	}
	if got := meter.Count(observertest.Key(telemetry.TraceExportRetries, telemetry.AttrExporter, ExporterName, telemetry.AttrReason, "request_too_large")); got != 2 {
		t.Errorf("splits = %d, want 2", got)
	}
}

func TestExportStopsAfterARequestFailsTemporarily(t *testing.T) {
	// 分けた前半が一時的な失敗で諦められたら、後半は送らずに同じ扱いにします。
	r := &receiver{t: t, responses: []func(http.ResponseWriter){
		status(http.StatusRequestEntityTooLarge),
		status(http.StatusTooManyRequests, "Retry-After", "60"),
	}}
	e, _, waits := newTestExporter(t, r, Config{Retries: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Second})
	batch := []tracing.SpanData{
		{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{1}, Name: "a"},
		{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{2}, Name: "b"},
	}
	err := e.Export(context.Background(), batch)
	var exportErr *traces.ExportError
	if !errors.As(err, &exportErr) || len(exportErr.Failures) != 2 {
		t.Fatalf("Export = %v, want two failures", err)
	}
	// Retry-AfterがRetryMaxDelayより長いため、待たずに諦め、期限をfailure storeへ渡します。
	for i, want := range []traces.SpanFailure{
		{Start: 0, End: 1, Disposition: traces.DispositionRetry, StatusCode: 429, Reason: "http_throttling", RetryAfter: time.Minute},
		{Start: 1, End: 2, Disposition: traces.DispositionRetry, StatusCode: 429, Reason: "http_throttling", RetryAfter: time.Minute},
	} {
		if got := exportErr.Failures[i]; !sameFailure(got, want) {
			t.Errorf("failure %d = %+v, want %+v", i, got, want)
		}
	}
	if r.count() != 2 || len(*waits) != 0 {
		t.Errorf("requests %d, waits %v; want 2 requests without waiting", r.count(), *waits)
	}
}

func TestExportWithACircuitBreakerStopsSendingAfterRepeatedFailures(t *testing.T) {
	r := &receiver{t: t}
	r.responses = make([]func(http.ResponseWriter), 100)
	for i := range r.responses {
		r.responses[i] = status(http.StatusServiceUnavailable)
	}
	e, meter, _ := newTestExporter(t, r, Config{Retries: 0, CircuitBreaker: true})
	if e.breaker == nil {
		t.Fatal("CircuitBreaker did not create a breaker")
	}
	// breakerは、失敗が1分以上続くと開きます。時刻を進められるbreakerに替えます。
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	e.breaker = circuitbreaker.NewOperationGuard(circuitbreaker.OperationGuardOptions{
		CircuitBreaker: retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{Clock: func() time.Time { return now }}),
		Meter:          meter,
		Backend:        BackendName,
		Classify:       classify,
	})
	// 7回目の失敗（60秒後）で開き、30秒の間は送りません。9回目（80秒後）は断られます。
	var err error
	for range 9 {
		err = e.Export(context.Background(), testBatch)
		now = now.Add(10 * time.Second)
	}
	// breakerが開いた後は要求を送らず、spanはfailure storeで送り直す扱いです。
	var openErr *retryable.CircuitBreakerOpenError
	if !errors.As(err, &openErr) {
		t.Fatalf("Export = %v, want the open circuit breaker", err)
	}
	if got := onlyFailure(t, err); got.Disposition != traces.DispositionRetry || got.Reason != "circuit_breaker_open" {
		t.Errorf("failure = %+v, want a retry because the breaker is open", got)
	}
	if r.count() != 7 {
		t.Errorf("requests = %d, want 7 before the breaker opened", r.count())
	}
	if got := meter.Count(observertest.Key(telemetry.CircuitBreakerRejectedRequests, telemetry.AttrBackend, BackendName)); got == 0 {
		t.Errorf("rejected requests = %d, want some", got)
	}
}

func TestExportReportsPartialSuccessWithoutRetrying(t *testing.T) {
	response, err := proto.Marshal(&collectortrace.ExportTraceServiceResponse{
		PartialSuccess: &collectortrace.ExportTracePartialSuccess{RejectedSpans: 1, ErrorMessage: "invalid span"},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := &receiver{t: t, responses: []func(http.ResponseWriter){func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(response)
	}}}
	e, _, _ := newTestExporter(t, r, Config{Retries: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond})
	err = e.Export(context.Background(), testBatch)
	// どのspanが受け付けられたか分からないため、quarantineとして残す扱いです。
	want := traces.SpanFailure{Start: 0, End: 1, Disposition: traces.DispositionQuarantine, Rejected: 1, Reason: "record_rejected"}
	if got := onlyFailure(t, err); !sameFailure(got, want) || r.count() != 1 {
		t.Errorf("failure = %+v after %d requests, want %+v without a retry", got, r.count(), want)
	}
}

func TestExportTimesOutEachRequest(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	r := &receiver{t: t, responses: []func(http.ResponseWriter){
		func(http.ResponseWriter) { <-release },
	}}
	e, meter, _ := newTestExporter(t, r, Config{Timeout: 50 * time.Millisecond, Retries: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond})
	if err := e.Export(context.Background(), testBatch); err != nil {
		t.Fatalf("Export = %v, want success on the retry after the timeout", err)
	}
	if r.count() != 2 || meter.Count(observertest.Key(telemetry.TraceExportRetries,
		telemetry.AttrExporter, ExporterName, telemetry.AttrReason, "ctx_deadline_exceeded")) != 1 {
		t.Errorf("requests = %d, counters %v; want a retry after the timeout", r.count(), meter.Counters())
	}
}

func TestExportRetriesWhenItCannotConnect(t *testing.T) {
	r := &receiver{t: t}
	e, _, waits := newTestExporter(t, r, Config{Retries: 2, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond})
	// 受信側を止め、接続を拒否させます。
	server := httptest.NewServer(r)
	e.cfg.Endpoint = server.URL
	server.Close()
	if err := e.Export(context.Background(), testBatch); err == nil {
		t.Fatal("Export = nil, want the connection error")
	}
	if len(*waits) != 2 {
		t.Errorf("waits = %v, want two retries", *waits)
	}
}
