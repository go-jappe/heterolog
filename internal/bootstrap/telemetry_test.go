package bootstrap_test

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// otlpReceiverは、受け取ったspanを名前で引けるように保持するOTLP/HTTPの受信側です。
type otlpReceiver struct {
	t     *testing.T
	mu    sync.Mutex
	spans map[string]*tracepb.Span
	// resourcesは、spanの名前ごとのResourceのservice.nameです。
	resources map[string]string
}

func newOTLPReceiver(t *testing.T) (*otlpReceiver, string) {
	r := &otlpReceiver{t: t, spans: map[string]*tracepb.Span{}, resources: map[string]string{}}
	server := httptest.NewServer(r)
	t.Cleanup(server.Close)
	return r, server.URL + "/v1/traces"
}

func (r *otlpReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	zr, err := gzip.NewReader(req.Body)
	if err != nil {
		r.t.Errorf("gzip body: %v", err)
		return
	}
	raw, _ := io.ReadAll(zr)
	var body collectortrace.ExportTraceServiceRequest
	if err := proto.Unmarshal(raw, &body); err != nil {
		r.t.Errorf("unmarshal: %v", err)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rs := range body.ResourceSpans {
		service := ""
		for _, kv := range rs.Resource.GetAttributes() {
			if kv.Key == "service.name" {
				service = kv.Value.GetStringValue()
			}
		}
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				r.spans[span.Name] = span
				r.resources[span.Name] = service
			}
		}
	}
}

func (r *otlpReceiver) span(name string) (*tracepb.Span, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.spans[name], r.resources[name]
}

func otlpTelemetryConfig(endpoint string) pkgtelemetry.Config {
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Resource.ServiceName = "checkout"
	cfg.Traces.Exporters = []pkgtelemetry.TraceExporter{pkgtelemetry.TraceExporterOTLP}
	cfg.Traces.OTLP.Endpoint = endpoint
	return cfg
}

func TestTelemetryDeliversApplicationAndLoggerSpansOverOTLP(t *testing.T) {
	receiver, endpoint := newOTLPReceiver(t)
	meter := observertest.NewMeter()
	ctx := context.Background()
	tel, err := bootstrap.NewTelemetry(ctx, otlpTelemetryConfig(endpoint), loggerruntime.WithObservers(nil, meter))
	if err != nil {
		t.Fatal(err)
	}

	testutil.CaptureStdout(t, func() {
		reqCtx, request := tel.Tracer().StartSpan(ctx, "GET /checkout")
		logger, err := bootstrap.New(reqCtx, bootstrap.DefaultConfig(), loggerruntime.WithObservers(tel.Tracer(), nil))
		if err != nil {
			t.Fatal(err)
		}
		if err := logger.Info(reqCtx, "checkout"); err != nil {
			t.Fatal(err)
		}
		if err := logger.Close(); err != nil {
			t.Fatal(err)
		}
		request.End()
	})
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	request, service := receiver.span("GET /checkout")
	if request == nil || service != "checkout" {
		t.Fatalf("request span = %v with service %q, want it with the configured resource", request, service)
	}
	// loggerの作成のspanは、contextの利用者のspanの子になります。
	create, _ := receiver.span("heterolog.stdout.create")
	if create == nil || string(create.TraceId) != string(request.TraceId) || string(create.ParentSpanId) != string(request.SpanId) {
		t.Errorf("logger create span = %v, want a child of the request span", create)
	}
	if got := meter.CountMatching(telemetry.TraceSpans, telemetry.AttrExporter, "otlp", telemetry.AttrOutcome, telemetry.OutcomeSuccess); got < 2 {
		t.Errorf("successful spans = %d, want at least the request and the logger span", got)
	}
}

func TestTelemetryWritesSpansToAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traces", "spans.jsonl")
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Traces.Exporters = []pkgtelemetry.TraceExporter{pkgtelemetry.TraceExporterFile}
	cfg.Traces.File.Path = path
	tel, err := bootstrap.NewTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, span := tel.Tracer().StartSpan(context.Background(), "job")
	span.End()
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), `"name":"job"`) || strings.Count(string(content), "\n") != 1 {
		t.Errorf("file = %s, want one OTLP JSON line with the span", content)
	}
}

func TestTelemetryWithoutExportersRecordsNothing(t *testing.T) {
	tel, err := bootstrap.NewTelemetry(context.Background(), bootstrap.DefaultTelemetryConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tel.Tracer().(observer.NopTracer); !ok {
		t.Errorf("Tracer = %T, want the no-op tracer", tel.Tracer())
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
}

func TestNewTelemetryRejectsInvalidInputs(t *testing.T) {
	if _, err := bootstrap.NewTelemetry(nil, bootstrap.DefaultTelemetryConfig()); !errors.Is(err, loggerruntime.ErrNilContext) {
		t.Errorf("nil context: err = %v, want ErrNilContext", err)
	}
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Traces.Exporters = []pkgtelemetry.TraceExporter{pkgtelemetry.TraceExporterOTLP}
	cfg.Traces.OTLP.Timeout = 0
	if _, err := bootstrap.NewTelemetry(context.Background(), cfg); !errors.Is(err, pkgtelemetry.ErrNonPositiveSetting) {
		t.Errorf("invalid config: err = %v, want the validation error", err)
	}
	// fileを開けない場合は、先に作った送り先を閉じてerrorを返します。
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg = otlpTelemetryConfig("http://127.0.0.1:1/v1/traces")
	cfg.Traces.Exporters = append(cfg.Traces.Exporters, pkgtelemetry.TraceExporterFile)
	cfg.Traces.File.Path = filepath.Join(blocker, "spans.jsonl")
	if _, err := bootstrap.NewTelemetry(context.Background(), cfg); err == nil {
		t.Error("NewTelemetry with an unusable file path succeeded")
	}
}

func TestTelemetrySpansFollowAnIncomingTraceParent(t *testing.T) {
	receiver, endpoint := newOTLPReceiver(t)
	cfg := otlpTelemetryConfig(endpoint)
	// 新しいtraceは記録しない設定でも、記録すると決めた呼び出し元のtraceは記録します。
	cfg.Traces.SamplingRatio = 0
	tel, err := bootstrap.NewTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	incoming, err := pkgobserver.ParseTraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := pkgobserver.StartSpanWithOptions(pkgobserver.ContextWithSpanContext(context.Background(), incoming),
		tel.Tracer(), "GET /items", pkgobserver.StartOptions{Kind: pkgobserver.SpanKindServer})
	outgoing := pkgobserver.SpanContextFromContext(ctx).TraceParent()
	span.End()
	_, unsampled := tel.Tracer().StartSpan(context.Background(), "not recorded")
	unsampled.End()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	got, _ := receiver.span("GET /items")
	if got == nil || got.Kind != tracepb.Span_SPAN_KIND_SERVER || got.ParentSpanId[0] != 0x00 || got.ParentSpanId[1] != 0xf0 {
		t.Fatalf("server span = %v, want a server span under the incoming parent", got)
	}
	if !strings.HasPrefix(outgoing, "00-4bf92f3577b34da6a3ce929d0e0e4736-") || !strings.HasSuffix(outgoing, "-01") {
		t.Errorf("outgoing traceparent = %q, want the same trace, sampled", outgoing)
	}
	if notRecorded, _ := receiver.span("not recorded"); notRecorded != nil {
		t.Error("a new trace was recorded with the sampling ratio 0")
	}
}
