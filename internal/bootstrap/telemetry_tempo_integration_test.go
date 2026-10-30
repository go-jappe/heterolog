package bootstrap_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// TestTelemetrySendsSpansToTempoOverOTLPHTTPは、NewTelemetryがOTLP/HTTPで送ったspanを、
// 実際のGrafana Tempoが受け付け、HTTP APIで読み戻せることを確かめます。送信先と問い合わせ先は、
// 専用の設定fileの値だけを使います。
func TestTelemetrySendsSpansToTempoOverOTLPHTTP(t *testing.T) {
	settings := testutil.RequireIntegrationEnvironment(t, "TEMPO_INTEGRATION_TEST")
	endpoint := strings.TrimSpace(settings["HETEROLOG_TRACES_OTLP_ENDPOINT"])
	queryAddr := strings.TrimSpace(settings["HETEROLOG_OTEL_TEMPO_QUERY_ADDR"])
	if endpoint == "" || queryAddr == "" {
		t.Fatal("the Tempo integration test needs HETEROLOG_TRACES_OTLP_ENDPOINT and HETEROLOG_OTEL_TEMPO_QUERY_ADDR in the dedicated environment file")
	}

	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Resource.ServiceName = "heterolog-it-telemetry"
	cfg.Traces.Exporters = []pkgtelemetry.TraceExporter{pkgtelemetry.TraceExporterOTLP}
	cfg.Traces.OTLP.Endpoint = endpoint
	meter := observertest.NewMeter()
	ctx := context.Background()
	tel, err := bootstrap.NewTelemetry(ctx, cfg, loggerruntime.WithObservers(nil, meter))
	if err != nil {
		t.Fatal(err)
	}

	spanCtx, parent := pkgobserver.StartSpanWithOptions(ctx, tel.Tracer(), "heterolog.it.request",
		pkgobserver.StartOptions{Kind: pkgobserver.SpanKindServer},
		pkgobserver.Attr{Key: "heterolog.it.attempt", Value: 1},
	)
	_, child := tel.Tracer().StartSpan(spanCtx, "heterolog.it.query")
	child.RecordError(errors.New("integration error"))
	child.End()
	parent.End()
	traceID := string(parent.TraceID())

	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := tel.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if got := meter.Count(observertest.Key(telemetry.TraceSpans,
		telemetry.AttrExporter, "otlp", telemetry.AttrOutcome, telemetry.OutcomeSuccess)); got != 2 {
		t.Fatalf("spans accepted by Tempo = %d, want 2; counters %v", got, meter.Counters())
	}

	// Tempoは、受け付けたspanをすぐには検索できないことがあるため、読み戻せるまで待ちます。
	url := "http://" + queryAddr + "/api/traces/" + traceID
	deadline := time.Now().Add(60 * time.Second)
	var body string
	for {
		body = fetchTempoTrace(t, url)
		if strings.Contains(body, "heterolog.it.request") && strings.Contains(body, "heterolog.it.query") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Tempo did not return trace %s with both spans; last response: %.2000s", traceID, body)
		}
		time.Sleep(time.Second)
	}
	for _, want := range []string{"heterolog-it-telemetry", "integration error", "SPAN_KIND_SERVER"} {
		if !strings.Contains(body, want) {
			t.Errorf("Tempo trace does not contain %q: %.2000s", want, body)
		}
	}
}

// fetchTempoTraceは、TempoのHTTP APIからtraceを読みます。まだ見つからない場合は空を返します。
func fetchTempoTrace(t *testing.T, url string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Logf("query Tempo: %v", err)
		return ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || resp.StatusCode != http.StatusOK {
		return ""
	}
	return string(body)
}
