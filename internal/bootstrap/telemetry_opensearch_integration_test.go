package bootstrap_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// openSearchSpanTemplatePathは、文書で案内するspanのindex templateです。試験は、この
// templateを登録してから書き、文書の手順どおりに動くことを確かめます。
const openSearchSpanTemplatePath = "../../docs/opensearch/otel-v1-apm-span-index-template.json"

// openSearchTraceSettingsは、OpenSearchの結合テストの接続先を読み、spanのindex templateを
// 登録します。
func openSearchTraceSettings(t *testing.T) string {
	t.Helper()
	settings := testutil.RequireIntegrationEnvironment(t, "OPENSEARCH_INTEGRATION_TEST")
	endpoint := strings.TrimRight(strings.TrimSpace(settings["HETEROLOG_OPENSEARCH_ENDPOINT"]), "/")
	if endpoint == "" {
		t.Fatal("the OpenSearch integration test needs HETEROLOG_OPENSEARCH_ENDPOINT in the dedicated environment file")
	}
	template, err := os.ReadFile(openSearchSpanTemplatePath)
	if err != nil {
		t.Fatal(err)
	}
	status, body := openSearchRequest(t, http.MethodPut, endpoint+"/_index_template/otel-v1-apm-span", template)
	if status != http.StatusOK {
		t.Fatalf("install the span index template: status %d: %s", status, body)
	}
	return endpoint
}

// openSearchTelemetryConfigは、spanだけをOpenSearchのindexへ書く設定です。結合テストの
// OpenSearchは認証なしで動きます。
func openSearchTelemetryConfig(endpoint, index string) pkgtelemetry.Config {
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Resource.ServiceName = "heterolog-it-opensearch"
	cfg.Traces.Exporters = []pkgtelemetry.TraceExporter{pkgtelemetry.TraceExporterOpenSearch}
	cfg.Traces.ExportInterval = 10 * time.Millisecond
	cfg.Traces.OpenSearch.Client.Endpoint = endpoint
	cfg.Traces.OpenSearch.Client.AuthConfig = pkgopensearch.AuthConfig{Type: pkgopensearch.AuthTypeNone}
	cfg.Traces.OpenSearch.Index = index
	return cfg
}

func openSearchRequest(t *testing.T, method, url string, body []byte) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, string(raw)
}

// waitForOpenSearchSpansは、indexからtraceのspanを読み、want件になるまで待ちます。
func waitForOpenSearchSpans(t *testing.T, endpoint, index, traceID string, want int) []map[string]any {
	t.Helper()
	query := []byte(`{"size":100,"query":{"term":{"traceId":"` + traceID + `"}}}`)
	deadline := time.Now().Add(30 * time.Second)
	var body string
	for {
		_, _ = openSearchRequest(t, http.MethodPost, endpoint+"/"+index+"/_refresh", nil)
		var status int
		status, body = openSearchRequest(t, http.MethodPost, endpoint+"/"+index+"/_search", query)
		var result struct {
			Hits struct {
				Hits []struct {
					Source map[string]any `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if status == http.StatusOK && json.Unmarshal([]byte(body), &result) == nil && len(result.Hits.Hits) == want {
			docs := make([]map[string]any, len(result.Hits.Hits))
			for i, hit := range result.Hits.Hits {
				docs[i] = hit.Source
			}
			return docs
		}
		if time.Now().After(deadline) {
			t.Fatalf("OpenSearch did not return %d spans of trace %s; last response: %.2000s", want, traceID, body)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func TestTelemetryWritesSpansToOpenSearch(t *testing.T) {
	endpoint := openSearchTraceSettings(t)
	index := "otel-v1-apm-span-heterolog-it-" + strings.ToLower(testRun())
	ctx := context.Background()
	tel, err := bootstrap.NewTelemetry(ctx, openSearchTelemetryConfig(endpoint, index))
	if err != nil {
		t.Fatal(err)
	}
	spanCtx, parent := pkgobserver.StartSpanWithOptions(ctx, tel.Tracer(), "heterolog.it.request",
		pkgobserver.StartOptions{Kind: pkgobserver.SpanKindServer},
		pkgobserver.Attr{Key: "http.request.method", Value: "GET"},
		pkgobserver.Attr{Key: "heterolog.it.attempt", Value: 1},
	)
	_, child := tel.Tracer().StartSpan(spanCtx, "heterolog.it.query")
	child.RecordError(errors.New("integration error"))
	child.End()
	parent.End()
	traceID := string(parent.TraceID())
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	docs := waitForOpenSearchSpans(t, endpoint, index, traceID, 2)
	var root map[string]any
	for _, doc := range docs {
		if doc["parentSpanId"] == "" {
			root = doc
		}
	}
	if root == nil {
		t.Fatalf("spans = %v, want a root span", docs)
	}
	for key, want := range map[string]any{
		"name": "heterolog.it.request", "kind": "SPAN_KIND_SERVER", "serviceName": "heterolog-it-opensearch",
		"traceGroup": "heterolog.it.request", "span.attributes.http@request@method": "GET",
		"resource.attributes.service@name": "heterolog-it-opensearch",
	} {
		if root[key] != want {
			t.Errorf("root span %s = %v, want %v", key, root[key], want)
		}
	}
	// templateのとおり、時刻はdate_nanos、属性はkeywordとして扱われます。
	_, mapping := openSearchRequest(t, http.MethodGet, endpoint+"/"+index+"/_mapping/field/startTime,span.attributes.heterolog@it@attempt", nil)
	if !strings.Contains(mapping, `"date_nanos"`) || !strings.Contains(mapping, `"keyword"`) {
		t.Errorf("mapping = %s, want date_nanos times and keyword attributes", mapping)
	}
}

func TestTelemetryResendsSpooledSpansToOpenSearch(t *testing.T) {
	endpoint := openSearchTraceSettings(t)
	index := "otel-v1-apm-span-heterolog-it-spool-" + strings.ToLower(testRun())
	spoolDir := t.TempDir()
	newConfig := func(endpoint string) pkgtelemetry.Config {
		cfg := openSearchTelemetryConfig(endpoint, index)
		cfg.Traces.OpenSearch.Retries = 0
		cfg.Traces.OpenSearch.FailureStore.Store.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeSpool}
		cfg.Traces.OpenSearch.FailureStore.Store.DurableStoreConfig.SpoolConfig = pkgdurable.SpoolConfig{
			SpoolPath: spoolDir, Target: "heterolog-it", Dir: "traces_opensearch",
			OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
				OverloadPolicy: pkgdurable.OverloadPolicyDrop, SpillDestination: pkgdurable.SpillDestinationStdout,
				MaxDirSize: 1 << 30, StatsScanInterval: time.Second, MaxCallsSinceScan: 100,
			},
		}
		return cfg
	}
	ctx := context.Background()
	// 接続できない受信先へ送ったspanは、spoolのOTLP/JSONのrecordに残ります。
	tel, err := bootstrap.NewTelemetry(ctx, newConfig("http://127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	_, span := tel.Tracer().StartSpan(ctx, "heterolog.it.spooled")
	span.End()
	traceID := string(span.TraceID())
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	// 同じspoolを使って再び始めると、OpenSearchへ送り直します。
	tel, err = bootstrap.NewTelemetry(ctx, newConfig(endpoint))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	docs := waitForOpenSearchSpans(t, endpoint, index, traceID, 1)
	if docs[0]["name"] != "heterolog.it.spooled" || docs[0]["serviceName"] != "heterolog-it-opensearch" {
		t.Errorf("resent span = %v", docs[0])
	}
}
