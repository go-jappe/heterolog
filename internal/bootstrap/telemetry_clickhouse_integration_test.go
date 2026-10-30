package bootstrap_test

import (
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
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// clickHouseTracesTablePathは、文書で案内するspanの表の定義です。試験は、この定義で表を
// 作ってから書き、文書の手順どおりに動くことを確かめます。
const clickHouseTracesTablePath = "../../docs/clickhouse/otel_traces.sql"

// clickHouseTraceSettingsは、ClickHouseの結合テストの接続先と、試験の表の名前です。
type clickHouseTraceSettings struct {
	endpoint, username, password, table string
}

// newClickHouseTraceTableは、接続先を読み、spanの表を、試験ごとの名前で作ります。
func newClickHouseTraceTable(t *testing.T) clickHouseTraceSettings {
	t.Helper()
	settings := testutil.RequireIntegrationEnvironment(t, "CLICKHOUSE_INTEGRATION_TEST")
	s := clickHouseTraceSettings{
		endpoint: strings.TrimSpace(settings["HETEROLOG_CLICKHOUSE_ENDPOINT"]),
		username: settings["HETEROLOG_CLICKHOUSE_USERNAME"],
		password: settings["HETEROLOG_CLICKHOUSE_PASSWORD"],
		table:    "otel_traces_it_" + strings.ToLower(testRun()),
	}
	if s.endpoint == "" {
		t.Fatal("the ClickHouse integration test needs HETEROLOG_CLICKHOUSE_ENDPOINT in the dedicated environment file")
	}
	ddl, err := os.ReadFile(clickHouseTracesTablePath)
	if err != nil {
		t.Fatal(err)
	}
	create := strings.Replace(string(ddl), "otel_traces", "default."+s.table, 1)
	if _, err := s.query(create); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.query("DROP TABLE IF EXISTS default." + s.table) })
	return s
}

func (s clickHouseTraceSettings) query(query string) (string, error) {
	req, err := http.NewRequest(http.MethodPost, s.endpoint, strings.NewReader(query))
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(s.username, s.password)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", errors.New(resp.Status + ": " + string(body))
	}
	return string(body), nil
}

func (s clickHouseTraceSettings) telemetryConfig(endpoint string) pkgtelemetry.Config {
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Resource.ServiceName = "heterolog-it-clickhouse"
	cfg.Traces.Exporters = []pkgtelemetry.TraceExporter{pkgtelemetry.TraceExporterClickHouse}
	cfg.Traces.ExportInterval = 10 * time.Millisecond
	cfg.Traces.ClickHouse.Client.Endpoint = endpoint
	cfg.Traces.ClickHouse.Client.Username = s.username
	cfg.Traces.ClickHouse.Client.Password = s.password
	cfg.Traces.ClickHouse.Client.Table = s.table
	return cfg
}

// waitForClickHouseSpansは、表からtraceの行を読み、want行になるまで待ちます。
func (s clickHouseTraceSettings) waitForSpans(t *testing.T, traceID string, want int) []map[string]any {
	t.Helper()
	query := "SELECT SpanName, SpanKind, ServiceName, ParentSpanId, SpanAttributes, StatusCode, Events.Name, toString(Timestamp) AS Time " +
		"FROM default." + s.table + " WHERE TraceId = '" + traceID + "' ORDER BY Timestamp FORMAT JSONEachRow"
	deadline := time.Now().Add(30 * time.Second)
	var body string
	for {
		var err error
		body, err = s.query(query)
		if err == nil {
			var rows []map[string]any
			for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
				var row map[string]any
				if json.Unmarshal([]byte(line), &row) == nil {
					rows = append(rows, row)
				}
			}
			if len(rows) == want {
				return rows
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("ClickHouse did not return %d spans of trace %s; last response: %.2000s (%v)", want, traceID, body, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func TestTelemetryInsertsSpansIntoClickHouse(t *testing.T) {
	s := newClickHouseTraceTable(t)
	ctx := context.Background()
	tel, err := bootstrap.NewTelemetry(ctx, s.telemetryConfig(s.endpoint))
	if err != nil {
		t.Fatal(err)
	}
	spanCtx, parent := pkgobserver.StartSpanWithOptions(ctx, tel.Tracer(), "heterolog.it.request",
		pkgobserver.StartOptions{Kind: pkgobserver.SpanKindServer},
		pkgobserver.Attr{Key: "http.request.method", Value: "GET"},
	)
	_, child := tel.Tracer().StartSpan(spanCtx, "heterolog.it.query")
	child.RecordError(errors.New("integration error"))
	child.End()
	parent.End()
	traceID := string(parent.TraceID())
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	rows := s.waitForSpans(t, traceID, 2)
	root, query := rows[0], rows[1]
	if root["SpanName"] != "heterolog.it.request" || root["SpanKind"] != "Server" || root["ParentSpanId"] != "" ||
		root["ServiceName"] != "heterolog-it-clickhouse" {
		t.Errorf("root span = %v", root)
	}
	if attrs, _ := root["SpanAttributes"].(map[string]any); attrs["http.request.method"] != "GET" {
		t.Errorf("root span attributes = %v", root["SpanAttributes"])
	}
	if query["StatusCode"] != "Error" || !strings.Contains(jsonString(query["Events.Name"]), "exception") {
		t.Errorf("child span = %v, want the error and its exception event", query)
	}
	// 時刻は、UTCのnanosecondまで保たれます。
	if got, _ := root["Time"].(string); len(got) != len("2006-01-02 15:04:05.000000000") {
		t.Errorf("timestamp = %q, want nanoseconds", got)
	}
}

func TestTelemetryResendsSpooledSpansToClickHouse(t *testing.T) {
	s := newClickHouseTraceTable(t)
	spoolDir := t.TempDir()
	newConfig := func(endpoint string) pkgtelemetry.Config {
		cfg := s.telemetryConfig(endpoint)
		cfg.Traces.ClickHouse.Retries = 0
		cfg.Traces.ClickHouse.FailureStore.Store.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeSpool}
		cfg.Traces.ClickHouse.FailureStore.Store.DurableStoreConfig.SpoolConfig = pkgdurable.SpoolConfig{
			SpoolPath: spoolDir, Target: "heterolog-it", Dir: "traces_clickhouse",
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
	// 同じspoolを使って再び始めると、ClickHouseへ送り直します。
	tel, err = bootstrap.NewTelemetry(ctx, newConfig(s.endpoint))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	rows := s.waitForSpans(t, traceID, 1)
	if rows[0]["SpanName"] != "heterolog.it.spooled" || rows[0]["ServiceName"] != "heterolog-it-clickhouse" {
		t.Errorf("resent span = %v", rows[0])
	}
}

func jsonString(v any) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}
