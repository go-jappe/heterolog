package opensearch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/retrying"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

var (
	testStart = time.Date(2026, 10, 30, 1, 2, 3, 123456789, time.UTC)
	testEnd   = testStart.Add(1500 * time.Microsecond)
)

func TestDocumentHasTheShapeOfDataPrepper(t *testing.T) {
	span := tracing.SpanData{
		TraceID: tracing.TraceID{0x0a, 15: 0x01}, SpanID: tracing.SpanID{0x0b, 7: 0x02},
		Name: "GET /orders", Kind: pkgobserver.SpanKindServer, StartTime: testStart, EndTime: testEnd,
		Attrs:  []pkgobserver.Attr{{Key: "http.method", Value: "GET"}, {Key: "http.status_code", Value: 500}, {Key: "ids", Value: []int64{1, 2}}},
		Events: []tracing.Event{{Name: "exception", Time: testStart.Add(time.Millisecond), Attrs: []pkgobserver.Attr{{Key: "exception.message", Value: "boom"}}}},
		Links:  []tracing.Link{{TraceID: tracing.TraceID{0x0c}, SpanID: tracing.SpanID{0x0d}, DroppedAttrs: 1}},
		Status: pkgobserver.StatusError, StatusMessage: "failed", DroppedAttrs: 2,
		Scope: tracing.Scope{Name: "orders", Version: "1.0.0"},
	}
	resource := &tracing.Resource{Attrs: []pkgobserver.Attr{{Key: "service.name", Value: "orders-api"}, {Key: "host.name", Value: "h1"}}}
	raw, err := json.Marshal(document(span, resource))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"traceId":                "0a000000000000000000000000000001",
		"spanId":                 "0b00000000000002",
		"parentSpanId":           "",
		"traceState":             "",
		"name":                   "GET /orders",
		"kind":                   "SPAN_KIND_SERVER",
		"startTime":              "2026-10-30T01:02:03.123456789Z",
		"endTime":                "2026-10-30T01:02:03.124956789Z",
		"durationInNanos":        1500000.0,
		"serviceName":            "orders-api",
		"status.code":            2.0,
		"status.message":         "failed",
		"droppedAttributesCount": 2.0,
		"droppedEventsCount":     0.0,
		"droppedLinksCount":      0.0,
		"traceGroup":             "GET /orders",
		"traceGroupFields": map[string]any{
			"endTime": "2026-10-30T01:02:03.124956789Z", "durationInNanos": 1500000.0, "statusCode": 2.0,
		},
		"instrumentationScope.name":        "orders",
		"instrumentationScope.version":     "1.0.0",
		"span.attributes.http@method":      "GET",
		"span.attributes.http@status_code": 500.0,
		"span.attributes.ids":              "[1,2]",
		"resource.attributes.service@name": "orders-api",
		"resource.attributes.host@name":    "h1",
		"events": []any{map[string]any{
			"name": "exception", "time": "2026-10-30T01:02:03.124456789Z",
			"attributes": map[string]any{"exception@message": "boom"}, "droppedAttributesCount": 0.0,
		}},
		"links": []any{map[string]any{
			"traceId": "0c000000000000000000000000000000", "spanId": "0d00000000000000", "traceState": "",
			"attributes": map[string]any{}, "droppedAttributesCount": 1.0,
		}},
	}
	if !reflect.DeepEqual(got, want) {
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		t.Errorf("document =\n%s", gotJSON)
	}
	if id := documentID(span); id != "0a0000000000000000000000000000010b00000000000002" {
		t.Errorf("documentID = %q", id)
	}
}

func TestDocumentOfAChildSpanHasNoTraceGroup(t *testing.T) {
	span := tracing.SpanData{
		TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{2}, ParentSpanID: tracing.SpanID{3},
		Name: "child", StartTime: testStart, EndTime: testEnd,
		Resource: &tracing.Resource{Attrs: []pkgobserver.Attr{{Key: "service.name", Value: "own"}}},
	}
	doc := document(span, &tracing.Resource{Attrs: []pkgobserver.Attr{{Key: "service.name", Value: "default"}}})
	if _, ok := doc["traceGroup"]; ok {
		t.Errorf("traceGroup = %v, want none for a child span", doc["traceGroup"])
	}
	if doc["parentSpanId"] != "0300000000000000" || doc["kind"] != "SPAN_KIND_INTERNAL" || doc["serviceName"] != "own" {
		t.Errorf("document = %v, want the parent, the internal kind and the resource of the span", doc)
	}
}

// fakeClientは、Bulk APIの応答を決めて返すclientです。
type fakeClient struct {
	requests [][]pkgopensearch.DocumentWriteRequest
	respond  func(docs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error)
}

func (c *fakeClient) IndexDocument(context.Context, pkgopensearch.DocumentWriteRequest) (*pkgopensearch.IndexResponse, error) {
	return nil, errors.New("not used")
}

func (c *fakeClient) IndexDocuments(_ context.Context, docs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
	c.requests = append(c.requests, docs)
	return c.respond(docs)
}

// bulkResponseは、docsへの応答を、statusesのstatusで作ります。201は作成、409は既に
// あった文書です。
func bulkResponse(docs []pkgopensearch.DocumentWriteRequest, statuses map[string]int) *pkgopensearch.BulkResponse {
	resp := &pkgopensearch.BulkResponse{}
	for _, doc := range docs {
		result := pkgopensearch.BulkItemResult{Index: doc.Index, ID: doc.ID, Status: http.StatusCreated, Result: "created"}
		if status, ok := statuses[doc.ID]; ok {
			result = pkgopensearch.BulkItemResult{Index: doc.Index, ID: doc.ID, Status: status, Result: ""}
			switch status {
			case http.StatusTooManyRequests:
				result.Error = &pkgopensearch.ErrorDetail{Type: "es_rejected_execution_exception", Reason: "queue full"}
			case http.StatusBadRequest:
				result.Error = &pkgopensearch.ErrorDetail{Type: pkgopensearch.ErrorTypeMapperParsing, Reason: "bad field"}
			case http.StatusNotFound:
				result.Error = &pkgopensearch.ErrorDetail{Type: "index_not_found_exception", Reason: "no index"}
			case http.StatusConflict:
				result.Error = &pkgopensearch.ErrorDetail{Type: "version_conflict_engine_exception", Reason: "exists"}
			}
			resp.Errors = true
		}
		resp.Items = append(resp.Items, map[string]pkgopensearch.BulkItemResult{"create": result})
	}
	return resp
}

func testSpans(n int) []tracing.SpanData {
	spans := make([]tracing.SpanData, n)
	for i := range spans {
		spans[i] = tracing.SpanData{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{byte(i + 1)}, Name: "op", StartTime: testStart, EndTime: testEnd}
	}
	return spans
}

func newTestExporter(client *fakeClient, retries int) (*retrying.Exporter[tracing.SpanData], *observertest.Meter) {
	meter := observertest.NewMeter()
	return NewExporter(client, Config{Index: "otel-v1-apm-span-test", Retry: retrying.Config{
		Retries: retries, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
		Sleep: func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil },
	}}, &tracing.Resource{}, meter), meter
}

func TestExportResendsOnlyTheDocumentsThatFailedTemporarily(t *testing.T) {
	spans := testSpans(4)
	ids := make([]string, len(spans))
	for i, span := range spans {
		ids[i] = documentID(span)
	}
	client := &fakeClient{}
	client.respond = func(docs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
		if len(client.requests) == 1 {
			// 1件目は既にあり、2件目は混んでいて断られ、3件目は内容の不正、4件目はindexがありません。
			return bulkResponse(docs, map[string]int{ids[0]: 409, ids[1]: 429, ids[2]: 400, ids[3]: 404}), nil
		}
		return bulkResponse(docs, nil), nil
	}
	e, meter := newTestExporter(client, 2)
	err := e.Export(context.Background(), spans)
	var exportErr *delivery.ExportError
	if !errors.As(err, &exportErr) {
		t.Fatalf("Export = %v, want an *ExportError", err)
	}
	var got []delivery.Failure
	for _, f := range exportErr.Failures {
		got = append(got, delivery.Failure{Start: f.Start, End: f.End, Disposition: f.Disposition, StatusCode: f.StatusCode, Reason: f.Reason})
	}
	want := []delivery.Failure{
		{Start: 2, End: 3, Disposition: delivery.DispositionQuarantine, StatusCode: 400, Reason: "record_rejected"},
		{Start: 3, End: 4, Disposition: delivery.DispositionDead, StatusCode: 404, Reason: "http_status"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("failures = %+v, want %+v", got, want)
	}
	if len(client.requests) != 2 || len(client.requests[1]) != 1 || client.requests[1][0].ID != ids[1] {
		t.Errorf("requests = %v, want the throttled span sent again alone", client.requests)
	}
	first := client.requests[0][0]
	if first.Index != "otel-v1-apm-span-test" || first.Op != pkgopensearch.WriteOpCreate || first.ID != ids[0] {
		t.Errorf("document request = %+v, want a create with the trace and span IDs", first)
	}
	if got := meter.Count(observertest.Key(telemetry.TraceExportRetries, telemetry.AttrExporter, ExporterName, telemetry.AttrReason, "http_throttling")); got != 1 {
		t.Errorf("retries = %d, want 1", got)
	}
}

func TestExportClassifiesRequestFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		requests int
		want     delivery.Disposition
	}{
		{"unavailable is resent", &pkgopensearch.HTTPStatusError{Code: 503}, 3, delivery.DispositionRetry},
		{"forbidden is dead", &pkgopensearch.HTTPStatusError{Code: 403}, 1, delivery.DispositionDead},
		{"unreadable response is resent from the spool", &pkgopensearch.ResponseDecodeError{Operation: "bulk", Err: errors.New("bad json")}, 1, delivery.DispositionRetry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeClient{respond: func([]pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) { return nil, tc.err }}
			e, _ := newTestExporter(client, 2)
			err := e.Export(context.Background(), testSpans(1))
			var exportErr *delivery.ExportError
			if !errors.As(err, &exportErr) || len(exportErr.Failures) != 1 || exportErr.Failures[0].Disposition != tc.want {
				t.Fatalf("Export = %v, want one failure with disposition %v", err, tc.want)
			}
			if len(client.requests) != tc.requests {
				t.Errorf("requests = %d, want %d", len(client.requests), tc.requests)
			}
		})
	}
}

func TestExportSplitsARequestThatIsTooLarge(t *testing.T) {
	client := &fakeClient{}
	client.respond = func(docs []pkgopensearch.DocumentWriteRequest) (*pkgopensearch.BulkResponse, error) {
		if len(docs) > 1 {
			return nil, &pkgopensearch.HTTPStatusError{Code: http.StatusRequestEntityTooLarge}
		}
		return bulkResponse(docs, nil), nil
	}
	e, _ := newTestExporter(client, 0)
	if err := e.Export(context.Background(), testSpans(3)); err != nil {
		t.Fatalf("Export = %v, want every span written after splitting", err)
	}
	if len(client.requests) != 5 {
		t.Errorf("requests = %d, want 5 (3, 1, 2, 1, 1)", len(client.requests))
	}
}
