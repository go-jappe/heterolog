package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/retrying"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

var (
	testStart = time.Date(2026, 10, 30, 1, 2, 3, 4005, time.UTC)
	testEnd   = testStart.Add(1500 * time.Microsecond)
)

func TestRowsHaveTheColumnsOfTheCollector(t *testing.T) {
	span := tracing.SpanData{
		TraceID: tracing.TraceID{0x0a, 15: 0x01}, SpanID: tracing.SpanID{0x0b}, ParentSpanID: tracing.SpanID{0x0c},
		Name: "SELECT orders", Kind: pkgobserver.SpanKindClient, StartTime: testStart, EndTime: testEnd,
		Attrs:  []pkgobserver.Attr{{Key: "db.system", Value: "postgresql"}, {Key: "rows", Value: 3}, {Key: "cached", Value: false}},
		Events: []tracing.Event{{Name: "retry", Time: testEnd, Attrs: []pkgobserver.Attr{{Key: "attempt", Value: 2}}}},
		Links:  []tracing.Link{{TraceID: tracing.TraceID{0x0d}, SpanID: tracing.SpanID{0x0e}}},
		Status: pkgobserver.StatusError, StatusMessage: "timeout",
		Scope: tracing.Scope{Name: "orders", Version: "1.0.0"},
	}
	body := marshalRows([]tracing.SpanData{span}, &tracing.Resource{Attrs: []pkgobserver.Attr{{Key: "service.name", Value: "orders-api"}}})
	if bytes.Count(body, []byte("\n")) != 1 || !bytes.HasSuffix(body, []byte("\n")) {
		t.Fatalf("body = %q, want one row ending with a newline", body)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"Timestamp": "1793322123.000004005", "TraceId": "0a000000000000000000000000000001", "SpanId": "0b00000000000000",
		"ParentSpanId": "0c00000000000000", "TraceState": "", "SpanName": "SELECT orders", "SpanKind": "Client",
		"ServiceName": "orders-api", "ResourceAttributes": map[string]any{"service.name": "orders-api"},
		"ScopeName": "orders", "ScopeVersion": "1.0.0",
		"SpanAttributes": map[string]any{"db.system": "postgresql", "rows": "3", "cached": "false"},
		"Duration":       1500000.0, "StatusCode": "Error", "StatusMessage": "timeout",
		"Events.Timestamp": []any{"1793322123.001504005"}, "Events.Name": []any{"retry"},
		"Events.Attributes": []any{map[string]any{"attempt": "2"}},
		"Links.TraceId":     []any{"0d000000000000000000000000000000"}, "Links.SpanId": []any{"0e00000000000000"},
		"Links.TraceState": []any{""}, "Links.Attributes": []any{map[string]any{}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("row =\n%s", body)
	}
}

func TestRowsOfARootSpanWithoutEventsUseEmptyValues(t *testing.T) {
	body := marshalRows([]tracing.SpanData{{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{2}, StartTime: testStart, EndTime: testStart}}, nil)
	for _, want := range []string{`"ParentSpanId":""`, `"SpanKind":"Internal"`, `"StatusCode":"Unset"`, `"ResourceAttributes":{}`, `"Events.Name":[]`, `"Links.TraceId":[]`} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("row %s does not contain %s", body, want)
		}
	}
}

// fakeInserterは、INSERTごとに決めた結果を返します。
type fakeInserter struct {
	bodies  []string
	tokens  []string
	respond func(body []byte) error
}

func (f *fakeInserter) InsertJSONEachRow(_ context.Context, body []byte, opts pkgclickhouse.InsertOptions) (*pkgclickhouse.InsertResponse, error) {
	f.bodies = append(f.bodies, string(body))
	f.tokens = append(f.tokens, opts.InsertDeduplicationToken)
	if err := f.respond(body); err != nil {
		return nil, err
	}
	return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
}

func testSpans(n int) []tracing.SpanData {
	spans := make([]tracing.SpanData, n)
	for i := range spans {
		spans[i] = tracing.SpanData{TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{byte(i + 1)}, Name: "op", StartTime: testStart, EndTime: testEnd}
	}
	return spans
}

func newTestExporter(client Inserter, retries int) *retrying.Exporter[tracing.SpanData] {
	return NewExporter(client, Config{Retry: retrying.Config{
		Retries: retries, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
		Sleep: func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil },
	}}, &tracing.Resource{}, nil)
}

func TestExportResendsTheSameInsertAfterATemporaryFailure(t *testing.T) {
	client := &fakeInserter{}
	client.respond = func([]byte) error {
		if len(client.bodies) == 1 {
			return &pkgclickhouse.HTTPStatusError{Code: http.StatusServiceUnavailable}
		}
		return nil
	}
	if err := newTestExporter(client, 2).Export(context.Background(), testSpans(2)); err != nil {
		t.Fatalf("Export = %v, want the spans written after one retry", err)
	}
	if len(client.bodies) != 2 || client.bodies[0] != client.bodies[1] || client.tokens[0] == "" || client.tokens[0] != client.tokens[1] {
		t.Errorf("inserts = %d, tokens %v; want the same body and deduplication token twice", len(client.bodies), client.tokens)
	}
}

func TestExportIsolatesARowThatClickHouseCannotRead(t *testing.T) {
	client := &fakeInserter{}
	client.respond = func(body []byte) error {
		// 2件目のspanの行だけを読めません。
		if bytes.Contains(body, []byte(`"SpanId":"0200000000000000"`)) {
			return &pkgclickhouse.HTTPStatusError{Code: http.StatusBadRequest, ExceptionType: pkgclickhouse.ErrorTypeCannotParseInputAssertionFailed}
		}
		return nil
	}
	err := newTestExporter(client, 3).Export(context.Background(), testSpans(4))
	var exportErr *delivery.ExportError
	if !errors.As(err, &exportErr) || len(exportErr.Failures) != 1 {
		t.Fatalf("Export = %v, want one failure", err)
	}
	if f := exportErr.Failures[0]; f.Start != 1 || f.End != 2 || f.Disposition != delivery.DispositionQuarantine {
		t.Errorf("failure = %+v, want the second span in quarantine", f)
	}
	// 4件、2件と2件、1件と1件に分けて、原因の行を確かめます。
	if len(client.bodies) != 5 {
		t.Errorf("inserts = %d, want 5", len(client.bodies))
	}
}

func TestExportClassifiesInsertFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		inserts int
		want    delivery.Disposition
	}{
		{"unavailable is resent", &pkgclickhouse.HTTPStatusError{Code: http.StatusServiceUnavailable}, 3, delivery.DispositionRetry},
		{"missing table is dead", &pkgclickhouse.HTTPStatusError{Code: http.StatusNotFound, ExceptionType: pkgclickhouse.ErrorTypeUnknownTable}, 1, delivery.DispositionDead},
		{"exception after the headers is resent from the spool", &pkgclickhouse.HTTPStatusError{Code: http.StatusOK, ExceptionCode: 999, ExceptionType: "SOMETHING_ELSE"}, 1, delivery.DispositionRetry},
		{"unreadable response is resent from the spool", &pkgclickhouse.ResponseDecodeError{Err: errors.New("broken")}, 1, delivery.DispositionRetry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeInserter{respond: func([]byte) error { return tc.err }}
			err := newTestExporter(client, 2).Export(context.Background(), testSpans(1))
			var exportErr *delivery.ExportError
			if !errors.As(err, &exportErr) || len(exportErr.Failures) != 1 || exportErr.Failures[0].Disposition != tc.want {
				t.Fatalf("Export = %v, want one failure with disposition %v", err, tc.want)
			}
			if len(client.bodies) != tc.inserts {
				t.Errorf("inserts = %d, want %d", len(client.bodies), tc.inserts)
			}
		})
	}
}
