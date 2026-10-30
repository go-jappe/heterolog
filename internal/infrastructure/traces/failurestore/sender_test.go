package failurestore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/spool"
)

// recordingExporterは、送ったspanの名前を記録し、errを返します。
type recordingExporter struct {
	err   error
	panic bool
	sent  []string
}

func (e *recordingExporter) Name() string { return "otlp" }

func (e *recordingExporter) Export(_ context.Context, spans []tracing.SpanData) error {
	if e.panic {
		panic("exporter bug")
	}
	for _, span := range spans {
		e.sent = append(e.sent, span.Name)
	}
	return e.err
}

func (e *recordingExporter) Close() error { return nil }

var testResource = &tracing.Resource{Attrs: []pkgobserver.Attr{{Key: "service.name", Value: "checkout"}}}

// recordは、spanをfailure storeのrecordと同じpayloadにします。
func record(t *testing.T, name string) spool.SpoolRecord {
	t.Helper()
	payload, err := encodePayload(tracing.SpanData{
		TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{1}, Name: name,
		StartTime: time.Unix(1, 0), EndTime: time.Unix(2, 0),
		Attrs: []pkgobserver.Attr{{Key: "count", Value: int64(1) << 60}, {Key: "ratio", Value: 0.5}},
	}, testResource)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return spool.SpoolRecord{Payload: data}
}

func TestReplaySenderClassifiesEachRecord(t *testing.T) {
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	exporter := &recordingExporter{err: &traces.ExportError{Failures: []traces.SpanFailure{
		{Start: 1, End: 2, Disposition: traces.DispositionRetry, RetryAfter: time.Minute, Err: errors.New("throttled")},
		{Start: 2, End: 3, Disposition: traces.DispositionDead, Err: errors.New("bad request")},
		{Start: 3, End: 4, Disposition: traces.DispositionQuarantine, Err: errors.New("partial")},
	}}}
	sender := replaySender{exporter: exporter, clock: clock.NewFixedClock(now)}
	records := []spool.SpoolRecord{
		record(t, "sent"), {Payload: []byte(`{"resourceSpans":[]}`)}, record(t, "retry"), record(t, "dead"), record(t, "partial"),
	}
	report, err := sender.Send(context.Background(), records)
	if err != nil {
		t.Fatal(err)
	}
	if err := report.Validate(len(records)); err != nil {
		t.Fatalf("the report breaks the contract: %v", err)
	}
	// spanに戻せないrecordは送らず、残りを1回で送ります。
	if want := []string{"sent", "retry", "dead", "partial"}; len(exporter.sent) != 4 || exporter.sent[0] != want[0] || exporter.sent[3] != want[3] {
		t.Errorf("sent = %v, want %v", exporter.sent, want)
	}
	want := map[int]spool.RecordOutcome{
		0: {Status: spool.RecordOutcomeStatusSuccess, AttemptedAt: now},
		1: {Status: spool.RecordOutcomeStatusInvalid},
		2: {Status: spool.RecordOutcomeStatusRetryable, AttemptedAt: now, RetryAfter: time.Minute},
		3: {Status: spool.RecordOutcomeStatusPermanent, AttemptedAt: now},
		4: {Status: spool.RecordOutcomeStatusInvalid, AttemptedAt: now},
	}
	for _, outcome := range report {
		w := want[outcome.InputIndex]
		if outcome.Status != w.Status || !outcome.AttemptedAt.Equal(w.AttemptedAt) || outcome.RetryAfter != w.RetryAfter ||
			(outcome.Status == spool.RecordOutcomeStatusSuccess) != (outcome.Err == nil) {
			t.Errorf("record %d: %+v, want %+v", outcome.InputIndex, outcome, w)
		}
	}
}

func TestReplaySenderRetriesEverythingAfterAPanicOrAnUnknownError(t *testing.T) {
	for name, exporter := range map[string]*recordingExporter{
		"panic":         {panic: true},
		"unknown error": {err: errors.New("broken")},
	} {
		sender := replaySender{exporter: exporter, clock: clock.NewUTCClock()}
		records := []spool.SpoolRecord{record(t, "a"), record(t, "b")}
		report, err := sender.Send(context.Background(), records)
		if err != nil || report.Validate(len(records)) != nil {
			t.Fatalf("%s: Send = %v, %v", name, report, err)
		}
		for _, outcome := range report {
			if outcome.Status != spool.RecordOutcomeStatusRetryable || outcome.Err == nil {
				t.Errorf("%s: record %d = %+v, want retryable", name, outcome.InputIndex, outcome)
			}
		}
	}
}

func TestPayloadKeepsTheSpanAsOTLPJSON(t *testing.T) {
	data := record(t, "op").Payload
	sender := replaySender{exporter: &recordingExporter{}, clock: clock.NewUTCClock()}
	report, err := sender.Send(context.Background(), []spool.SpoolRecord{{Payload: data}})
	if err != nil || len(report) != 1 || report[0].Status != spool.RecordOutcomeStatusSuccess {
		t.Fatalf("Send = %+v, %v; want the stored span to decode", report, err)
	}
	// 64-bitの整数は、OTLP/JSONの規則どおり文字列で、精度を失いません。
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	span := decoded["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)[0].(map[string]any)
	if got := span["attributes"].([]any)[0].(map[string]any)["value"].(map[string]any)["intValue"]; got != "1152921504606846976" {
		t.Errorf("intValue = %v, want the exact value as a string", got)
	}
}
