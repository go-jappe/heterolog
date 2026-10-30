package failurestore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/spool"
)

// recordingExporterは、送ったものを記録し、errを返します。
type recordingExporter struct {
	err   error
	panic bool
	sent  []string
}

func (e *recordingExporter) Name() string { return "otlp" }

func (e *recordingExporter) Export(_ context.Context, items []string) error {
	if e.panic {
		panic("exporter bug")
	}
	e.sent = append(e.sent, items...)
	return e.err
}

func (e *recordingExporter) Close() error { return nil }

// testCodecは、文字列を{"name": ...}のrecordにするCodecです。
var testCodec = Codec[string]{
	Encode: func(item string) (map[string]any, error) { return map[string]any{"name": item}, nil },
	Decode: func(payload []byte) (string, error) {
		var record struct {
			Name *string `json:"name"`
		}
		if err := json.Unmarshal(payload, &record); err != nil || record.Name == nil {
			return "", errors.New("not a test record")
		}
		return *record.Name, nil
	},
}

// recordは、itemをfailure storeのrecordと同じpayloadにします。
func record(t *testing.T, item string) spool.SpoolRecord {
	t.Helper()
	payload, err := testCodec.Encode(item)
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
	exporter := &recordingExporter{err: &delivery.ExportError{Failures: []delivery.Failure{
		{Start: 1, End: 2, Disposition: delivery.DispositionRetry, RetryAfter: time.Minute, Err: errors.New("throttled")},
		{Start: 2, End: 3, Disposition: delivery.DispositionDead, Err: errors.New("bad request")},
		{Start: 3, End: 4, Disposition: delivery.DispositionQuarantine, Err: errors.New("partial")},
	}}}
	sender := replaySender[string]{exporter: exporter, decode: testCodec.Decode, clock: clock.NewFixedClock(now)}
	records := []spool.SpoolRecord{
		record(t, "sent"), {Payload: []byte(`{}`)}, record(t, "retry"), record(t, "dead"), record(t, "partial"),
	}
	report, err := sender.Send(context.Background(), records)
	if err != nil {
		t.Fatal(err)
	}
	if err := report.Validate(len(records)); err != nil {
		t.Fatalf("the report breaks the contract: %v", err)
	}
	// 戻せないrecordは送らず、残りを1回で送ります。
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
		sender := replaySender[string]{exporter: exporter, decode: testCodec.Decode, clock: clock.NewUTCClock()}
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
