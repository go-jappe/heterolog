package provider

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type countingDropMeter struct {
	totals map[string]int64
}

func (m *countingDropMeter) Counter(name string) pkgobserver.Counter {
	return countingDropCounter{meter: m, name: name}
}

func (m *countingDropMeter) Gauge(string) pkgobserver.Gauge {
	return observer.NopGauge{}
}

func (m *countingDropMeter) Histogram(string) pkgobserver.Histogram {
	return observer.NopHistogram{}
}

type countingDropCounter struct {
	meter *countingDropMeter
	name  string
}

func (c countingDropCounter) Add(_ context.Context, value int64, _ ...pkgobserver.Attr) {
	if c.meter.totals == nil {
		c.meter.totals = make(map[string]int64)
	}
	c.meter.totals[c.name] += value
}

type recordingDropSpan struct {
	observer.NopSpan
	events     map[string][]pkgobserver.Attr
	attributes []pkgobserver.Attr
}

func (s *recordingDropSpan) AddEvent(name string, attrs ...pkgobserver.Attr) {
	if s.events == nil {
		s.events = make(map[string][]pkgobserver.Attr)
	}
	s.events[name] = append(s.events[name], attrs...)
}

func (s *recordingDropSpan) SetAttributes(attrs ...pkgobserver.Attr) {
	s.attributes = append(s.attributes, attrs...)
}

// dropは保存せずに破棄したことを、metricとspan eventに加えてErrLogDroppedで返します。
func TestOverloadDropHandlerReportsDroppedRecords(t *testing.T) {
	meter := &countingDropMeter{}
	span := &recordingDropSpan{}
	ctx := observer.WithSpan(context.Background(), span)
	result := loggerfailure.SendResult{
		RetryableRecords:  []loggerfailure.FailedRecord{{}, {}},
		DeadRecords:       []loggerfailure.FailedRecord{{}},
		QuarantineRecords: []loggerfailure.FailedRecord{{}},
	}

	err := NewOverloadDropHandler(meter).Handle(ctx, result)
	if !errors.Is(err, pkglogger.ErrLogDropped) {
		t.Fatalf("Handle error = %v, want ErrLogDropped", err)
	}
	if got := meter.totals[FailureOverloadDropTotal]; got != 1 {
		t.Fatalf("%s = %d, want 1", FailureOverloadDropTotal, got)
	}
	attrs, ok := span.events[SpanEventDrop]
	if !ok {
		t.Fatalf("span events = %v, want %s", span.events, SpanEventDrop)
	}
	want := map[string]any{
		"reason":             "overload",
		"retryable_records":  2,
		"dead_records":       1,
		"quarantine_records": 1,
	}
	if len(attrs) != len(want) {
		t.Fatalf("drop event attributes = %v, want %v", attrs, want)
	}
	for _, attr := range attrs {
		if want[attr.Key] != attr.Value {
			t.Fatalf("drop event attribute %s = %v, want %v", attr.Key, attr.Value, want[attr.Key])
		}
	}
	if len(span.attributes) != 1 ||
		span.attributes[0].Key != DropOccurredAttribute ||
		span.attributes[0].Value != true {
		t.Fatalf("span attributes = %v, want %s=true", span.attributes, DropOccurredAttribute)
	}
}
