package tracer_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/trace/span"
	"github.com/go-jappe/heterolog/internal/application/trace/tracer"
)

func TestTracerExportsCallerStartTimeAndLinks(t *testing.T) {
	tr, exporter := recordingTracer()
	started := time.Date(2024, time.January, 2, 3, 4, 5, 123, time.FixedZone("caller", 9*60*60))
	links := []span.SpanLink{
		{TraceID: "0123456789abcdef0123456789abcdef", SpanID: "0123456789abcdef"},
		{TraceID: "fedcba9876543210fedcba9876543210", SpanID: "fedcba9876543210"},
	}
	_, sp := tr.StartSpan(context.Background(), "continued-operation",
		tracer.WithStartTime(started), tracer.WithLinks(links...))
	sp.End()
	got := awaitExportedSpan(t, exporter)
	if !got.StartTime.Equal(started) {
		t.Errorf("exported start = %v, want caller time %v", got.StartTime, started)
	}
	if !reflect.DeepEqual(got.Links, links) {
		t.Errorf("exported links = %#v, want %#v", got.Links, links)
	}
}
