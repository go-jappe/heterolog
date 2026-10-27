package tracer_test

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/go-jappe/heterolog/internal/application/trace/span"
	"github.com/go-jappe/heterolog/internal/application/trace/tracer"
)

func TestSamplerControlsExportedSpans(t *testing.T) {
	for _, test := range []struct {
		name          string
		sampler       tracer.Sampler
		parentSampler tracer.Sampler
		wantExport    bool
	}{
		{name: "never", sampler: tracer.NeverSample()},
		{name: "zero probability", sampler: tracer.ProbabilitySample(0)},
		{name: "always", sampler: tracer.AlwaysSample(), wantExport: true},
		{name: "full probability", sampler: tracer.ProbabilitySample(1), wantExport: true},
		{name: "unsampled parent", sampler: tracer.ParentBasedSample(tracer.AlwaysSample()), parentSampler: tracer.NeverSample()},
		{name: "sampled parent", sampler: tracer.ParentBasedSample(tracer.NeverSample()), parentSampler: tracer.AlwaysSample(), wantExport: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				if test.parentSampler != nil {
					parentTracer := tracer.NewTracer(tracer.WithSampler(test.parentSampler))
					var parent tracer.Span
					ctx, parent = parentTracer.StartSpan(ctx, "parent")
					defer parent.End()
				}
				exporter := &testExporter{spans: make(chan span.SpanInput, 8)}
				tr := tracer.NewTracer(tracer.WithSampler(test.sampler), tracer.WithExporter(exporter))
				_, record := tr.StartSpan(ctx, "sampling-export")
				record.End()
				// 静止状態まで待つことで、exportがないことを検証する前に、非同期のExportがあれば
				// 必ず実行済みになります。sleepや即時のchannel読み取りでは、それを見逃すことがあります。
				synctest.Wait()
				wantCount := 0
				if test.wantExport {
					wantCount = 1
				}
				if got := len(exporter.spans); got != wantCount {
					t.Fatalf("exported %d spans, want %d", got, wantCount)
				}
				if test.wantExport {
					got := <-exporter.spans
					if got.Name != "sampling-export" || !got.Sampled {
						t.Fatalf("incorrect exported span: %+v", got)
					}
				}
			})
		})
	}
}
