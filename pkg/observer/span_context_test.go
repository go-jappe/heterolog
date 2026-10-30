package observer_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/pkg/observer"
)

func TestParseTraceParentReadsTheFields(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		sampled bool
	}{
		{"sampled", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", true},
		{"not sampled", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00", false},
		// sampledはflagsの最下位bitだけで決まり、意味の決まっていないbitは無視します。
		{"other flag bits", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-fe", false},
		{"later version", "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03-extra", true},
		{"later version without extra fields", "cc-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := observer.ParseTraceParent(tc.value)
			if err != nil {
				t.Fatalf("ParseTraceParent(%q): %v", tc.value, err)
			}
			want := observer.SpanContext{
				TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
				SpanID:  "00f067aa0ba902b7",
				Sampled: tc.sampled,
			}
			if sc != want {
				t.Errorf("ParseTraceParent(%q) = %+v, want %+v", tc.value, sc, want)
			}
		})
	}
}

func TestParseTraceParentRejectsValuesOutsideTheSpecification(t *testing.T) {
	for _, value := range []string{
		"",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
		// version 00は、flagsの後に項目を持てません。
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
		"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01x",
		"ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"0g-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		"00_4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g",
	} {
		sc, err := observer.ParseTraceParent(value)
		if !errors.Is(err, observer.ErrInvalidTraceParent) {
			t.Errorf("ParseTraceParent(%q) = %+v, %v; want ErrInvalidTraceParent", value, sc, err)
		}
	}
}

func TestSpanContextTraceParentRoundTrips(t *testing.T) {
	for _, sampled := range []bool{true, false} {
		sc := observer.SpanContext{
			TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			SpanID:  "00f067aa0ba902b7",
			Sampled: sampled,
		}
		got, err := observer.ParseTraceParent(sc.TraceParent())
		if err != nil || got != sc {
			t.Errorf("ParseTraceParent(%q) = %+v, %v; want %+v", sc.TraceParent(), got, err, sc)
		}
	}
	if got := (observer.SpanContext{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736"}).TraceParent(); got != "" {
		t.Errorf("TraceParent of an invalid span context = %q, want empty", got)
	}
}

func TestContextWithSpanContextStoresOnlyValidSpanContexts(t *testing.T) {
	if got := observer.SpanContextFromContext(context.Background()); got != (observer.SpanContext{}) {
		t.Errorf("SpanContextFromContext of an empty context = %+v, want the zero value", got)
	}
	sc := observer.SpanContext{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7"}
	ctx := observer.ContextWithSpanContext(context.Background(), sc)
	if got := observer.SpanContextFromContext(ctx); got != sc {
		t.Errorf("SpanContextFromContext = %+v, want %+v", got, sc)
	}
	// 不正な値は入れず、先に入れた値を残します。
	ctx = observer.ContextWithSpanContext(ctx, observer.SpanContext{TraceID: "zz"})
	if got := observer.SpanContextFromContext(ctx); got != sc {
		t.Errorf("SpanContextFromContext after an invalid value = %+v, want %+v", got, sc)
	}
}

// optionsTracerは、受け取ったStartOptionsを記録します。
type optionsTracer struct {
	plainTracer
	got *observer.StartOptions
}

func (t optionsTracer) StartSpanWithOptions(ctx context.Context, _ string, opts observer.StartOptions, _ ...observer.Attr) (context.Context, observer.Span) {
	*t.got = opts
	return ctx, nil
}

// plainTracerは、StartOptionsを受け付けないTracerです。
type plainTracer struct{ calls *int }

func (t plainTracer) StartSpan(ctx context.Context, _ string, _ ...observer.Attr) (context.Context, observer.Span) {
	*t.calls++
	return ctx, nil
}

func TestStartSpanWithOptionsPassesOptionsOnlyToOptionsTracers(t *testing.T) {
	opts := observer.StartOptions{Kind: observer.SpanKindServer}

	var got observer.StartOptions
	calls := 0
	observer.StartSpanWithOptions(context.Background(), optionsTracer{plainTracer{&calls}, &got}, "op", opts)
	if got.Kind != observer.SpanKindServer || calls != 0 {
		t.Errorf("OptionsTracer received %+v with %d StartSpan calls, want the options and no StartSpan call", got, calls)
	}

	observer.StartSpanWithOptions(context.Background(), plainTracer{&calls}, "op", opts)
	if calls != 1 {
		t.Errorf("StartSpan calls = %d, want 1 for a Tracer without options", calls)
	}
}
