package observer

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// SpanID identifies a span within a trace. A valid SpanID is 16 lowercase
// hexadecimal digits that are not all zero.
type SpanID string

// SpanContext identifies a span across processes, as the W3C traceparent
// header carries it.
type SpanContext struct {
	// TraceID is 32 lowercase hexadecimal digits.
	TraceID TraceID
	// SpanID is 16 lowercase hexadecimal digits.
	SpanID SpanID
	// Sampled reports whether the trace is recorded and exported.
	Sampled bool
}

// ErrInvalidTraceParent reports that ParseTraceParent received a value that is
// not a W3C traceparent header.
var ErrInvalidTraceParent = errors.New("invalid traceparent")

// IsValid reports whether TraceID and SpanID are well formed and not all zero.
func (sc SpanContext) IsValid() bool {
	return validHexID(string(sc.TraceID), 32) && validHexID(string(sc.SpanID), 16)
}

// TraceParent returns the W3C traceparent header value of sc in version 00,
// such as "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01". It returns
// an empty string when sc is not valid.
func (sc SpanContext) TraceParent() string {
	if !sc.IsValid() {
		return ""
	}
	flags := "00"
	if sc.Sampled {
		flags = "01"
	}
	return "00-" + string(sc.TraceID) + "-" + string(sc.SpanID) + "-" + flags
}

// ParseTraceParent reads a W3C traceparent header value. It accepts the
// fields of version 00 from later versions too, as the W3C Trace Context
// specification requires. It returns an error wrapping ErrInvalidTraceParent
// for a value that does not follow the specification, such as one with
// uppercase digits or an all-zero ID. The error does not include the value.
func ParseTraceParent(value string) (SpanContext, error) {
	const version00Length = 55
	invalid := func(reason string) (SpanContext, error) {
		return SpanContext{}, fmt.Errorf("%w: %s", ErrInvalidTraceParent, reason)
	}
	if len(value) < version00Length {
		return invalid("too short")
	}
	version := value[0:2]
	if !isLowerHex(version) || version == "ff" {
		return invalid("unsupported version")
	}
	// version 00は長さが決まっています。後の版は、version 00の項目の後に「-」で項目を
	// 足せるため、その区切りだけを確かめます。
	if version == "00" && len(value) != version00Length {
		return invalid("unexpected length for version 00")
	}
	if len(value) > version00Length && value[version00Length] != '-' {
		return invalid("unexpected data after the flags")
	}
	if value[2] != '-' || value[35] != '-' || value[52] != '-' {
		return invalid("misplaced separator")
	}
	sc := SpanContext{TraceID: TraceID(value[3:35]), SpanID: SpanID(value[36:52])}
	if !sc.IsValid() {
		return invalid("malformed or all-zero ID")
	}
	flags := value[53:55]
	if !isLowerHex(flags) {
		return invalid("malformed flags")
	}
	// sampledは、flagsの最下位bitです。ほかのbitは、まだ意味が決まっていません。
	sc.Sampled = strings.ContainsRune("13579bdf", rune(flags[1]))
	return sc, nil
}

// spanContextKeyは、contextに入れたSpanContextのkeyです。
type spanContextKey struct{}

// ContextWithSpanContext returns a copy of ctx that carries sc. The Tracer of
// heterolog.NewTelemetry starts a span as a child of the span context in ctx,
// so pass the span context of an incoming request, read with
// ParseTraceParent, to continue its trace. An invalid sc is not stored.
func ContextWithSpanContext(ctx context.Context, sc SpanContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if !sc.IsValid() {
		return ctx
	}
	return context.WithValue(ctx, spanContextKey{}, sc)
}

// SpanContextFromContext returns the span context in ctx: that of the span
// the Tracer of heterolog.NewTelemetry started last, or the one that
// ContextWithSpanContext stored. It returns the zero SpanContext when ctx has
// none. Use its TraceParent to propagate the trace to an outgoing request.
func SpanContextFromContext(ctx context.Context) SpanContext {
	if ctx == nil {
		return SpanContext{}
	}
	sc, _ := ctx.Value(spanContextKey{}).(SpanContext)
	return sc
}

// validHexIDは、sがlength桁の小文字の16進数で、すべて0ではないかを返します。
func validHexID(s string, length int) bool {
	return len(s) == length && isLowerHex(s) && strings.Trim(s, "0") != ""
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
