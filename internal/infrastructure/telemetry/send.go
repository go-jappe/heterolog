package telemetry

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// RecordSendResultは、1回の送信の結果を、成功と失敗の分類ごとのrecord数で数えます。
// 同じ結果を2回渡すと2回数えるため、結果が確定した場所で1回だけ呼びます。
func RecordSendResult(ctx context.Context, meter pkgobserver.Meter, result failure.SendResult) {
	counter := observer.EnsureMeter(meter).Counter(BackendRecords)
	backend := pkgobserver.Attr{Key: AttrBackend, Value: result.Meta.Backend}
	for _, outcome := range []struct {
		name  string
		count int
	}{
		{OutcomeSuccess, result.SuccessCount()},
		{OutcomeRetryable, len(result.RetryableRecords)},
		{OutcomeDead, len(result.DeadRecords)},
		{OutcomeQuarantine, len(result.QuarantineRecords)},
	} {
		if outcome.count > 0 {
			counter.Add(ctx, int64(outcome.count), backend, pkgobserver.Attr{Key: AttrOutcome, Value: outcome.name})
		}
	}
}

// RecordRecordsは、SendResultを作らない出力先（Slackなど）が送ったrecordの数を数えます。
func RecordRecords(ctx context.Context, meter pkgobserver.Meter, backend, outcome string, count int) {
	if count <= 0 {
		return
	}
	observer.EnsureMeter(meter).Counter(BackendRecords).Add(ctx, int64(count),
		pkgobserver.Attr{Key: AttrBackend, Value: backend},
		pkgobserver.Attr{Key: AttrOutcome, Value: outcome},
	)
}

// StartFlushは、bufferから切り離したbatchを送るspanを始め、batchの件数とbyte数を記録します。
// bytesが負の場合は、byte数を記録しません。返すcontextはbatchの送信に渡し、spanは
// EndSpanで終えます。
func StartFlush(
	ctx context.Context,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	spanName string,
	backend string,
	records int,
	bytes int,
) (context.Context, pkgobserver.Span) {
	meter = observer.EnsureMeter(meter)
	attr := pkgobserver.Attr{Key: AttrBackend, Value: backend}
	meter.Histogram(BackendFlushRecords).Record(ctx, float64(records), attr)
	spanAttrs := []pkgobserver.Attr{{Key: SpanAttrRecords, Value: records}}
	if bytes >= 0 {
		meter.Histogram(BackendFlushBytes).Record(ctx, float64(bytes), attr)
		spanAttrs = append(spanAttrs, pkgobserver.Attr{Key: SpanAttrBytes, Value: bytes})
	}
	return observer.EnsureTracer(tracer).StartSpan(ctx, spanName, spanAttrs...)
}

// EndSpanは、errがあればspanに記録してから、spanを終えます。
func EndSpan(span pkgobserver.Span, err error) {
	if observer.IsNilInterface(span) {
		return
	}
	if err != nil {
		span.RecordError(err)
	}
	span.End()
}

// Requestは、出力先への1回の送信を、spanと所要時間のmetricsで計装します。
type Request struct {
	ctx     context.Context
	span    pkgobserver.Span
	meter   pkgobserver.Meter
	backend string
	start   time.Time
}

// StartRequestは、records件のrecordを送る1回の送信のspanを始めます。返すcontextは
// clientへ渡し、送信の結果が確定したらEndを1回呼びます。
func StartRequest(
	ctx context.Context,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	spanName string,
	backend string,
	records int,
) (context.Context, Request) {
	if ctx == nil {
		ctx = context.Background()
	}
	spanCtx, span := observer.EnsureTracer(tracer).StartSpan(ctx, spanName,
		pkgobserver.Attr{Key: SpanAttrRecords, Value: records},
	)
	return spanCtx, Request{
		ctx:     ctx,
		span:    span,
		meter:   observer.EnsureMeter(meter),
		backend: backend,
		start:   time.Now(),
	}
}

// Endは、送信の所要時間を結果と合わせて記録し、spanを終えます。errは送信の失敗です。
// 応答の中で一部のrecordだけが拒否された場合は、送信自体は成功として渡します。
func (r Request) End(err error) {
	outcome := OutcomeSuccess
	if err != nil {
		outcome = OutcomeFailure
	}
	if r.meter != nil {
		r.meter.Histogram(BackendRequestDuration).Record(r.ctx, time.Since(r.start).Seconds(),
			pkgobserver.Attr{Key: AttrBackend, Value: r.backend},
			pkgobserver.Attr{Key: AttrOutcome, Value: outcome},
		)
	}
	EndSpan(r.span, err)
}
