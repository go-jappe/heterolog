package telemetry

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// StartAttemptは、client wrapperが送るattempt回目（1から数えます）の試行のspanを始めます。
// 試行の前の待ち時間はspanに含めないため、待った後に呼びます。spanはEndSpanで終えます。
func StartAttempt(
	ctx context.Context,
	tracer pkgobserver.Tracer,
	spanName string,
	attempt int,
) (context.Context, pkgobserver.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	return observer.EnsureTracer(tracer).StartSpan(ctx, spanName,
		pkgobserver.Attr{Key: SpanAttrAttempt, Value: attempt},
	)
}

// RecordRetryは、reasonの理由で送信をやり直すことを数えます。
func RecordRetry(ctx context.Context, meter pkgobserver.Meter, backend, reason string) {
	observer.EnsureMeter(meter).Counter(BackendRetries).Add(ctx, 1,
		pkgobserver.Attr{Key: AttrBackend, Value: backend},
		pkgobserver.Attr{Key: AttrReason, Value: reason},
	)
}

// RecordCircuitBreakerRejectionは、circuit breakerが送信を断ったことを数えます。
func RecordCircuitBreakerRejection(ctx context.Context, meter pkgobserver.Meter, backend string) {
	observer.EnsureMeter(meter).Counter(CircuitBreakerRejectedRequests).Add(ctx, 1,
		pkgobserver.Attr{Key: AttrBackend, Value: backend},
	)
}

// RecordCircuitBreakerTransitionは、circuit breakerがstate（StateOpenかStateClosed）に
// 変わったことを数えます。
func RecordCircuitBreakerTransition(ctx context.Context, meter pkgobserver.Meter, backend, state string) {
	observer.EnsureMeter(meter).Counter(CircuitBreakerTransitions).Add(ctx, 1,
		pkgobserver.Attr{Key: AttrBackend, Value: backend},
		pkgobserver.Attr{Key: AttrState, Value: state},
	)
}
