package provider

import (
	"context"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type dropHandler struct {
	meter pkgobserver.Meter
}

func NewOverloadDropHandler(meter pkgobserver.Meter) *dropHandler {
	return &dropHandler{meter: meter}
}

// Handleは過負荷のためfailure recordを保存せずに破棄し、drop件数のmetricとspan eventを
// 記録します。破棄したことを同期APIの呼び出し元が判別できるよう、
// pkglogger.ErrLogDroppedを包んだerrorを返します。
func (h *dropHandler) Handle(ctx context.Context, result loggerfailure.SendResult) error {
	h.meter.Counter(FailureOverloadDropTotal).Add(ctx, 1)

	span := observer.SpanFromContext(ctx)
	if span != nil {
		span.AddEvent(
			SpanEventDrop,
			pkgobserver.Attr{Key: "reason", Value: "overload"},
			pkgobserver.Attr{Key: "retryable_records", Value: len(result.RetryableRecords)},
			pkgobserver.Attr{Key: "dead_records", Value: len(result.DeadRecords)},
			pkgobserver.Attr{Key: "quarantine_records", Value: len(result.QuarantineRecords)},
		)
		span.SetAttributes(
			pkgobserver.Attr{Key: DropOccurredAttribute, Value: true},
		)
	}

	return fmt.Errorf(
		"%w: failure store is overloaded: dropped_records=%d",
		pkglogger.ErrLogDropped,
		result.FailedCount(),
	)
}
