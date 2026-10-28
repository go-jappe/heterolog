package telemetry

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// RecordFailureRecordsは、送れなかったrecordのうちcount件を、actionの行き先で処理したと
// 数えます。
func RecordFailureRecords(ctx context.Context, meter pkgobserver.Meter, backend, action string, count int) {
	if count <= 0 {
		return
	}
	observer.EnsureMeter(meter).Counter(FailureRecords).Add(ctx, int64(count),
		pkgobserver.Attr{Key: AttrBackend, Value: backend},
		pkgobserver.Attr{Key: AttrAction, Value: action},
	)
}

// failureResultHandlerは、送れなかったrecordを1つの行き先で処理するhandlerです。
// overloadの処置（overload.Handler）と退避先のwriter（evacuation.Handler）が満たします。
type failureResultHandler interface {
	Handle(context.Context, failure.SendResult) error
}

// CountingFailureHandlerは、内側のhandlerが処理したrecordを数えます。
type CountingFailureHandler struct {
	next   failureResultHandler
	meter  pkgobserver.Meter
	action string
}

// CountFailureRecordsは、nextが処理したrecordをactionとして数えるhandlerを返します。
// nextがerrorを返した場合は、ActionFailedとして数えます。
func CountFailureRecords(next failureResultHandler, meter pkgobserver.Meter, action string) *CountingFailureHandler {
	return &CountingFailureHandler{next: next, meter: observer.EnsureMeter(meter), action: action}
}

func (h *CountingFailureHandler) Handle(ctx context.Context, result failure.SendResult) error {
	err := h.next.Handle(ctx, result)
	action := h.action
	if err != nil {
		action = ActionFailed
	}
	RecordFailureRecords(ctx, h.meter, result.Meta.Backend, action, result.FailedCount())
	return err
}
