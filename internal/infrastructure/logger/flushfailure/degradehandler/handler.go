// Package degradehandlerは、主たるdurable failure storeが明示的に無効化されたときに使う、
// 直接退避の経路を提供します。
package degradehandler

import (
	"context"
	"errors"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/evacuation"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

var ErrNilHandler = errors.New("direct degrade handler is nil")

// Handlerは、退避先のwriterをfailure handlerの境界に合わせます。
//
// 退避先のwriterはoverload時の退避と共有しますが、このhandlerはdurable storeを
// 構築する前に選ばれます。storageやmemoryの逼迫を調べず、overload policyも実行しません。
type Handler struct {
	delegate evacuation.Handler
}

// Newは、設定された1つの退避先へ直接退避するhandlerを構築します。
func New(destination pkgdurable.SpillType) (*Handler, error) {
	delegate, err := evacuation.NewHandler(pkgdurable.SpillDestination(destination))
	if err != nil {
		return nil, err
	}

	return newHandler(delegate), nil
}

func newHandler(delegate evacuation.Handler) *Handler {
	return &Handler{delegate: delegate}
}

func (h *Handler) HandleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	return h.handleFailure(ctx, result)
}

func (h *Handler) handleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	if h == nil || h.delegate == nil {
		return ErrNilHandler
	}

	return h.delegate.Handle(ctx, result)
}

// HandleFailureWithReport は直接退避の完了を待ち、その結果だけを返します。
// 本来の送信先の結果は呼び出し側が設定するため、Targetsは空のままです。
func (h *Handler) HandleFailureWithReport(
	ctx context.Context,
	result loggerfailure.SendResult,
) (pkglogger.DeliveryReport, error) {
	if result.FailedCount() == 0 {
		return pkglogger.DeliveryReport{}, nil
	}

	err := h.HandleFailure(ctx, result)
	status := pkglogger.EvacuationSucceeded
	if err != nil {
		status = pkglogger.EvacuationFailed
	}

	return pkglogger.DeliveryReport{
		Evacuations: []pkglogger.EvacuationResult{{
			Kind:   pkglogger.EvacuationSpilled,
			Status: status,
			Err:    err,
		}},
	}, err
}
