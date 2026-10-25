package loki

import (
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func lokiDeliverySucceededResult() pkglogger.DeliveryResult {
	return pkglogger.DeliveryResult{
		Backend: deliveryBackendLoki,
		Status:  pkglogger.DeliverySucceeded,
	}
}

func lokiDeliveryFailedResult(err error) pkglogger.DeliveryResult {
	return pkglogger.DeliveryResult{
		Backend: deliveryBackendLoki,
		Status:  pkglogger.DeliveryFailed,
		Err:     err,
	}
}

func lokiDeliveryRejectedResult(err error) pkglogger.DeliveryResult {
	return pkglogger.DeliveryResult{
		Backend: deliveryBackendLoki,
		Status:  pkglogger.DeliveryRejected,
		Err:     err,
	}
}

func lokiTargetDeliveryResult(r loggerfailure.SendResult) pkglogger.DeliveryResult {
	return r.TargetDeliveryResult(deliveryBackendLoki)
}
