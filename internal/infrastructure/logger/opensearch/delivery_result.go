package opensearch

import (
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/pkg/logger"
)

const deliveryBackendOpenSearch = "opensearch"

func openSearchDeliverySucceededResult() logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: deliveryBackendOpenSearch,
		Status:  logger.DeliverySucceeded,
	}
}

func openSearchDeliveryFailedResult(err error) logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: deliveryBackendOpenSearch,
		Status:  logger.DeliveryFailed,
		Err:     err,
	}
}

func openSearchDeliveryRejectedResult(err error) logger.DeliveryResult {
	return logger.DeliveryResult{
		Backend: deliveryBackendOpenSearch,
		Status:  logger.DeliveryRejected,
		Err:     err,
	}
}

func openSearchTargetDeliveryResult(r loggerfailure.SendResult) logger.DeliveryResult {
	return r.TargetDeliveryResult(deliveryBackendOpenSearch)
}
