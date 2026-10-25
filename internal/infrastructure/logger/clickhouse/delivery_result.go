package clickhouse

import pkglogger "github.com/go-jappe/heterolog/pkg/logger"

func clickHouseDeliveryFailedResult(err error) pkglogger.DeliveryResult {
	return pkglogger.DeliveryResult{
		Backend: deliveryBackendClickHouse,
		Status:  pkglogger.DeliveryFailed,
		Err:     err,
	}
}

func clickHouseDeliveryEnqueuedResult() pkglogger.DeliveryResult {
	return pkglogger.DeliveryResult{
		Backend: deliveryBackendClickHouse,
		Status:  pkglogger.DeliveryEnqueued,
	}
}

func clickHouseDeliveryRejectedResult(err error) pkglogger.DeliveryResult {
	return pkglogger.DeliveryResult{
		Backend: deliveryBackendClickHouse,
		Status:  pkglogger.DeliveryRejected,
		Err:     err,
	}
}
