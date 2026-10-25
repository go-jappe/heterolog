package cloudwatchlogs

import (
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func cloudWatchLogsDeliverySucceededResult() pkglogger.DeliveryResult {
	return pkglogger.DeliveryResult{
		Backend: deliveryBackendCloudWatchLogs,
		Status:  pkglogger.DeliverySucceeded,
	}
}

func cloudWatchLogsDeliveryFailedResult(err error) pkglogger.DeliveryResult {
	return pkglogger.DeliveryResult{
		Backend: deliveryBackendCloudWatchLogs,
		Status:  pkglogger.DeliveryFailed,
		Err:     err,
	}
}

func cloudWatchLogsDeliveryRejectedResult(err error) pkglogger.DeliveryResult {
	return pkglogger.DeliveryResult{
		Backend: deliveryBackendCloudWatchLogs,
		Status:  pkglogger.DeliveryRejected,
		Err:     err,
	}
}

// cloudWatchLogsTargetDeliveryResult は send result から target としての成否を判定します。
func cloudWatchLogsTargetDeliveryResult(r loggerfailure.SendResult) pkglogger.DeliveryResult {
	return r.TargetDeliveryResult(deliveryBackendCloudWatchLogs)
}
