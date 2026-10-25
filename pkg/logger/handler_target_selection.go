package logger

// LoggerConfigSelectsHandlerTargetは、sync/async loggerの有効なhandler設定（通常出力、failure
// store、appendlog maintenance runnerのHandlerConfig）のいずれかが、targetを内部ログの通知先
// （OnErrorConfig・OnWarnConfig・OnInfoConfig・OnDebugConfig）に選ぶかを返します。対象とする
// handler設定はValidateLoggerHandlerRoutingが検証するものと同じです。通知先に選ばれたbackendの
// 設定をloggerの構築時に検証する範囲の判定に使います。
func LoggerConfigSelectsHandlerTarget(cfg LoggerConfig, target LogOutputDestination) bool {
	return handlerRoutingSourcesUseTarget(loggerActiveHandlerRoutingSources(cfg), target)
}

// DeliveryLoggerConfigSelectsHandlerTargetは、Delivery loggerについて
// LoggerConfigSelectsHandlerTargetと同じ判定を返します。対象とするhandler設定は
// ValidateDeliveryLoggerHandlerRoutingが検証するものと同じです。
func DeliveryLoggerConfigSelectsHandlerTarget(cfg DeliveryLoggerConfig, target LogOutputDestination) bool {
	return handlerRoutingSourcesUseTarget(deliveryLoggerActiveHandlerRoutingSources(cfg), target)
}
