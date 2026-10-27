package logger

// LoggerConfigSelectsHandlerTarget reports whether any active handler
// configuration of a sync or async logger (the HandlerConfig of a regular
// output, a failure store or the appendlog maintenance runner) selects target
// as an internal log notification target (OnErrorConfig, OnWarnConfig,
// OnInfoConfig or OnDebugConfig). The handler configurations it considers are
// the same as those that ValidateLoggerHandlerRouting validates. It is used to
// decide which backends selected as notification targets have their settings
// validated when the logger is constructed.
func LoggerConfigSelectsHandlerTarget(cfg LoggerConfig, target LogOutputDestination) bool {
	return handlerRoutingSourcesUseTarget(loggerActiveHandlerRoutingSources(cfg), target)
}

// DeliveryLoggerConfigSelectsHandlerTarget reports, for a Delivery logger, the
// same as LoggerConfigSelectsHandlerTarget. The handler configurations it
// considers are the same as those that ValidateDeliveryLoggerHandlerRouting
// validates.
func DeliveryLoggerConfigSelectsHandlerTarget(cfg DeliveryLoggerConfig, target LogOutputDestination) bool {
	return handlerRoutingSourcesUseTarget(deliveryLoggerActiveHandlerRoutingSources(cfg), target)
}
