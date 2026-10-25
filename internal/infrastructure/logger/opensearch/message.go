package opensearch

const (
	OperationOpenSearchError = "Opensearch error"
	OperationIndexDelay      = "Opensearch index delay"

	MsgLogLevelTooHighWarn        = "log level is set higher than error. this may suppress important error logs. please review your configuration."
	MsgOpenSearchError            = "OpenSearch post error"
	MsgFailedCreateFallbackLogger = "error occured. failed to create fallback Opensearch logger. err :%v"

	ErrorMsgIndexDelayed = "Opensearch index delayed error: %v; error threshold is %v; opaqueueid %v"
	MsgIndexDelayed      = "Opensearch index delayed: %v; threshold is %v; opaqueueid %v"
	MsgIndexNoDelayed    = "Opensearch index elasped: %v; opaqueueid %v"
)
