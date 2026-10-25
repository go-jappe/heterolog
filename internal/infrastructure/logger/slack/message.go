package slack

const (
	MsgFailedCreateFallbackLogger = "error occured. failed to create fallback slack logger. err :%v"
	MsgDispachError               = "slack logger error"
	MsgLogLevelTooHighWarn        = "log level is set higher than error. this may suppress important error logs. please review your configuration."
	MsgSlackPostDelay             = "Slack post delayed. Duration: %v, threshhold %v"
	OperationSlackPostError       = "slack post api error"
	OperationSlackPostDelay       = "slack post api delayed"
)
