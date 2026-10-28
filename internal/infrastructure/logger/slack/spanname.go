package slack

const (
	SpanSlackLoggerCreate         = "heterolog.slack.create"
	SpanFallbackSlackLoggerCreate = "heterolog.fallback.slack.create"
	SpanSlackLoggerSendBatch      = "heterolog.slack.send"
	// SpanSlackPostは、1つのchannelへの1回の投稿です。
	SpanSlackPost = "heterolog.slack.post"
)
