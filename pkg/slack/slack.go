package slack

import "time"

// MaxMessageLength is the maximum length of one Slack message, and
// DefaultHTTPTimeout is the default timeout of a Slack API request.
const (
	MaxMessageLength   = 4000
	DefaultHTTPTimeout = 2 * time.Second
)
