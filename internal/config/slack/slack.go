package slack

import "time"

const (
	SlackPostWarnThresholdDuration  = 1 * time.Second
	SlackPostErrorThresholdDuration = 2 * time.Second
)

type SlackConfig struct {
	SlackPostWarnThresholdDuration  time.Duration
	SlackPostErrorThresholdDuration time.Duration
}
