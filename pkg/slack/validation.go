package slack

import (
	"errors"
	"strings"
)

var (
	// ErrTokenRequired reports that the Slack token is empty or only whitespace.
	ErrTokenRequired = errors.New("slack token is required")
	// ErrChannelRequired reports that the Slack channel is empty or only
	// whitespace.
	ErrChannelRequired = errors.New("slack channel is required")
)

// Config holds the destination settings needed to post to the Slack API.
type Config struct {
	Token   string
	Channel string
}

// ValidateConfig validates the settings needed to create a Slack client. It
// makes no HTTP requests, and it treats values that are only whitespace, not
// just empty strings, as unset. When both values are missing, the returned
// error reports both. A missing value can be identified with errors.Is against
// ErrTokenRequired and ErrChannelRequired.
func ValidateConfig(cfg Config) error {
	var errs []error
	if strings.TrimSpace(cfg.Token) == "" {
		errs = append(errs, ErrTokenRequired)
	}
	if strings.TrimSpace(cfg.Channel) == "" {
		errs = append(errs, ErrChannelRequired)
	}
	return errors.Join(errs...)
}
